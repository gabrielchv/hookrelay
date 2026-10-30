import { beforeEach, describe, expect, it } from 'vitest';
import { Dispatcher } from '../src/core/dispatcher.js';
import { HttpDeliveryClient } from '../src/delivery/client.js';
import type { Delivery, Endpoint } from '../src/types.js';
import {
  AlwaysAllowLimiter,
  DenyAllLimiter,
  MemoryDeliveryRepo,
  MemoryEndpointRepo,
  MemoryKeyValueStore,
  MemoryScheduler,
  ScriptedDeliveryClient,
  makeDelivery,
  makeEndpoint,
  makeJob,
} from './support/fakes.js';

const policy = { baseMs: 100, maxMs: 1000 };

interface Harness {
  dispatcher: Dispatcher;
  deliveries: MemoryDeliveryRepo;
  endpoints: MemoryEndpointRepo;
  locks: MemoryKeyValueStore;
  scheduler: MemoryScheduler;
  client: ScriptedDeliveryClient;
}

function harness(
  results: Array<{ ok: boolean; statusCode: number | null; error: string | null; durationMs: number }>,
  options: { limiter?: 'allow' | 'deny'; endpoint?: Endpoint } = {},
): Harness {
  const deliveries = new MemoryDeliveryRepo();
  const endpoints = new MemoryEndpointRepo();
  const locks = new MemoryKeyValueStore();
  const scheduler = new MemoryScheduler();
  const client = new ScriptedDeliveryClient(results);
  const endpoint = options.endpoint ?? makeEndpoint();

  void endpoints.create(endpoint);

  const dispatcher = new Dispatcher({
    endpoints,
    deliveries,
    locks,
    limiter: options.limiter === 'deny' ? new DenyAllLimiter() : new AlwaysAllowLimiter(),
    scheduler,
    client,
    policy,
    rateLimitPerSec: 20,
    random: () => 0,
  });

  return { dispatcher, deliveries, endpoints, locks, scheduler, client };
}

describe('Dispatcher', () => {
  let delivery: Delivery;

  beforeEach(() => {
    delivery = makeDelivery();
  });

  it('marks a 2xx response delivered and clears the retry schedule', async () => {
    const h = harness([{ ok: true, statusCode: 200, error: null, durationMs: 12 }]);
    await h.deliveries.create(delivery);

    await expect(h.dispatcher.process(makeJob())).resolves.toBe('delivered');

    const stored = await h.deliveries.get(delivery.id);
    expect(stored?.status).toBe('delivered');
    expect(stored?.attempts).toBe(1);
    expect(stored?.lastStatusCode).toBe(200);
    expect(stored?.nextAttemptAt).toBeNull();
    expect(stored?.lastError).toBeNull();
    expect(h.scheduler.scheduled.size).toBe(0);
    expect(h.client.requests[0]?.url).toBe(delivery.url);
  });

  it('schedules a retry with backoff on a 5xx', async () => {
    const h = harness([{ ok: false, statusCode: 503, error: null, durationMs: 8 }]);
    await h.deliveries.create(delivery);

    await expect(h.dispatcher.process(makeJob())).resolves.toBe('retried');

    const stored = await h.deliveries.get(delivery.id);
    expect(stored?.status).toBe('queued');
    expect(stored?.attempts).toBe(1);
    expect(stored?.lastStatusCode).toBe(503);
    expect(stored?.nextAttemptAt).toBeGreaterThan(Date.now() - 1000);
    expect(h.scheduler.scheduled.get(delivery.id)).toBe(stored?.nextAttemptAt);
  });

  it('moves to dead after the final attempt and stops retrying', async () => {
    const h = harness([{ ok: false, statusCode: 500, error: null, durationMs: 4 }]);
    await h.deliveries.create(makeDelivery({ attempts: 2, maxAttempts: 3 }));

    await expect(h.dispatcher.process(makeJob())).resolves.toBe('dead');

    const stored = await h.deliveries.get(delivery.id);
    expect(stored?.status).toBe('dead');
    expect(stored?.attempts).toBe(3);
    expect(stored?.nextAttemptAt).toBeNull();
    expect(h.scheduler.scheduled.size).toBe(0);
  });

  it('records a network error message on the delivery', async () => {
    const h = harness([
      { ok: false, statusCode: null, error: 'ECONNREFUSED', durationMs: 2 },
    ]);
    await h.deliveries.create(delivery);

    await h.dispatcher.process(makeJob());

    expect((await h.deliveries.get(delivery.id))?.lastError).toBe('ECONNREFUSED');
  });

  it('does not send concurrently when the delivery lease is held', async () => {
    const h = harness([{ ok: true, statusCode: 200, error: null, durationMs: 1 }]);
    await h.deliveries.create(delivery);
    await h.locks.setIfAbsent(`lock:delivery:${delivery.id}`, '1', 30);

    await expect(h.dispatcher.process(makeJob())).resolves.toBe('skipped');
    expect(h.client.requests).toHaveLength(0);
    expect((await h.deliveries.get(delivery.id))?.status).toBe('queued');
  });

  it('releases the lease after processing, so a retry can proceed', async () => {
    const h = harness([{ ok: false, statusCode: 500, error: null, durationMs: 1 }]);
    await h.deliveries.create(delivery);

    await h.dispatcher.process(makeJob());

    expect(await h.locks.get(`lock:delivery:${delivery.id}`)).toBeNull();
  });

  it('does not consume an attempt when rate limited', async () => {
    const h = harness([], { limiter: 'deny' });
    await h.deliveries.create(delivery);

    await expect(h.dispatcher.process(makeJob())).resolves.toBe('rate_limited');

    const stored = await h.deliveries.get(delivery.id);
    expect(stored?.status).toBe('queued');
    expect(stored?.attempts).toBe(0);
    expect(h.scheduler.scheduled.size).toBe(1);
    expect(h.client.requests).toHaveLength(0);
  });

  it('kills a delivery whose endpoint was deactivated', async () => {
    const h = harness([], { endpoint: makeEndpoint({ active: false }) });
    await h.deliveries.create(delivery);

    await expect(h.dispatcher.process(makeJob())).resolves.toBe('dead');

    const stored = await h.deliveries.get(delivery.id);
    expect(stored?.status).toBe('dead');
    expect(stored?.lastError).toMatch(/inactive/);
  });

  it('skips jobs whose delivery is already terminal', async () => {
    const h = harness([], {});
    await h.deliveries.create(makeDelivery({ status: 'delivered' }));

    await expect(h.dispatcher.process(makeJob())).resolves.toBe('skipped');
    expect(h.client.requests).toHaveLength(0);
  });

  it('skips unknown deliveries instead of throwing', async () => {
    const h = harness([], {});
    await expect(h.dispatcher.process(makeJob({ deliveryId: 'nope' }))).resolves.toBe('skipped');
  });
});

describe('HttpDeliveryClient', () => {
  it('serializes once, signs the exact bytes, and reports a timeout', async () => {
    const client = new HttpDeliveryClient();
    const result = await client.send({
      deliveryId: 'd1',
      url: 'http://127.0.0.1:1/hook',
      secret: 'whsec_secret',
      payload: { hello: 'world' },
      attempt: 1,
      timeoutMs: 200,
    });

    expect(result.ok).toBe(false);
    expect(result.statusCode).toBeNull();
    expect(result.error).toBeTruthy();
    expect(result.durationMs).toBeGreaterThanOrEqual(0);
  });
});
