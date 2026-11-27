import type { FastifyInstance } from 'fastify';
import { afterEach, describe, expect, it } from 'vitest';
import { buildServer } from '../src/api/server.js';
import { loadConfig } from '../src/config.js';
import { createLogger } from '../src/logger.js';
import type { DeliveryJob } from '../src/types.js';
import {
  MemoryDeliveryRepo,
  MemoryEndpointRepo,
  MemoryKeyValueStore,
} from './support/fakes.js';

/**
 * Fan-out must not strand or duplicate deliveries when the broker rejects one.
 *
 * The original shape created every delivery row and *then* published in a second
 * loop, so a publish failure part-way through left rows already published (they
 * still go out) and rows never published (`queued` with no broker message and no
 * schedule entry, so they never retry). The request then failed and its catch
 * deleted the idempotency key, so the caller's retry re-ran the whole fan-out and
 * created a second set of deliveries — every endpoint receiving the event twice
 * under two different delivery ids.
 */

const logger = createLogger({ LOG_LEVEL: 'silent' });

/** A broker that fails for a chosen endpoint id and records the rest. */
class PartiallyFailingBroker {
  readonly published: DeliveryJob[] = [];
  readonly failingEndpointIds = new Set<string>();
  handler: ((job: DeliveryJob) => Promise<void>) | null = null;

  async publish(job: DeliveryJob): Promise<void> {
    if (this.failingEndpointIds.has(job.endpointId)) {
      throw new Error(`broker unavailable for ${job.endpointId}`);
    }
    this.published.push(job);
  }

  async subscribe(handler: (job: DeliveryJob) => Promise<void>): Promise<void> {
    this.handler = handler;
  }

  async close(): Promise<void> {}
}

interface Harness {
  app: FastifyInstance;
  endpoints: MemoryEndpointRepo;
  deliveries: MemoryDeliveryRepo;
  broker: PartiallyFailingBroker;
}

let current: Harness | null = null;

afterEach(async () => {
  await current?.app.close();
  current = null;
});

function harness(): Harness {
  const endpoints = new MemoryEndpointRepo();
  const deliveries = new MemoryDeliveryRepo();
  const broker = new PartiallyFailingBroker();
  const app = buildServer({
    config: loadConfig({}),
    endpoints,
    deliveries,
    kv: new MemoryKeyValueStore(),
    broker,
    logger,
    ping: async () => true,
  });
  current = { app, endpoints, deliveries, broker };
  return current;
}

async function subscribe(h: Harness, url: string): Promise<string> {
  const response = await h.app.inject({
    method: 'POST',
    url: '/v1/endpoints',
    payload: { url, eventTypes: ['order.created'] },
  });
  expect(response.statusCode).toBe(201);
  return response.json().id as string;
}

async function sendEvent(h: Harness, idempotencyKey = 'key-1') {
  return h.app.inject({
    method: 'POST',
    url: '/v1/events',
    headers: { 'idempotency-key': idempotencyKey },
    payload: { type: 'order.created', data: { orderId: 'ord_1' } },
  });
}

describe('fan-out when the broker rejects one target', () => {
  it('still delivers to the endpoints it can reach', async () => {
    const h = harness();
    const good = await subscribe(h, 'https://good.test/hook');
    const bad = await subscribe(h, 'https://bad.test/hook');
    h.broker.failingEndpointIds.add(bad);

    const response = await sendEvent(h);

    expect(response.statusCode).toBe(202);
    const byEndpoint = Object.fromEntries(
      h.broker.published.map((job) => [job.endpointId, job]),
    );
    expect(byEndpoint[good]).toBeDefined();
    expect(byEndpoint[bad]).toBeUndefined();
  });

  it('records the delivery it could not enqueue instead of leaving it queued', async () => {
    const h = harness();
    await subscribe(h, 'https://good.test/hook');
    const bad = await subscribe(h, 'https://bad.test/hook');
    h.broker.failingEndpointIds.add(bad);

    await sendEvent(h);

    const stored = [...h.deliveries.items.values()].find((d) => d.endpointId === bad)!;
    // `queued` here would mean "will be sent" and nothing would ever send it.
    expect(stored.status).toBe('dead');
    expect(stored.lastError).toContain('could not enqueue');

    const goodStored = [...h.deliveries.items.values()].find((d) => d.endpointId !== bad)!;
    expect(goodStored.status).toBe('queued');
  });

  it('a retry with the same idempotency key creates no second set of deliveries', async () => {
    const h = harness();
    await subscribe(h, 'https://good.test/hook');
    const bad = await subscribe(h, 'https://bad.test/hook');
    h.broker.failingEndpointIds.add(bad);

    const first = await sendEvent(h, 'key-dup');
    const countAfterFirst = h.deliveries.items.size;
    const publishedAfterFirst = h.broker.published.length;

    const second = await sendEvent(h, 'key-dup');

    // A replayed request returns the cached response with 200, not 202 — it did
    // not accept a new event. What matters is that nothing was created again.
    expect(second.statusCode).toBe(200);
    expect(h.deliveries.items.size).toBe(countAfterFirst);
    expect(h.broker.published.length).toBe(publishedAfterFirst);
    expect(second.json().eventId).toBe(first.json().eventId);
  });

  it('a delivery that could not be enqueued is replayable', async () => {
    const h = harness();
    const bad = await subscribe(h, 'https://bad.test/hook');
    h.broker.failingEndpointIds.add(bad);

    await sendEvent(h);
    const stored = [...h.deliveries.items.values()].find((d) => d.endpointId === bad)!;
    expect(stored.status).toBe('dead');

    // The broker recovers; the operator replays the dead delivery.
    h.broker.failingEndpointIds.delete(bad);
    const replay = await h.app.inject({
      method: 'POST',
      url: `/v1/deliveries/${stored.id}/replay`,
    });

    expect(replay.statusCode).toBeLessThan(400);
    expect(h.broker.published.map((job) => job.deliveryId)).toContain(stored.id);
  });
});
