import type { FastifyInstance } from 'fastify';
import { afterEach, describe, expect, it } from 'vitest';
import { buildServer } from '../src/api/server.js';
import { loadConfig } from '../src/config.js';
import { createLogger } from '../src/logger.js';
import {
  MemoryDeliveryRepo,
  MemoryEndpointRepo,
  MemoryKeyValueStore,
  RecordingBroker,
} from './support/fakes.js';

const logger = createLogger({ LOG_LEVEL: 'silent' });

interface Harness {
  app: FastifyInstance;
  endpoints: MemoryEndpointRepo;
  deliveries: MemoryDeliveryRepo;
  broker: RecordingBroker;
}

function harness(): Harness {
  const endpoints = new MemoryEndpointRepo();
  const deliveries = new MemoryDeliveryRepo();
  const broker = new RecordingBroker();
  const app = buildServer({
    config: loadConfig({}),
    endpoints,
    deliveries,
    kv: new MemoryKeyValueStore(),
    broker,
    logger,
    ping: async () => true,
  });
  return { app, endpoints, deliveries, broker };
}

let current: Harness | null = null;

afterEach(async () => {
  await current?.app.close();
  current = null;
});

async function withApp(): Promise<Harness> {
  current = harness();
  return current;
}

async function registerEndpoint(
  h: Harness,
  payload: Record<string, unknown> = {
    url: 'https://example.test/hook',
    eventTypes: ['order.created'],
  },
) {
  const response = await h.app.inject({
    method: 'POST',
    url: '/v1/endpoints',
    payload,
  });
  return response;
}

describe('POST /v1/endpoints', () => {
  it('registers an endpoint, returns the secret once, then hides it', async () => {
    const h = await withApp();
    const created = await registerEndpoint(h);

    expect(created.statusCode).toBe(201);
    const body = created.json();
    expect(body.id).toBeTruthy();
    expect(body.secret).toHaveLength(64);
    expect(body.active).toBe(true);

    const fetched = await h.app.inject({ method: 'GET', url: `/v1/endpoints/${body.id}` });
    expect(fetched.statusCode).toBe(200);
    expect(fetched.json().secret).toBeUndefined();
  });

  it('rejects non-http urls', async () => {
    const h = await withApp();
    const response = await registerEndpoint(h, { url: 'ftp://example.test/hook' });
    expect(response.statusCode).toBe(400);
    expect(response.json().error).toBe('validation_error');
  });

  it('404s an unknown endpoint', async () => {
    const h = await withApp();
    const response = await h.app.inject({ method: 'GET', url: '/v1/endpoints/missing' });
    expect(response.statusCode).toBe(404);
  });
});

describe('POST /v1/events', () => {
  it('requires an Idempotency-Key', async () => {
    const h = await withApp();
    const response = await h.app.inject({
      method: 'POST',
      url: '/v1/events',
      payload: { type: 'order.created', data: {} },
    });
    expect(response.statusCode).toBe(400);
    expect(response.json().error).toBe('missing_idempotency_key');
  });

  it('fans out only to subscribed endpoints and publishes one job each', async () => {
    const h = await withApp();
    await registerEndpoint(h, {
      url: 'https://a.test/hook',
      eventTypes: ['order.created'],
    });
    await registerEndpoint(h, {
      url: 'https://b.test/hook',
      eventTypes: ['invoice.paid'],
    });

    const response = await h.app.inject({
      method: 'POST',
      url: '/v1/events',
      headers: { 'idempotency-key': 'evt-key-1' },
      payload: { type: 'order.created', data: { orderId: 'ord_1' } },
    });

    expect(response.statusCode).toBe(202);
    expect(response.json().deliveries).toHaveLength(1);
    expect(h.broker.published).toHaveLength(1);
    expect(h.deliveries.items.size).toBe(1);
  });

  it('is idempotent across retries: same key returns the same deliveries', async () => {
    const h = await withApp();
    await registerEndpoint(h);

    const request = {
      method: 'POST' as const,
      url: '/v1/events',
      headers: { 'idempotency-key': 'same-key' },
      payload: { type: 'order.created', data: { orderId: 'ord_1' } },
    };

    const first = await h.app.inject(request);
    const second = await h.app.inject(request);

    expect(first.statusCode).toBe(202);
    expect(second.statusCode).toBe(200);
    expect(second.json().eventId).toBe(first.json().eventId);
    expect(second.json().deliveries).toEqual(first.json().deliveries);
    expect(h.broker.published).toHaveLength(1);
    expect(h.deliveries.items.size).toBe(1);
  });

  it('accepts an event with no matching subscribers', async () => {
    const h = await withApp();
    await registerEndpoint(h, {
      url: 'https://b.test/hook',
      eventTypes: ['invoice.paid'],
    });

    const response = await h.app.inject({
      method: 'POST',
      url: '/v1/events',
      headers: { 'idempotency-key': 'no-subscribers' },
      payload: { type: 'order.created', data: {} },
    });

    expect(response.statusCode).toBe(202);
    expect(response.json().deliveries).toEqual([]);
    expect(h.broker.published).toHaveLength(0);
  });

  it('validates the event type', async () => {
    const h = await withApp();
    const response = await h.app.inject({
      method: 'POST',
      url: '/v1/events',
      headers: { 'idempotency-key': 'k' },
      payload: { type: '', data: {} },
    });
    expect(response.statusCode).toBe(400);
  });
});

describe('deliveries', () => {
  it('returns delivery state and supports replay', async () => {
    const h = await withApp();
    await registerEndpoint(h);

    const created = await h.app.inject({
      method: 'POST',
      url: '/v1/events',
      headers: { 'idempotency-key': 'replay-key' },
      payload: { type: 'order.created', data: {} },
    });
    const deliveryId = created.json().deliveries[0].id;

    const fetched = await h.app.inject({ method: 'GET', url: `/v1/deliveries/${deliveryId}` });
    expect(fetched.statusCode).toBe(200);
    expect(fetched.json().status).toBe('queued');

    const stored = h.deliveries.items.get(deliveryId);
    if (!stored) throw new Error('delivery missing');
    h.deliveries.items.set(deliveryId, { ...stored, status: 'dead', attempts: 3 });

    const replay = await h.app.inject({
      method: 'POST',
      url: `/v1/deliveries/${deliveryId}/replay`,
    });
    expect(replay.statusCode).toBe(202);
    expect(h.deliveries.items.get(deliveryId)?.status).toBe('queued');
    expect(h.deliveries.items.get(deliveryId)?.attempts).toBe(0);
    expect(h.broker.published).toHaveLength(2);
  });

  it('404s an unknown delivery', async () => {
    const h = await withApp();
    const response = await h.app.inject({ method: 'GET', url: '/v1/deliveries/missing' });
    expect(response.statusCode).toBe(404);
  });
});

describe('GET /healthz', () => {
  it('reports ok when redis answers', async () => {
    const h = await withApp();
    const response = await h.app.inject({ method: 'GET', url: '/healthz' });
    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({ status: 'ok', redis: true });
  });
});
