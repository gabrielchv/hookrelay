import type { FastifyInstance } from 'fastify';
import { afterEach, describe, expect, it } from 'vitest';
import { apiKeyEquals, extractApiKey } from '../src/api/auth.js';
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
const KEY = 'test-key-that-is-long-enough';

let current: FastifyInstance | null = null;

afterEach(async () => {
  await current?.close();
  current = null;
});

function app(env: NodeJS.ProcessEnv = {}): FastifyInstance {
  current = buildServer({
    config: loadConfig(env),
    endpoints: new MemoryEndpointRepo(),
    deliveries: new MemoryDeliveryRepo(),
    kv: new MemoryKeyValueStore(),
    broker: new RecordingBroker(),
    logger,
    ping: async () => true,
  });
  return current;
}

const endpointPayload = {
  url: 'https://example.test/hook',
  eventTypes: ['order.created'],
};

describe('API key auth', () => {
  it('leaves /v1 open when API_KEY is unset', async () => {
    const response = await app().inject({
      method: 'POST',
      url: '/v1/endpoints',
      payload: endpointPayload,
    });
    expect(response.statusCode).toBe(201);
  });

  it('rejects a /v1 request with no credential', async () => {
    const response = await app({ API_KEY: KEY }).inject({
      method: 'POST',
      url: '/v1/endpoints',
      payload: endpointPayload,
    });
    expect(response.statusCode).toBe(401);
    expect(response.json()).toEqual({ error: 'unauthorized' });
    expect(response.headers['www-authenticate']).toBe('Bearer realm="hookrelay"');
  });

  it('rejects a wrong key', async () => {
    const response = await app({ API_KEY: KEY }).inject({
      method: 'POST',
      url: '/v1/endpoints',
      payload: endpointPayload,
      headers: { authorization: `Bearer ${KEY}nope` },
    });
    expect(response.statusCode).toBe(401);
  });

  it('accepts Authorization: Bearer', async () => {
    const response = await app({ API_KEY: KEY }).inject({
      method: 'POST',
      url: '/v1/endpoints',
      payload: endpointPayload,
      headers: { authorization: `Bearer ${KEY}` },
    });
    expect(response.statusCode).toBe(201);
  });

  it('accepts X-Api-Key, case-insensitively in the scheme', async () => {
    const response = await app({ API_KEY: KEY }).inject({
      method: 'GET',
      url: '/v1/endpoints/does-not-exist',
      headers: { 'x-api-key': KEY },
    });
    // Authenticated, so the handler answers; the id simply does not exist.
    expect(response.statusCode).toBe(404);

    const bearer = await app({ API_KEY: KEY }).inject({
      method: 'GET',
      url: '/v1/endpoints/does-not-exist',
      headers: { authorization: `bearer ${KEY}` },
    });
    expect(bearer.statusCode).toBe(404);
  });

  it('leaves /healthz reachable without a credential', async () => {
    const response = await app({ API_KEY: KEY }).inject({ method: 'GET', url: '/healthz' });
    expect(response.statusCode).toBe(200);
  });

  it('treats an empty API_KEY as unset', () => {
    expect(loadConfig({ API_KEY: '' }).API_KEY).toBeUndefined();
  });

  it('refuses a trivially short API_KEY', () => {
    expect(() => loadConfig({ API_KEY: 'short' })).toThrow(/at least 16 characters/);
  });
});

describe('extractApiKey', () => {
  it('returns null when no credential is present', () => {
    expect(extractApiKey({})).toBeNull();
  });

  it('prefers the Bearer token and falls back to X-Api-Key', () => {
    expect(extractApiKey({ authorization: `Bearer ${KEY}` })).toBe(KEY);
    expect(extractApiKey({ 'x-api-key': KEY })).toBe(KEY);
  });

  it('ignores malformed Authorization headers', () => {
    expect(extractApiKey({ authorization: 'Basic abc' })).toBeNull();
    expect(extractApiKey({ authorization: 'Bearer' })).toBeNull();
  });
});

describe('apiKeyEquals', () => {
  it('matches equal keys and rejects everything else', () => {
    expect(apiKeyEquals(KEY, KEY)).toBe(true);
    expect(apiKeyEquals('other-key', KEY)).toBe(false);
    expect(apiKeyEquals('', KEY)).toBe(false);
    expect(apiKeyEquals(KEY + 'x', KEY)).toBe(false);
  });
});
