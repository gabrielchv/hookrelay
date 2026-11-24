import { randomBytes, randomUUID } from 'node:crypto';
import Fastify, { type FastifyError, type FastifyInstance } from 'fastify';
import { z, ZodError } from 'zod';
import type { Config } from '../config.js';
import type { Logger } from '../logger.js';
import type {
  Broker,
  DeliveryRepo,
  EndpointRepo,
  KeyValueStore,
} from '../ports.js';
import type { Delivery, Endpoint, EventEnvelope } from '../types.js';
import { assertDeliverableUrl } from '../core/url-guard.js';
import { apiKeyEquals, extractApiKey } from './auth.js';

/**
 * Registration is the cheap half of the SSRF guard: refuse an obviously
 * undeliverable URL at write time with a 400 the caller can act on. The
 * delivery path checks again, because a hostname can resolve differently
 * later.
 */
function endpointSchema(allowPrivate: boolean) {
  return z.object({
    url: z
      .string()
      .url()
      .superRefine((value, ctx) => {
        try {
          assertDeliverableUrl(value, { allowPrivate });
        } catch (error) {
          ctx.addIssue({
            code: z.ZodIssueCode.custom,
            message: error instanceof Error ? error.message : 'url is not deliverable',
          });
        }
      }),
    eventTypes: z.array(z.string().min(1).max(128)).min(1).default(['*']),
    secret: z.string().min(16).max(256).optional(),
  });
}

const eventSchema = z.object({
  type: z.string().min(1).max(128),
  data: z.unknown(),
});

export interface ApiDeps {
  config: Config;
  endpoints: EndpointRepo;
  deliveries: DeliveryRepo;
  kv: KeyValueStore;
  broker: Broker;
  logger: Logger;
  /** Redis liveness probe. */
  ping: () => Promise<boolean>;
}

function publicEndpoint(endpoint: Endpoint) {
  const { secret: _secret, ...rest } = endpoint;
  return rest;
}

export function buildServer(deps: ApiDeps): FastifyInstance {
  const { config, endpoints, deliveries, kv, broker, logger } = deps;
  const app = Fastify({ logger: false, bodyLimit: 256 * 1024 });
  const inputSchema = endpointSchema(config.ALLOW_PRIVATE_ENDPOINTS);

  app.setErrorHandler((error: FastifyError, request, reply) => {
    if (error instanceof ZodError) {
      return reply.status(400).send({
        error: 'validation_error',
        issues: error.issues.map((issue) => ({
          path: issue.path.join('.'),
          message: issue.message,
        })),
      });
    }
    if (error.statusCode && error.statusCode < 500) {
      return reply.status(error.statusCode).send({ error: error.message });
    }
    logger.error({ err: error, url: request.url }, 'request failed');
    return reply.status(500).send({ error: 'internal_error' });
  });

  // Registered before the routes so Fastify applies it to every /v1 handler.
  // /healthz is intentionally left out: probes cannot carry a credential.
  if (config.API_KEY) {
    const expected = config.API_KEY;
    app.addHook('onRequest', async (request, reply) => {
      if (!request.url.startsWith('/v1/')) return;

      const presented = extractApiKey(request.headers);
      if (!presented || !apiKeyEquals(presented, expected)) {
        logger.warn({ url: request.url, ip: request.ip }, 'rejected unauthenticated request');
        return reply
          .status(401)
          .header('www-authenticate', 'Bearer realm="hookrelay"')
          .send({ error: 'unauthorized' });
      }
    });
    logger.info('API key required for /v1 routes');
  } else {
    logger.warn('API_KEY is not set: /v1 routes are unauthenticated');
  }

  app.get('/healthz', async (_request, reply) => {
    let redis = false;
    try {
      redis = await deps.ping();
    } catch {
      redis = false;
    }
    if (!redis) {
      return reply.status(503).send({ status: 'degraded', redis });
    }
    return { status: 'ok', redis, broker: config.BROKER };
  });

  app.post('/v1/endpoints', async (request, reply) => {
    const input = inputSchema.parse(request.body);
    const endpoint: Endpoint = {
      id: randomUUID(),
      url: input.url,
      secret: input.secret ?? randomBytes(32).toString('hex'),
      eventTypes: input.eventTypes,
      active: true,
      createdAt: new Date().toISOString(),
    };
    await endpoints.create(endpoint);
    logger.info({ endpointId: endpoint.id, url: endpoint.url }, 'endpoint registered');

    // The secret is returned exactly once. GET never exposes it again.
    return reply.status(201).send(endpoint);
  });

  app.get<{ Params: { id: string } }>('/v1/endpoints/:id', async (request, reply) => {
    const endpoint = await endpoints.get(request.params.id);
    if (!endpoint) return reply.status(404).send({ error: 'endpoint_not_found' });
    return publicEndpoint(endpoint);
  });

  app.post('/v1/events', async (request, reply) => {
    const idempotencyKey = request.headers['idempotency-key'];
    if (typeof idempotencyKey !== 'string' || idempotencyKey.length === 0) {
      return reply.status(400).send({
        error: 'missing_idempotency_key',
        message: 'Send an Idempotency-Key header to make event ingestion safe to retry.',
      });
    }
    if (idempotencyKey.length > 200) {
      return reply.status(400).send({ error: 'idempotency_key_too_long' });
    }

    const input = eventSchema.parse(request.body);
    const idemKey = `idem:${idempotencyKey}`;
    const resultKey = `${idemKey}:result`;

    const acquired = await kv.setIfAbsent(idemKey, 'in_progress', config.IDEMPOTENCY_TTL_SEC);
    if (!acquired) {
      const cached = await kv.get(resultKey);
      if (cached) {
        return reply.status(200).send(JSON.parse(cached));
      }
      // A concurrent request holds the key but has not finished yet.
      return reply.status(409).send({
        error: 'idempotency_in_flight',
        message: 'A request with this Idempotency-Key is still being processed.',
      });
    }

    try {
      const event: EventEnvelope = {
        id: randomUUID(),
        type: input.type,
        data: input.data,
        createdAt: new Date().toISOString(),
      };

      const all = await endpoints.listAll();
      const targets = all.filter(
        (endpoint) =>
          endpoint.active &&
          (endpoint.eventTypes.includes('*') || endpoint.eventTypes.includes(event.type)),
      );

      const created: Delivery[] = [];
      for (const endpoint of targets) {
        const delivery: Delivery = {
          id: randomUUID(),
          eventId: event.id,
          endpointId: endpoint.id,
          url: endpoint.url,
          payload: event,
          status: 'queued',
          attempts: 0,
          maxAttempts: config.MAX_ATTEMPTS,
          lastStatusCode: null,
          lastError: null,
          nextAttemptAt: null,
          createdAt: new Date().toISOString(),
          updatedAt: new Date().toISOString(),
        };
        await deliveries.create(delivery);
        created.push(delivery);
      }

      for (const delivery of created) {
        await broker.publish({
          deliveryId: delivery.id,
          eventId: delivery.eventId,
          endpointId: delivery.endpointId,
          enqueuedAt: Date.now(),
        });
      }

      const response = {
        eventId: event.id,
        type: event.type,
        deliveries: created.map((delivery) => ({
          id: delivery.id,
          endpointId: delivery.endpointId,
          url: delivery.url,
        })),
      };

      await kv.set(resultKey, JSON.stringify(response), config.IDEMPOTENCY_TTL_SEC);
      logger.info(
        { eventId: event.id, type: event.type, deliveries: created.length },
        'event accepted',
      );
      return reply.status(202).send(response);
    } catch (error) {
      // Do not poison the idempotency key: the caller must be able to retry.
      await kv.del(idemKey);
      throw error;
    }
  });

  app.get<{ Params: { id: string } }>('/v1/deliveries/:id', async (request, reply) => {
    const delivery = await deliveries.get(request.params.id);
    if (!delivery) return reply.status(404).send({ error: 'delivery_not_found' });
    return delivery;
  });

  app.post<{ Params: { id: string } }>(
    '/v1/deliveries/:id/replay',
    async (request, reply) => {
      const delivery = await deliveries.get(request.params.id);
      if (!delivery) return reply.status(404).send({ error: 'delivery_not_found' });
      if (delivery.status === 'delivered') {
        return reply.status(409).send({ error: 'delivery_already_delivered' });
      }

      const replay: Delivery = {
        ...delivery,
        status: 'queued',
        attempts: 0,
        lastError: null,
        lastStatusCode: null,
        nextAttemptAt: null,
        updatedAt: new Date().toISOString(),
      };
      await deliveries.update(replay);
      await broker.publish({
        deliveryId: replay.id,
        eventId: replay.eventId,
        endpointId: replay.endpointId,
        enqueuedAt: Date.now(),
      });

      logger.info({ deliveryId: replay.id }, 'delivery replay requested');
      return reply.status(202).send({ deliveryId: replay.id, status: replay.status });
    },
  );

  return app;
}
