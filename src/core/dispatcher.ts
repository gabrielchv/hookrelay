/**
 * The dispatcher is the heart of the service: it turns one DeliveryJob into
 * zero or more HTTP attempts and records the outcome.
 *
 * Guarantees it provides:
 *  - at-least-once delivery (a crash after the HTTP call can duplicate a send)
 *  - per-delivery mutual exclusion via a short-lived Redis lease, so a Kafka
 *    rebalance or an SQS visibility timeout does not produce concurrent sends
 *  - exponential backoff with full jitter between attempts
 *  - a terminal `dead` state instead of infinite retries
 *
 * It intentionally does NOT provide exactly-once delivery. Receivers get a
 * stable X-Hookrelay-Delivery header and should deduplicate on it. See
 * docs/architecture.md for why exactly-once is not on the menu.
 */
import { nextDelayMs, type BackoffPolicy } from './backoff.js';
import type {
  DeliveryClient,
  DeliveryRepo,
  EndpointRepo,
  KeyValueStore,
  RateLimiter,
  RetryScheduler,
} from '../ports.js';
import type { DeliveryJob, Delivery } from '../types.js';

export type ProcessOutcome = 'delivered' | 'retried' | 'dead' | 'rate_limited' | 'skipped';

export interface DispatcherDeps {
  endpoints: EndpointRepo;
  deliveries: DeliveryRepo;
  locks: KeyValueStore;
  limiter: RateLimiter;
  scheduler: RetryScheduler;
  client: DeliveryClient;
  policy: BackoffPolicy;
  rateLimitPerSec: number;
  /** Per-request timeout handed to the delivery client. */
  deliveryTimeoutMs: number;
  now?: () => number;
  random?: () => number;
}

const LOCK_TTL_SECONDS = 30;

export class Dispatcher {
  constructor(private readonly deps: DispatcherDeps) {}

  private now(): number {
    return this.deps.now ? this.deps.now() : Date.now();
  }

  async process(job: DeliveryJob): Promise<ProcessOutcome> {
    const { endpoints, deliveries, locks, limiter, scheduler, client } = this.deps;

    const delivery = await deliveries.get(job.deliveryId);
    if (!delivery) return 'skipped';
    if (delivery.status === 'delivered' || delivery.status === 'dead') return 'skipped';

    const lockKey = `lock:delivery:${delivery.id}`;
    const acquired = await locks.setIfAbsent(lockKey, '1', LOCK_TTL_SECONDS);
    if (!acquired) return 'skipped';

    try {
      const endpoint = await endpoints.get(delivery.endpointId);
      if (!endpoint || !endpoint.active) {
        await this.finish(delivery, 'dead', {
          lastError: endpoint ? 'endpoint is inactive' : 'endpoint no longer exists',
        });
        return 'dead';
      }

      const allowed = await limiter.allow(
        `rl:endpoint:${endpoint.id}`,
        this.deps.rateLimitPerSec,
        1000,
      );
      if (!allowed) {
        // Rate limiting must not consume an attempt: the endpoint is healthy,
        // we are simply protecting it. Re-queue shortly after the window.
        const at = this.now() + 1000;
        await this.finish(delivery, 'queued', { nextAttemptAt: at });
        await scheduler.schedule(delivery.id, at);
        return 'rate_limited';
      }

      const attempt = delivery.attempts + 1;
      let result;
      try {
        result = await client.send({
          deliveryId: delivery.id,
          url: endpoint.url,
          secret: endpoint.secret,
          payload: delivery.payload,
          attempt,
          timeoutMs: this.deps.deliveryTimeoutMs,
        });
      } catch (error) {
        result = {
          ok: false,
          statusCode: null,
          error: error instanceof Error ? error.message : String(error),
          durationMs: 0,
        };
      }

      if (result.ok) {
        await this.finish(delivery, 'delivered', {
          attempts: attempt,
          lastStatusCode: result.statusCode,
          lastError: null,
          nextAttemptAt: null,
        });
        return 'delivered';
      }

      if (attempt >= delivery.maxAttempts) {
        await this.finish(delivery, 'dead', {
          attempts: attempt,
          lastStatusCode: result.statusCode,
          lastError: result.error ?? `HTTP ${result.statusCode}`,
          nextAttemptAt: null,
        });
        return 'dead';
      }

      const delay = nextDelayMs(attempt, this.deps.policy, this.deps.random);
      const nextAttemptAt = this.now() + delay;
      await this.finish(delivery, 'queued', {
        attempts: attempt,
        lastStatusCode: result.statusCode,
        lastError: result.error ?? `HTTP ${result.statusCode}`,
        nextAttemptAt,
      });
      await scheduler.schedule(delivery.id, nextAttemptAt);
      return 'retried';
    } finally {
      await locks.del(lockKey);
    }
  }

  private async finish(
    delivery: Delivery,
    status: Delivery['status'],
    patch: Partial<Delivery>,
  ): Promise<void> {
    const updated: Delivery = {
      ...delivery,
      ...patch,
      status,
      updatedAt: new Date(this.now()).toISOString(),
    };
    await this.deps.deliveries.update(updated);
  }
}
