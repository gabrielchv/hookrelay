/**
 * Ports (hexagonal architecture). The API and worker depend on these
 * interfaces; Redis, DynamoDB, Kafka and SQS are adapters behind them.
 * Every port is small enough to fake in a unit test.
 */
import type { Delivery, DeliveryJob, Endpoint } from './types.js';

export interface EndpointRepo {
  create(endpoint: Endpoint): Promise<void>;
  get(id: string): Promise<Endpoint | null>;
  listAll(): Promise<Endpoint[]>;
}

export interface DeliveryRepo {
  create(delivery: Delivery): Promise<void>;
  get(id: string): Promise<Delivery | null>;
  update(delivery: Delivery): Promise<void>;
}

export interface KeyValueStore {
  /** SET key value NX EX ttl. Returns false when the key already existed. */
  setIfAbsent(key: string, value: string, ttlSeconds: number): Promise<boolean>;
  get(key: string): Promise<string | null>;
  set(key: string, value: string, ttlSeconds: number): Promise<void>;
  del(key: string): Promise<void>;
}

export interface RateLimiter {
  /** Returns true when the call is allowed under `limit` per `windowMs`. */
  allow(key: string, limit: number, windowMs: number): Promise<boolean>;
}

export interface RetryScheduler {
  schedule(deliveryId: string, atEpochMs: number): Promise<void>;
  /** Atomically pops up to `limit` deliveries whose retry time has passed. */
  popDue(limit: number): Promise<string[]>;
}

export interface DeliveryRequest {
  deliveryId: string;
  url: string;
  secret: string;
  payload: unknown;
  attempt: number;
  timeoutMs: number;
}

export interface DeliveryResult {
  ok: boolean;
  statusCode: number | null;
  error: string | null;
  durationMs: number;
  /**
   * A failure that retrying cannot fix (for example a delivery target the SSRF
   * guard refuses). The dispatcher dead-letters these immediately instead of
   * spending the remaining attempts on them.
   */
  permanent?: boolean;
}

export interface DeliveryClient {
  send(request: DeliveryRequest): Promise<DeliveryResult>;
}

export type JobHandler = (job: DeliveryJob) => Promise<void>;

export interface Broker {
  publish(job: DeliveryJob): Promise<void>;
  subscribe(handler: JobHandler): Promise<void>;
  close(): Promise<void>;
}
