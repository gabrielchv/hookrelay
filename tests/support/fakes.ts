import type {
  Broker,
  DeliveryClient,
  DeliveryRepo,
  DeliveryRequest,
  DeliveryResult,
  EndpointRepo,
  JobHandler,
  KeyValueStore,
  RateLimiter,
  RetryScheduler,
} from '../../src/ports.js';
import type { Delivery, DeliveryJob, Endpoint } from '../../src/types.js';

export class MemoryKeyValueStore implements KeyValueStore {
  private readonly map = new Map<string, string>();

  async setIfAbsent(key: string, value: string, _ttlSeconds?: number): Promise<boolean> {
    if (this.map.has(key)) return false;
    this.map.set(key, value);
    return true;
  }

  async get(key: string): Promise<string | null> {
    return this.map.get(key) ?? null;
  }

  async set(key: string, value: string, _ttlSeconds?: number): Promise<void> {
    this.map.set(key, value);
  }

  async del(key: string): Promise<void> {
    this.map.delete(key);
  }
}

export class MemoryEndpointRepo implements EndpointRepo {
  readonly items = new Map<string, Endpoint>();

  async create(endpoint: Endpoint): Promise<void> {
    this.items.set(endpoint.id, endpoint);
  }

  async get(id: string): Promise<Endpoint | null> {
    return this.items.get(id) ?? null;
  }

  async listAll(): Promise<Endpoint[]> {
    return [...this.items.values()];
  }
}

export class MemoryDeliveryRepo implements DeliveryRepo {
  readonly items = new Map<string, Delivery>();

  async create(delivery: Delivery): Promise<void> {
    this.items.set(delivery.id, { ...delivery });
  }

  async get(id: string): Promise<Delivery | null> {
    const found = this.items.get(id);
    return found ? { ...found } : null;
  }

  async update(delivery: Delivery): Promise<void> {
    this.items.set(delivery.id, { ...delivery });
  }
}

export class AlwaysAllowLimiter implements RateLimiter {
  calls: Array<{ key: string; limit: number; windowMs: number }> = [];

  async allow(key: string, limit: number, windowMs: number): Promise<boolean> {
    this.calls.push({ key, limit, windowMs });
    return true;
  }
}

export class DenyAllLimiter implements RateLimiter {
  async allow(): Promise<boolean> {
    return false;
  }
}

export class MemoryScheduler implements RetryScheduler {
  readonly scheduled = new Map<string, number>();

  async schedule(deliveryId: string, atEpochMs: number): Promise<void> {
    this.scheduled.set(deliveryId, atEpochMs);
  }

  async popDue(limit?: number): Promise<string[]> {
    const now = Date.now();
    const due = [...this.scheduled.entries()]
      .filter(([, at]) => at <= now)
      .map(([id]) => id)
      .slice(0, limit ?? undefined);
    for (const id of due) this.scheduled.delete(id);
    return due;
  }
}

export class ScriptedDeliveryClient implements DeliveryClient {
  readonly requests: DeliveryRequest[] = [];
  private readonly results: DeliveryResult[];

  constructor(results: DeliveryResult[]) {
    this.results = [...results];
  }

  async send(request: DeliveryRequest): Promise<DeliveryResult> {
    this.requests.push(request);
    return (
      this.results.shift() ?? {
        ok: false,
        statusCode: 500,
        error: 'no scripted result left',
        durationMs: 1,
      }
    );
  }
}

export class RecordingBroker implements Broker {
  readonly published: DeliveryJob[] = [];
  handler: JobHandler | null = null;

  async publish(job: DeliveryJob): Promise<void> {
    this.published.push(job);
  }

  async subscribe(handler: JobHandler): Promise<void> {
    this.handler = handler;
  }

  async close(): Promise<void> {}
}

export function makeDelivery(overrides: Partial<Delivery> = {}): Delivery {
  const now = new Date().toISOString();
  return {
    id: 'delivery-1',
    eventId: 'event-1',
    endpointId: 'endpoint-1',
    url: 'https://example.test/hook',
    payload: {
      id: 'event-1',
      type: 'order.created',
      data: { orderId: 'ord_1' },
      createdAt: now,
    },
    status: 'queued',
    attempts: 0,
    maxAttempts: 3,
    lastStatusCode: null,
    lastError: null,
    nextAttemptAt: null,
    createdAt: now,
    updatedAt: now,
    ...overrides,
  };
}

export function makeEndpoint(overrides: Partial<Endpoint> = {}): Endpoint {
  return {
    id: 'endpoint-1',
    url: 'https://example.test/hook',
    secret: 'super-secret-value-1234',
    eventTypes: ['*'],
    active: true,
    createdAt: new Date().toISOString(),
    ...overrides,
  };
}

export function makeJob(overrides: Partial<DeliveryJob> = {}): DeliveryJob {
  return {
    deliveryId: 'delivery-1',
    eventId: 'event-1',
    endpointId: 'endpoint-1',
    enqueuedAt: Date.now(),
    ...overrides,
  };
}
