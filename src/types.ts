/**
 * Domain types. These are transport-agnostic: the same shapes travel through
 * HTTP, Kafka messages and SQS message bodies.
 */

export interface Endpoint {
  id: string;
  url: string;
  /** HMAC secret. Returned once at creation time, stored server-side. */
  secret: string;
  /** Event types this endpoint subscribes to. `["*"]` means everything. */
  eventTypes: string[];
  active: boolean;
  createdAt: string;
}

export interface EventEnvelope {
  id: string;
  type: string;
  data: unknown;
  createdAt: string;
}

export type DeliveryStatus = 'queued' | 'delivered' | 'dead';

export interface Delivery {
  id: string;
  eventId: string;
  endpointId: string;
  url: string;
  payload: EventEnvelope;
  status: DeliveryStatus;
  attempts: number;
  maxAttempts: number;
  lastStatusCode: number | null;
  lastError: string | null;
  /** Epoch millis. Null when no retry is pending. */
  nextAttemptAt: number | null;
  createdAt: string;
  updatedAt: string;
}

/** The unit of work published to Kafka or SQS. Deliberately tiny. */
export interface DeliveryJob {
  deliveryId: string;
  eventId: string;
  endpointId: string;
  enqueuedAt: number;
}
