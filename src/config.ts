import { z } from 'zod';

const schema = z.object({
  NODE_ENV: z.string().default('development'),
  LOG_LEVEL: z.string().default('info'),

  PORT: z.coerce.number().int().positive().default(3000),
  HOST: z.string().default('0.0.0.0'),

  /**
   * When set, every `/v1` route requires this key (`Authorization: Bearer` or
   * `X-Api-Key`). Unset means an open API, which is the local/demo default.
   * An empty value counts as unset so `API_KEY=` in a compose env file is safe.
   */
  API_KEY: z.preprocess(
    (value) => (value === '' ? undefined : value),
    z.string().min(16, 'API_KEY must be at least 16 characters').optional(),
  ),

  /** Which transport the worker consumes and the API publishes to. */
  BROKER: z.enum(['kafka', 'sqs']).default('kafka'),

  REDIS_URL: z.string().default('redis://localhost:6379'),

  KAFKA_BROKERS: z.string().default('localhost:9092'),
  KAFKA_TOPIC: z.string().default('hookrelay.deliveries'),
  KAFKA_GROUP_ID: z.string().default('hookrelay-workers'),
  KAFKA_PARTITIONS: z.coerce.number().int().positive().default(3),

  SQS_QUEUE_URL: z.string().optional(),
  AWS_REGION: z.string().default('us-east-1'),
  /** Set to http://localhost:4566 for LocalStack. Unset for real AWS. */
  AWS_ENDPOINT_URL: z.string().optional(),
  AWS_ACCESS_KEY_ID: z.string().optional(),
  AWS_SECRET_ACCESS_KEY: z.string().optional(),

  ENDPOINTS_TABLE: z.string().default('hookrelay_endpoints'),
  DELIVERIES_TABLE: z.string().default('hookrelay_deliveries'),

  DELIVERY_TIMEOUT_MS: z.coerce.number().int().positive().default(5000),
  MAX_ATTEMPTS: z.coerce.number().int().positive().default(5),
  BASE_BACKOFF_MS: z.coerce.number().int().positive().default(250),
  MAX_BACKOFF_MS: z.coerce.number().int().positive().default(30_000),
  RATE_LIMIT_PER_SEC: z.coerce.number().int().positive().default(20),
  SCHEDULER_INTERVAL_MS: z.coerce.number().int().positive().default(200),
  IDEMPOTENCY_TTL_SEC: z.coerce.number().int().positive().default(86_400),
});

export type Config = z.infer<typeof schema>;

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const parsed = schema.safeParse(env);
  if (!parsed.success) {
    const details = parsed.error.issues
      .map((issue) => `${issue.path.join('.') || '(root)'}: ${issue.message}`)
      .join('; ');
    throw new Error(`Invalid configuration: ${details}`);
  }
  const config = parsed.data;
  if (config.BROKER === 'sqs' && !config.SQS_QUEUE_URL) {
    throw new Error('SQS_QUEUE_URL is required when BROKER=sqs');
  }
  return config;
}
