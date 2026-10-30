/**
 * Redis adapters.
 *
 * Three distinct jobs, three distinct data structures:
 *  - idempotency + delivery locks  -> string keys with TTL (SET NX EX)
 *  - per-endpoint rate limiting    -> sorted-set sliding window (atomic Lua)
 *  - retry scheduling              -> sorted set scored by next-attempt time,
 *                                     popped atomically so two workers cannot
 *                                     claim the same due delivery
 */
import { Redis } from 'ioredis';
import { randomUUID } from 'node:crypto';
import type { KeyValueStore, RateLimiter, RetryScheduler } from '../ports.js';

const RATE_LIMIT_SCRIPT = `
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local member = ARGV[4]

redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)
local count = redis.call('ZCARD', key)
if count < limit then
  redis.call('ZADD', key, now, member)
  redis.call('PEXPIRE', key, window)
  return 1
end
return 0
`;

const POP_DUE_SCRIPT = `
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
if #ids == 0 then
  return {}
end
for i = 1, #ids do
  redis.call('ZREM', KEYS[1], ids[i])
end
return ids
`;

export function createRedis(url: string): Redis {
  const redis = new Redis(url, {
    maxRetriesPerRequest: 3,
    enableReadyCheck: true,
    lazyConnect: false,
  });
  redis.on('error', () => {
    // ioredis emits connection errors continuously during retries; the health
    // endpoint reports real state, so we do not crash the process here.
  });
  return redis;
}

export class RedisKeyValueStore implements KeyValueStore {
  constructor(private readonly redis: Redis) {}

  async setIfAbsent(key: string, value: string, ttlSeconds: number): Promise<boolean> {
    const result = await this.redis.set(key, value, 'EX', ttlSeconds, 'NX');
    return result === 'OK';
  }

  async get(key: string): Promise<string | null> {
    return this.redis.get(key);
  }

  async set(key: string, value: string, ttlSeconds: number): Promise<void> {
    await this.redis.set(key, value, 'EX', ttlSeconds);
  }

  async del(key: string): Promise<void> {
    await this.redis.del(key);
  }
}

export class RedisRateLimiter implements RateLimiter {
  constructor(private readonly redis: Redis) {}

  async allow(key: string, limit: number, windowMs: number): Promise<boolean> {
    const now = Date.now();
    const member = `${now}:${randomUUID()}`;
    const result = (await this.redis.eval(
      RATE_LIMIT_SCRIPT,
      1,
      key,
      String(now),
      String(windowMs),
      String(limit),
      member,
    )) as number;
    return result === 1;
  }
}

export class RedisRetryScheduler implements RetryScheduler {
  constructor(
    private readonly redis: Redis,
    private readonly key = 'retry:schedule',
  ) {}

  async schedule(deliveryId: string, atEpochMs: number): Promise<void> {
    await this.redis.zadd(this.key, atEpochMs, deliveryId);
  }

  async popDue(limit: number): Promise<string[]> {
    const result = (await this.redis.eval(
      POP_DUE_SCRIPT,
      1,
      this.key,
      String(Date.now()),
      String(limit),
    )) as string[];
    return result ?? [];
  }
}
