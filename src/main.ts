import { buildServer } from './api/server.js';
import { createBroker } from './broker/index.js';
import { loadConfig } from './config.js';
import { Dispatcher } from './core/dispatcher.js';
import { HttpDeliveryClient } from './delivery/client.js';
import { createLogger } from './logger.js';
import {
  createDocumentClient,
  DynamoDeliveryRepo,
  DynamoEndpointRepo,
} from './store/dynamo.js';
import {
  createRedis,
  RedisKeyValueStore,
  RedisRateLimiter,
  RedisRetryScheduler,
} from './store/redis.js';
import { runWorker } from './worker/worker.js';

async function main(): Promise<void> {
  const mode = process.argv[2] ?? 'api';
  const config = loadConfig();
  const logger = createLogger(config);

  const redis = createRedis(config.REDIS_URL);
  const documentClient = createDocumentClient(config);
  const endpoints = new DynamoEndpointRepo(documentClient, config.ENDPOINTS_TABLE);
  const deliveries = new DynamoDeliveryRepo(documentClient, config.DELIVERIES_TABLE);
  const kv = new RedisKeyValueStore(redis);
  const scheduler = new RedisRetryScheduler(redis);
  const broker = createBroker(config, logger);

  if (mode === 'api') {
    const app = buildServer({
      config,
      endpoints,
      deliveries,
      kv,
      broker,
      logger,
      ping: async () => (await redis.ping()) === 'PONG',
    });

    await app.listen({ port: config.PORT, host: config.HOST });
    logger.info(
      { port: config.PORT, broker: config.BROKER },
      'hookrelay api listening',
    );

    const shutdown = async (signal: string) => {
      logger.info({ signal }, 'shutting down api');
      await app.close().catch(() => undefined);
      await broker.close().catch(() => undefined);
      await redis.quit().catch(() => undefined);
      process.exit(0);
    };
    process.on('SIGTERM', () => void shutdown('SIGTERM'));
    process.on('SIGINT', () => void shutdown('SIGINT'));
    return;
  }

  if (mode === 'worker') {
    const dispatcher = new Dispatcher({
      endpoints,
      deliveries,
      locks: kv,
      limiter: new RedisRateLimiter(redis),
      scheduler,
      client: new HttpDeliveryClient(),
      policy: { baseMs: config.BASE_BACKOFF_MS, maxMs: config.MAX_BACKOFF_MS },
      rateLimitPerSec: config.RATE_LIMIT_PER_SEC,
    });

    const stop = await runWorker({
      config,
      broker,
      dispatcher,
      scheduler,
      deliveries,
      logger,
    });
    logger.info({ broker: config.BROKER }, 'hookrelay worker consuming');

    const shutdown = async (signal: string) => {
      logger.info({ signal }, 'shutting down worker');
      await stop().catch(() => undefined);
      await redis.quit().catch(() => undefined);
      process.exit(0);
    };
    process.on('SIGTERM', () => void shutdown('SIGTERM'));
    process.on('SIGINT', () => void shutdown('SIGINT'));
    return;
  }

  throw new Error(`Unknown mode "${mode}". Expected "api" or "worker".`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
