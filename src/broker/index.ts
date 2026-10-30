import type { Config } from '../config.js';
import type { Logger } from '../logger.js';
import type { Broker } from '../ports.js';
import { KafkaBroker } from './kafka.js';
import { SqsBroker } from './sqs.js';

export function createBroker(config: Config, logger: Logger): Broker {
  return config.BROKER === 'kafka'
    ? new KafkaBroker(config, logger)
    : new SqsBroker(config, logger);
}
