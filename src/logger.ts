import { pino } from 'pino';
import type { Config } from './config.js';

export function createLogger(config: Pick<Config, 'LOG_LEVEL'>) {
  return pino({
    level: config.LOG_LEVEL,
    base: { service: 'hookrelay' },
    timestamp: pino.stdTimeFunctions.isoTime,
  });
}

export type Logger = ReturnType<typeof createLogger>;
