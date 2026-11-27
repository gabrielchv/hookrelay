import { describe, expect, it } from 'vitest';
import { KafkaBroker } from '../src/broker/kafka.js';
import { loadConfig } from '../src/config.js';
import { createLogger } from '../src/logger.js';
import type { DeliveryJob } from '../src/types.js';

/**
 * The two transports must fail the same way.
 *
 * SQS leaves a message on the queue when its handler throws (no delete, so the
 * visibility timeout returns it). Kafka used to swallow the error instead, which
 * let kafkajs commit the offset: the job was acknowledged and gone. The same
 * handler failure was retried on one transport and lost on the other, while the
 * adapter's own comment claimed a Redis retry was already scheduled — true only
 * if the handler got that far, which a Dynamo or Redis error does not.
 */

const logger = createLogger({ LOG_LEVEL: 'silent' });

function broker(): KafkaBroker {
  // Never connected: `handleMessage` touches no Kafka client.
  return new KafkaBroker(loadConfig({}), logger);
}

const JOB: DeliveryJob = {
  deliveryId: 'd1',
  eventId: 'e1',
  endpointId: 'ep1',
  enqueuedAt: Date.now(),
};

const encoded = Buffer.from(JSON.stringify(JOB));

describe('KafkaBroker.handleMessage', () => {
  it('passes a well-formed job to the handler', async () => {
    const seen: DeliveryJob[] = [];

    await broker().handleMessage(encoded, async (job) => {
      seen.push(job);
    });

    expect(seen).toEqual([JOB]);
  });

  it('propagates a handler failure so the offset is not committed', async () => {
    await expect(
      broker().handleMessage(encoded, async () => {
        throw new Error('dynamo unavailable');
      }),
    ).rejects.toThrow('dynamo unavailable');
  });

  it('drops a malformed payload instead of blocking the partition', async () => {
    // A payload that cannot parse will never parse, so redelivering it forever
    // would stall every later message on the partition.
    await expect(
      broker().handleMessage(Buffer.from('{not json'), async () => {
        throw new Error('should not be called');
      }),
    ).resolves.toBeUndefined();
  });

  it('ignores a tombstone', async () => {
    await expect(
      broker().handleMessage(null, async () => {
        throw new Error('should not be called');
      }),
    ).resolves.toBeUndefined();
  });
});
