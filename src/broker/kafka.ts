/**
 * Kafka transport.
 *
 * Design notes:
 *  - the delivery id is the message key, so all attempts for one delivery land
 *    on the same partition and stay ordered
 *  - the topic is created explicitly with N partitions rather than relying on
 *    broker auto-creation
 *  - a handler failure is *not* swallowed: the offset is left uncommitted so the
 *    message is redelivered, which is what the SQS adapter already does by not
 *    deleting it
 */
import { Kafka, logLevel, type Consumer, type Producer } from 'kafkajs';
import type { Config } from '../config.js';
import type { Logger } from '../logger.js';
import type { Broker, JobHandler } from '../ports.js';
import type { DeliveryJob } from '../types.js';

export class KafkaBroker implements Broker {
  private readonly kafka: Kafka;
  private producer: Producer | null = null;
  private consumer: Consumer | null = null;
  private topicReady = false;

  constructor(
    private readonly config: Config,
    private readonly logger: Logger,
  ) {
    this.kafka = new Kafka({
      clientId: 'hookrelay',
      brokers: config.KAFKA_BROKERS.split(',')
        .map((broker) => broker.trim())
        .filter(Boolean),
      logLevel: logLevel.NOTHING,
      retry: { retries: 8, initialRetryTime: 300 },
    });
  }

  private async ensureTopic(): Promise<void> {
    if (this.topicReady) return;
    const admin = this.kafka.admin();
    await admin.connect();
    try {
      await admin.createTopics({
        topics: [
          {
            topic: this.config.KAFKA_TOPIC,
            numPartitions: this.config.KAFKA_PARTITIONS,
            replicationFactor: 1,
          },
        ],
        waitForLeaders: true,
      });
    } catch (error) {
      // TopicAlreadyExists is expected on every start after the first.
      this.logger.debug({ err: error }, 'kafka topic already present');
    } finally {
      await admin.disconnect();
    }
    this.topicReady = true;
  }

  private async getProducer(): Promise<Producer> {
    if (!this.producer) {
      const producer = this.kafka.producer({ allowAutoTopicCreation: false });
      await producer.connect();
      await this.ensureTopic();
      this.producer = producer;
    }
    return this.producer;
  }

  async publish(job: DeliveryJob): Promise<void> {
    const producer = await this.getProducer();
    await producer.send({
      topic: this.config.KAFKA_TOPIC,
      messages: [
        {
          key: job.deliveryId,
          value: JSON.stringify(job),
          headers: { 'content-type': 'application/json' },
        },
      ],
    });
  }

  async subscribe(handler: JobHandler): Promise<void> {
    if (this.consumer) throw new Error('KafkaBroker.subscribe called twice');
    await this.ensureTopic();

    const consumer = this.kafka.consumer({
      groupId: this.config.KAFKA_GROUP_ID,
      sessionTimeout: 30_000,
    });
    await consumer.connect();
    await consumer.subscribe({ topic: this.config.KAFKA_TOPIC, fromBeginning: false });

    await consumer.run({
      eachMessage: async ({ message, partition }) => {
        try {
          await this.handleMessage(message.value, handler);
        } catch (error) {
          // Deliberately rethrown. kafkajs commits the offset after `eachMessage`
          // resolves, so swallowing the error here *acknowledges* a delivery that
          // was never sent — and the comment that used to sit here claimed a
          // Redis retry was already scheduled, which is only true if the handler
          // got far enough to schedule one. A Dynamo or Redis error inside
          // `Dispatcher.process` happens before that point.
          //
          // Throwing leaves the offset uncommitted, so the message is redelivered
          // — the same at-least-once behaviour the SQS adapter has by not
          // deleting it. The cost is that a permanently failing message blocks
          // its partition until an operator intervenes; SQS gets a redrive policy
          // for free, Kafka does not, so this is the honest trade rather than a
          // silent drop.
          this.logger.error(
            { err: error, partition, offset: message.offset },
            'job handler failed; leaving offset for redelivery',
          );
          throw error;
        }
      },
    });

    this.consumer = consumer;
  }

  /**
   * Parse and dispatch one message. Exposed for tests: a real Kafka is not
   * needed to check that a handler failure is propagated rather than swallowed.
   *
   * A malformed payload is dropped — it can never succeed on redelivery, so
   * retrying it would block the partition forever. A handler failure is
   * rethrown, because the message is valid and its delivery is still owed.
   */
  async handleMessage(value: Buffer | null, handler: JobHandler): Promise<void> {
    if (!value) return;

    let job: DeliveryJob;
    try {
      job = JSON.parse(value.toString()) as DeliveryJob;
    } catch (error) {
      this.logger.error({ err: error }, 'malformed kafka message dropped');
      return;
    }

    await handler(job);
  }

  async close(): Promise<void> {
    await this.consumer?.disconnect().catch(() => undefined);
    await this.producer?.disconnect().catch(() => undefined);
    this.consumer = null;
    this.producer = null;
  }
}
