/**
 * Kafka transport.
 *
 * Design notes:
 *  - the delivery id is the message key, so all attempts for one delivery land
 *    on the same partition and stay ordered
 *  - the topic is created explicitly with N partitions rather than relying on
 *    broker auto-creation
 *  - handlers catch their own failures (retries are scheduled in Redis, not by
 *    Kafka), so a poison message cannot stall the consumer group
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
        if (!message.value) return;
        let job: DeliveryJob;
        try {
          job = JSON.parse(message.value.toString()) as DeliveryJob;
        } catch (error) {
          this.logger.error(
            { err: error, partition, offset: message.offset },
            'malformed kafka message dropped',
          );
          return;
        }
        try {
          await handler(job);
        } catch (error) {
          this.logger.error(
            { err: error, deliveryId: job.deliveryId },
            'job handler threw; retry is already scheduled in redis',
          );
        }
      },
    });

    this.consumer = consumer;
  }

  async close(): Promise<void> {
    await this.consumer?.disconnect().catch(() => undefined);
    await this.producer?.disconnect().catch(() => undefined);
    this.consumer = null;
    this.producer = null;
  }
}
