/**
 * AWS SQS transport (LocalStack-compatible).
 *
 * SQS gives us at-least-once delivery and a visibility timeout, not ordering.
 * A failed handler leaves the message undeleted; the queue's redrive policy is
 * the last-resort DLQ after N receives. Application-level retries with backoff
 * still happen first, scheduled in Redis, so SQS redrive only catches poison
 * messages.
 */
import {
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SQSClient,
  SendMessageCommand,
} from '@aws-sdk/client-sqs';
import type { Config } from '../config.js';
import type { Logger } from '../logger.js';
import type { Broker, JobHandler } from '../ports.js';
import type { DeliveryJob } from '../types.js';

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export class SqsBroker implements Broker {
  private readonly client: SQSClient;
  private readonly queueUrl: string;
  private running = false;
  private loopPromise: Promise<void> | null = null;
  private readonly abort = new AbortController();

  constructor(
    private readonly config: Config,
    private readonly logger: Logger,
  ) {
    if (!config.SQS_QUEUE_URL) throw new Error('SQS_QUEUE_URL is required');
    this.queueUrl = config.SQS_QUEUE_URL;
    this.client = new SQSClient({
      region: config.AWS_REGION,
      ...(config.AWS_ENDPOINT_URL
        ? {
            endpoint: config.AWS_ENDPOINT_URL,
            credentials: {
              accessKeyId: config.AWS_ACCESS_KEY_ID ?? 'test',
              secretAccessKey: config.AWS_SECRET_ACCESS_KEY ?? 'test',
            },
          }
        : {}),
    });
  }

  async publish(job: DeliveryJob): Promise<void> {
    await this.client.send(
      new SendMessageCommand({
        QueueUrl: this.queueUrl,
        MessageBody: JSON.stringify(job),
        MessageAttributes: {
          deliveryId: { DataType: 'String', StringValue: job.deliveryId },
        },
      }),
    );
  }

  /** Starts the long-poll loop in the background, mirroring kafkajs.run(). */
  async subscribe(handler: JobHandler): Promise<void> {
    if (this.running) throw new Error('SqsBroker.subscribe called twice');
    this.running = true;
    this.loopPromise = this.poll(handler);
  }

  private async poll(handler: JobHandler): Promise<void> {
    while (this.running) {
      let messages;
      try {
        const response = await this.client.send(
          new ReceiveMessageCommand({
            QueueUrl: this.queueUrl,
            MaxNumberOfMessages: 10,
            WaitTimeSeconds: 20,
            VisibilityTimeout: 30,
            MessageAttributeNames: ['All'],
          }),
          { abortSignal: this.abort.signal },
        );
        messages = response.Messages ?? [];
      } catch (error) {
        if (!this.running) break;
        this.logger.error({ err: error }, 'sqs receive failed; retrying in 1s');
        await sleep(1000);
        continue;
      }

      for (const message of messages) {
        if (!message.Body || !message.ReceiptHandle) continue;

        let job: DeliveryJob;
        try {
          job = JSON.parse(message.Body) as DeliveryJob;
        } catch (error) {
          this.logger.error({ err: error }, 'malformed sqs message left for redrive');
          continue;
        }

        try {
          await handler(job);
          await this.client.send(
            new DeleteMessageCommand({
              QueueUrl: this.queueUrl,
              ReceiptHandle: message.ReceiptHandle,
            }),
          );
        } catch (error) {
          // Not deleted: visibility timeout returns it to the queue and the
          // redrive policy eventually moves it to the dead-letter queue.
          this.logger.error(
            { err: error, deliveryId: job.deliveryId },
            'job handler failed; message left for redrive',
          );
        }
      }
    }
  }

  async close(): Promise<void> {
    this.running = false;
    this.abort.abort();
    this.client.destroy();
    await this.loopPromise?.catch(() => undefined);
    this.loopPromise = null;
  }
}
