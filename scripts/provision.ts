/**
 * Idempotent provisioning of the AWS resources the service needs: an SQS queue
 * with a dead-letter queue + redrive policy, and two DynamoDB tables.
 *
 * Works unchanged against Moto, LocalStack, or real AWS — only AWS_ENDPOINT_URL
 * differs (unset means real AWS). Terraform in ./terraform describes the same
 * resources for a real account; this script exists so the local stack needs no
 * cloud credentials.
 */
import {
  CreateQueueCommand,
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import {
  CreateTableCommand,
  DescribeTableCommand,
  DynamoDBClient,
} from '@aws-sdk/client-dynamodb';
import { loadConfig } from '../src/config.js';

const config = loadConfig();

const clientOptions = {
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
};

const sqs = new SQSClient(clientOptions);
const dynamo = new DynamoDBClient(clientOptions);

async function ensureQueue(
  name: string,
  attributes: Record<string, string> = {},
): Promise<{ url: string; arn: string }> {
  try {
    const existing = await sqs.send(new GetQueueUrlCommand({ QueueName: name }));
    if (existing.QueueUrl) {
      const attrs = await sqs.send(
        new GetQueueAttributesCommand({
          QueueUrl: existing.QueueUrl,
          AttributeNames: ['QueueArn'],
        }),
      );
      return { url: existing.QueueUrl, arn: attrs.Attributes?.QueueArn ?? '' };
    }
  } catch {
    // QueueNotFound -> create below
  }

  const created = await sqs.send(
    new CreateQueueCommand({ QueueName: name, Attributes: attributes }),
  );
  if (!created.QueueUrl) throw new Error(`could not create queue ${name}`);

  const attrs = await sqs.send(
    new GetQueueAttributesCommand({
      QueueUrl: created.QueueUrl,
      AttributeNames: ['QueueArn'],
    }),
  );
  return { url: created.QueueUrl, arn: attrs.Attributes?.QueueArn ?? '' };
}

async function ensureTable(name: string): Promise<void> {
  try {
    await dynamo.send(new DescribeTableCommand({ TableName: name }));
    return;
  } catch {
    // ResourceNotFound -> create below
  }

  await dynamo.send(
    new CreateTableCommand({
      TableName: name,
      AttributeDefinitions: [{ AttributeName: 'id', AttributeType: 'S' }],
      KeySchema: [{ AttributeName: 'id', KeyType: 'HASH' }],
      BillingMode: 'PAY_PER_REQUEST',
    }),
  );
}

async function main(): Promise<void> {
  const endpoint = config.AWS_ENDPOINT_URL ?? 'real aws';
  console.log(`provisioning against ${endpoint} (region ${config.AWS_REGION})`);

  const dlq = await ensureQueue('hookrelay-deliveries-dlq');
  console.log(`  dlq     ${dlq.url}`);

  // maxReceiveCount=5: after five failed receives SQS moves the message to the
  // DLQ. Application retries happen first, so this only catches poison pills.
  const queue = await ensureQueue('hookrelay-deliveries', {
    VisibilityTimeout: '30',
    RedrivePolicy: JSON.stringify({
      deadLetterTargetArn: dlq.arn,
      maxReceiveCount: '5',
    }),
  });
  console.log(`  queue   ${queue.url}`);

  await ensureTable(config.ENDPOINTS_TABLE);
  await ensureTable(config.DELIVERIES_TABLE);
  console.log(`  tables  ${config.ENDPOINTS_TABLE}, ${config.DELIVERIES_TABLE}`);
  console.log('done');
}

main().catch((error) => {
  console.error('provisioning failed:', error instanceof Error ? error.message : error);
  process.exit(1);
});
