/**
 * DynamoDB adapters, used identically against LocalStack and real AWS.
 *
 * Schema is deliberately boring: one item per endpoint, one per delivery,
 * partition key `id`. At this scale a Scan for endpoints is correct and cheap;
 * the README documents the access pattern and the GSI we would add first.
 */
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DeleteCommand,
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  ScanCommand,
} from '@aws-sdk/lib-dynamodb';
import type { Config } from '../config.js';
import type { DeliveryRepo, EndpointRepo } from '../ports.js';
import type { Delivery, Endpoint } from '../types.js';

export function createDocumentClient(config: Config): DynamoDBDocumentClient {
  const endpoint = config.AWS_ENDPOINT_URL;
  const client = new DynamoDBClient({
    region: config.AWS_REGION,
    ...(endpoint
      ? {
          endpoint,
          credentials: {
            accessKeyId: config.AWS_ACCESS_KEY_ID ?? 'test',
            secretAccessKey: config.AWS_SECRET_ACCESS_KEY ?? 'test',
          },
        }
      : {}),
  });

  return DynamoDBDocumentClient.from(client, {
    marshallOptions: { removeUndefinedValues: true },
  });
}

export class DynamoEndpointRepo implements EndpointRepo {
  constructor(
    private readonly client: DynamoDBDocumentClient,
    private readonly table: string,
  ) {}

  async create(endpoint: Endpoint): Promise<void> {
    await this.client.send(
      new PutCommand({
        TableName: this.table,
        Item: endpoint,
        ConditionExpression: 'attribute_not_exists(id)',
      }),
    );
  }

  async get(id: string): Promise<Endpoint | null> {
    const result = await this.client.send(
      new GetCommand({ TableName: this.table, Key: { id } }),
    );
    return (result.Item as Endpoint | undefined) ?? null;
  }

  async listAll(): Promise<Endpoint[]> {
    const endpoints: Endpoint[] = [];
    let ExclusiveStartKey: Record<string, unknown> | undefined;

    do {
      const page = await this.client.send(
        new ScanCommand({ TableName: this.table, ExclusiveStartKey }),
      );
      for (const item of page.Items ?? []) {
        endpoints.push(item as Endpoint);
      }
      ExclusiveStartKey = page.LastEvaluatedKey as Record<string, unknown> | undefined;
    } while (ExclusiveStartKey);

    return endpoints;
  }
}

export class DynamoDeliveryRepo implements DeliveryRepo {
  constructor(
    private readonly client: DynamoDBDocumentClient,
    private readonly table: string,
  ) {}

  async create(delivery: Delivery): Promise<void> {
    await this.client.send(
      new PutCommand({
        TableName: this.table,
        Item: delivery,
        ConditionExpression: 'attribute_not_exists(id)',
      }),
    );
  }

  async get(id: string): Promise<Delivery | null> {
    const result = await this.client.send(
      new GetCommand({ TableName: this.table, Key: { id } }),
    );
    return (result.Item as Delivery | undefined) ?? null;
  }

  async update(delivery: Delivery): Promise<void> {
    await this.client.send(
      new PutCommand({ TableName: this.table, Item: delivery }),
    );
  }

  /** Only used by the test helper, never by the request path. */
  async delete(id: string): Promise<void> {
    await this.client.send(new DeleteCommand({ TableName: this.table, Key: { id } }));
  }
}
