import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import { afterEach, describe, expect, it } from 'vitest';
import { HttpDeliveryClient } from '../src/delivery/client.js';
import { createGuardedLookup } from '../src/core/url-guard.js';
import { ATTEMPT_HEADER, DELIVERY_HEADER, SIGNATURE_HEADER, verifySignature } from '../src/core/signature.js';
import type { DeliveryRequest } from '../src/ports.js';

interface Received {
  url: string;
  headers: IncomingMessage['headers'];
  body: string;
}

interface Receiver {
  port: number;
  received: Received[];
  close: () => Promise<void>;
}

const servers: Server[] = [];

async function startReceiver(
  handler: (request: IncomingMessage, response: ServerResponse, body: string) => void,
  body = true,
): Promise<Receiver> {
  const received: Received[] = [];
  const server = createServer((request, response) => {
    if (!body) {
      handler(request, response, '');
      return;
    }
    let data = '';
    request.setEncoding('utf8');
    request.on('data', (chunk: string) => {
      data += chunk;
    });
    request.on('end', () => {
      received.push({ url: request.url ?? '', headers: request.headers, body: data });
      handler(request, response, data);
    });
  });
  servers.push(server);

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;

  return {
    port,
    received,
    close: () =>
      new Promise<void>((resolve) => {
        server.closeAllConnections();
        server.close(() => resolve());
      }),
  };
}

afterEach(async () => {
  await Promise.all(servers.splice(0).map((server) => new Promise<void>((resolve) => {
    server.closeAllConnections();
    server.close(() => resolve());
  })));
});

function request(url: string, overrides: Partial<DeliveryRequest> = {}): DeliveryRequest {
  return {
    deliveryId: 'delivery-1',
    url,
    secret: '[redacted]',
    payload: { hello: 'world' },
    attempt: 2,
    timeoutMs: 2000,
    ...overrides,
  };
}

describe('HttpDeliveryClient', () => {
  it('refuses a reserved IP literal without opening a socket', async () => {
    const receiver = await startReceiver((_request, response) => {
      response.writeHead(200).end('should not happen');
    });

    const client = new HttpDeliveryClient();
    const result = await client.send(request(`http://127.0.0.1:${receiver.port}/hook`));

    expect(result.ok).toBe(false);
    expect(result.statusCode).toBeNull();
    expect(result.permanent).toBe(true);
    expect(result.error).toMatch(/private or reserved/);
    expect(receiver.received).toHaveLength(0);
  });

  it('refuses a hostname that resolves into the private network (rebinding)', async () => {
    const receiver = await startReceiver((_request, response) => {
      response.writeHead(200).end('should not happen');
    });

    // Registration would pass for this name: it is not an IP literal and it
    // looks public. Only the connect-time lookup can catch it.
    const lookup = createGuardedLookup(
      {},
      ((_hostname: string, _options: unknown, callback: (...args: unknown[]) => void) => {
        callback(null, '127.0.0.1', 4);
      }) as never,
    );
    const client = new HttpDeliveryClient({ lookup });
    const result = await client.send(request(`http://rebind.test:${receiver.port}/hook`));

    expect(result.ok).toBe(false);
    expect(result.permanent).toBe(true);
    expect(result.error).toMatch(/private or reserved/);
    expect(receiver.received).toHaveLength(0);
  });

  it('delivers to a permitted target and signs the exact bytes sent', async () => {
    const receiver = await startReceiver((_request, response) => {
      response.writeHead(204).end();
    });

    const client = new HttpDeliveryClient({ allowPrivateEndpoints: true });
    const result = await client.send(
      request(`http://127.0.0.1:${receiver.port}/hook?tenant=acme`),
    );

    expect(result).toMatchObject({ ok: true, statusCode: 204, error: null });
    expect(receiver.received).toHaveLength(1);

    const delivered = receiver.received[0];
    expect(delivered.url).toBe('/hook?tenant=acme');
    expect(delivered.headers['content-type']).toBe('application/json');
    expect(delivered.headers[DELIVERY_HEADER]).toBe('delivery-1');
    expect(delivered.headers[ATTEMPT_HEADER]).toBe('2');
    expect(delivered.body).toBe(JSON.stringify({ hello: 'world' }));

    expect(
      verifySignature({
        secret: '[redacted]',
        header: String(delivered.headers[SIGNATURE_HEADER]),
        body: delivered.body,
      }),
    ).toBe(true);
  });

  it('reports non-2xx as a retryable failure', async () => {
    const receiver = await startReceiver((_request, response) => {
      response.writeHead(500).end('nope');
    });

    const client = new HttpDeliveryClient({ allowPrivateEndpoints: true });
    const result = await client.send(request(`http://127.0.0.1:${receiver.port}/hook`));

    expect(result).toMatchObject({ ok: false, statusCode: 500, error: null });
    expect(result.permanent).toBeUndefined();
  });

  it('does not follow redirects', async () => {
    const target = await startReceiver((_request, response) => {
      response.writeHead(200).end('internal');
    });
    const receiver = await startReceiver((_request, response) => {
      response
        .writeHead(302, { location: `http://127.0.0.1:${target.port}/internal` })
        .end();
    });

    const client = new HttpDeliveryClient({ allowPrivateEndpoints: true });
    const result = await client.send(request(`http://127.0.0.1:${receiver.port}/hook`));

    expect(result).toMatchObject({ ok: false, statusCode: 302 });
    expect(target.received).toHaveLength(0);
  });

  it('gives up on a receiver that never answers', async () => {
    const receiver = await startReceiver(() => undefined, false);

    const client = new HttpDeliveryClient({ allowPrivateEndpoints: true });
    const result = await client.send(
      request(`http://127.0.0.1:${receiver.port}/hook`, { timeoutMs: 150 }),
    );

    expect(result.ok).toBe(false);
    expect(result.statusCode).toBeNull();
    expect(result.error).toMatch(/timed out after 150ms/);
    expect(result.permanent).toBeUndefined();
  });
});
