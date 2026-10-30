/**
 * Demo receiver. Not part of the product: it is the "customer endpoint" used by
 * scripts/demo.ts to prove retries, signatures and the dead state end to end.
 *
 * FAIL_FIRST=2 makes the first two attempts for each delivery id return 503.
 */
import Fastify from 'fastify';

const port = Number(process.env.SINK_PORT ?? 3100);
const failFirst = Number(process.env.FAIL_FIRST ?? 0);

interface Received {
  deliveryId: string;
  attempt: number;
  url: string;
  headers: Record<string, unknown>;
  raw: string;
  at: number;
}

const attempts = new Map<string, number>();
const received: Received[] = [];

const app = Fastify({ logger: false });

// Keep the exact bytes: signature verification depends on them.
app.addContentTypeParser('application/json', { parseAs: 'string' }, (_req, body, done) => {
  done(null, body);
});

function record(
  url: string,
  headers: Record<string, unknown>,
  raw: string,
): { deliveryId: string; attempt: number } {
  const deliveryId = String(headers['x-hookrelay-delivery'] ?? 'unknown');
  const attempt = (attempts.get(deliveryId) ?? 0) + 1;
  attempts.set(deliveryId, attempt);
  received.push({ deliveryId, attempt, url, headers, raw, at: Date.now() });
  return { deliveryId, attempt };
}

app.post('/hook', async (request, reply) => {
  const raw = typeof request.body === 'string' ? request.body : '';
  const { deliveryId, attempt } = record(
    '/hook',
    request.headers as Record<string, unknown>,
    raw,
  );

  if (attempt <= failFirst) {
    return reply.status(503).send({ error: 'simulated_failure', attempt });
  }
  return { ok: true, deliveryId, attempt };
});

app.post('/always-fail', async (request, reply) => {
  const raw = typeof request.body === 'string' ? request.body : '';
  record('/always-fail', request.headers as Record<string, unknown>, raw);
  return reply.status(500).send({ error: 'always_fails' });
});

app.post('/reset', async () => {
  attempts.clear();
  received.length = 0;
  return { ok: true };
});

app.get('/received', async () => ({ received }));
app.get('/healthz', async () => ({ status: 'ok' }));

await app.listen({ port, host: '0.0.0.0' });
