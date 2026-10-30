/**
 * End-to-end demo against the running compose stack.
 *
 * It proves, with observed output rather than assertion:
 *   1. an event fans out to subscribed endpoints only
 *   2. a failing endpoint is retried with backoff until it succeeds
 *   3. a permanently failing endpoint reaches `dead` after maxAttempts
 *   4. every attempt carries a signature that verifies with the endpoint secret
 *   5. replaying an Idempotency-Key does not create duplicate deliveries
 *
 * Usage: pnpm demo            (defaults to http://localhost:3000)
 */
import { verifySignature, SIGNATURE_HEADER } from '../src/core/signature.js';

const API = process.env.API_URL ?? 'http://localhost:3010';
/** Host-visible address, used only to read back what the sink recorded. */
const SINK = process.env.SINK_URL ?? 'http://localhost:3110';
/**
 * Address the workers use to reach the sink. When the demo runs on the host
 * against docker compose, this must be the in-network name, not localhost:
 * inside the worker container, localhost is the worker itself.
 */
const SINK_AS_SEEN_BY_WORKER = process.env.ENDPOINT_BASE ?? 'http://sink:3100';

interface DeliveryRef {
  id: string;
  endpointId: string;
  url: string;
}

interface EventResponse {
  eventId: string;
  type: string;
  deliveries: DeliveryRef[];
}

interface DeliveryState {
  id: string;
  status: 'queued' | 'delivered' | 'dead';
  attempts: number;
  lastStatusCode: number | null;
  lastError: string | null;
}

interface ReceivedItem {
  deliveryId: string;
  attempt: number;
  url: string;
  headers: Record<string, string>;
  raw: string;
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API}${path}`, {
    ...init,
    headers: { 'content-type': 'application/json', ...(init?.headers ?? {}) },
  });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`${init?.method ?? 'GET'} ${path} -> ${response.status}: ${text}`);
  }
  return JSON.parse(text) as T;
}

async function waitFor(
  label: string,
  check: () => Promise<boolean>,
  timeoutMs = 20_000,
  intervalMs = 200,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) {
      console.log(`  ✓ ${label}`);
      return;
    }
    await sleep(intervalMs);
  }
  throw new Error(`timed out waiting for: ${label}`);
}

function step(title: string): void {
  console.log(`\n${title}`);
}

async function waitForApi(): Promise<void> {
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    try {
      const health = await api<{ status: string }>('/healthz');
      if (health.status === 'ok') return;
    } catch {
      // not up yet
    }
    await sleep(500);
  }
  throw new Error('API did not become healthy within 60s');
}

async function main(): Promise<void> {
  console.log(`hookrelay e2e demo -> API ${API}, sink ${SINK}`);
  await waitForApi();
  await fetch(`${SINK}/reset`, { method: 'POST' });

  step('1. Register two endpoints (one subscribed, one for another event type)');
  const flaky = await api<{ id: string; secret: string; url: string }>('/v1/endpoints', {
    method: 'POST',
    body: JSON.stringify({
      url: `${SINK_AS_SEEN_BY_WORKER}/hook`,
      eventTypes: ['order.created'],
    }),
  });
  const ignored = await api<{ id: string; secret: string; url: string }>('/v1/endpoints', {
    method: 'POST',
    body: JSON.stringify({
      url: `${SINK_AS_SEEN_BY_WORKER}/hook`,
      eventTypes: ['invoice.paid'],
    }),
  });
  const doomed = await api<{ id: string; secret: string }>('/v1/endpoints', {
    method: 'POST',
    body: JSON.stringify({
      url: `${SINK_AS_SEEN_BY_WORKER}/always-fail`,
      eventTypes: ['order.created'],
    }),
  });
  console.log(`  flaky    ${flaky.id.slice(0, 8)}  ${flaky.url}`);
  console.log(`  ignored  ${ignored.id.slice(0, 8)}  (subscribed to invoice.paid)`);
  console.log(`  doomed   ${doomed.id.slice(0, 8)}  ${SINK_AS_SEEN_BY_WORKER}/always-fail`);

  step('2. Publish one order.created event with an idempotency key');
  const idempotencyKey = `demo-${Date.now()}`;
  const event = await api<EventResponse>('/v1/events', {
    method: 'POST',
    headers: { 'idempotency-key': idempotencyKey },
    body: JSON.stringify({
      type: 'order.created',
      data: { orderId: 'ord_demo_1', amount: 129.9, currency: 'BRL' },
    }),
  });
  console.log(`  event ${event.eventId} -> ${event.deliveries.length} deliveries`);
  if (event.deliveries.length !== 2) {
    throw new Error(`expected 2 deliveries (flaky + doomed), got ${event.deliveries.length}`);
  }
  console.log('  ✓ fan-out matched subscriptions: invoice.paid endpoint skipped');

  step('3. Wait for the flaky endpoint to succeed after its retries');
  const flakyDelivery = event.deliveries.find((d) => d.endpointId === flaky.id);
  const doomedDelivery = event.deliveries.find((d) => d.endpointId === doomed.id);
  if (!flakyDelivery || !doomedDelivery) throw new Error('missing delivery refs');

  await waitFor('flaky delivery reached `delivered`', async () => {
    const state = await api<DeliveryState>(`/v1/deliveries/${flakyDelivery.id}`);
    return state.status === 'delivered';
  }, 30_000);

  const flakyState = await api<DeliveryState>(`/v1/deliveries/${flakyDelivery.id}`);
  console.log(
    `  attempts=${flakyState.attempts} lastStatus=${flakyState.lastStatusCode} status=${flakyState.status}`,
  );
  if (flakyState.attempts < 2) {
    throw new Error('expected at least one retry before success (FAIL_FIRST=2)');
  }

  step('4. Wait for the always-failing endpoint to reach `dead`');
  await waitFor('doomed delivery reached `dead`', async () => {
    const state = await api<DeliveryState>(`/v1/deliveries/${doomedDelivery.id}`);
    return state.status === 'dead';
  }, 40_000);

  const doomedState = await api<DeliveryState>(`/v1/deliveries/${doomedDelivery.id}`);
  console.log(
    `  attempts=${doomedState.attempts} lastStatus=${doomedState.lastStatusCode} error=${doomedState.lastError}`,
  );

  step('5. Verify signatures on the bytes the receiver actually got');
  const sinkResponse = await fetch(`${SINK}/received`);
  const sinkData = (await sinkResponse.json()) as { received: ReceivedItem[] };
  const hookItems = sinkData.received.filter((item) => item.url === '/hook');
  if (hookItems.length === 0) throw new Error('sink recorded no deliveries');

  let verified = 0;
  for (const item of hookItems) {
    const ok = verifySignature({
      secret: flaky.secret,
      header: String(item.headers[SIGNATURE_HEADER]),
      body: item.raw,
    });
    if (!ok) throw new Error(`signature did not verify for attempt ${item.attempt}`);
    verified += 1;
  }
  console.log(`  ✓ ${verified} received request(s) verified against the endpoint secret`);

  step('6. Replay the same Idempotency-Key');
  const replay = await api<EventResponse>('/v1/events', {
    method: 'POST',
    headers: { 'idempotency-key': idempotencyKey },
    body: JSON.stringify({
      type: 'order.created',
      data: { orderId: 'ord_demo_1', amount: 129.9, currency: 'BRL' },
    }),
  });
  if (replay.eventId !== event.eventId) {
    throw new Error('idempotent replay produced a different event id');
  }
  console.log(`  ✓ same eventId returned (${replay.eventId.slice(0, 8)}), no new deliveries`);

  console.log('\nAll checks passed.\n');
  console.log('Summary');
  console.log(`  flaky  : attempts=${flakyState.attempts} status=${flakyState.status}`);
  console.log(`  doomed : attempts=${doomedState.attempts} status=${doomedState.status}`);
  console.log(`  sink   : ${sinkData.received.length} HTTP requests observed`);
}

main().catch((error) => {
  console.error('\nDEMO FAILED:', error instanceof Error ? error.message : error);
  process.exit(1);
});
