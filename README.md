# hookrelay

Reliable webhook delivery with a pluggable transport: **Kafka or AWS SQS**,
DynamoDB for state, Redis for locks, rate limits and retry scheduling. Runs
entirely on your machine for free — no cloud account, no credentials, no spend.

The reason this exists: calling a customer's HTTP endpoint directly from your
application means inheriting every one of their outages. hookrelay owns the
retry decision instead of leaving it to the caller.

```
producer ──▶ API ──▶ Kafka | SQS ──▶ worker ──▶ your endpoint
                  │                     │
                  └── DynamoDB ─────────┘   (Redis: locks, rate limit, retries)
```

## What it actually does

- **Fan-out**: one event, one delivery per subscribed endpoint (`*` or exact type).
- **At-least-once delivery** with a stable `X-Hookrelay-Delivery` id so receivers
  can deduplicate. Exactly-once is not promised — see
  [docs/architecture.md](docs/architecture.md#why-not-exactly-once).
- **Exponential backoff with full jitter**, so a fleet of workers does not
  retry a struggling endpoint in lockstep.
- **Dead-lettering** after `MAX_ATTEMPTS`, with the last status code and error
  preserved on the delivery record.
- **Per-endpoint sliding-window rate limiting**. A throttled delivery is
  re-queued without consuming an attempt.
- **HMAC-SHA256 request signing** (Stripe-style `t=<ts>,v1=<sig>`), with replay
  protection and multi-signature support for secret rotation.
- **Idempotent ingestion** via a caller-supplied `Idempotency-Key`.
- **Dead-letter replay** through the API.

## Quick start

Requires Docker. Nothing else.

```bash
docker compose up -d --wait      # redis, kafka, moto (AWS mock), api, worker, sink
pnpm install                     # only to run the demo script
pnpm demo
```

Or run it on the SQS transport instead of Kafka:

```bash
BROKER=sqs docker compose up -d --wait
pnpm demo
```

The demo is not a mock test — it drives the real API and asserts on the results
it reads back:

```
3. Wait for the flaky endpoint to succeed after its retries
  ✓ flaky delivery reached `delivered`
  attempts=3 lastStatus=200 status=delivered

4. Wait for the always-failing endpoint to reach `dead`
  ✓ doomed delivery reached `dead`
  attempts=3 lastStatus=500 error=HTTP 500

5. Verify signatures on the bytes the receiver actually got
  ✓ 3 received request(s) verified against the endpoint secret
```

The sink service is configured with `FAIL_FIRST=2`, so success on attempt 3 is
only reachable through the retry path. The second endpoint always returns 500
and must reach `dead`.

## Running on Kubernetes (locally, free)

The manifests in `k8s/` are the real ones — same probes, HPAs, PDBs and security
contexts you would deploy. They are backed by the same Moto mock instead of a
real AWS account.

```bash
k3d cluster create hookrelay --wait
k3d image import hookrelay:local -c hookrelay     # build it first, see Dockerfile
kubectl apply -f k8s/00-namespace.yaml -f k8s/local/00-prereqs.yaml \
              -f k8s/05-redis.yaml -f k8s/10-api.yaml -f k8s/20-worker.yaml

# provision SQS + DynamoDB inside the cluster's moto, then exercise it
kubectl port-forward -n hookrelay svc/moto 5000:5000 &
AWS_ENDPOINT_URL=http://localhost:5000 AWS_REGION=us-east-1 \
  AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test pnpm provision

kubectl port-forward -n hookrelay svc/hookrelay-api 3010:80 &
kubectl port-forward -n hookrelay svc/hookrelay-sink 3110:3100 &
ENDPOINT_BASE=http://hookrelay-sink:3100 pnpm demo
```

`k3d image import` cannot load multi-arch images into containerd, so `redis` and
`moto` are pulled by the cluster from the registry rather than imported.

## API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/endpoints` | Register an endpoint. Secret returned **once**. |
| `GET` | `/v1/endpoints/:id` | Read an endpoint (never includes the secret). |
| `POST` | `/v1/events` | Ingest an event. `Idempotency-Key` header required. |
| `GET` | `/v1/deliveries/:id` | Delivery state, attempts, last error. |
| `POST` | `/v1/deliveries/:id/replay` | Re-queue a non-delivered delivery. |
| `GET` | `/healthz` | Liveness/readiness, including Redis. |

```bash
# register
curl -sX POST localhost:3010/v1/endpoints \
  -H 'content-type: application/json' \
  -d '{"url":"https://example.com/hook","eventTypes":["order.created"]}'

# publish
curl -sX POST localhost:3010/v1/events \
  -H 'content-type: application/json' \
  -H 'idempotency-key: order-1234-created' \
  -d '{"type":"order.created","data":{"orderId":"1234"}}'
```

## Verifying a delivery (receiver side)

Every attempt carries:

```
X-Hookrelay-Signature: t=1700000000,v1=<hex hmac-sha256 of "t.body">
X-Hookrelay-Timestamp: 1700000000
X-Hookrelay-Delivery:  <stable delivery id, for deduplication>
X-Hookrelay-Attempt:   <1-based attempt number>
```

Verify against the **raw request body**, before JSON parsing:

```ts
import { createHmac, timingSafeEqual } from 'node:crypto';

const [tPart, vPart] = header.split(',');
const timestamp = Number(tPart.slice(2));
const received = Buffer.from(vPart.slice(3), 'hex');
const expected = createHmac('sha256', secret).update(`${timestamp}.${rawBody}`).digest();

const fresh = Math.abs(Date.now() / 1000 - timestamp) <= 300;
const valid = fresh && received.length === expected.length
  && timingSafeEqual(received, expected);
```

## Configuration

All settings are environment variables, validated by Zod at startup —
misconfiguration fails fast instead of surfacing as a confusing runtime error.

| Variable | Default | Notes |
|---|---|---|
| `BROKER` | `kafka` | `kafka` or `sqs` |
| `REDIS_URL` | `redis://localhost:6379` | |
| `KAFKA_BROKERS` / `KAFKA_TOPIC` | `localhost:9092` / `hookrelay.deliveries` | |
| `SQS_QUEUE_URL` | — | required when `BROKER=sqs` |
| `AWS_ENDPOINT_URL` | — | `http://moto:5000` locally; unset for real AWS |
| `MAX_ATTEMPTS` | `5` | |
| `BASE_BACKOFF_MS` / `MAX_BACKOFF_MS` | `250` / `30000` | |
| `RATE_LIMIT_PER_SEC` | `20` | per endpoint |

## Layout

```
src/
  core/        backoff, signatures, dispatcher   ← the domain, no I/O
  ports.ts     interfaces the core depends on    ← hexagonal boundary
  store/       Redis + DynamoDB adapters
  broker/      Kafka + SQS adapters
  api/         Fastify routes
  worker/      broker consumer + retry scheduler
tests/         36 unit tests, all against in-memory fakes
scripts/       demo (e2e) and provisioning
terraform/     real-AWS SQS + DynamoDB + least-privilege IAM
k8s/           Deployments, HPAs, PDBs, probes, security contexts
```

The core (`src/core/`) imports nothing from `store/`, `broker/` or `api/`. That
is what lets the dispatcher's retry, dead-letter and rate-limit semantics be
tested in milliseconds without a single container.

## Verification status

What I ran, and what it showed:

- `pnpm typecheck` — clean.
- `pnpm test` — **36/36 passing**. Covers backoff bounds, signature
  verification (tampering, wrong secret, replay, rotation), dispatcher state
  transitions, lease behaviour, rate-limit not consuming attempts, API fan-out,
  idempotent replay, and secret redaction.
- `pnpm demo` against docker compose — **passes on both Kafka and SQS**.
  Delivered after retries, reached `dead` at `maxAttempts`, every received
  request's signature verified against the endpoint secret, idempotent replay
  returned the same `eventId`.
- `terraform init -backend=false && terraform validate` — valid. `plan`/`apply`
  were **not** run: that needs a real AWS account and would cost money.
- **Kubernetes: deployed and exercised in a real cluster** (k3d, k8s v1.30.4).
  All 7 pods reached `Running` with **0 restarts**; the API ran with 2 replicas
  and the worker with 2, and the full demo passed in-cluster on the SQS broker
  (delivered after retries, `dead` at `maxAttempts`, signatures verified,
  idempotent replay). The sink observed exactly 6 requests for 2 deliveries —
  no duplicates, which is the Redis delivery lease holding across pods. Both
  HPAs report live CPU metrics (`1%/70%`, `7%/80%`), so autoscaling is wired
  correctly; it was not driven to scale under sustained load.

## Known issues / honest gaps

- **`plan`/`apply` for Terraform is untested** — no AWS account exercised. IAM
  policies are written least-privilege but not proven against real API calls.
- **HPA behaviour under sustained load is untested.** Both autoscalers read live
  metrics and are wired correctly, but nothing was pushed hard enough to trigger
  a scale-out, so the scaling curve is unproven.
- **`k3d image import` cannot load multi-arch images** (`redis`, `moto`) into
  containerd — it fails with a content-digest error. The cluster pulls them from
  the registry instead. Not a defect in these manifests, but it will bite anyone
  following the local k3d recipe with a restricted registry.
- **A `Service` named after a tool can break its env config.** The `moto`
  Service injects `MOTO_PORT=tcp://…` into its own pods, and moto parses that as
  an int. Fixed by setting `MOTO_PORT` explicitly on the container; worth
  remembering before naming any Service after a config prefix.
- **kafkajs 2.2.4 emits `TimeoutNegativeWarning` on startup.** Traced to
  `RequestQueue.scheduleCheckPendingRequests` in kafkajs itself — an upstream
  bug where the computed delay goes negative. Harmless (Node clamps to 1 ms),
  and not from this codebase, but noisy.
- **Fan-out scans every endpoint.** Fine for tens, wrong for thousands; the fix
  is a GSI, tracked in [docs/architecture.md](docs/architecture.md).
- **No auth on the API itself.** Any caller who can reach `/v1/endpoints` can
  register one. This is a local/demo service; putting it behind an API key is
  the first thing to add before exposing it.

## License

MIT.
