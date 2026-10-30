# Architecture

## The problem

An HTTP endpoint that must receive events has three failure modes: it is down,
it is slow, or it is flaky. A producer that calls it directly inherits all
three. hookrelay sits between them and owns the retry decision.

```
                 ┌──────────────┐
   producer ───▶ │  API (HTTP)  │  idempotency key, fan-out, persistence
                 └──────┬───────┘
                        │ publish DeliveryJob
              ┌─────────┴──────────┐
              ▼                    ▼
        ┌──────────┐        ┌────────────┐
        │  Kafka   │        │  AWS SQS   │   (BROKER env selects one)
        └────┬─────┘        └─────┬──────┘
             └─────────┬──────────┘
                       ▼
                 ┌─────────────┐
                 │   Worker    │  lease → rate limit → POST → record
                 └──────┬──────┘
                        │ status / failure
                 ┌──────▼──────┐        ┌──────────────┐
                 │  DynamoDB   │        │    Redis     │
                 │  state      │        │  locks, rate │
                 └─────────────┘        │  limit, retry│
                                        │  schedule    │
                                        └──────────────┘
```

## Why the broker is pluggable

Kafka and SQS fail differently, and the differences are the interesting part:

- **Kafka** gives ordering within a partition and consumer-group rebalancing.
  We key messages by delivery id so every attempt for one delivery is ordered.
- **SQS** gives at-least-once delivery, a visibility timeout and a native
  dead-letter queue, but no ordering. We lean on the redrive policy instead.

Because retries are scheduled in Redis (per-message due time) rather than by
redelivering a whole topic, the two transports need identical application logic.
The broker only carries the "attempt this delivery now" signal.

## Why not exactly-once

Exactly-once delivery to an arbitrary HTTP endpoint is not achievable: the
receiver can process the request and die before responding, and no amount of
protocol work on our side detects that. So the design is:

- **at-least-once delivery**, made safe for receivers to consume
- a stable `X-Hookrelay-Delivery` id on every attempt, so receivers deduplicate
- attempt numbers in `X-Hookrelay-Attempt`, so receivers can spot retries

This is stated plainly instead of being papered over with a retry that only
looks idempotent.

## Concurrency control

Three separate mechanisms, each doing one job:

1. **Delivery lease** (`SET lock:delivery:<id> NX EX 30`) — stops a Kafka
   rebalance or an SQS visibility expiry from producing two concurrent sends of
   the same delivery.
2. **Sliding-window rate limit** per endpoint (sorted set + Lua, atomic) —
   protects a struggling receiver from the retry storm it caused. A rate-limited
   delivery is re-queued *without consuming an attempt*: being throttled by us
   is not the endpoint's failure.
3. **Atomic scheduler pop** (`ZRANGEBYSCORE` + `ZREM` in one script) — two
   workers cannot both claim the same due delivery.

## Backoff

Exponential with **full jitter**: `delay = rand(1, min(max, base * 2^(n-1)))`.
Full jitter is what stops a fleet of workers from retrying the same failing
endpoint in lockstep after an outage — the herd arrives spread out instead of
all at once.

## State model

```
queued ──▶ delivered        (2xx, terminal)
  │
  ├────▶ queued             (non-2xx / network error, attempts < max)
  ├────▶ dead               (attempts == max, terminal)
  └────▶ dead               (endpoint deleted or deactivated)
```

`queued` after a failure carries `nextAttemptAt`, so the scheduler is
reconstructible from DynamoDB alone if Redis loses the schedule set.

## Idempotency at the edge

`POST /v1/events` requires an `Idempotency-Key`. The first request takes the key
and stores the response; a replay returns that same response with `200` instead
of `202`, and creates no new deliveries. A concurrent request for a key still in
flight gets `409`, not a duplicate.

Crucially, a *failed* request deletes the key, so the caller can legitimately
retry rather than being locked out by their own failed attempt.

## Storage choices

- **DynamoDB** for endpoints and deliveries: single-digit-millisecond point
  reads on a natural key (`id`), pay-per-request billing, no server to run.
  A `Scan` fetches endpoints on fan-out — correct at this scale, and the first
  thing to replace with a GSI when the endpoint count grows.
- **Redis** for everything ephemeral: locks, rate limits, the retry schedule.
  If Redis is wiped, in-flight retries stall but the delivery state survives in
  DynamoDB.

## What I would change next

1. **Fan-out via a GSI on event type** instead of scanning every endpoint.
2. **Batch writes** in the fan-out path (`BatchWriteItem`) once an event targets
   many endpoints.
3. **Per-tenant queue isolation** for noisy neighbours.
4. **Circuit breaker** per endpoint: after N consecutive failures, stop
   attempting and fast-fail to `dead` with a reason, instead of burning the
   full retry budget on a host that is clearly down.
5. **Signature rotation** — the verifier already accepts multiple `v1=`
   signatures, so the receiver side of rotation exists; the API side (issuing a
   second secret) does not yet.
