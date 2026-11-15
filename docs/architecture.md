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

## Delivery targets are untrusted input

An endpoint URL is caller-supplied, and the worker dials it from inside the
network. That is an SSRF primitive: without a guard, a registered endpoint can
reach the cloud metadata service, Redis, or any internal admin surface the
worker can route to.

The guard is split in two because either half alone is insufficient:

- **At registration** (`assertDeliverableUrl`) the URL is checked statically:
  http(s) only, no embedded credentials, and no IP literal in a private,
  loopback, link-local, CGNAT, multicast or reserved range. No DNS is
  performed, so registration stays fast and works offline. This catches the
  obvious cases and gives the caller a `400` they can act on.
- **At connect time** (`createGuardedLookup`) the address returned by
  `dns.lookup` is validated and then handed back to the socket. Because the
  hook runs inside the connect path, the address that was checked is the
  address that is dialled; there is no window for a second DNS answer. This is
  what stops rebinding — a name that looked public at registration and now
  resolves to `127.0.0.1`.

The delivery client uses `node:http(s).request` rather than `fetch` for exactly
this hook. It also never follows redirects: a `3xx` is a non-2xx attempt, so a
receiver cannot bounce the worker to an address the guard never saw.

IPv4-mapped (`::ffff:127.0.0.1`), NAT64 (`64:ff9b::/96`) and 6to4 (`2002::/16`)
addresses are unwrapped and judged by their embedded IPv4 address — otherwise
the IPv6 form would be a way around the IPv4 rules.

A blocked target is a `permanent` failure: the dispatcher dead-letters it on
the first attempt instead of spending the retry budget on an answer that
cannot change. `ALLOW_PRIVATE_ENDPOINTS=true` reopens the private space for
the local demo, where deliveries intentionally target Docker/k3s service names.

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
6. **An egress allowlist** in place of `ALLOW_PRIVATE_ENDPOINTS`. The flag is
   all-or-nothing: it reopens the entire private space rather than naming the
   hosts a deployment is actually allowed to reach. The right answer is a
   network policy or a configured allowlist checked in the same lookup hook.
