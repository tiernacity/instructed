# Maybe later

Capabilities not in scope for v1 but that we want to keep visible
so v1 design choices don't accidentally preclude them. Each entry
records what the thing is, why it's deferred, and what the v1
design must keep compatible.

This is distinct from [`non-goals.md`](non-goals.md), which is
about things we deliberately *won't* do.

---

## ML-0001 — Concurrent partitioned subscription consumers

Allow a single named subscription to be consumed by N workers in
parallel, with a partitioning function (typically over
`stream_id`) that preserves per-partition order while sacrificing
global order across partitions.

**Why deferred:** a single cursor advanced atomically with the
consumer's work is the simplest model that demonstrates the core
hypothesis, and is correct for any single-worker projection. The
two realisations both add real complexity (a side table of
per-shard offsets makes cursor advance non-atomic with projection
writes; N independent sibling subscriptions over disjoint shards
push shard management out of the contract and into the SDK).
Workloads that need this throughput today can split into multiple
named subscriptions manually.

**Forward-compat constraints on v1:**

- The `subscriptions` table identity is `(stream_id, name, shard)`
  with `shard = 0` reserved as the v1 default. A future shard
  dimension fits the existing schema without migration.
- `claim_subscription` / `advance_subscription` already accept a
  `p_options jsonb` argument; future shard-aware variants can
  grow keys without breaking v1 callers.
- The conformance harness has `test.skip` placeholders for
  `INV-SUB-P-040..042` so the partitioned-consumer cases can land
  without rewriting the unpartitioned tests.

---

## ML-0002 — `LISTEN`/`NOTIFY` wake-up optimisation

On commit, `append_to_stream` could emit `pg_notify` on a channel
keyed by subscription scope. SDK worker loops would `LISTEN` and
wake immediately rather than waiting for the next poll tick.

**Why deferred:** notifications are not durable. They must be a
strict optimisation on top of polling, never the primary
delivery mechanism. Adding the path now means two code paths to
keep consistent; deferring it leaves a simpler operational
surface.

**Forward-compat constraints on v1:**

- `append_to_stream` is free to add a `pg_notify` call at the end
  of its transaction without changing its return type or error
  contract.
- The polling-based worker loop must remain the primary
  correctness mechanism. Adding `LISTEN` must be a pure
  optimisation: if every notification is dropped, the system
  must still make progress.
- Subscription identity (name + scope) must be stable enough to
  map cleanly onto a notification channel name.

---

## ML-0003 — Server-side selector evaluation

Allow `read_subscription_batch` to accept a `selector` key in its
`p_options` argument (a JSONB-path expression or SQL predicate
over `data` / `metadata` / `event_type`) and filter events
server-side before returning. Reduces bandwidth for sparse
selectors at the cost of restricting the selector vocabulary to
the server's predicate language.

**Why deferred:** v1 ships SDK-side selectors only. The SDK reads
a batch, runs the application's predicate locally, calls the
handler only on matches, and advances the cursor to the last
*fetched* event_number. SDK-side is the simplest and most
expressive option — the predicate is arbitrary application code.

**Forward-compat constraints on v1:**

- `read_subscription_batch.p_options` is documented to accept a
  future `selector` key. Adding it MUST NOT change the v1
  default semantics (no key → all events returned).
- The SDK's arbitrary-predicate selector stays for use cases the
  server-evaluable vocabulary doesn't cover; the two are
  composable.
- Cursor-advance semantics don't change: the highest delivered-
  or-skipped event_number is what's acked, whether the skip
  happened server-side or client-side.

---

## ML-0004 — `bindToConnection` for caller-supplied transaction sharing

An SDK-level helper that lets a caller share their own open
Postgres transaction with `instructed` calls. Two distinct use
cases:

- **Dispatcher side:** `app.bindToConnection(myTx).dispatch(...)`
  runs the aggregate's load-execute-append cycle inside the
  caller's transaction, so command dispatch commits atomically
  with the caller's own application writes.
- **Projection side:** an opt-in `coTransactional: true` flag
  for projections that target the same Postgres database the
  event store lives in — the handler runs inside a transaction
  that also performs `advance_subscription`, re-enabling
  exactly-once consistency between projection write and cursor
  advance for that narrow case.

**Why deferred:**

- The layered API in v1 already lets advanced callers compose
  their own load-execute-append against `Client.appendToStream`
  inside a caller-owned transaction. ML-0004 is the ergonomic
  wrapper, not a new capability.
- The projection-side opt-in needs a per-registration plumbing
  story (which connection? owned by whom? what about lease loss
  mid-handler?) we shouldn't design speculatively.
- Purely additive on the facade and the layered helpers — no v1
  signature changes required.

**Forward-compat constraints on v1:**

- `Client` already accepts a `Queryable` (`pg.Pool`,
  `pg.Client`, or `pg.PoolClient`), so the underlying primitive
  is in place.
- The facade's registration options shape must be free to grow
  a `coTransactional?: boolean` flag without breaking v1
  callers.
- The PM dispatch path uses a separate connection per
  [D-0011](decisions.md#d-0011) / [D-0012](decisions.md#d-0012)
  lock-set disjointness; ML-0004 will not cover PMs in a first
  cut. Aggregates and projections only.

---

## ML-0005 — Coalesce `advance_subscription` for runs of ignored events

A PM (or any worker, but PMs feel it most because they're
typically subscribed to `$all`) currently ack's every event in
a fetched batch with its own `advance_subscription` round-trip:
the routed ones inside the persist-and-ack tx, the ignored ones
as standalone calls. A PM that routes 1-in-100 event types on a
busy `$all` pays 99 ignored-event round-trips per useful one.

The optimisation: walk the batch in order and accumulate a
"pending ignored run". On hitting a routed event (or the end
of the batch), flush the pending run as a single
`advance_subscription(last_ignored.event_number)`, then process
the routed event normally. Runs of N ignored events become one
round-trip instead of N.

**Why deferred:**

- v1 is single-active-worker per subscription with modest
  batch sizes; the constant factor is not currently a problem.
- The optimisation is purely SDK-side and additive; no SQL
  contract change is needed.
- We want to measure under the soak harness (TODO #3b) before
  optimising blindly.

**Forward-compat constraints on v1:**

- The semantics are identical to the per-event acks: cursor
  monotone, no state change for ignored events, redelivery on
  crash mid-run is absorbed by the route function returning
  "ignore" again. So no v1 invariant or SQL change is required
  to preserve compatibility.
- The optimisation belongs in the SDK's worker loop, not in
  the SQL surface. `advance_subscription` already accepts an
  arbitrary `position` so the underlying primitive is in
  place.

**Where it lives:** `sdks/typescript/src/process-manager.ts`
(and `subscription.ts` if projections want the same
treatment for SDK-side selectors that skip events).
