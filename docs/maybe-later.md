# Maybe later

Ideas and capabilities that are explicitly *not* in scope for v1, but
which we want to keep visible so that v1 design choices do not
accidentally preclude them. Each entry records what the thing is, why
it is deferred, and what about the current design must remain
compatible with adding it later.

This is distinct from [`non-goals.md`](non-goals.md) (created in
Phase 5): non-goals are things we are deliberately *not* going to do.
Maybe-laters are things we likely *will* do, just not now.

---

## ML-0003 — Server-side selector evaluation for persistent subscriptions

**Deferred at:** Phase 8 SDK design (OQ-0003 resolution).

**What:** allow `read_subscription_batch` to accept a `selector`
key in its `p_options jsonb` argument (a JSONB-path expression or
a SQL predicate fragment over `data` / `metadata` / `event_type`)
and filter events server-side before returning. Reduces bandwidth
for sparse selectors (1–5% match rate) at the cost of restricting
the selector vocabulary to whatever the server's predicate
language supports.

**Why deferred:**

- v1 ships SDK-side selectors only: the SDK reads a batch, runs
  the application's predicate locally, calls the handler only on
  matches, and advances the cursor to the last *fetched*
  event_number (INV-SUB-P-050). This is the simplest and most
  expressive option — the predicate is arbitrary application
  code.
- Adding server-side filtering is purely additive once we know
  what vocabulary the workloads actually need. The bank-account
  example (Phase 8 done-criterion) is unlikely to stress
  bandwidth at all; a real test case would be a single subscription
  feeding many handlers each interested in a small slice of
  event types.

**Forward compatibility constraints:**

- `read_subscription_batch.p_options` is already documented to
  accept a future `selector` key. Adding it MUST NOT change the
  v1 default semantics (no selector key → all events returned).
- The SDK's `selector?: (e) => boolean` field on the subscription
  worker stays for arbitrary predicates; a future API addition
  accepts a server-evaluable predicate alongside it. The two are
  not mutually exclusive (a workload could pre-filter
  server-side and then refine SDK-side).
- Cursor-advance semantics do not change: the highest delivered-
  or-skipped event_number is what the SDK passes to
  `advance_subscription`, whether the skip happened server-side
  or client-side.

---

## ML-0002 — Push via `LISTEN`/`NOTIFY` as a latency optimisation

**Deferred at:** D-0003.

**What:** when `append_to_stream` commits, emit `pg_notify` on a
channel keyed by subscription scope (or by stream, or by a fixed
"appended" channel). SDK worker loops `LISTEN` on the relevant channel
and wake immediately rather than waiting for the next poll tick.

**Why deferred:**

- Adds a second code path that must be kept consistent with polling
  (workers still need polling for catch-up, restart, missed
  notifications). Two paths means two sets of bugs.
- Notifications are not durable. They have to be a strict optimisation
  on top of polling, never the primary delivery mechanism.
- Channel naming, payload size limits (8000 bytes), and the per-session
  `LISTEN` connection requirement are real operational complications
  worth deferring until we have a workload that demonstrates the need.

**Forward compatibility constraints:**

- `append_to_stream` must be free to add a `pg_notify` call at the end
  of its transaction without changing its return type or error
  contract.
- The polling-based worker loop must be the *primary* correctness
  mechanism. Adding `LISTEN` must be a pure wake-up optimisation: if
  every notification is dropped, the system must still make progress.
- Subscription identity (name + scope) must be stable enough to map
  cleanly onto a notification channel name.

---

## ML-0001 — Concurrent partitioned subscription consumers (`partition_by`)

**Deferred at:** D-0002.

**What:** allow a single named subscription to be consumed by N
workers in parallel, with a partitioning function (typically over
`stream_id`) that preserves per-partition order while sacrificing
global order. Matches Commanded's `concurrency` + `partition_by`
options on event handlers.

**Why deferred:**

- A single cursor advanced atomically with projection writes is the
  simplest model that demonstrates the core hypothesis, and is correct
  for any single-worker projection.
- The two realisations both add real complexity:
  - **Side table of per-shard offsets**: cursor advance is no longer a
    single row update; the projection's atomicity story gets harder.
  - **N independent sibling subscriptions over disjoint shards**:
    pushes shard management out of the contract and into the SDK or
    application, and complicates restart/rebalance.
- Workloads that need this throughput today can split into multiple
  named subscriptions manually.

**Forward compatibility constraints:**

- The `subscriptions` table schema should not bake in an assumption
  that `(name)` is the full identity of a cursor. A future shard
  dimension may need to be added; reserve naming room (e.g.
  `(name, shard)` with `shard = 0` as the v1 default) or be prepared
  to migrate.
- `claim_subscription` / `advance_subscription` signatures should be
  free to grow a shard parameter without breaking the v1 caller
  shape (consider an options object / jsonb param from the start, as
  absurd does).
- The conformance harness in Phase 9 should be designed so that
  partitioned-consumer tests can be added later without rewriting the
  unpartitioned tests.
