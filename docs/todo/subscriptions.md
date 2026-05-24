# Subscriptions — outstanding work

**Origin:** Commanded re-review §2.5 (HND-050 / partitioned consumers)
plus the work-queue model that emerged from §2.4 (PM-B). What started
as a PM-specific question turned out to be a question about the shape
of every subscription in the system.

This file collects subscription-substrate work that is not
PM-specific. PM-only concerns (RouteFn shape, fan-out modelling,
handle/apply split, deterministic command IDs) live in
[`process-manager.md`](./process-manager.md).

Status legend: **Decided** / **Designed, awaiting confirmation** /
**Open**. Release relevance: **Pre-release** / **Post-release**.

---

## SUB-A — Routing-vs-processing architecture (the work-queue model)

**Status:** Open. User leans toward the decoupled router + work-queue
design (Design 3); decision deferred pending a dedicated design pass.
**Release-relevance:** Pre-release.

### The question

Today's model: each subscription has a single cursor (`last_seen`)
advanced by a single active worker. Works for strict-sequential
consumption with one worker.

It doesn't work for:

- **PM types with many active instances** (one stuck instance stalls
  the whole type via the shared cursor).
- **PM types that want throughput scaling** (one worker per type).
- **Projections that want concurrent processing** (a single cursor
  can't express "items N and M are both being processed; M
  finished, N hasn't").
- **Projections that want per-key ordering with cross-key parallelism**
  (the `partition_by` case).

These all reduce to the same underlying need: **per-partition
progress tracking**, where the partition key varies by use case.

### Framing (user, during §2.4 walkthrough)

A subscription has two distinct responsibilities, and the schema
should be organised around the distinction:

1. **Type-level routing.** Decide whether each event is of interest,
   and to which "partition" (PM instance, projection partition key,
   etc.).
2. **Per-partition processing.** Do the work the routing decision
   produced, in order within a partition, in parallel across
   partitions.

The corollary: workers should not need to poll *the whole event log
on behalf of every partition*. They should poll for **potential
work once per event** (at the routing layer) and then use the
database to fan out work to the partitions that actually need it.

### Three designs

#### Design 1 — one subscription row per partition

Each partition (PM instance, projection key, etc.) has its own row in
`subscriptions`. A type-level "discovery cursor" exists for `:start`
detection.

- **Pro:** symmetric with snapshots (one row per instance for each).
  No new schema beyond what we have.
- **Pro:** per-partition progress is automatic.
- **Pro:** worker scaling is straightforward — a worker pool grabs
  whatever leases are unclaimed.
- **Con — read amplification.** Each event in `$all` is read once
  per active partition whose cursor is behind it. Negligible at
  hundreds of partitions; ~10,000× at 10,000.
- **Con:** worker fan-out at the process level (one worker per
  partition, or pool of workers cycling through unclaimed leases).
- **Con:** still need a type-level discovery cursor for `:start`,
  so the design is "discovery cursor + per-partition cursors", not
  just "per-partition".

#### Design 2 — partitioned cursors (one subscription, per-partition cursors in a side table)

The ML-0001 design generalised. One subscription on `$all`, one
cursor that scans events once, a side table of per-partition
progress. Workers claim partitions, not subscriptions; one worker
can hold many partitions.

- **Pro:** no read amplification.
- **Pro:** per-partition progress automatic.
- **Pro:** worker scaling independent of partition count.
- **Pro:** unifies the partitioned-consumer story across PMs and
  projections.
- **Con:** more schema (per-partition cursor side table).
- **Con:** more coordination (partition leasing, partition
  rebalancing on worker join/leave).
- **Con:** more design work upfront.

#### Design 3 — decoupled router + work queue

A two-layer separation:

- **Routing worker.** One per subscription. Holds a subscription on
  the source stream (`$all` or named stream). Reads events
  sequentially, calls `RouteFn` per event, and:
  - On a routing decision: inserts a row into a new
    `subscription_work_items` table keyed
    `(subscription_id, partition_key, event_number)` with
    `state = 'pending'` and the work payload (kind, triggering
    event reference).
  - On `:ignore`: no insert.
  - Advances the subscription's *routing cursor* in the same tx as
    the inserts.

  Routing is "detect work" only — neither loads instance/projection
  state nor invokes user code beyond `RouteFn`.

- **Processing workers.** Any number. Poll
  `subscription_work_items` for the next claimable item, with the
  claim predicate enforcing per-partition ordering: an item is
  claimable only if no earlier item for the same `partition_key` is
  still `pending`, `claimed`, or `failed`. Realised via
  `SELECT … FOR UPDATE SKIP LOCKED` with a `NOT EXISTS` subquery.

  On claim: load whatever state the work needs, run user handler,
  on success mark `state = 'done'`; on failure mark `state =
  'failed'` with the captured error. Subsequent items for that
  partition stay unclaimable until the failure is resolved by
  operator action.

- **Pro:** matches the framing directly — routing per-subscription,
  processing per-partition, database fans out.
- **Pro:** no read amplification (source scanned once by the router).
- **Pro:** worker scaling independent of partition count.
- **Pro:** per-partition progress is the natural shape of the queue.
  A stuck partition has a `failed` item that blocks only its own
  subsequent items.
- **Pro:** operator tooling has a first-class observability surface
  — query `subscription_work_items` directly, see pending /
  claimed / failed / done per partition.
- **Pro:** failure semantics are explicit rows, not implicit cursor
  state. Retry / skip / discard are row-level operations.
- **Con:** one new table.
- **Con:** write amplification — one INSERT per routed event into
  `subscription_work_items`; one UPDATE per processed event.
  Bounded by routing rate; rows age out after retention once `done`.
- **Con:** more moving parts at runtime (routing worker + processing
  workers + work queue + per-partition state).
- **Con:** needs a retention/cleanup policy for `done` rows.
- **Con:** the claim query needs care to enforce per-partition
  ordering under concurrent claimants.
- **Note:** this is the pattern durable-task / job-queue systems
  converge on. The PM primitive fits it naturally; so does the
  partitioned-projection primitive.

### Generalisation across subscription kinds

Under Design 3, four common shapes are all the same mechanism with a
different `partition_by`:

| Application wants… | `partition_by` |
|---|---|
| Strict-sequential projection, one worker | constant (e.g. `'_default'`) |
| Concurrent commutative projection | event-unique (every item is its own partition) |
| Concurrent projection, per-key ordering | application-supplied function |
| PM instance routing | `process_uuid` |

Concurrency upper bound is `min(workers, distinct_active_partitions)`.
The application picks the partition key; the SDK enforces ordering
per partition.

### Cost trade for strict-sequential subscriptions

A subscription that wants "one worker, in order, no parallelism" pays
for a work-items table it doesn't need: every routed event becomes a
row, immediately gets claimed, then marked done. Today's pure-cursor
model is one UPDATE per batch; the work-queue model is one INSERT +
one UPDATE per event. Write amplification is real.

Two ways to handle this:

- **One mechanism for everything.** Always work-queue. Simpler
  architecture; uniform operator surface; perf cost accepted as
  the price of unification. Lean: this.
- **Two mechanisms, application chooses.** Cursor-based for the
  strict-sequential case; work-queue for everything else. Two
  code paths in the SDK, two operator surfaces.

The routing layer of Design 3 still has a cursor internally ("how
far has the router read in the source stream"). It's
single-active-worker and leased, identical in shape to today's
subscription. The application doesn't see it; it sees only the
work queue.

### Comparison on the user-facing contract

All three designs deliver the same observable behaviour:

- Each partition has its own progress.
- A stuck partition stalls in isolation; others run.
- Throughput scales by adding workers.
- Operator tooling (`instructedctl`) sees per-partition positions
  and per-partition health.

The choice is engineering-level: scale ceiling vs. design
complexity vs. observability surface.

### Operator escape on persistent failure (applies to all three designs)

- Default: the failing partition is parked at the failing event.
- Operator explicitly invokes "skip this event for this partition,
  state-loss acknowledged". Per-event, per-partition, requires
  explicit invocation each time, writes an audit row.
- No silent skip, no automatic skip-after-N, no
  quarantine-with-state-loss as default.

### Catch-up predicate (for `waitForProjection` and equivalents)

Subscription S is caught up to target event_number T iff **both**:

1. The routing cursor for S has reached T (`routing_cursor[S] >= T`),
   AND
2. No outstanding work items for S exist with `event_number <= T`
   (states `pending`, `claimed`, or `failed`).

Neither condition alone is sufficient. The cursor check alone is
insufficient because being past T only tells us routing has
finished for every event ≤ T — it says nothing about whether the
work items those routings produced have been processed. The
work-items check alone is insufficient because "no rows" is
ambiguous: it could mean "every event ≤ T was inspected and none
routed to S" (caught up) or "the router hasn't reached T yet" (not
caught up). The cursor disambiguates.

### Current state of the decision

Parked pending further design thought. Acknowledged factors:

- The initial framing of "Designs 1 and 2" was anchored on
  patterns inherited from other event-sourcing libraries; Design 3
  emerged once the problem was re-stated as "routing vs.
  processing" rather than "how do cursors work".
- The walkthrough of §2.5 surfaced that the same primitive serves
  projections; the decision is larger than "how do PMs work" and
  reaches into "what is a subscription, full stop".
- All three are pre-release work. None is small. Picking the one
  that fits the next 5–10 years is worth the thinking time.

The next pass should at minimum:

- Sketch the SQL for each design's hot-path query (routing-cursor
  advance, per-partition work-item claim, etc.) to make the
  performance comparison concrete rather than asserted.
- Sketch the operator surface (`instructedctl` commands) for each
  to make the observability claim concrete.
- Decide one-mechanism vs. two-mechanisms for the strict-sequential
  case.
- Cross-reference what this means for existing `INV-SUB-*`
  invariants (most of which assume the cursor model).
- Specify the `waitForProjection` reimplementation (catch-up
  predicate above; cross-stream guard from review §2.3.2 still
  applies).
- Note: ML-0001 as a separately planned feature dissolves into
  this decision. Partitioned consumers are not a separate thing to
  build; they are the shape of every subscription.

---

## SUB-B — Handler error policy

**Status:** Decided (shape); design detail follows SUB-A.
**Release-relevance:** Pre-release.

### What's settled

The core SDK exposes a minimal error-policy surface; the
application-convenience SDK layers richer wrappers over it.

**Core SDK vocabulary:**

- `retry-in: <ms>` — schedule the work item for re-attempt after a
  delay. The work item stays in flight (claimed or returns to
  `pending` depending on SUB-A realisation); the partition stays
  blocked behind it.
- `stop` — terminate the worker. Other workers may still pick up
  the work item; the failure that produced the `stop` is recorded
  and surfaced.

That's the whole core surface. Two return values, both
unambiguous about state.

**Application/convenience SDK wrappers** (per language; not
specified at the core layer) may layer on:

- "retry up to N times with exponential backoff, then quarantine"
- "retry with linear backoff up to a wall-clock deadline"
- domain-specific error matchers
- whatever idiomatic shape the host language prefers

Under SUB-A Design 3, "quarantine" maps to the work item's
`state = 'failed'` and operator-required resolution.

### What lands

1. Worker-level error policy hook in core: `(error, ctx) =>
   { kind: 'retry-in', delayMs: number } | { kind: 'stop' }`.
2. Default policy (when no hook is supplied): equivalent to
   today's behaviour — exponential backoff capped at 30 s,
   retry forever. This keeps current consumers running unchanged.
3. Convenience helpers in the TypeScript SDK: a couple of
   pre-built policies (`retryUpTo(n, backoff)`,
   `retryUntil(deadline, backoff)`, `quarantineAfter(n)` —
   implemented in terms of the core vocabulary).
4. Per-PM and per-projection registration accepts an optional
   policy. PMs and projections use the same surface.

### Tied to SUB-A

The "quarantine" outcome only makes sense under a work-items
schema (the failed item is the quarantine). Pre-SUB-A, the only
realisable outcomes are `retry-in` and `stop`. The convenience
helpers can ship pre-SUB-A using just those primitives;
quarantine semantics activate when SUB-A lands.

---

## SUB-C — Routing-boundary batching

**Status:** Open / "probably yes" pending SUB-A.
**Release-relevance:** Pre-release, contingent on SUB-A.

### The opportunity

Under SUB-A Design 3, the routing worker reads events from the
source stream and writes work items. The routing call sequence is
naturally batchable:

```
read N events from source
for each event:
  RouteFn(event)         -- pure user code, no I/O
collect routing decisions
INSERT … VALUES (...), (...), ...   -- one round-trip per batch
UPDATE routing_cursor              -- one round-trip per batch
```

vs. today's per-event UPDATE per ack.

For high-volume subscriptions, batched routing is a meaningful
DB-throughput win. The win is at the routing layer; processing-side
batching is a separate question (and largely an application
concern — a handler that wants to batch its work can collect items
from the queue and process them together).

### Open question

Does routing-side batching ship with SUB-A (as part of the design),
or as a follow-up? My lean: ship with SUB-A. The semantics are
unambiguous (atomic batch of INSERTs + cursor UPDATE in one tx) and
the SDK surface is one option (batch size). Designing SUB-A without
routing-side batching and adding it later means a second pass on
the same hot path.

### Processing-side batching

Probably a separate feature for later. The work-items table makes
it tractable: a worker can claim N items for the same partition
(or N items in disjoint partitions) in one query and invoke a
`handleBatch` callback. But the API surface (per-event vs.
per-batch handler, partial-batch failure semantics) needs its own
design and is application-specific enough that it can wait for a
concrete request. Maybe-later candidate post-SUB-A.

---

## Connections to other files

- PM-specific concerns (RouteFn shape, fan-out modelling,
  handle/apply split, deterministic command IDs for PM-dispatched
  commands) live in [`process-manager.md`](./process-manager.md).
- The `waitForProjection` reimplementation under SUB-A is captured
  in the review decisions log (§2.3.2 two-stage note) and in this
  file (SUB-A "catch-up predicate" subsection).
- ML-0001 (partitioned consumers) as a separately planned feature
  dissolves into SUB-A.
