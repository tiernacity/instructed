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

**Status:** Decided. Design 3 (decoupled router + work queue),
with the PM-F routing shape and one-mechanism-for-everything
(no separate cursor-only path for strict-sequential). The
"Proposed design" subsection below is the canonical reference;
the Three designs / Cost trade / Current state of the decision
subsections above are retained as the reasoning trail.
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
  the price of unification.
- **Two mechanisms, application chooses.** Cursor-based for the
  strict-sequential case; work-queue for everything else. Two
  code paths in the SDK, two operator surfaces.

The routing layer of Design 3 still has a cursor internally ("how
far has the router read in the source stream"). It's
single-active-worker and leased, identical in shape to today's
subscription. The application doesn't see it; it sees only the
work queue.

**Decided: one mechanism for everything.** A strict-sequential
subscription pays one INSERT + one UPDATE per event for the
uniformity. The cost is bounded by the routing rate and is
acceptable at the throughputs the library targets. Operator
surface, SDK code paths, and conformance tests all collapse to
one shape.

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

**Resolved**: Design 3 chosen, with the PM-F routing shape and
one-mechanism-for-everything. See the "Proposed design"
subsection below for the canonical write-up. The factors
acknowledged during the parked period (retained below for the
reasoning trail):

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

### Proposed design (Design 3 with the PM-F routing shape)

This subsection promotes Design 3 from candidate to **proposed**
design and fleshes it out under PM-F's simplified routing shape
(`RouteFn → { partitionKey } | "ignore"`; see
[`process-manager.md`](./process-manager.md) PM-F for rationale).
The remaining artifacts in the "next pass should at minimum"
checklist (three-way SQL comparison, operator surface,
`INV-SUB-*` triage, `waitForProjection` SDK shape +
cross-stream guard) build on this section and follow as
separate artifacts. The one-mechanism-vs-two decision is
resolved: one mechanism (see "Cost trade for strict-sequential
subscriptions" above).

Designs 1 and 2 are kept in this file above for reference. The
short version of why Design 3 is proposed: read amplification
(D1 and D2 both pay O(N partitions) per event read; D3 pays
O(1) by storing matched positions), and first-class failure
observability (D3's `state = 'failed'` row vs. D1/D2's implicit
"this cursor isn't advancing"). D2-with-positions is
isomorphic to D3.

#### Schema additions

```sql
CREATE TABLE subscription_work_items (
  subscription_id INT     NOT NULL REFERENCES subscriptions(subscription_id),
  partition_key   TEXT    NOT NULL,
  event_number    BIGINT  NOT NULL,
  state           TEXT    NOT NULL
    CHECK (state IN ('pending','claimed','failed','done')),
  claimed_by      TEXT,
  lease_expires_at TIMESTAMPTZ,
  failed_at       TIMESTAMPTZ,
  error_text      TEXT,
  PRIMARY KEY (subscription_id, partition_key, event_number)
);

-- Hot-path index for the claim query (excludes done rows).
CREATE INDEX subscription_work_items_claimable
  ON subscription_work_items (subscription_id, event_number)
  WHERE state IN ('pending','claimed','failed');
```

`subscriptions` retains a single cursor column; it is now
conceptually the **routing cursor**, advanced only by the
routing worker.

#### Routing worker — hot path

One routing worker per subscription. Holds a lease on the
subscription row. Reads a batch from `$all`, runs `RouteFn` on
each event, inserts a work item per matched event, advances the
routing cursor — all in one transaction.

```sql
-- (a) Read next batch from $all.
SELECT event_number /* , payload */
FROM stream_events_all
WHERE event_number > (
  SELECT last_seen FROM subscriptions WHERE subscription_id = $1
)
ORDER BY event_number ASC
LIMIT $batch_size;
```

Application runs `RouteFn` on each row of the batch in memory,
collecting routed `(partition_key, event_number)` pairs.
`"ignore"` decisions are dropped.

```sql
-- (b) In one tx: insert all routed pairs, advance routing cursor.
INSERT INTO subscription_work_items
  (subscription_id, partition_key, event_number, state)
VALUES
  ($1, $pk_1, $en_1, 'pending'),
  ($1, $pk_2, $en_2, 'pending'),
  /* ... */ ;

UPDATE subscriptions
SET last_seen = $max_event_number_in_batch
WHERE subscription_id = $1;
```

Atomic batch advance: the cursor only moves past events whose
routing decisions are durably recorded. Routing is crash-safe —
the worker may re-read a partially-routed batch on restart, but
`(subscription_id, partition_key, event_number)` is the primary
key so re-inserts hit the unique constraint and the second
attempt completes the batch. (Equivalent: use `INSERT ... ON
CONFLICT DO NOTHING`; the cursor advance is the commit point
either way.)

Routing calls only `RouteFn` — no instance state, no aggregate
loads, no user handlers. The work it does is bounded by
`O(batch_size)` plus one round-trip per batch (SUB-C).

#### Processing worker — claim and complete

Any number of processing workers per subscription. Each polls
for a claimable work item, runs the user handler, marks the
item terminal.

The claim query enforces per-partition ordering: a work item is
claimable only if no earlier work item for the same partition
is still in a non-terminal state.

```sql
-- (c) Claim next work item, enforcing per-partition ordering.
UPDATE subscription_work_items w
SET state            = 'claimed',
    claimed_by       = $worker_id,
    lease_expires_at = now() + interval '30 seconds'
FROM (
  SELECT wi.subscription_id, wi.partition_key, wi.event_number
  FROM   subscription_work_items wi
  WHERE  wi.subscription_id = $1
    AND  wi.state = 'pending'
    AND  NOT EXISTS (
      SELECT 1 FROM subscription_work_items earlier
      WHERE earlier.subscription_id = wi.subscription_id
        AND earlier.partition_key   = wi.partition_key
        AND earlier.event_number    < wi.event_number
        AND earlier.state IN ('pending','claimed','failed')
    )
  ORDER BY wi.event_number ASC
  FOR UPDATE SKIP LOCKED
  LIMIT 1
) c
WHERE w.subscription_id = c.subscription_id
  AND w.partition_key   = c.partition_key
  AND w.event_number    = c.event_number
RETURNING w.*;

-- Worker then reads the event payload by primary key:
SELECT /* payload */
FROM stream_events_all
WHERE event_number = $work_item.event_number;
```

The `NOT EXISTS` subquery is the per-partition ordering
enforcement. Combined with `FOR UPDATE SKIP LOCKED`, this gives
concurrent claims **across** partitions and serial claims
**within** a partition. The partial index
(`subscription_work_items_claimable`) keeps the subquery cheap
by excluding `done` rows from the scan.

Terminal transitions on the just-claimed item depend on the
subscription kind — see "Work-item lifecycle by subscription
kind" below for the full contract. Failure is symmetric across
kinds:

```sql
-- Failure (per SUB-B error policy).
UPDATE subscription_work_items
SET state      = 'failed',
    failed_at  = now(),
    error_text = $err,
    claimed_by = NULL,
    lease_expires_at = NULL
WHERE subscription_id = $1
  AND partition_key   = $2
  AND event_number    = $3;
```

A `failed` row blocks subsequent items **for its partition
only** (via the `NOT EXISTS` subquery). Other partitions are
unaffected. `failed` rows are not subject to retention.

Success transitions differ by kind:

```sql
-- Success, projection (PRJ-E): DELETE in the same tx as the handler's
-- read-model write.
DELETE FROM subscription_work_items
WHERE subscription_id = $1
  AND partition_key   = $2
  AND event_number    = $3;

-- Success, PM (non-terminal): UPDATE to 'done' alongside snapshot upsert.
UPDATE subscription_work_items
SET state = 'done', claimed_by = NULL, lease_expires_at = NULL
WHERE subscription_id = $1
  AND partition_key   = $2
  AND event_number    = $3;

-- Success, PM (terminal: handle returned { complete: true }):
-- in one tx, DELETE the snapshot AND every work-item for the partition
-- (including the triggering one).
DELETE FROM aggregate_snapshots
WHERE source_uuid = $pm_snapshot_uuid;
DELETE FROM subscription_work_items
WHERE subscription_id = $1
  AND partition_key   = $2;
```

#### Lease takeover

If a worker dies mid-claim, `lease_expires_at` is in the past
and the next claim attempt for that subscription will treat
`claimed` rows with expired leases as eligible. Concretely the
claim query above changes its inner predicate to:

```sql
AND (
  wi.state = 'pending'
  OR (wi.state = 'claimed' AND wi.lease_expires_at < now())
)
```

— with the rest unchanged. Spelled out separately here so the
default hot path stays simple.

#### Catch-up predicate (for `waitForProjection`)

Subscription `S` is caught up to target `event_number` `T` iff
**both**:

```sql
SELECT
  (SELECT last_seen FROM subscriptions WHERE subscription_id = $S) >= $T
  AND NOT EXISTS (
    SELECT 1 FROM subscription_work_items
    WHERE subscription_id = $S
      AND event_number <= $T
      AND state IN ('pending','claimed','failed')
  );
```

The routing cursor disambiguates the "no rows" case (events
≤ T were routed and produced nothing, vs. routing hasn't
reached T yet). The work-items check guarantees that what was
routed has been processed. Rationale in the SUB-A "Catch-up
predicate" subsection above.

Works uniformly for both kinds. For projections (PRJ-E
immediate-delete), the `state IN (...)` filter is logically
redundant (`done` rows don't exist) but harmless. For PMs the
filter excludes the retained `done` rows from blocking
catch-up.

**Race safety at the start of `waitForProjection`.** A caller
that appends event N and immediately calls
`waitForProjection(S, N)` must not observe a spurious
"caught-up" before the routing worker has processed N. The
safety property holds **iff the routing cursor advance and the
work-item INSERTs commit in a single transaction** — which is
what the routing hot path above does. Without that atomicity,
a window would exist where `last_seen >= N` is observable but
the corresponding work-items haven't yet been inserted, and
the predicate would falsely report caught-up. Load-bearing
invariant.

#### Work-item lifecycle by subscription kind

Retention is not a global age-based policy with knobs. The unit
of retention is the subscription kind, because the *reason* the
framework keeps a `done` row differs:

| Kind        | Why we'd retain a `done` row                                          | Lifecycle on success                                                              |
| ----------- | --------------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| Projection  | Nothing depends on it. Read model is the state; framework neither reads nor writes projection state. | DELETE in the same tx as the handler write.                                       |
| PM          | The set of `done` rows for a partition IS the durable "which events were routed to me" record. PM-C's snapshot rebuild via `apply` reads it. | UPDATE to `done` in the same tx as the snapshot upsert. Retained until `complete: true`, at which point snapshot and ALL work-items for the partition are DELETEd in one tx. |

This gives a clean per-kind contract:

- **Projection** `done` rows do not exist as persisted state.
  The framework can be queried for in-flight work
  (`pending`/`claimed`/`failed`) only.
- **PM** `done` rows accumulate per partition until the PM
  explicitly signals it's finished. The promise to the
  application is: *until you tell us a PM instance is complete,
  we keep enough to reconstruct its state from origin via
  `apply`. When you say complete, we discard everything for
  the instance.*

Consequences:

- Long-running PM instances accumulate one work-item row per
  routed event for the life of the instance. Storage cost is
  real but bounded by routing rate; the alternative (silent
  unrecoverability on snapshot-version-mismatch) is worse.
- Snapshot cadence and work-item retention are *independent*:
  taking a snapshot doesn't release work-items; deleting one
  doesn't affect work-items. The only releaser is
  `complete: true`.
- `failed` rows are never subject to retention regardless of
  kind. Operator action required.

A configurable post-success retention window for projections
("keep `done` rows for K minutes for ops visibility") is
recorded as a maybe-later item; default ships as immediate
DELETE.

#### PM-B constraints — how they're satisfied

| Constraint                                                | Mechanism                                                                 |
| --------------------------------------------------------- | ------------------------------------------------------------------------- |
| Per-instance progress                                     | Each `(subscription_id, partition_key)` slice of work items has its own state. |
| Stuck instance stalls only itself                         | `NOT EXISTS` per-partition ordering blocks the partition; others run.     |
| Throughput scales with workers                            | Workers compete on the same `subscription_work_items` table; `SKIP LOCKED` distributes claims. |
| SDK MUST NOT silently skip an event for any instance      | No code path deletes or skips a `failed` row implicitly; operator action required (see below). |
| Operator skip is per-event per-instance, audited          | `instructedctl` command (deferred artifact) explicitly transitions a single `(subscription_id, partition_key, event_number)` row from `failed` to `done` and writes an audit row. |

#### What survives unchanged

- The store contract for `$all`, `stream_events`, `streams`,
  `aggregate_snapshots`. Untouched.
- The aggregate path (`runCommand`, OCC, `IS001`). Untouched.
- The subscription concept as the application sees it in the
  SDK convenience layer — projections still register handlers,
  PMs still declare `RouteFn` plus `apply`/`handle`.
- `LISTEN/NOTIFY` plumbing (ML-0002): routing worker can use
  it to wake on new `$all` events instead of polling;
  processing workers can use it to wake on new
  `subscription_work_items` rows.

#### What changes

- `subscriptions.last_seen` is renamed conceptually to the
  *routing cursor*. Per-event ack at the SQL layer is replaced
  by per-work-item state transition.
- New core procedure surface (the porting checklist gains
  these):
  - `route_batch(subscription_id, decisions[], new_cursor)` —
    atomic INSERT-of-work-items + cursor advance.
  - `claim_work_item(subscription_id, worker_id, lease_seconds)`
    — the claim query with the per-partition `NOT EXISTS`
    predicate; includes lease-takeover branch.
  - `complete_work_item_projection(subscription_id,
    partition_key, event_number)` — DELETEs the row.
  - `complete_work_item_pm(subscription_id, partition_key,
    event_number, snapshot_data)` — UPDATEs the row to `done`,
    UPSERTs the snapshot. One tx.
  - `complete_pm_instance(subscription_id, partition_key,
    event_number)` — DELETEs the snapshot AND every work-item
    for the partition. One tx. Invoked when `handle` returns
    `complete: true`.
  - `fail_work_item(..., error_text)` — same shape for both
    kinds.
  - `skip_work_item_with_audit(..., operator, reason)` —
    operator-only; transitions a `failed` row to a terminal
    state with an audit trail.
- Several `INV-SUB-*` invariants move from "user-facing
  contract" to "routing-layer mechanism". Triage is a separate
  artifact.

#### What this section does not yet specify

- **Operator commands** (`instructedctl`): the skip-with-audit
  command, the work-item inspection commands, the
  rebuild-projection command (PRJ-D), the drop-pm-instance
  command. Deferred to the operator-surface artifact.
- **`INV-SUB-*` triage**: which survive, which reshape, which
  become `[mechanism-only]`. Deferred.
- **`waitForProjection` SDK reimplementation**: the SQL
  predicate is above; the SDK call shape (and the cross-stream
  guard from review §2.3.2) is a separate artifact.

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

**Status:** Decided as part of SUB-A. The routing hot path in
the SUB-A proposed design reads a batch from `$all`, collects
routing decisions, and commits a multi-row INSERT plus the
cursor UPDATE in one tx. Batch size is an SDK option. The
"Open question" subsection below is resolved.
**Release-relevance:** Pre-release, lands with SUB-A.

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

### Resolved: ships with SUB-A

Routing-side batching is part of the SUB-A proposed design.
The routing hot path commits an atomic batch of INSERTs + a
cursor UPDATE in one tx. Batch size is an SDK option;
default to be tuned during implementation. Designing SUB-A
without this and adding it later would have meant a second
pass on the same hot path; doing it once is cheaper.

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
  `handle`/`apply` split, deterministic command IDs for
  PM-dispatched commands, simplified routing surface, lifecycle
  via `complete: true`) live in
  [`process-manager.md`](./process-manager.md).
- Projection-specific concerns (registration surface, no
  `apply`/`handle` split, read-model transactionality,
  immediate-delete on success, rebuild as operator action)
  live in [`projections.md`](./projections.md).
- The `waitForProjection` reimplementation under SUB-A is
  captured in the review decisions log (§2.3.2 two-stage note)
  and in this file (SUB-A "catch-up predicate" subsection); the
  cross-stream guard lives in [`consistency.md`](./consistency.md)
  CON-B.
- ML-0001 (partitioned consumers) as a separately planned
  feature dissolves into SUB-A.
- ML-0010 (configurable projection done-row retention) is the
  opt-in opposite of PRJ-E's immediate-delete default.
