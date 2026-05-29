# Invariants

The formal catalogue of constraints `instructed` enforces. Two
layers:

- **Store contract (INV-\*)** — what the SQL procedures in
  `sql/instructed.sql` guarantee. This is what the conformance
  harness in [`tests/conformance/`](../tests/conformance/) checks,
  and what every SDK port inherits for free by pointing at a
  conformant Postgres.
- **SDK contract (AGG-\*, HND-\*, PM-\*, CON-\*, DSP-\*, SNAP-\*)**
  — what an SDK must do on top of the store contract to provide
  a working CQRS/ES surface to applications.

Each entry has a stable identifier referenced from code comments,
tests, and ADRs. **MUST / MUST NOT / MAY** are RFC-2119.

Implementation-mechanism details are tagged `[mechanism-only]`
when the constraint is on outcome, not mechanism — an SDK port or
a future schema revision may use a different mechanism so long as
the outcome holds.

---

## Data shapes

### Event

| Field | Source | Constraint |
|---|---|---|
| `event_id` | caller-supplied UUID | Globally unique. |
| `event_number` | store-assigned | Global, monotonic, gapless across the entire store. |
| `stream_id` / `stream_uuid` | derived from caller-supplied target | Echoes the **original** stream the event was appended to, even when delivered via `$all`. |
| `stream_version` | store-assigned | Per-stream, monotonic, gapless, starts at 1. |
| `event_type` | caller-supplied | Opaque string. |
| `data` | caller-supplied | JSONB; round-tripped verbatim. |
| `metadata` | caller-supplied | JSONB or null; round-tripped verbatim. |
| `causation_id` | caller-supplied or SDK-defaulted | UUID or null; round-tripped verbatim. |
| `correlation_id` | caller-supplied or SDK-defaulted | UUID or null; round-tripped verbatim. |
| `created_at` | store-assigned `NOW()` | UTC. |

### Snapshot

A snapshot is addressed by `source_uuid` alone — at most one per
source. Fields: `source_uuid`, `source_type`, `source_version`
(non-negative integer), `data` (JSONB), `metadata` (JSONB),
`created_at`.

---

## Store contract — Part B — Append

`append_to_stream(stream_uuid, expected_version_type, expected_version, events, options)`.

### Identity and ordering

- **INV-APPEND-001** — Every event MUST have a unique `event_id`.
- **INV-APPEND-002** — Per-stream `stream_version` is contiguous
  starting at 1.
- **INV-APPEND-003** — Global `event_number` is contiguous starting
  at 1, monotonic across the entire store.
- **INV-APPEND-004** — An N-event append MUST produce contiguous
  `stream_version` values and contiguous `event_number` values
  with no concurrent-append interleaving.
- **INV-APPEND-005** — `created_at` MUST be UTC and SHOULD be
  non-decreasing across the global event order (modulo clock
  skew tolerated by the implementation; `NOW()` is the realisation).

### Atomicity

- **INV-APPEND-006** — An N-event append is atomic: all N
  persist, or none do.
- **INV-APPEND-007** — The atomicity boundary includes the
  per-stream version bump, the global `event_number` reservation,
  and the event row writes.

### Expected-version semantics

- **INV-APPEND-010** — `'any_version'` appends unconditionally,
  creating the stream if absent.
- **INV-APPEND-011** — `'no_stream'` succeeds only if the stream
  does not exist; otherwise raises `IS002 stream_exists`.
- **INV-APPEND-012** — `'stream_exists'` succeeds only if the
  stream exists; otherwise raises `IS003 stream_not_found`.
- **INV-APPEND-013** — `'exact'` with version V succeeds only if
  the stream's current version equals V at the moment of append;
  otherwise raises `IS001 wrong_expected_version`.
- **INV-APPEND-014** — V = 0 with `'exact'` is the "fresh stream"
  case: succeeds against a non-existent stream (creating it) or
  against a stream whose current version is 0. Two concurrent
  callers with V = 0 against the same non-existent stream MUST
  resolve as one winner (the stream is created) and one IS001
  loser; the implementation MUST translate the underlying
  `streams_stream_uuid_key` unique-violation to `IS001`, parallel
  to INV-APPEND-022's translation on the `stream_events` index.

### Concurrent appends

- **INV-APPEND-020** — Under concurrent same-stream appends with
  `'exact'` and the same V, at most one MAY succeed; the rest
  raise `IS001`.
- **INV-APPEND-021** — Under concurrent `'any_version'` appends,
  all MAY succeed; ordering is implementation-defined but
  contiguity (INV-APPEND-002, INV-APPEND-003) holds.
- **INV-APPEND-022** `[mechanism-only]` — The OCC mechanism is
  the unique constraint on `stream_events (stream_id, stream_version)`.
  Two concurrent appenders computing the same next version both
  try to insert; one succeeds, the other gets `unique_violation`
  translated to `IS001`. A predicate-based check or a per-stream
  advisory lock are alternative mechanisms that satisfy the same
  outcome.

### Idempotency and replay

- **INV-APPEND-030** — Re-appending an event with an `event_id`
  that already exists raises `IS004 duplicate_event`. It MUST NOT
  silently succeed or duplicate the event.

### Immutability

- **INV-APPEND-040** — A persisted event MUST NOT be modified by
  any later operation. Realised by triggers on `events` and
  `stream_events` that raise `IS006` on `UPDATE` / `DELETE`.
- **INV-APPEND-041** — A persisted event MUST NOT be deleted.
  `instructed` provides no hard-delete path.

### Closed error set for append

`IS001 wrong_expected_version`, `IS002 stream_exists`,
`IS003 stream_not_found`, `IS004 duplicate_event`,
`IS005 reserved_stream_uuid`, plus standard Postgres
infrastructure errors.

---

## Store contract — Part C — Read

`read_stream(stream_uuid, from_version, batch_size)` and
`read_all(from_event_number, batch_size)`.

- **INV-READ-001** — Reading a stream that has never been appended
  to raises `IS003 stream_not_found`.
- **INV-READ-002** — Returned events are in strictly increasing
  `stream_version` order.
- **INV-READ-003** — Returns every event in the requested range;
  internal paging is permitted but invisible to the caller.
- **INV-READ-004** — `from_version` is inclusive.
- **INV-READ-005** — Reading `$all` returns every event in the
  store ordered by strictly increasing `event_number`.
- **INV-READ-006** — Each returned event carries the **original**
  `stream_uuid` in its row, even when delivered via `$all`.
- **INV-READ-007** — `stream_version` returned via `$all` is the
  event's per-stream version in its original stream, not its
  position within `$all`.
- **INV-READ-008** — `event_number` is the global position in
  `$all`, regardless of which stream the read came from.
- **INV-READ-020** — A reader starting after an append MUST see
  that append's events. A reader started before an append MUST
  NOT see them in its current page (MVCC); whether a later page
  picks them up is implementation-defined.

---

## Store contract — Part D — Snapshots

`record_snapshot`, `read_snapshot`, `delete_snapshot`.

- **INV-SNAP-001** — At most one snapshot exists per `source_uuid`.
- **INV-SNAP-002** — `record_snapshot` is an upsert; existing
  rows are replaced wholesale.
- **INV-SNAP-003** — `read_snapshot` for a missing `source_uuid`
  raises `IS010 snapshot_not_found`.
- **INV-SNAP-004** — `delete_snapshot` is idempotent — deleting
  a missing snapshot succeeds silently.
- **INV-SNAP-005** — Snapshots are NOT versioned history. No
  prior snapshot is retained after `record_snapshot`.
- **INV-SNAP-006** — Snapshots are advisory; correct aggregate
  reconstruction MUST NOT depend on them being present.

---

## Store contract — Part E — Subscriptions

A subscription is a **routing cursor** plus a **work queue**:

- The cursor (`subscriptions.last_seen`) is advanced by a
  single-active **routing worker** that turns events into work
  items.
- The queue (`subscription_work_items`) carries one row per
  routed event per partition. Items are claimed and processed
  by **N processing workers in parallel**, distributed by
  `FOR UPDATE SKIP LOCKED` plus a per-partition predicate that
  preserves within-partition ordering.

The routing-side lease lives on the `subscriptions` row. The
processing-side lease lives on each `subscription_work_items`
row. The two are unrelated: a processing worker does NOT take
a subscription lease, and a routing worker does NOT claim work
items.

The `INV-SUB-P-*` series describes the routing-side invariants;
the `INV-SUB-W-*` series describes the work-queue invariants.
Identity is `(stream_uuid, name)` with a reserved `shard`
dimension at v1 default 0 for a future operator-facing shard
extension.

### Identity

- **INV-SUB-P-001** — Identity is `(stream_uuid, name)`. Two
  subscriptions with the same pair are the same subscription.
- **INV-SUB-P-002** — Claiming a subscription that already
  exists attaches to its existing cursor; `start_from` is
  ignored.

### Routing worker — single-active

- **INV-SUB-P-010** — At most one live routing worker holds the
  subscription lease at any moment. Other routing workers
  calling `claim_subscription` get a non-error
  `'already_claimed'` result row.
- **INV-SUB-P-011** `[mechanism-only]` — Realised via the
  `claimed_by` / `claim_expires_at` columns on the subscription
  row with TTL-bounded leases. A routing worker whose
  `worker_id` no longer matches the current `claimed_by` raises
  `IS022 subscription_lease_lost` on its next
  `route_batch` or `release_subscription`.
- **INV-SUB-P-012** — When a routing worker fails (process exit,
  network failure, lease expiry without renewal), the
  subscription slot MUST become available for another routing
  worker without administrative action.

### Start position

- **INV-SUB-P-020** — On *first* claim, `start_from` determines
  the initial cursor: `'origin'` → start at 0 (first event
  routed is #1); `'current'` → start at the current global
  head; integer N → start at N (first event routed is #N+1).
- **INV-SUB-P-021** — On *subsequent* claims with the same
  identity, `start_from` is ignored; the cursor resumes from
  `last_seen`.

### Routing-cursor advance

- **INV-SUB-P-030** — The routing cursor (`last_seen`) advances
  in event-number space (for `$all` subscriptions) or
  event-number space against the source stream (for per-stream
  subscriptions). It is strictly non-decreasing; concurrent
  re-reads after a routing-worker takeover are absorbed by the
  monotone advance ([INV-SUB-P-034]).
- **INV-SUB-P-031** — At-least-once delivery: a work item
  whose processing worker fails to call
  `complete_work_item_*` before its lease expires is redelivered
  to another processing worker.
- **INV-SUB-P-032** — `route_batch` advances `last_seen` and
  INSERTs the new work-item rows in **one transaction**. A
  reader observing `last_seen >= N` MUST also see the
  corresponding work-item rows for every routed event up to N
  (load-bearing for the catch-up predicate; see
  [INV-SUB-CATCHUP-001]).
- **INV-SUB-P-033** — The routing cursor MAY advance past
  events that route to `"ignore"` (no work item written for
  them). It MUST NOT advance past events that the routing
  worker has not yet inspected.
- **INV-SUB-P-034** — The routing cursor is monotone: a
  `route_batch` advancing to a position lower than the current
  `last_seen` is silently absorbed (no regression).

### Lifecycle

- **INV-SUB-P-060** — `release_subscription` detaches a live
  routing worker. The cursor and the queue are preserved; a
  subsequent claim resumes from `last_seen`.
- **INV-SUB-P-061** — `delete_subscription` removes the row.
  By the schema's `ON DELETE CASCADE` from
  `subscription_work_items.subscription_*` to `subscriptions`,
  every queued work item for the subscription is also removed.
  A subsequent claim with the same identity behaves as a first
  claim (honours `start_from`).
- **INV-SUB-P-062** — `delete_subscription` on a non-existent
  subscription raises `IS020 subscription_not_found`.

### Closed error set for the routing-worker surface

`IS020 subscription_not_found`, `IS022 subscription_lease_lost`,
plus standard Postgres infrastructure errors.

### Work queue (`INV-SUB-W-*`)

- **INV-SUB-W-001** — Each row in `subscription_work_items` is
  identified by `(stream_id, subscription_name, shard,
  partition_key, event_number)`. The PK absorbs duplicate
  INSERTs on routing-worker re-run (after crash, after
  takeover), making `route_batch` idempotent.
- **INV-SUB-W-002** — State machine:
  `pending → claimed → (done | failed)`.
  - `pending`: just routed; not yet claimed.
  - `claimed`: a processing worker holds a per-item lease.
    `claimed_by` and `lease_expires_at` are both set.
  - `done`: terminal-success (PM path -- the row stays for
    catch-up bookkeeping and for PM-state rebuild). Projection
    path DELETEs the row directly without an intermediate
    `done` state.
  - `failed`: terminal-failure, operator-only resolution. The
    soak harness and the default error policy never produce
    these; explicit `fail_work_item` (reserved for a future
    `quarantineAfter` convenience wrapper) does. `error_text`
    is non-null on `failed` rows.
- **INV-SUB-W-003** `[mechanism-only]` — Per-state column
  invariants are enforced by CHECK constraints on the row:
  `claimed ⇔ (claimed_by AND lease_expires_at)`,
  `failed ⇔ failed_at`, `error_text` only on `failed` rows.
- **INV-SUB-W-010** — Per-partition ordering: at most one
  *unexpired-claimed* work item per
  `(subscription, partition_key)`, and `claim_work_item`
  refuses to claim a row whose partition has a predecessor in
  any non-terminal state with a lower `event_number`. This is
  what makes per-partition processing serial.
- **INV-SUB-W-011** — Across partitions, processing is
  concurrent: `claim_work_item` distributes work across
  partitions via `FOR UPDATE SKIP LOCKED`. The number of
  partitions a subscription has determines its maximum
  processing parallelism.
- **INV-SUB-W-012** — A processing worker that takes over an
  expired `claimed` row (lease takeover) sees the same work
  item the original worker was holding. The previous worker's
  next call to `extend_work_item_claim`,
  `complete_work_item_*`, or `fail_work_item` raises
  `IS030 work_item_lease_lost`.
- **INV-SUB-W-013** — `failed` rows are operator-only: no
  procedure auto-skips or auto-deletes them. They permanently
  block their partition until operator action transitions them
  (planned: `instructedctl` admin path).
- **INV-SUB-W-020** — Projection-side terminal success:
  `complete_work_item_projection` DELETEs the row. No `done`
  state persists for projections.
- **INV-SUB-W-021** — PM-side terminal success (non-terminal
  instance): `complete_work_item_pm` UPDATEs the row to `done`
  AND UPSERTs the PM-state snapshot in **one transaction**.
  Snapshot `source_version = claimed_event.event_number`.
- **INV-SUB-W-022** — PM-side terminal success (terminal
  instance): `complete_pm_instance` DELETEs the PM-state
  snapshot AND DELETEs every work item for the partition (the
  triggering one included) in **one transaction**. Future
  events that route to the same partition run from
  `initialState()` again.
- **INV-SUB-W-030** — Closed error set for the work-queue
  surface: `IS020 subscription_not_found`,
  `IS030 work_item_lease_lost`, plus standard Postgres errors.

### Catch-up predicate (`INV-SUB-CATCHUP-*`)

- **INV-SUB-CATCHUP-001** —
  `is_subscription_caught_up(stream, name, target)` returns
  true iff **both** conjuncts hold:
  1. The routing cursor has reached the target
     (`subscriptions.last_seen >= target`).
  2. No `subscription_work_items` row for the subscription with
     `event_number <= target` is in a non-terminal state
     (`pending`, `claimed`, or `failed`).
  Either conjunct alone is insufficient. The atomic write of
  `route_batch` ([INV-SUB-P-032]) ensures (1) is never
  observable without (2)'s relevant rows being observable too.
  `waitForProjection` polls this predicate; the same predicate
  is the conformance-level definition of "caught up".

---

## Store contract — Part F — Cross-cutting

### Causation and correlation

- **INV-META-001** — `causation_id` and `correlation_id`, when
  set on input, MUST be persisted and echoed on the
  corresponding event. The store assigns no meaning.

### Event types and payloads

- **INV-META-010** — `event_type` is an opaque string chosen by
  the caller.
- **INV-META-011** — `data` and `metadata` are JSONB,
  round-tripped verbatim.

### Streams as identifiers

- **INV-STREAM-001** — A stream is identified by an opaque
  string (`stream_uuid`).
- **INV-STREAM-002** `[mechanism-only]` — Internally a numeric
  `stream_id` is assigned per stream and used for joins; this
  is not part of the external contract.
- **INV-STREAM-003** — The reserved name `$all` refers to the
  global stream. A user-supplied `stream_uuid` MUST NOT collide
  with it; the schema enforces this with
  `CHECK (stream_uuid <> '$all')`.

---

## SDK contract

What an SDK must implement on top of the store contract to give
applications a working CQRS/ES surface. Items below are observable
behavioural constraints, not API shapes — each SDK chooses its
own idiomatic shape (see [`architecture.md`](architecture.md)
"SDK structure").

### Aggregate execution (AGG-\*)

- **AGG-001** — First load reconstructs state by optionally
  reading a snapshot, streaming the events after that snapshot,
  and folding via the aggregate's applier.
- **AGG-002** — The event applier MUST NOT fail. Once stored,
  an event is replayed without veto.
- **AGG-003** — If a snapshot's `metadata.snapshot_module_version`
  does not match the configured value, the snapshot MUST be
  ignored and the aggregate MUST hydrate from the full event
  stream. The v1 TypeScript SDK enforces this in
  `loadAggregate` (`aggregate.ts`): the metadata's
  `SNAPSHOT_MODULE_VERSION_KEY` value is compared strictly to
  `def.snapshotModuleVersion`; on mismatch, the snapshot's
  `data` is discarded and the stream is paged from version 0.
  See SNAP-002 for the shared treatment with PM snapshots.
- **AGG-005** — Command handlers run as `(state, command) → events`.
- **AGG-006** — Return values: zero, one, or many events, or an
  error / thrown exception. An error or exception leaves state
  unchanged.
- **AGG-007** — Events produced by a command MUST be applied to
  in-memory state *before* the next command runs against the
  same state, and they MUST be the same events that were
  appended.
- **AGG-009** — The append at the end of a successful command
  MUST use `expected_version = currently_observed_version`.
- **AGG-010** — On `IS001 wrong_expected_version`, the SDK MUST
  retry: re-load, re-fold, re-execute, re-append. The retry
  budget is configurable per aggregate.
- **AGG-020** — Events produced by a command share
  `causation_id = command_id` and inherit `correlation_id`
  from the caller's context (or default to a fresh UUID).
- **AGG-021** — Events produced by a single command MUST all
  share the same `causation_id` and `correlation_id`.

### Snapshotting policy (SNAP-\*)

- **SNAP-001** — Snapshots are taken according to a policy
  attached to each aggregate (`snapshot_every: N`).
- **SNAP-002** — `metadata.snapshot_module_version` is the
  aggregate-module schema marker. Mismatch on read MUST cause
  the snapshot to be ignored and the source to be rebuilt from
  events. The v1 TS SDK enforces this for **both** aggregate
  snapshots (`loadAggregate` in `aggregate.ts`) and
  process-manager snapshots (`pm-substrate.ts:loadState`); the
  metadata key constant `SNAPSHOT_MODULE_VERSION_KEY` lives in
  `snapshot-version.ts` as the shared source of truth.
  Comparison is strict: "version on one side, absent on the
  other" counts as mismatch (prevents accidental version
  adoption). On the aggregate side, fall-back is a full replay
  from origin via the existing `readStream` pagination; on the
  PM side, via `listPmRebuildEvents`. Failure mode is silent
  (no warning emitted) because a deliberate version bump would
  otherwise produce a warning on every aggregate's next touch.
- **SNAP-003** — A failed snapshot write MUST NOT fail the
  command that triggered it; the events are already durable.
- **SNAP-004** — `source_version` recorded in a snapshot equals
  the aggregate version at the moment the snapshot was
  captured. Hydration resumes from `source_version + 1`.

### Event handler (HND-\*)

- **HND-001** — A handler is identified by `(stream, name)` —
  the same identity as the underlying subscription.
- **HND-002** — Handler names are stable across deployments;
  changing a name creates a new subscription that re-processes
  from `start_from`.
- **HND-010** — Events are delivered in order, one at a time,
  to a handler `(event, ctx) → Promise<void>`.
- **HND-011** — Handler return → SDK advances the cursor.
  Handler throw → SDK does not advance; redelivery on next
  iteration.
- **HND-023** — Delivery is at-least-once. Handlers MUST be
  idempotent; the SDK does not provide handler-side
  transactional atomicity with the cursor advance.

### Process manager (PM-\*)

The routing function decides which partition an event belongs
to; the processing layer's `apply` + `handle` split decides
what to do with it.

- **PM-001** — The router callback is
  `(event) -> { partitionKey: string } | "ignore"`. It is a
  pure function of the event; it does not see state, and it
  does not emit lifecycle directives. (See [D-0018] for why
  the Commanded-style `start` / `continue` / `stop` set is not
  inherited.)
- **PM-002** — The `apply` callback is
  `(state, event) -> state`. It is a pure fold; the framework
  invokes it during PM-state rebuild (when the snapshot is
  missing, or carries a `snapshot_module_version` that no
  longer matches `def.snapshotModuleVersion`) and on the
  claimed event before `handle`. It MUST NOT have side effects.
- **PM-010** — The `handle` callback is
  `(staged_state, event) -> { commands?, complete? }`.
  `staged_state` is `apply(loaded_state, event)` (i.e. the
  triggering event folded in). `commands` defaults to empty;
  `complete` defaults to false.
- **PM-011** — Order, per claimed work item: state-load
  (snapshot if version matches, otherwise rebuild via `apply`)
  → `apply(state, event)` → `handle(staged_state, event)` →
  dispatch all commands on the dispatch session → terminal
  step (non-terminal: `complete_work_item_pm` UPSERTs the
  snapshot + UPDATEs the work item to `done` in one tx;
  terminal: `complete_pm_instance` DELETEs the snapshot + every
  work item for the partition in one tx).
- **PM-012** — Dispatched commands inherit
  `causation_id = triggering_event.event_id` and
  `correlation_id = triggering_event.correlation_id`.
- **PM-020** — PM state is persisted as a snapshot keyed by
  `"<pm_name>-<partition_key>"` with `source_version =
  triggering_event.event_number`.
- **PM-022** — On `handle` returning `{ complete: true }`, the
  PM's snapshot AND every work item for the partition are
  DELETEd in one transaction. Future events to the same
  partition route as normal and run from `initialState()`
  again; permanent termination is an application-level pattern.
- **PM-024** — A PM tracks two positions, not one. The
  subscription's `last_seen` (the routing cursor) determines
  where the routing worker resumes reading; it advances past
  every event the routing worker has inspected, regardless of
  whether the event was routed or ignored. The snapshot's
  `source_version` is the per-instance state-version marker:
  the `event_number` of the most recent *routed* event folded
  into state. So `source_version <= last_seen` always, with
  equality iff the routing worker has processed exactly up to
  the last routed event. On PM-state rebuild after a snapshot
  miss the SDK loads every `done` work-item for the partition
  with `event_number < claimed_event_number` and folds it
  through `apply` from `initialState()`.

### Strong consistency on dispatch (CON-\*)

- **CON-001** — Each subscription is consumed at one of two
  consistency levels: eventual (default) or "wait on dispatch".
- **CON-010** — A dispatcher requesting
  `consistency: [name1, name2, ...]` waits, after a successful
  append, until each named subscription's cursor has caught up
  to the appended events' position. On timeout, dispatch
  raises a typed timeout error; the events remain appended.
- **CON-011** — The consistency list is **explicit**. There is
  no "wait for everything" shorthand.
- **CON-012** — The wait is bounded by a configurable timeout.

### Dispatch surface (DSP-\*)

- **DSP-001** — `dispatch(command)` resolves the aggregate
  identity from the command (via an application-supplied
  identity function), runs the load-execute-append cycle, and
  returns.
- **DSP-002** — Dispatch options accepted: `correlationId`,
  `causationId`, `metadata`, `timeout`, `consistency`,
  `consistencyTimeout`, `returning`, `retryAttempts`.
- **DSP-003** — `returning` controls the reply shape:
  acknowledgement-only, aggregate state, aggregate version,
  events, or full execution result.

---

## Honest gaps in v1

Two places where what's specified above is not (yet) what's
delivered by the v1 TypeScript SDK. Each is recorded so the
gaps don't get lost.

1. **PM-E — deterministic event IDs for PM-dispatched
   commands.** When a PM `handle` is re-invoked for the same
   claimed event (a `retry-in` after a post-dispatch failure,
   or a lease-takeover redelivery), commands dispatched in the
   prior attempt that already committed at the aggregate will
   be re-dispatched and may produce duplicate events at the
   aggregate (no `IS004 duplicate_event` protection without
   deterministic event IDs). Closing this gap is a separate
   slice of work (PM-E); the PM-024 snapshot atomicity
   guarantee still holds (the
   PM's `forwarded` counter is incremented exactly once per
   triggering event regardless of redelivery count -- see
   `tests/soak/` for the verification under churn).
2. **Routing-worker `close()` is a drop, not a flush.** On
   graceful close mid-batch the routing worker drops the
   accumulated decisions; the relaunched worker re-reads from
   `last_seen` and the work-items PK absorbs duplicate
   INSERTs. Observationally equivalent to a crash from outside
   the worker; see ML-0012 for the future-option flush
   alternative.
