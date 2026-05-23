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
  against a stream whose current version is 0.

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

Subscriptions are persistent, leased, and addressed by
`(stream_uuid, name)` (with a reserved `shard` dimension for a
future partitioned-consumer extension; v1 uses `shard = 0`).

### Identity

- **INV-SUB-P-001** — Identity is `(stream_uuid, name)`. Two
  subscriptions with the same pair are the same subscription.
- **INV-SUB-P-002** — Claiming a subscription that already exists
  attaches to its existing cursor; `start_from` is ignored.

### Single-active-worker

- **INV-SUB-P-010** — At most one live worker holds the lease
  for a subscription at any moment. Other workers calling
  `claim_subscription` get a non-error `'already_claimed'`
  result row.
- **INV-SUB-P-011** `[mechanism-only]` — Realised via the
  `claimed_by` / `claim_expires_at` columns on the subscription
  row with TTL-bounded leases. A worker calling
  `read_subscription_batch`, `advance_subscription`,
  `extend_subscription_claim`, or `release_subscription` whose
  `worker_id` does not match the current `claimed_by` raises
  `IS022 subscription_lease_lost`.
- **INV-SUB-P-012** — When a live worker fails (process exit,
  network failure, lease expiry without renewal), its slot
  MUST become available for another worker without
  administrative action.

### Start position

- **INV-SUB-P-020** — On *first* claim, `start_from` determines
  the initial cursor: `'origin'` → start at 0 (first event
  delivered is #1); `'current'` → start at the current global
  head; integer N → start at N (first event delivered is #N+1).
- **INV-SUB-P-021** — On *subsequent* claims with the same
  identity, `start_from` is ignored; the cursor resumes from
  `last_seen`.

### Delivery and ack

- **INV-SUB-P-030** — Events are delivered in strictly
  increasing order: by `stream_version` for single-stream
  subscriptions, by `event_number` for `$all` subscriptions.
- **INV-SUB-P-031** — Delivery is at-least-once. An event the
  worker fails to ack before disconnecting redelivers on the
  next claim.
- **INV-SUB-P-032** — `advance_subscription` advances the
  cursor to a caller-supplied position. Acking position N is
  taken as acknowledging all events up to and including N.
- **INV-SUB-P-033** — The cursor MUST NOT advance past unacked
  events. (In-flight buffer past the cursor is permitted, as
  long as the durable cursor does not move.)
- **INV-SUB-P-034** — `advance_subscription` is monotone: a
  call to advance to a position lower than the current
  `last_seen` is silently absorbed (no regression).

### Partitioned consumers — deferred

- **INV-SUB-P-040 / 041 / 042** — Multi-worker subscriptions
  with optional `partition_by` are **deferred** in v1; the
  conformance harness has placeholder `test.skip` slots. See
  [`maybe-later.md`](maybe-later.md) ML-0001 for the
  forward-compatibility constraints on the v1 schema.

### Selector — above the adapter line

- **INV-SUB-P-050** — A selector predicate, if supplied, filters
  events before delivery. Events for which the predicate
  returns false are not delivered, **but the cursor MUST still
  advance past them** (otherwise an unmatched event would
  permanently block the subscription).

  Realised SDK-side in v1: the SDK reads a batch from the store,
  applies the predicate locally, invokes the handler only for
  matches, and advances the cursor to the last *fetched*
  event_number. A future server-side variant is allowed; see
  [`maybe-later.md`](maybe-later.md) ML-0003.

### Lifecycle

- **INV-SUB-P-060** — `release_subscription` detaches a live
  worker. The cursor is preserved; a subsequent claim resumes
  from `last_seen`.
- **INV-SUB-P-061** — `delete_subscription` removes the row.
  A subsequent claim with the same identity behaves as a first
  claim (honours `start_from`).
- **INV-SUB-P-062** — `delete_subscription` on a non-existent
  subscription raises `IS020 subscription_not_found`.

### Closed error set for subscriptions

`IS020 subscription_not_found`, `IS022 subscription_lease_lost`,
plus standard Postgres infrastructure errors.

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
  stream. **(Honest gap, v1 TS SDK):** the store provides the
  metadata column; the v1 TypeScript SDK does not enforce this
  check. Applications that evolve aggregate schemas should
  implement the check themselves until the SDK does.
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
  the snapshot to be ignored. **(Honest gap; see AGG-003.)**
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

- **PM-001** — A router callback inspects each event and
  returns `start` / `continue` / `stop` / `ignore` plus the
  `process_uuid` (for `start` / `continue` / `stop`).
- **PM-010** — For each routed event the PM's `handle` runs
  as `(state, event) → (new_state, [commands])`.
- **PM-011** — Order: handle → dispatch all commands → persist
  new state (snapshot upsert) and advance the subscription
  cursor in the same short SDK transaction → done.
- **PM-012** — Dispatched commands inherit
  `causation_id = triggering_event.event_id` and
  `correlation_id = triggering_event.correlation_id`.
- **PM-020** — PM state is persisted as a snapshot keyed by
  `"<pm_name>-<process_uuid>"` with `source_version =
  triggering_event.event_number`.
- **PM-022** — On `stop`, the PM's snapshot MUST be deleted.
- **PM-024** — The snapshot's `source_version` doubles as the
  PM's `last_seen` marker, used to absorb redelivery on
  restart.

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

Three places where what's specified above is not (yet) what's
delivered by the v1 TypeScript SDK. Each is recorded so the
gaps don't get lost.

1. **`SNAP-002` / `AGG-003` — snapshot module versioning.** The
   store provides the metadata column; the v1 TS SDK does not
   enforce reject-on-mismatch. Applications must handle schema
   evolution themselves until the SDK adopts it.
2. **`INV-SUB-P-040..042` — partitioned consumers deferred.**
   See ML-0001. Apps needing throughput beyond what a single
   worker provides today must split into multiple named
   subscriptions over disjoint slices.
3. **`INV-SUB-P-050` — selector realised above the adapter
   line.** SDK-side filtering, cursor advances past skipped
   events. Functionally correct; bandwidth-inefficient for
   sparse selectors. ML-0003 reserves room for a server-side
   variant.
