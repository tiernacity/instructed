# Invariants — Phase 2

A mechanical specification of what a CQRS/ES event store must guarantee,
derived from Commanded's adapter behaviour
(`commanded/lib/commanded/event_store/adapter.ex`), its data shapes
(`event_data.ex`, `recorded_event.ex`, `snapshot_data.ex`,
`subscription.ex`), the reference Postgres adapter
(`eventstore/lib/event_store/...`, `eventstore/priv/event_store/...`),
and the adapter conformance tests
(`commanded/test/event_store/support/{append_events,subscription,snapshot}_test_case.ex`).

This document records *constraints*. It does **not** propose how
`instructed` will realise them. That comes later
(Phase 4, `mapping.md`).

Where a constraint exists only because of how the reference adapter
happens to be built — i.e. it is a mechanism, not a guarantee — that is
called out explicitly so we can choose a different mechanism without
breaking the contract.

## Conventions

- **MUST / MUST NOT / MAY** in the RFC-2119 sense.
- Each numbered constraint has a stable identifier (e.g. `INV-APPEND-001`)
  for cross-referencing from `mapping.md` and `decisions.md`.
- "Reference adapter" means the Postgres-backed `EventStore` library
  (`commanded/eventstore`). Constraints originating from its mechanism
  rather than from the abstract contract are flagged
  **[reference-only]**.

---

## Part A — Data shapes that cross the contract boundary

### EventData (input to `append_to_stream`)

Fields:

- `event_type` — string. The serialised type name.
- `data` — struct / arbitrary payload to be serialised.
- `metadata` — map. May be nil.
- `correlation_id` — UUID-string. Optional.
- `causation_id` — UUID-string. Optional.

The store MUST NOT alter, drop, or reorder these fields. They reappear
verbatim on the corresponding `RecordedEvent`.

The store assigns: `event_id`, `event_number`, `stream_id`,
`stream_version`, `created_at`.

### RecordedEvent (output of reads and subscriptions)

| Field            | Source                       | Constraint |
|------------------|------------------------------|------------|
| `event_id`       | store-assigned UUID          | Globally unique. |
| `event_number`   | store-assigned integer       | Global, monotonically increasing, **gapless** across the entire store. |
| `stream_id`      | caller-supplied stream UUID  | Echoes the stream the event belongs to (the *original* stream, even when delivered via `:all`). |
| `stream_version` | store-assigned integer       | Per-stream, monotonically increasing, **gapless** starting at 1 for the first event in the stream. |
| `event_type`     | from `EventData`             | Echoed. |
| `data`           | from `EventData`             | Echoed (after round-trip through serializer). |
| `metadata`       | from `EventData`             | Echoed. |
| `causation_id`   | from `EventData`             | Echoed. |
| `correlation_id` | from `EventData`             | Echoed. |
| `created_at`     | store-assigned `DateTime`    | UTC. |

### SnapshotData

- `source_uuid` — string. Identifies the snapshotted entity.
- `source_version` — non-neg integer. The aggregate version the snapshot
  represents.
- `source_type` — string. The aggregate module / type.
- `data` — opaque (binary in the type spec; in practice serialised
  arbitrary payload).
- `metadata` — opaque.
- `created_at` — `DateTime`.

Snapshots are addressed by `source_uuid` alone — there is at most one
snapshot per source.

---

## Part B — Append (`append_to_stream`)

Signature:
`append_to_stream(meta, stream_uuid, expected_version, events, opts) :: :ok | {:error, ...}`.

### Identity and ordering

- **INV-APPEND-001** — Every event appended MUST be assigned a unique
  `event_id`.
- **INV-APPEND-002** — Every event MUST be assigned a `stream_version`
  in its stream. Versions MUST be contiguous integers starting at 1, with
  no gaps. The first event ever appended to a stream is version 1.
- **INV-APPEND-003** — Every event MUST be assigned a globally-unique,
  monotonically-increasing, gapless `event_number`. This number orders
  the event amongst *all* events in the store, across all streams.
- **INV-APPEND-004** — A successful `append_to_stream` call appending
  N events MUST result in N events whose `stream_version` values are
  contiguous and whose `event_number` values are contiguous. (No
  interleaving with concurrent appends inside a single call.)
- **INV-APPEND-005** — `created_at` MUST be a UTC timestamp set at
  append time and MUST NOT decrease across the global `event_number`
  ordering, modulo clock skew tolerated by the implementation.
  (Reference adapter uses `NOW()` and does not enforce monotonicity
  beyond what the clock guarantees.)

### Atomicity

- **INV-APPEND-006** — `append_to_stream` MUST be atomic with respect
  to N events: either all N are persisted with their assigned versions
  and numbers, or none are.
- **INV-APPEND-007** — The atomicity boundary includes the per-stream
  version bump *and* the global event_number assignment *and* the
  event row itself. A reader MUST NOT see an event whose stream version
  or global number was assigned to a sibling event that did not also
  succeed.

### Expected-version semantics

`expected_version` is one of: `:any_version`, `:no_stream`,
`:stream_exists`, or a non-negative integer.

- **INV-APPEND-010** — `:any_version` MUST append without checking the
  current stream version. The stream is created if it does not exist.
- **INV-APPEND-011** — `:no_stream` MUST succeed only if the stream
  does not exist. If the stream exists, it MUST return
  `{:error, :stream_exists}`.
- **INV-APPEND-012** — `:stream_exists` MUST succeed only if the stream
  already exists. If the stream does not exist, it MUST return
  `{:error, :stream_not_found}`.
- **INV-APPEND-013** — A non-negative integer V MUST succeed only if
  the stream's current version equals V *at the moment of append*. If
  the current version is anything else (including the stream not
  existing when V > 0), it MUST return
  `{:error, :wrong_expected_version}`.
- **INV-APPEND-014** — V = 0 MUST succeed against a non-existent stream
  (creating it) or against a stream whose current version is 0. (See
  conformance test `should fail to append to a stream because of wrong
  expected version when no stream`: V=1 against a missing stream is
  `:wrong_expected_version`, implying V=0 is the "fresh stream" case.)

### Concurrent appends

- **INV-APPEND-020** — Under concurrent appends targeting the same
  stream with integer `expected_version`, at most one MAY succeed for a
  given value of V. All others MUST receive
  `{:error, :wrong_expected_version}`.
- **INV-APPEND-021** — Under concurrent appends with `:any_version`,
  all MAY succeed; the relative ordering of their assigned versions and
  event_numbers is defined by the implementation. (No serialisation
  promise is made to the *caller*, only the post-condition that the
  resulting per-stream and global sequences remain gapless and
  contiguous.)
- **INV-APPEND-022** **[reference-only]** — The reference adapter
  enforces optimistic locking via a **unique constraint on
  `stream_events (stream_id, stream_version)`**, not via a
  `WHERE current_version = expected` predicate. Two concurrent
  appenders both computing "next version = 6" both try to insert that
  row; one succeeds, the other gets `unique_violation`, which the
  adapter translates to `:wrong_expected_version`. **The abstract
  contract does not mandate this mechanism**; `instructed` may use a
  predicated update, a per-stream advisory lock, or a constraint, so
  long as INV-APPEND-013 and INV-APPEND-020 hold.

### Idempotency and replay

- **INV-APPEND-030** — Re-appending an event with an `event_id` that
  already exists in the store MUST NOT silently succeed and MUST NOT
  duplicate the event. The reference adapter returns
  `{:error, :duplicate_event}` in this case. The abstract contract
  permits either an explicit duplicate-event error or a
  `:wrong_expected_version` error if the implementation chooses to
  derive `event_id` from `(stream_id, stream_version)`. (Commanded's
  aggregate layer never relies on this distinction; it always supplies
  fresh UUIDs.)

### Immutability after append

- **INV-APPEND-040** — A persisted event MUST NOT be modified by any
  later operation. The reference adapter enforces this with triggers
  that raise on `UPDATE` and `DELETE` against the `events` and
  `stream_events` tables.
- **INV-APPEND-041** — A persisted event MUST NOT be deleted, except
  through an explicit hard-delete administrative operation gated
  behind a configuration flag. `instructed` MAY choose not to provide
  any deletion path at all in v1.

### Error closed set

Returned errors from `append_to_stream` are drawn from this set:

- `:ok`
- `{:error, :wrong_expected_version}`
- `{:error, :stream_exists}` (only with `expected_version = :no_stream`)
- `{:error, :stream_not_found}` (only with `expected_version = :stream_exists`)
- `{:error, :duplicate_event}` **[reference-only, optional]**
- `{:error, :duplicate_stream_uuid}` **[reference-only, internal-retried, not propagated]**
- `{:error, other}` for storage / serialisation failures.

---

## Part C — Read (`stream_forward`)

Signature:
`stream_forward(meta, stream_uuid, start_version, read_batch_size) :: Enumerable.t | {:error, ...}`.

- **INV-READ-001** — Reading a stream that has never been appended to
  MUST return `{:error, :stream_not_found}`.
- **INV-READ-002** — Reading a stream that exists MUST return an
  enumerable of `RecordedEvent` in **strictly increasing
  `stream_version` order**.
- **INV-READ-003** — The enumerable MUST be exhaustive for the
  requested range. Implementations MAY page the underlying queries
  internally (the `read_batch_size` argument), but the consumer-visible
  sequence MUST contain every event whose `stream_version >=
  start_version`.
- **INV-READ-004** — `start_version` is **inclusive**.
- **INV-READ-005** — Reading the special `:all` stream MUST return all
  events in the store ordered by strictly increasing `event_number`.
  (The reference adapter implements `:all` as a real stream with
  `stream_id = 0` into which every event is linked at append time.)
- **INV-READ-006** — Each `RecordedEvent` returned MUST carry the
  *original* stream identity in its `stream_id` field, even when
  delivered via the `:all` stream. (This is the meaning of the
  reference adapter's `original_stream_id` / `original_stream_version`
  columns.)
- **INV-READ-007** — `RecordedEvent.stream_version` returned via `:all`
  MUST be the event's *per-stream* version in its original stream, not
  its position within `:all`.
- **INV-READ-008** — The `event_number` field MUST be the global
  position in `:all`, regardless of which stream the read came from.

### Concurrency with appends

- **INV-READ-020** — A reader started after an append `A` MUST observe
  the events of `A`. A reader started before `A` returns an enumerable
  whose contents up to that point are not affected by `A`; whether the
  enumerable lazily picks up `A`'s events on a later page is
  implementation-defined and Commanded does not rely on either
  behaviour.

---

## Part D — Snapshots (`record_snapshot` / `read_snapshot` /
`delete_snapshot`)

- **INV-SNAP-001** — At most one snapshot exists per `source_uuid`.
- **INV-SNAP-002** — `record_snapshot` is an upsert: if a snapshot for
  `source_uuid` exists, it is replaced wholesale (all fields, including
  `source_version`, `source_type`, `data`, `metadata`).
- **INV-SNAP-003** — `read_snapshot` for a missing `source_uuid` MUST
  return `{:error, :snapshot_not_found}`.
- **INV-SNAP-004** — `delete_snapshot` is idempotent. Deleting a
  missing snapshot returns `:ok` (per the conformance test sequence:
  record → delete → assert subsequent `read_snapshot` returns
  `:snapshot_not_found`).
- **INV-SNAP-005** — Snapshots are NOT versioned history. There is no
  obligation to retain past snapshots.
- **INV-SNAP-006** — Snapshots are advisory. They MUST NOT be required
  for correct aggregate reconstruction: the event stream from version
  0 onwards is always the source of truth.

---

## Part E — Subscriptions

The adapter exposes two flavours of subscription:

- **Transient** (`subscribe/2`) — fire-and-forget pub/sub for a single
  process, no persistent cursor, no ack.
- **Persistent** (`subscribe_to/6`) — named, durable cursor, ack-based,
  redelivery on reconnect.

Plus `ack_event/3`, `unsubscribe/2`, `delete_subscription/3`.

### Transient subscriptions

- **INV-SUB-T-001** — `subscribe(meta, stream_uuid_or_:all)` MUST cause
  the calling process to receive `{:events, events}` messages for every
  event appended to that stream (or any stream, if `:all`) **after the
  subscription is established**.
- **INV-SUB-T-002** — Transient subscriptions MUST NOT replay history.
  Only events appended after subscription are delivered.
- **INV-SUB-T-003** — Transient subscriptions require no acknowledgement.
- **INV-SUB-T-004** — Subscribing to a specific `stream_uuid` MUST NOT
  deliver events from other streams.
- **INV-SUB-T-005** — Transient subscriptions are lost when the
  subscriber process exits. There is no persistent state.

### Persistent subscriptions — identity

- **INV-SUB-P-001** — A persistent subscription is identified by the
  pair `(stream_uuid_or_:all, subscription_name)`. Two subscriptions
  with the same pair are the same subscription.
- **INV-SUB-P-002** — `subscribe_to` is *idempotent with respect to
  persistent state*: subscribing again with the same identity attaches
  to the existing subscription and resumes from its `last_seen`
  cursor.

### Persistent subscriptions — single-active-subscriber

- **INV-SUB-P-010** — At most `concurrency_limit` (default 1) live
  subscribers MAY be attached to a persistent subscription at any one
  time. Attempts to attach beyond the limit MUST receive
  `{:error, :too_many_subscribers}` (or
  `:subscription_already_exists` at the default limit of 1).
- **INV-SUB-P-011** **[reference-only]** — The reference adapter
  enforces this with `pg_try_advisory_lock` keyed on the subscriptions
  table OID and the subscription's primary key, *per session*. The
  persistent row does not record "who is attached"; the lock is held
  only as long as the subscriber's database session is alive. This is
  a mechanism, not a contract requirement.
- **INV-SUB-P-012** — When a live subscriber disconnects (process exit,
  network failure, lease loss), its slot MUST become available for
  another attach without administrative action.

### Persistent subscriptions — start position

- **INV-SUB-P-020** — On *first* subscribe with a given identity, the
  `start_from` parameter determines the initial cursor position:
  - `:origin` → start at event_number 0 (first event delivered is #1).
  - `:current` → start at the current global head; no historical
    events delivered.
  - integer N → start at event_number N (first event delivered is #N+1).
- **INV-SUB-P-021** — On *subsequent* subscribes with the same identity,
  `start_from` MUST be ignored; the subscription resumes from
  `last_seen`.

### Persistent subscriptions — delivery and ack

- **INV-SUB-P-030** — Events MUST be delivered in strictly increasing
  order:
  - For a single-stream subscription: by `stream_version`.
  - For an `:all` subscription: by global `event_number`.
- **INV-SUB-P-031** — Delivery is **at-least-once**. A subscriber that
  fails to ack before disconnecting MUST receive the unacked events
  again on reconnect.
- **INV-SUB-P-032** — `ack_event(meta, subscriber, recorded_event)`
  advances the cursor to the event's number (or stream_version for
  single-stream subscriptions). Acking event N MUST be taken as
  acknowledging all events up to and including N.
- **INV-SUB-P-033** — Until ack, the subscription MUST NOT advance its
  persistent cursor past unacked events. (The store may have already
  delivered later events to the subscriber's in-flight buffer; this is
  acceptable as long as the durable cursor is not advanced.)
- **INV-SUB-P-034** — Subscriber implementations are expected to
  ack-in-order. The abstract contract does not specify out-of-order
  ack behaviour beyond INV-SUB-P-032.

### Persistent subscriptions — partitioned consumers

- **INV-SUB-P-040** — When `concurrency_limit > 1`, the subscription
  MAY have multiple live subscribers. The store MUST distribute events
  across them.
- **INV-SUB-P-041** — When a `partition_by` function is supplied, all
  events for which `partition_by(event)` returns the same value MUST
  be delivered to the same subscriber (modulo subscriber failures and
  rebalances). Order is preserved within a partition.
- **INV-SUB-P-042** — Without `partition_by`, the contract is silent
  on which subscriber receives which event; only "every event is
  delivered to exactly one of the live subscribers" is guaranteed.

(See [`maybe-later.md`](maybe-later.md) ML-0001 — partitioned
consumers are deferred in `instructed` v1. The above remain in the
invariant catalogue so we know what we are deferring.)

### Persistent subscriptions — selector

- **INV-SUB-P-050** — An optional `selector` predicate filters events
  before delivery. Events for which the predicate returns false are
  not delivered to the subscriber, **but the persistent cursor MUST
  still advance past them** (otherwise an unmatched event would
  permanently block the subscription).

### Persistent subscriptions — lifecycle

- **INV-SUB-P-060** — `unsubscribe(meta, subscription)` detaches a
  live subscriber. It MUST NOT delete the persistent cursor.
  Resubscribing with the same identity resumes from `last_seen`.
- **INV-SUB-P-061** — `delete_subscription(meta, stream, name)`
  removes the persistent cursor. A subsequent `subscribe_to` with the
  same identity behaves as a first subscribe (honours `start_from`).
- **INV-SUB-P-062** — `delete_subscription` on a non-existent
  subscription MUST return `{:error, :subscription_not_found}`. (Per
  the adapter behaviour; the reference adapter actually returns `:ok`
  silently — this is an instance where the reference adapter is more
  lenient than the abstract contract.)

### Subscription error closed set

- `:ok`
- `{:ok, subscription}`
- `{:error, :subscription_already_exists}`
- `{:error, :too_many_subscribers}`
- `{:error, :subscription_not_found}` (delete only)
- `{:error, other}`

---

## Part F — Cross-cutting invariants

### Causation and correlation

- **INV-META-001** — `causation_id` and `correlation_id`, when set on
  input `EventData`, MUST be persisted and echoed on the corresponding
  `RecordedEvent`. The store assigns no meaning to them; they are
  application-level metadata.

### Event types and serialisation

- **INV-META-010** — `event_type` is a string chosen by the caller
  (typically a module name) and is opaque to the store.
- **INV-META-011** — `data` and `metadata` round-trip through a
  serializer chosen by the application (`Jason`, term-binary, etc.).
  The store contract treats them as opaque payloads; equality after
  round-trip is the application's concern.

### Hard delete

- **INV-DELETE-001** — The reference adapter exposes a gated
  hard-delete path (a session-local `eventstore.enable_hard_deletes`
  setting). The abstract contract does not require it. `instructed`
  MAY omit it entirely.

### Streams as a first-class concept

- **INV-STREAM-001** — A stream is identified by an opaque string
  (`stream_uuid`). The store MUST treat this string as the stable
  external identity of the stream.
- **INV-STREAM-002** **[reference-only]** — Internally the reference
  adapter assigns a numeric `stream_id` per stream and uses it for
  joins. This is an implementation detail; the contract is only on
  `stream_uuid`.
- **INV-STREAM-003** — The reserved name `$all` (in the reference
  adapter; or the atom `:all` at the API level) refers to the global
  stream. A user-supplied `stream_uuid` MUST NOT collide with this.

### Linked events

- **INV-LINK-001** **[reference-only]** — The reference adapter
  supports "linking" an existing event by `event_id` into additional
  streams (the `stream_events` join table with `original_stream_id`
  != `stream_id`). This enables building category streams without
  duplicating event rows. **The Commanded adapter behaviour does not
  expose this**; it is only used internally to populate `$all`.
  `instructed` MAY omit user-facing linking. If we keep `$all` as an
  internal feature, we MUST decide how to populate it (insert-time vs
  query-time view).

---

## Part G — Things the adapter contract does NOT specify

For symmetry, the contract is silent on these. They surface higher up
(in the aggregate / handler / process manager layers, covered in
Phase 3) or in operational concerns:

- Per-aggregate command serialisation. (Provided by Commanded's
  `Aggregate` GenServer; not by the event store.)
- Optimistic-locking retry on `:wrong_expected_version`. (Provided by
  Commanded's `ExecutionContext`.)
- Snapshot policy (when to take snapshots, version compatibility).
  (Provided by Commanded's `Snapshotting`.)
- Strong-consistency-on-dispatch waiting on handler acks. (Provided by
  Commanded's `Subscriptions.Registry`.)
- Process manager state persistence. (Provided by Commanded's
  `ProcessManagers.ProcessManagerInstance`.)
- Pubsub / event broadcast for non-CQRS internal use.
- Multi-tenant schema isolation (the reference adapter's `schema`
  config option).
- Pagination of stream listings, search, administrative views.

Phase 3 produces a parallel catalogue for these layers.

---

## Phase 2 status

Every callback in the adapter behaviour is covered:

| Callback                       | Covered in |
|--------------------------------|------------|
| `child_spec/2`                 | Out of scope (host concern). |
| `append_to_stream/5`           | Part B. |
| `stream_forward/4`             | Part C. |
| `subscribe/2`                  | Part E (transient). |
| `subscribe_to/6`               | Part E (persistent). |
| `ack_event/3`                  | Part E (INV-SUB-P-032..033). |
| `unsubscribe/2`                | Part E (INV-SUB-P-060). |
| `delete_subscription/3`        | Part E (INV-SUB-P-061..062). |
| `read_snapshot/2`              | Part D. |
| `record_snapshot/2`            | Part D. |
| `delete_snapshot/2`            | Part D. |

Every field on `RecordedEvent` has a sourcing rule in Part A.

Open question raised during this phase:

- **OQ-0001** — The contract does not specify ordering of events when
  multiple appends happen concurrently with `:any_version`. The
  reference adapter happens to serialise them through the `$all`
  stream's row-level lock. `instructed` will need to pick an
  equivalent serialisation point; revisit in Phase 4.

(Open questions get their own document once a second one lands.)
