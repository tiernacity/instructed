# Design: Fix Postgres Event Store Subscription Delivery

> **Date**: 2026-02-19
> **Status**: Proposed
> **Relates to**: REVIEW.md §8, §11, §15

## Problem

Under concurrent load, the postgres event store adapter loses events from
persistent subscriptions. Events are written to the database successfully but
never delivered to event handlers or projections.

**Reproduction**: `./stress-test.sh 10000 100` against the todo-server with
`--store postgres`. All 10,000 dispatches succeed, but ~16 todos are missing
from projections.

**Root cause**: Two interacting defects in `instructed_postgres.gleam`.

### Defect 1: Out-of-order notification delivery

`append_to_stream` runs in the caller's process (not a serialised actor). After
inserting events, it reads them back and pushes `NotifyEvent` messages to a
notifier actor, which forwards them to subscription handler callbacks.

Under concurrency, PostgreSQL's `BIGSERIAL` assigns `event_number` values at
INSERT time, but transactions commit in arbitrary order. Notification messages
therefore arrive at the handler in commit-completion order, not `event_number`
order.

The event handler's idempotency guard (`event_handler.gleam` line 308) skips
any event where `event_number <= last_seen_event`. When a low-numbered event
commits after a high-numbered one, it is permanently skipped — the projection
never sees it.

### Defect 2: No transactional OCC

`append_to_stream` performs a separate `SELECT MAX(stream_version)` before
individual `INSERT` statements, with no transaction. Two concurrent appends to
the same stream can both read version 0, both pass the `NoStream` check, and
race on the UNIQUE constraint. The loser gets a generic `StorageError` instead
of `VersionConflict`, so the aggregate server's OCC retry loop does not fire.

(This defect is masked in the stress test's Test 1 because each todo has a
unique aggregate ID. It would surface with concurrent mutations to the same
aggregate.)

### Why in-memory and SQLite don't have this bug

Both adapters serialise **all** operations (append + notify) through a single
OTP actor's message loop. Event numbers are assigned and subscribers notified in
the same sequential step. The notification order always matches `event_number`
order. No transaction boundary issues exist because there is no concurrent
database access.

## Reference: How Commanded's EventStore Solves This

The [commanded/eventstore](https://github.com/commanded/eventstore) Elixir
library uses three architectural elements:

### 1. Database trigger for notification (`pg_notify`)

A PostgreSQL trigger on the `streams` table fires `pg_notify` with a payload of
`stream_uuid,stream_id,from_stream_version,to_stream_version`. The NOTIFY is
part of the INSERT transaction — it becomes visible to listeners only after the
commit. The payload carries no event data, only a version range.

### 2. Notification is a wake-up signal; events are always read from the DB

A `Listener` (GenStage producer) receives `pg_notify` messages and enqueues
lightweight `Notification` structs. A `Publisher` (GenStage consumer with
`max_demand: 1`) reads each notification, then fetches the actual events from
PostgreSQL via `read_stream_forward(stream_id, from_version, count)`. Events
are broadcast to subscribers via PubSub.

The event data always comes from an ordered database read, never from the
notification payload.

### 3. Subscription FSM with gap detection and catch-up

The `SubscriptionFsm` maintains `last_received`, `last_sent`, and `last_ack`
cursors. When it receives events via `notify_events`:

- If `first_event_number < expected` → ignore (already seen)
- If `first_event_number > expected` → transition to `:request_catch_up`, which
  reads from the database starting at `last_sent + 1`
- If `first_event_number == expected` → enqueue for delivery

Gap detection guarantees gapless, ordered delivery even if PubSub notifications
arrive out of order.

### 4. Transactional append with OCC in a single SQL CTE

The `insert_events.sql.eex` template is a single SQL CTE that atomically:
creates/updates the stream, inserts events, links events to the `$all` stream,
and returns the new stream_id. The expected version check is part of the same
statement. A unique constraint violation maps to `:wrong_expected_version`.

## Design

### Principle

Adopt Commanded's core insight: **notifications are wake-up signals, not the
delivery mechanism.** Subscriptions must always read events from the database in
order. This makes the system immune to out-of-order commits, `BIGSERIAL`
visibility gaps, and notification loss.

### Non-goals

- PostgreSQL `LISTEN/NOTIFY` via a trigger. This requires DDL changes and a
  dedicated Postgrex notifications connection. It's an optimisation for latency
  and can be added later. Polling achieves correctness with simpler code.
- Rewriting the in-memory or SQLite adapters. They are correct as-is.
- Changing the `EventStore` trait interface. The fix must work within the
  existing record-of-functions contract.

### Component 1: Subscription Poller Actor

A new actor, one per persistent subscription, owned by the postgres adapter.
It replaces the current model where the handler callback is registered as a
transient subscriber on the notifier.

```
┌─────────────────────────────────────────────────────────┐
│                   Postgres Adapter                      │
│                                                         │
│  append_to_stream ──► INSERT events                     │
│                       then send Wake to poller(s)       │
│                                                         │
│  ┌──────────────────────────────────────────────┐       │
│  │  SubscriptionPoller actor (one per sub)      │       │
│  │                                              │       │
│  │  state:                                      │       │
│  │    last_seen: Int        (cursor)            │       │
│  │    handler: fn(event)    (callback)          │       │
│  │    poll_interval: Int    (ms, fallback)      │       │
│  │    stream: String        ("$all" or id)      │       │
│  │                                              │       │
│  │  on Wake or timer:                           │       │
│  │    1. SELECT events WHERE number > last_seen │       │
│  │       ORDER BY event_number ASC              │       │
│  │       LIMIT batch_size                       │       │
│  │    2. For each event: call handler           │       │
│  │    3. Update last_seen = max delivered       │       │
│  │    4. If batch was full, immediately poll    │       │
│  │       again (drain loop)                     │       │
│  │    5. Otherwise, arm timer for next poll     │       │
│  └──────────────────────────────────────────────┘       │
│                                                         │
│  Notifier actor (retained, simplified)                  │
│    - Still handles transient subscribers                │
│    - Still receives NotifyEvent from append             │
│    - Additionally sends Wake to all pollers             │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

**Polling vs pure push**: The poller uses a hybrid model. `append_to_stream`
sends a `Wake` message to the notifier, which forwards it to all registered
pollers. This gives low-latency delivery in the common case. A fallback timer
(default 1000ms, configurable) ensures liveness if a wake is lost (process
crash, message drop). The timer is reset on each successful poll so it does not
add overhead during active periods.

**Batch size**: The poller reads up to 1,000 events per poll. If the batch is
full, it immediately polls again without waiting for a wake or timer. This
handles large backlogs efficiently.

**Ordering guarantee**: Events are always read from postgres with
`ORDER BY event_number ASC`. The poller's `last_seen` cursor advances
monotonically. The event handler's idempotency guard (`event_number <=
last_seen`) remains valid because events now arrive in order.

**Startup / catch-up**: When `subscribe_persistent` is called, the poller
initialises `last_seen` from the subscription's `last_seen_event_number` in the
database (or from `start_from` for new subscriptions). It immediately performs
a poll to deliver any historical events. There is no gap between historical
delivery and live delivery because both use the same poll-from-cursor mechanism.

This eliminates the current race in `subscribe_persistent` where historical
events are delivered, then a transient subscription is registered, and events
in between are lost.

### Component 2: Transactional Append with Proper OCC

Replace the current SELECT-then-INSERT sequence with a single SQL transaction:

```sql
BEGIN;

-- Read current version inside the transaction (row-level lock via FOR UPDATE
-- on a streams tracking table, or simply relying on the UNIQUE constraint)
SELECT COALESCE(MAX(stream_version), 0)
FROM event_store_events
WHERE stream_id = $1;

-- Validate expected_version (application-side, within the transaction)
-- If mismatch: ROLLBACK, return VersionConflict

-- Insert all events in one statement
INSERT INTO event_store_events
  (event_id, stream_id, stream_version, event_type, data, metadata,
   causation_id, correlation_id, created_at)
VALUES ($1, $2, $3, ...), ($1, $2, $4, ...), ...;

COMMIT;
```

If the UNIQUE constraint on `(stream_id, stream_version)` is violated, catch
the postgres error and map it to `VersionConflict` (not `StorageError`). This
allows the aggregate server's OCC retry loop to function correctly.

Note: `pog` supports transactions via `pog.transaction`. The implementation
should use this to wrap the version check and insert into a single atomic unit.

**Bonus — gapless event_number assignment**: Consider adding a `streams` table
(as Commanded does) to track the `$all` stream version explicitly, with an
`UPDATE ... RETURNING` to allocate contiguous event_number ranges atomically.
This eliminates `BIGSERIAL` gaps. However, this is an optimisation — the
poll-based subscription model is correct even with gaps, because it reads
`WHERE event_number > last_seen ORDER BY event_number ASC`, which naturally
skips gaps.

### Component 3: Notifier Simplification

The `NotifierMessage` type changes:

- `NotifyEvent(stream_id, event)` → `Wake` (no event payload)
- `AddTransientSub` — retained for transient (non-persistent) subscribers
- `AddPoller(Subject(PollerMessage))` — registers a poller for wake forwarding
- `RemovePoller(Subject(PollerMessage))` — deregisters a poller
- `RemoveSub` — retained

On `Wake`, the notifier iterates over registered pollers and sends each a
`PollNow` message. It also still delivers to transient subscribers as before
(transient subscribers are used by aggregate server self-subscription, which
needs individual events pushed — this path is unchanged).

For transient subscribers, `append_to_stream` still reads back and sends full
events via `NotifyEvent`. These are two separate notification paths:

```
append_to_stream
  ├─► NotifyEvent(stream_id, event)  → transient subscribers (aggregate self-sub)
  └─► Wake                          → pollers (persistent subscriptions)
```

### Changes by File

| File | Change |
|------|--------|
| `instructed_postgres.gleam` | Add `SubscriptionPoller` actor type and `start_poller` function. Rewrite `subscribe_persistent` to start a poller instead of registering a transient sub. Wrap append in `pog.transaction`. Map UNIQUE violation to `VersionConflict`. Add `Wake`/`AddPoller`/`RemovePoller` to `NotifierMessage`. Send `Wake` after successful append. |
| `instructed_postgres.gleam` | Keep `NotifyEvent` path for transient subscribers (used by aggregate self-subscription). |
| `instructed/event_handler.gleam` | No changes needed. The idempotency guard remains correct because events now arrive in order. |
| `instructed/in_memory_event_store.gleam` | No changes. |
| `instructed_sqlite.gleam` | No changes. |

### Why Not Just Serialise Through a Single Actor (Like SQLite)?

Routing all postgres operations through one actor would fix ordering but would
make postgres no faster than SQLite for writes — defeating the purpose of using
a concurrent database. The poll-based model preserves concurrent write
throughput while guaranteeing ordered read delivery.

### Sequence Diagram: Append + Delivery (After Fix)

```
Caller A             Caller B             Postgres        Notifier      Poller       Handler
   |                    |                    |               |            |             |
   |--- BEGIN TX ------>|                    |               |            |             |
   |                    |--- BEGIN TX ------>|               |            |             |
   |                    |    INSERT evt#2    |               |            |             |
   |                    |    COMMIT -------->|               |            |             |
   |                    |--- Wake ---------------------------------------->|             |
   |    INSERT evt#1    |                    |               |            |             |
   |    COMMIT -------->|                    |               |            |             |
   |--- Wake --------------------------------------------- >|            |             |
   |                    |                    |               |-- PollNow->|             |
   |                    |                    |               |            |             |
   |                    |                    |  SELECT WHERE |            |             |
   |                    |                    |  num > 0      |            |             |
   |                    |                    |  ORDER BY num |            |             |
   |                    |                    |<-- evt#1,#2 --|            |             |
   |                    |                    |               |            |-- evt#1 --->|
   |                    |                    |               |            |-- evt#2 --->|
   |                    |                    |               |            |             |
```

Even though evt#2 committed before evt#1, the poller reads both in order from
postgres. The handler sees evt#1 then evt#2. No events are skipped.

### Testing

#### Integration test

The existing stress test is the primary acceptance test:

```
./stress-test.sh 10000 100   # with --store postgres
./stress-test.sh 10000 100   # with --store sqlite
./stress-test.sh 10000 100   # with --store memory
```

All three must pass with 0 failures.

#### Event Store Adapter Conformance Test Suite

Commanded provides a shared test suite in `test/event_store/support/` that
every adapter must pass. The tests are defined as reusable ExUnit test case
modules (`AppendEventsTestCase`, `SubscriptionTestCase`, `SnapshotTestCase`).
Each adapter (in-memory, postgres via commanded-eventstore-adapter, EventStoreDB
via commanded-extreme-adapter) `use`s these same modules, running the identical
test suite against its own backend. This catches adapter-specific bugs that
unit tests miss.

Currently, instructed has **no shared test suite**. Each adapter has its own
copy-pasted tests (`instructed_postgres_test.gleam`, `instructed_sqlite_test.gleam`,
`instructed/test/event_store_test.gleam`). The postgres tests are notably
weaker — they don't test subscription resume, ordered delivery, concurrent
append OCC, or gap-free event_number assignment.

**We should create a shared conformance test suite** in the `instructed` package
that all three adapters import and run. Gleam doesn't have ExUnit's `use`-based
test case injection, but we can achieve the same thing with a module that
exports test functions accepting an `EventStore(event)` factory function.

##### What the suite should cover

Based on Commanded's test cases, mapped to our `EventStore` record-of-functions
interface:

**1. Append events** (`append_events_conformance`)
- Append events with `ExactVersion` — sequential versions succeed
- Append with `AnyVersion` — always succeeds regardless of current version
- Append with `NoStream` — succeeds on new stream, fails on existing
- Append with `StreamExists` — succeeds on existing, fails on new
- Wrong expected version returns `VersionConflict` (not `StorageError`)
- Read from unknown stream returns `StreamNotFound`
- Read events back: correct data, stream_version, event_type, metadata,
  causation_id, correlation_id
- Read with start_version offset and count limit (batched reads)
- Read from single stream doesn't return events from other streams
- `read_all_forward` returns events across all streams in event_number order

**2. Persistent subscriptions** (`subscription_conformance`)
- Subscribe from `:origin` — receives all events including historical
- Subscribe from `:current` — skips existing events, receives only new ones
- Subscribe from `FromEventNumber(n)` — receives events from n onwards
- Subscription to specific stream only receives events from that stream
- Subscription to `$all` receives events from all streams
- Duplicate subscription name returns `SubscriptionAlreadyExists`
- **Events delivered in event_number order** (the bug we're fixing)
- **Resume from checkpoint**: subscribe, receive + ack some events, delete
  subscription handler, re-subscribe — resumes from last ack'd position,
  un-ack'd events are re-delivered
- **No gap in delivery**: append N events concurrently, verify subscriber
  receives exactly N events with no gaps and no duplicates
- Delete subscription, re-subscribe from origin — replays all events
- Ack advances the checkpoint — verified by re-subscribing after ack

**3. Snapshots** (`snapshot_conformance`)
- Record and read back a snapshot — data matches
- Read non-existent snapshot returns `SnapshotNotFound`
- Delete snapshot — subsequent read returns `SnapshotNotFound`
- Overwrite snapshot (same source_uuid) — read returns latest

**4. Transient subscriptions** (`transient_subscription_conformance`)
- Subscribe to all streams — receives events from any stream
- Subscribe to specific stream — only receives events from that stream
- Unsubscribe — stops receiving events

**5. Concurrency** (`concurrency_conformance`) — **new, not in Commanded**
- Concurrent appends to different streams: all succeed, all events readable
- Concurrent appends to same stream with `ExactVersion`: exactly one succeeds
  per version, others get `VersionConflict`
- Concurrent appends + persistent subscription: subscriber receives all events
  in order with no gaps (this is the stress test scenario, distilled)

##### Implementation sketch

```gleam
// instructed/test/conformance/append_events.gleam

import instructed/event_store.{type EventStore}

/// Run all append conformance tests against the given event store.
/// The factory function creates a fresh, empty event store for each test.
pub fn run_all(factory: fn() -> EventStore(TestEvent)) -> Nil {
  test_append_exact_version(factory())
  test_append_any_version(factory())
  test_append_no_stream(factory())
  test_append_stream_exists(factory())
  test_wrong_version_returns_version_conflict(factory())
  test_read_nonexistent_stream(factory())
  test_read_events_correct_data(factory())
  test_read_batched(factory())
  test_read_single_stream_isolation(factory())
  test_read_all_forward_ordering(factory())
}
```

Each adapter's test file becomes:

```gleam
// instructed_postgres/test/instructed_postgres_test.gleam

import conformance/append_events
import conformance/subscription
import conformance/snapshot
import conformance/concurrency

pub fn append_conformance_test() { append_events.run_all(pg_factory) }
pub fn subscription_conformance_test() { subscription.run_all(pg_factory) }
pub fn snapshot_conformance_test() { snapshot.run_all(pg_factory) }
pub fn concurrency_conformance_test() { concurrency.run_all(pg_factory) }
```

##### Where the test suite lives

The conformance modules should live in the `instructed` package under
`test/conformance/`. The adapter packages (`instructed-postgres`,
`instructed-sqlite`) already depend on `instructed` and can import these modules
in their test code. The `instructed` package's own tests run the suite against
the in-memory adapter.

##### Priority

The conformance suite should be built **alongside the postgres fix**, not after.
The "concurrent appends + subscription ordering" test is the automated
reproduction of the bug we diagnosed. Writing it first gives us a failing test
to drive the fix.
