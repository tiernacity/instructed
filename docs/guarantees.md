# Guarantees

What `instructed` promises to applications, in plain language.
For the formal catalogue see [`invariants.md`](invariants.md);
for the mechanisms see [`architecture.md`](architecture.md).

---

## What gets stored

- Every event you append is stored durably with a globally
  unique id, a globally monotonic position number, and a
  per-stream position number — all assigned by the store.
- Events are immutable. The store has no API to modify or
  delete an event. Direct `UPDATE` / `DELETE` on the events
  tables raises an exception.
- The order of events on any one stream is exactly the order
  in which they were successfully appended. The global order
  across `$all` is also exactly the order in which appends
  committed.
- An N-event append is atomic: either all N persist with
  contiguous positions, or none do. Concurrent appends do not
  interleave within one call.

## What commands do

- A command dispatches against an aggregate, loads its state
  from the event log, runs the command handler, and appends
  any resulting events. The whole cycle is one logical
  operation from the caller's view.
- If two commands race for the same aggregate, the optimistic
  concurrency check means at most one wins each round. The
  loser re-reads and re-runs automatically, up to a
  configurable retry budget.
- A command that produces no events is a no-op as far as the
  store is concerned (no append happens).
- A command handler that throws or returns an error leaves
  the aggregate's state unchanged. No events are written.
- Command handlers and event appliers must be pure functions.
  Under contention the handler may be re-run from scratch on
  each OCC retry; side effects in the handler body will fire
  once per attempt, not once per command. Set deterministic
  identifiers on the command before dispatch, not inside the
  handler.

## What subscribers see

- Every subscriber sees every event for which it is
  responsible — every event on its subscribed stream (or
  every event in `$all`), modulo the subscriber's own routing
  decision (events the subscriber routes to `"ignore"` are
  skipped at the routing layer).
- Within a partition, events are delivered to the handler in
  strictly increasing `event_number` order. Across partitions,
  delivery is concurrent: a subscriber that partitions by some
  domain key (per-account, per-tenant, etc.) sees parallelism
  across keys with serial order within each key.
- Delivery is **at-least-once**. A subscriber that crashes
  mid-handler will see the work item redelivered when its
  per-item lease expires. Handlers must be written to tolerate
  seeing the same event more than once.
- A subscription is durable. The routing cursor and the work
  queue both survive process restarts.
- At any moment, at most one routing worker is active per
  subscription, and any number of processing workers may be
  active. Routing workers compete for the subscription lease;
  processing workers compete for individual work items.
  Neither path double-delivers.

## What `dispatch` returns

- By default, `dispatch` returns when the events are durably
  appended. Projections will catch up on their own time.
- If `consistency: [name1, name2, ...]` is supplied, `dispatch`
  returns only after the named subscriptions have caught up to
  the appended events. If a configured timeout elapses first,
  `dispatch` raises — but the events remain durably appended;
  only the wait failed.
- The consistency list must be explicit. There is no shorthand
  for "wait for everything".
- A per-stream subscription target can only wait on appends to
  its own stream. Passing a per-stream subscription target for
  an append to a different stream raises
  `ConsistencyTargetError` before the wait begins. `$all`
  targets are exempt.

## What snapshots give you

- Snapshots are advisory caches. Deleting a snapshot at any
  time changes load latency but not correctness — the events
  remain the source of truth.
- Snapshots are at-most-one per source: writing a snapshot
  replaces any prior one wholesale.
- The library provides the storage. *When* to snapshot is a
  per-aggregate policy choice you make. A typical choice is
  "every N events past the last snapshot".

## What process managers give you

- Process managers are the saga primitive. A PM subscribes to
  the event log, observes events, and dispatches commands —
  exactly the same primitives an application uses, run
  automatically on event delivery.
- A PM's state is persisted as a snapshot. On restart, the PM
  reads its state, sees which event_number it last processed,
  and absorbs any redelivery.
- Compensation is "whatever commands you dispatch when a
  failure event arrives". There is no separate compensation
  primitive. Many sagas need no explicit compensation because
  failures are atomic (the failed command emits a refusal
  event; the PM observes it and stops).

## What causation and correlation give you

- Every event a command produces carries
  `causation_id = command_id` — automatically.
- Every command a process manager dispatches carries
  `causation_id = triggering_event_id` — automatically.
- A `correlation_id` set on a top-level dispatch is propagated
  through every event and every downstream command in the
  resulting workflow — automatically.
- Both are opaque UUIDs to the store. You can query them
  directly against the JSONB metadata column for debugging or
  audit.

## What's promised about errors

- Every store-level error has a typed exception with a stable
  identifier. The [SQL contract](sql-contract.md) has the full
  catalogue.
- The errors a command can raise:
  - **`WrongExpectedVersionError`** (`IS001`) — concurrent
    update; usually retried automatically by the SDK.
  - **`StreamExistsError`** (`IS002`) — appended with
    `'no_stream'` to a stream that already exists.
  - **`StreamNotFoundError`** (`IS003`) — appended with
    `'stream_exists'` to a stream that doesn't.
  - **`DuplicateEventError`** (`IS004`) — re-used an
    `event_id` that already exists.
- The errors a subscriber can raise:
  - **`SubscriptionLeaseLostError`** (`IS022`) — another
    routing worker took over the subscription lease; the
    routing worker should stop.
  - **`WorkItemLeaseLostError`** (`IS030`) — another
    processing worker took over the in-flight work item; the
    processing worker should abort the item (redelivery via
    the next claim).
  - **`SubscriptionNotFoundError`** (`IS020`) — the
    subscription was deleted out from under a worker.
- The errors a snapshot operation can raise:
  - **`SnapshotNotFoundError`** (`IS010`) — read a snapshot
    that doesn't exist. The SDK typically catches and ignores
    this in the aggregate hydration path.

## What's NOT promised

A small but important list:

- **Aggregate state is not cached between commands.** Every
  dispatch reloads from the store. Snapshots make this cheap
  for long streams, but the load happens.
- **Handler atomicity with the cursor advance is not
  provided.** Handlers run outside any SDK transaction. The
  cursor advances in a separate short transaction after the
  handler returns successfully. Handlers must be idempotent.
- **Strong consistency is opt-in and explicit.** Without a
  `consistency: [...]` list, projections are eventually
  consistent — the gap between an append and a projection
  processing it is bounded by the projection's poll interval
  (typically tens to hundreds of milliseconds), not zero.
- **A single subscription has one active routing worker but
  may have many active processing workers.** Routing
  (event -> work-item) is single-active per subscription;
  processing (work-item -> handler -> ack) parallelises across
  partitions. Within a partition, processing is strictly
  serial; across partitions, it is concurrent.
- **Event payload schema evolution is the application's
  concern.** The store treats `data` and `metadata` as opaque
  JSONB. Upcasting old event shapes to new ones happens in
  application code, before the applier sees the event.
- **Snapshot schema evolution is the application's concern.**
  An aggregate that changes its state shape should stamp a
  version into `snapshot.metadata` and reject mismatched
  snapshots on load. The v1 TypeScript SDK does not do this
  for you.
- **Push delivery is not provided.** Workers poll. A future
  wake-up optimisation using `LISTEN`/`NOTIFY` is allowed but
  is not in the v1 correctness contract.
