# Architecture

How `instructed` realises the concepts in
[`concepts.md`](concepts.md) on top of Postgres. For the formal
catalogue of invariants the realisation must satisfy see
[`invariants.md`](invariants.md); for the SQL-level reference see
[`sql-contract.md`](sql-contract.md).

---

## Principles

1. **Postgres is the system of record.** All invariants — gapless
   ordering, optimistic locking, single-active-worker leasing —
   live in SQL: schema constraints, stored procedures with
   documented lock ordering, and a closed set of custom
   SQLSTATEs. SDKs are thin clients; they never hold
   invariant-bearing state.
2. **Pull, don't push.** Workers discover new events by polling.
   There is no `LISTEN`/`NOTIFY` in the correctness contract. A
   future wake-up optimisation is allowed only if every
   notification is droppable without affecting correctness.
3. **One subscription, one cursor, one active worker.** Leases
   provide failover; only one worker holds the lease at any
   moment.
4. **Handlers are opaque to the store and the SDK.** Projection
   targets are application-domain (Postgres, Elasticsearch,
   Redis, an HTTP API, an in-memory map). The SDK never passes
   a transaction or connection to a handler. Idempotency on
   redelivery is the handler's responsibility.
5. **No coordinator service.** Multiple SDK processes can
   dispatch and consume against the same Postgres without any
   external registry, leader election, or coordination plane.

## Schema

Six tables in the `instructed` schema:

| Table | Purpose |
|---|---|
| `streams` | One row per logical stream. The seeded row `(stream_id = 0, stream_uuid = '$all')` is the global stream. |
| `events` | Caller-keyed event rows. Append-only — `UPDATE` and `DELETE` triggers raise. |
| `stream_events` | The `(event, stream)` join. Carries per-stream version and original-stream identity. The unique constraint on `(stream_id, stream_version)` is the optimistic-locking mechanism. |
| `snapshots` | At most one row per `source_uuid`. Backs aggregate snapshots and process-manager state. |
| `subscriptions` | Persistent routing cursor + lease per `(stream_id, name)`. The routing worker advances this row; processing workers do not touch it. |
| `subscription_work_items` | The per-subscription work queue. One row per routed event per partition; carries the per-item lease (`claimed_by`, `lease_expires_at`) processing workers compete on. |

All mutation goes through stored procedures; the tables are not
written to directly. Triggers on `events` and `stream_events`
enforce append-only by raising on direct `UPDATE` / `DELETE`. The
`subscription_work_items` table cascades from
`subscriptions` on delete, so removing a subscription removes its
queued work.

## How an append works

`append_to_stream(stream_uuid, expected_version_type, expected_version, events, options)`
is one transaction. It:

1. Takes a row lock on `streams[target_stream]` (creating the row
   if `expected_version_type = 'any_version'` or `'no_stream'`
   permits it).
2. Validates the expected-version condition. On mismatch raises
   `IS001 wrong_expected_version`, or `IS002 stream_exists`, or
   `IS003 stream_not_found`.
3. Takes a row lock on `streams[$all]` and reserves a contiguous
   range of `event_number` values for the N incoming events with
   `UPDATE streams SET stream_version = stream_version + N WHERE stream_id = 0`.
4. Inserts N rows into `events` (one per event) and N rows into
   `stream_events` — one per `(event, target_stream)` pair with
   contiguous `stream_version`, and one per `(event, $all)` pair
   with the reserved `event_number` values.
5. Commits. Lock order: `streams[target] → streams[$all] →
   events → stream_events`.

This is **`$all`-as-stream with row-level lock**. The same
mechanism — a real row in `streams` for `$all`, locked for the
duration of the append — provides both global gapless ordering
and the serialisation point that makes concurrent appends with
`'any_version'` well-defined.

The unique constraint on `stream_events (stream_id, stream_version)`
is what makes optimistic concurrency cheap: two appenders both
computing "next version is V" both try to insert that row; one
succeeds, one fails with a Postgres `unique_violation`, which the
procedure translates to `IS001 wrong_expected_version`.

## How a command runs

The aggregate command cycle is implemented by the SDK as a loop
against the store:

```
loop up to retry_attempts times:
    snapshot      = read_snapshot(aggregate_uuid)        -- optional; IS010 is fine
    events_tail   = read_stream(aggregate_uuid, from = snapshot.version + 1)
    state         = fold(snapshot.state, events_tail)
    new_events    = command_handler(state, command)
    if new_events is empty: return
    try:
        append_to_stream(aggregate_uuid, 'exact', state.version, new_events)
        return
    on IS001 wrong_expected_version:
        continue                                          -- re-read, re-run
raise after exhausting retries
```

The retry on `IS001` is the per-aggregate serialisation
mechanism. There is no per-aggregate advisory lock, no
in-memory aggregate cache, and no aggregate registry. Two
concurrent commands on the same aggregate from different
processes will both go through this loop; one wins each round
of the race.

Snapshots are advisory. The application configures
`snapshot_every: N` on an aggregate; after a command commits N
or more events past the last snapshot, the SDK upserts a fresh
snapshot. A failed snapshot write does not fail the command.

## How a worker runs

A subscription is served by two cooperating worker kinds. Both
are spun up automatically by `Instructed.startWorker()`;
lower-level callers can compose them with `startRoutingWorker`
+ `startProjectionWorker` / `startPmWorker` directly.

### Routing worker (single-active per subscription, per-batch claim)

The SDK's routing worker claims the subscription's lease per
batch, not once at startup. At any single instant there is at
most one routing worker holding the lease for a given
subscription (INV-SUB-P-010); the *identity* of that worker
rotates per batch across whichever processes are polling. See
[D-0025](decisions.md#d-0025) for the rationale.

```
loop while not stopped:
    result = claim_subscription(stream, name, worker_id, lease_seconds)
    if result.result == 'already_claimed':
        sleep poll_interval                  -- another worker is mid-batch
        continue
    events = read_all(result.last_seen + 1, batch_size)   -- or read_stream
    if events empty:
        release_subscription(stream, name, worker_id)
        sleep poll_interval
        continue
    decisions = []
    for e in events:
        d = routeFn(e)                       -- pure; no I/O; fast
        if d != "ignore":
            decisions.append((e.event_number, d.partitionKey))
    try:
        route_batch(stream, name, worker_id,
                    decisions, max(events).event_number)
        -- ONE tx: cursor advance + work-item INSERTs
    catch IS022 subscription_lease_lost:
        continue        -- lease expired mid-batch; drop and re-loop
    release_subscription(stream, name, worker_id)

on shutdown:
    release_subscription(stream, name, worker_id)   -- best-effort
```

Key points:

- **Work-stealing across processes is the natural default.** M
  processes each running this loop for subscription S share the
  routing load: each tick is a race for `claim_subscription`,
  the winner does one batch, releases, and the next tick is a
  fresh race. No process monopolises a subscription. This is
  symmetric with the per-work-item claim model on the processing
  side. See [D-0025](decisions.md#d-0025).
- **`routeFn` is pure user code: no I/O, no aggregate loads,
  fast.** "Fast" is bounded: a batch that exceeds `lease_seconds`
  has its `route_batch` raise `IS022` and the work is redone by
  the next worker. The right configuration is `lease_seconds`
  comfortably larger than expected worst-case `batch_size ×
  routeFn` duration; if you can't bound that, shrink
  `batch_size`. A thrown `routeFn` stalls the worker (and is
  surfaced via `onError`) — the alternative, silent skip, would
  violate "no silent skip on any event".
- **`route_batch` commits the cursor advance and the work-item
  INSERTs in one transaction.** This atomicity is load-bearing
  for the catch-up predicate: once `last_seen >= N` is
  observable, the work items for events up to N are observable
  too.
- **The work-item PK absorbs duplicate INSERTs**
  (`ON CONFLICT DO NOTHING`). A crashed or `IS022`-aborted
  mid-batch routing worker is recoverable: the next routing
  worker re-reads from `last_seen` and the re-inserts are no-ops.
- **`IS022` during `route_batch` is not fatal.** It means the
  lease expired mid-batch (likely because `routeFn` was slower
  than `lease_seconds`) and another worker may have taken over.
  The SDK drops the partial batch and loops; the next
  `claim_subscription` decides what to do.
- **The SDK does not heartbeat.** The lease covers one batch;
  `route_batch` re-verifies `claimed_by` on each call, so a lost
  lease surfaces as `IS022` and the per-batch loop never needs a
  separate heartbeat.

### Processing worker (parallel per subscription)

```
loop while not stopped:
    claim = claim_work_item(stream, name, worker_id, lease_seconds)
        -- FOR UPDATE SKIP LOCKED + per-partition predicate
    if claim is null:
        sleep poll_interval
        continue
    event = read_all(claim.event_number, 1)
    in parallel, every heartbeat_interval:
        extend_work_item_claim(...)
    await handler(event, ctx)            -- NO SDK transaction
    on success:
        complete_work_item_projection(...)   -- DELETE the row, or
        complete_work_item_pm(...)           -- UPDATE-to-done + UPSERT snapshot, or
        complete_pm_instance(...)            -- terminal: DELETE snapshot + all items
    on handler throw:
        error policy decides retry-in / stop
        -- 'retry-in' re-runs handler against the same claim;
        -- 'stop' exits the worker; the lease expires and another
        --   processing worker takes over.
```

Key points:

- Multiple processing workers per subscription are normal and
  intended. They distribute work via `FOR UPDATE SKIP LOCKED`
  on the claim row; the per-partition predicate keeps
  within-partition ordering serial.
- The handler runs outside any SDK transaction. The terminal
  step (DELETE for projections, UPDATE+UPSERT for PMs) runs as
  its own short SDK-owned transaction after the handler
  returns. If the handler throws, no terminal step fires; the
  item stays `claimed` under the retry-in policy and the lease
  is held by the heartbeat.
- If the worker crashes (or the error policy returns `stop`)
  the lease expires and another processing worker takes the
  same item over. The original worker's next call to
  `extend_work_item_claim` / `complete_*` / `fail_work_item`
  raises `IS030 work_item_lease_lost`.

### PM-specific processing

A PM processing worker adds a state-load step before `handle`
and a dispatch step after:

1. Load state: read the partition's snapshot if its
   `$instructed.snapshot_module_version` matches; otherwise rebuild by
   folding every prior `done` work-item's event through
   `apply` from `initialState()`.
2. `apply(state, event)` -> staged_state.
3. `handle(staged_state, event)` -> `{ commands?, complete? }`.
4. Dispatch each command via `runCommand` on the same client.
   Lock sets stay disjoint by virtue of the SQL contract's
   per-procedure lock-acquisition orders — the dispatch path
   locks `streams` + the events tables; the persist-and-ack path
   locks `subscriptions` + `snapshots` + `subscription_work_items`
   (see [D-0026](decisions.md#d-0026)).
5. Terminal step: `complete_work_item_pm` (non-terminal,
   updates work item to `done` and upserts the snapshot in one
   tx) or `complete_pm_instance` (terminal, DELETEs the
   snapshot and every work item for the partition in one tx).

A poison event stalls only its **own partition**: other
partitions on the same PM type keep draining. Per-partition
isolation is a property of the work-queue substrate.

## How leases work

There are two lease scopes: the subscription-level (routing)
lease and the per-work-item (processing) lease.

### Lock vs lease

Two distinct mechanisms protect the routing surface, doing two
different jobs. Getting the distinction right matters for
reasoning about the per-batch loop and ML-0013-style extensions.

| | Postgres row lock | Application-level lease |
|---|---|---|
| **What** | `FOR UPDATE` on the `subscriptions` row | `claimed_by` + `claim_expires_at` columns |
| **Held for** | The duration of one procedure call (claim / extend / route_batch / release) | The lease window the SDK negotiated (one batch under D-0025) |
| **Released by** | `COMMIT` of the procedure call's transaction | Lease expiry, `release_subscription`, or another worker's takeover |
| **Scope** | Microseconds | Seconds (per batch) |
| **Enforces** | Atomic read-modify-write of one row | INV-SUB-P-010 "at most one live routing worker holds the lease at any moment" |

The routing worker does **not** hold a Postgres row lock for the
duration of a batch. `claim_subscription`'s transaction commits
as soon as `claimed_by` / `claim_expires_at` are written; the
row lock is released at that commit. The worker then spends its
batch time outside any subscription-row lock, reading events and
running `routeFn`. `route_batch` opens a second short
transaction, briefly re-locks the row, verifies `claimed_by =
worker_id` (this is the lease check, expressed as a column
comparison), advances `last_seen`, INSERTs work-items, and
commits.

INV-SUB-P-010 is enforced by the application-level lease, not
by Postgres locks. The lock just makes each procedure call
atomic.

### Subscription-level lease (routing worker)

Lives on `subscriptions.claimed_by` /
`subscriptions.claim_expires_at`.

`claim_subscription(stream, name, worker_id, lease_seconds)`:

- If no row exists, creates one with `claimed_by = worker_id`
  and `claim_expires_at = now() + lease_seconds`. Returns
  `'claimed'`.
- If a row exists with `claim_expires_at > now()` and a
  different `claimed_by`, returns `'already_claimed'` (not an
  error — the calling worker backs off and retries on the next
  poll).
- If a row exists with the same `claimed_by`, refreshes the
  expiry and returns `'claimed'`. (Re-attach on restart, or
  per-batch re-claim by the same worker.)
- If a row exists with an expired claim, takes it over,
  returns `'claimed'`.

Internally, `claim_subscription` does an MVCC-snapshot
existence + lease check before taking any row lock; only when
the row appears free or expired does it escalate to `FOR UPDATE
SKIP LOCKED` and re-verify under the lock. The losing side of a
contended claim never queues on a row lock; it either reads a
live lease from the snapshot or gets 0 rows from `SKIP LOCKED`,
and both translate to `'already_claimed'`. See
[D-0025](decisions.md#d-0025).

There is no routing-side heartbeat under D-0025: the lease
covers one batch and is released explicitly. If a worker pauses
long enough for its lease to expire and another worker takes
over, the original worker's next call to `route_batch` or
`release_subscription` raises `IS022 subscription_lease_lost`.

`release_subscription` is called per batch by the SDK routing
worker in the steady state, and also on graceful shutdown if
the worker happens to hold the lease at that moment. It clears
the lease so the next polling worker claims immediately rather
than waiting for the expiry.

### Per-work-item lease (processing worker)

Lives on `subscription_work_items.claimed_by` /
`subscription_work_items.lease_expires_at`.

`claim_work_item(stream, name, worker_id, lease_seconds)`:

- Atomically selects the next eligible work item using `FOR
  UPDATE SKIP LOCKED` plus a per-partition predicate
  (`NOT EXISTS` for any earlier non-terminal item in the same
  partition), and stamps it with the caller's `worker_id` +
  lease expiry.
- Returns null if no eligible item is available; the caller
  sleeps `poll_interval` and tries again.
- If the eligible item is currently `claimed` but its lease
  has expired, takes it over (lease takeover branch).

`extend_work_item_claim` is the heartbeat the processing
worker runs alongside long handler executions. On lease loss
it raises `IS030 work_item_lease_lost` and the worker exits
the in-flight item; redelivery happens via lease expiry. The
terminal-success calls (`complete_work_item_*`) and the
operator-only `fail_work_item` also raise `IS030` on lease
loss.

Processing workers do NOT take a subscription-level lease.
Multiple processing workers per subscription are normal.

## Strong consistency on dispatch

After a successful append, the SDK knows the assigned
`(stream_version, event_number)` range for the new events. When
the dispatcher requests
`consistency: ["BalancesProjector", "OrderProjector"]`, the SDK
polls `is_subscription_caught_up(stream, name, target)` for each
named subscription until every predicate returns true. If the
configured timeout elapses, dispatch returns a
`ConsistencyTimeoutError` — the events remain durably appended;
only the wait failed.

The catch-up predicate has two conjuncts and both must hold:

1. The routing cursor has reached the target
   (`subscriptions.last_seen >= target`).
2. No `subscription_work_items` row for the subscription with
   `event_number <= target` is in a non-terminal state
   (`pending`, `claimed`, or `failed`).

Either alone is insufficient. Conjunct (1) is necessary because
routing may lag the append; conjunct (2) is necessary because
routing may have run ahead but processing not yet caught up to
the target. The atomic write of `route_batch` ensures (1) is
never observable without the relevant rows for (2) being
observable, so there is no race where a caller sees "caught up"
before work items are visible to claim.

The list is explicit. There is no "wait for everything"
shorthand because there is no in-store registry of which
subscriptions exist for which application.

A per-stream subscription target can only wait on appends to its
own stream. Passing a per-stream subscription target for an
append to a different stream raises a typed error
(`ConsistencyTargetError`) before the wait begins, because the
subscription's cursor lives in its own stream's coordinate space
and the comparison would otherwise vacuously succeed. `$all`
targets are exempt; they validly observe every append.

## Concurrency model summary

| Scenario | Mechanism |
|---|---|
| Two appenders, same stream, same expected version | Unique constraint on `stream_events (stream_id, stream_version)`. One wins, the other gets `IS001`. |
| Two appenders, any streams, both targeting `'any_version'` | Row lock on `streams[target]` serialises same-stream; row lock on `streams[$all]` orders globally. |
| Two routing workers, same subscription | MVCC pre-check + `FOR UPDATE SKIP LOCKED` in `claim_subscription` — only one holds the subscription lease at a time; the loser sees `already_claimed` without queueing on a row lock. Under D-0025 the lease is released per batch, so the next tick is a fresh race. |
| One routing worker, action after another worker has taken over the lease | `route_batch` / `release_subscription` check `claimed_by`; raise `IS022 subscription_lease_lost` if it doesn't match. Under D-0025 `IS022` is recoverable: the worker drops the batch and loops. |
| Two processing workers, same subscription, same partition | Per-partition predicate in `claim_work_item` -- only the next-eligible item in a partition can be claimed; other workers skip to other partitions. Within a partition, processing is serial. |
| Two processing workers, same subscription, different partitions | `FOR UPDATE SKIP LOCKED` on the work-items row — both workers proceed concurrently. |
| One processing worker, terminal call after another worker has taken over its item | `complete_work_item_*` / `extend_work_item_claim` / `fail_work_item` check `claimed_by`; raise `IS030 work_item_lease_lost` if it doesn't match. |
| Reader and appender on the same stream | MVCC. Reads run outside any locks taken by the appender. |
| Dispatcher and processing worker, same Postgres | Lock sets disjoint: dispatch holds `streams` + events; worker terminal step holds `subscription_work_items` + `snapshots`. The disjointness is a property of the SQL contract per [D-0026](decisions.md#d-0026); it does not require pool or client separation. |
| Routing worker mid-batch + processing worker on the same subscription | Lock sets disjoint: routing holds `subscriptions` + the work-items rows it's inserting; processing holds the work-items row it's claiming. |

## What's outside the store

- **Aggregate state caching** — none. Every command reloads.
  Snapshots make reload cheap for long streams.
- **Subscription routing / handler invocation** — SDK concern.
  The store delivers a batch; the SDK invokes the handler.
- **Routing decisions** — user code (`routeFn`) runs in the
  routing worker. Pure: no I/O, no aggregate loads. Routing-side
  filtering is expressed as `"ignore"`; partition shape is
  expressed as the returned `partitionKey`. The SDK ships sugar
  (`partitionBy: { kind: 'sequential' | 'per-event' | 'per-key' }`)
  over the bare `routeFn` for projections.
- **Snapshot policy** — application concern. `snapshot_every: N`
  is the SDK convention.
- **Strong-consistency polling** — SDK concern. The store
  exposes `is_subscription_caught_up`; the SDK does the polling
  and timeout.
- **Dispatch wait orchestration, retries, backoff** — SDK
  concern.
- **Error policy on handler throw** — SDK concern via the
  `ErrorPolicy` hook (`retry-in` / `stop`). The default is
  exponential backoff capped at 30s, retry forever. The store
  contract does not specify retry behaviour.

## SDK structure

Each SDK splits into a **core** layer that drives the SQL
contract and a **conveniences** layer that offers idiomatic
APIs on top.

**Core** (every SDK must provide, in some form):

- Procedure wrappers with `SQLSTATE → typed-error` translation.
- The aggregate load-execute-append loop with OCC retry.
- A **routing worker** loop (claim subscription + read events +
  run `routeFn` + `route_batch` + release) with per-batch
  subscription-level lease. See [D-0025](decisions.md#d-0025).
- A **processing worker** loop (claim work item + run handler +
  terminal step) with per-item lease and heartbeat. PM
  processing additionally loads/rebuilds PM state and
  dispatches commands via the same client.
- Snapshot read/write/delete primitives.
- A catch-up-predicate poll (`is_subscription_caught_up`) for
  strong-consistency waits.

**Conveniences** (each SDK may shape these per language idiom):

- Routing events to projectors / PMs / aggregates declaratively.
- A facade that registers definitions and runs them under one
  handle.
- A `dispatch(..., { consistency: [...] })` helper that wraps
  the polling.
- Saga workflow helpers — rollback/compensation/steps — if and
  when added. The PM primitive is core; conveniences over it
  are not.

The TypeScript SDK currently exposes both in one package; the
layering is documented in [`sdks/typescript/README.md`](../sdks/typescript/README.md).

## What's not provided

For each, the application is on its own:

- **Handler idempotency** — at-least-once delivery means
  handlers must be idempotent. Typical approaches: `INSERT ON CONFLICT DO NOTHING`,
  upserts keyed on `event_id`, a `processed_events` side table,
  or whatever the target store offers (`SETNX`, `_id`-keyed
  PUTs, etc.).
- **Snapshot module versioning** — the store has a `metadata`
  column on snapshots; an application that evolves its
  aggregate state schema should stamp a
  `$instructed.snapshot_module_version` into metadata and reject
  mismatched snapshots on load. The
  v1 TypeScript SDK does not do this automatically.
- **Throughput scaling for a single subscription** — single
  active worker per cursor. Throughput scales by splitting into
  multiple named subscriptions over disjoint slices of the
  event log, not by adding workers to one subscription.
- **Server-side selectors** — selectors run SDK-side. A future
  capability may push them server-side; the v1 contract leaves
  room.
- **Push delivery** — workers poll. A future wake-up
  optimisation using `LISTEN`/`NOTIFY` is allowed but is not
  in the v1 correctness contract.
