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

Five tables in the `instructed` schema:

| Table | Purpose |
|---|---|
| `streams` | One row per logical stream. The seeded row `(stream_id = 0, stream_uuid = '$all')` is the global stream. |
| `events` | Caller-keyed event rows. Append-only — `UPDATE` and `DELETE` triggers raise. |
| `stream_events` | The `(event, stream)` join. Carries per-stream version and original-stream identity. The unique constraint on `(stream_id, stream_version)` is the optimistic-locking mechanism. |
| `snapshots` | At most one row per `source_uuid`. Backs aggregate snapshots and process-manager state. |
| `subscriptions` | Persistent leased cursors. Identity is `(stream_id, name, shard)` with `shard = 0` reserved for the single-active-worker model. |

All mutation goes through stored procedures; the tables are not
written to directly. Triggers on `events` and `stream_events`
enforce append-only by raising on direct `UPDATE` / `DELETE`.

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

A projection or process manager worker runs the same loop:

```
claim_subscription(stream, name, worker_id, lease_seconds)
    → 'claimed' or 'already_claimed'   -- if already_claimed, retry later

loop while not stopped:
    in a short read tx:
        events = read_subscription_batch(stream, name, worker_id, batch_size)
    if events empty:
        sleep poll_interval
        continue
    for e in events:
        await handler(e, ctx)            -- NO SDK transaction
    in a short ack tx:
        advance_subscription(stream, name, worker_id, last_position)
    in parallel, every heartbeat_interval:
        extend_subscription_claim(...)

on shutdown:
    release_subscription(stream, name, worker_id)
```

The handler runs outside any SDK transaction. The cursor
advance is a separate short transaction after the handler
returns. If the handler throws, the cursor is not advanced;
the events redeliver on the next iteration. If the worker
crashes between handler-return and ack-commit, the same:
redelivery. This is at-least-once delivery; handlers are
idempotent.

Process-manager workers run an extra step in the ack tx:
they upsert the PM's state snapshot keyed by the instance id
in the same short transaction that advances the cursor. The
snapshot's `source_version` always equals the subscription's
`last_seen` — that's how redelivery is absorbed when a PM
restarts and re-reads its state.

PMs dispatch commands as part of handling an event. Dispatch
opens its own connection — the persist-and-ack transaction and
the dispatch transaction run on different sessions, so their
lock sets stay disjoint. (The dispatch path locks `streams` and
the events tables; the persist-and-ack path locks
`subscriptions` and `snapshots`.)

## How leases work

`claim_subscription(stream, name, worker_id, lease_seconds)`:

- If no row exists, creates one with `claimed_by = worker_id`
  and `claim_expires_at = now() + lease_seconds`. Returns
  `'claimed'`.
- If a row exists with `claim_expires_at > now()` and a
  different `claimed_by`, returns `'already_claimed'` (not an
  error — the calling worker should back off and retry).
- If a row exists with the same `claimed_by`, refreshes the
  expiry and returns `'claimed'`. (Re-attach on restart.)
- If a row exists with an expired claim, takes it over,
  returns `'claimed'`.

`extend_subscription_claim` is the heartbeat: workers call it
periodically (typically every `lease_seconds / 3`) to refresh
the expiry. If a worker pauses long enough for its lease to
expire and another worker takes over, the original worker's
next call to `extend_subscription_claim` or
`read_subscription_batch` or `advance_subscription` raises
`IS022 subscription_lease_lost` — its signal to stop.

`release_subscription` is the graceful shutdown — it clears
the lease so another worker can claim immediately rather than
waiting for the expiry.

## Strong consistency on dispatch

After a successful append, the SDK knows the assigned
`(stream_version, event_number)` range for the new events. When
the dispatcher requests
`consistency: ["BalancesProjector", "OrderProjector"]`, the SDK
polls `read_subscription_position(stream, name)` for each named
subscription until every returned `last_seen` is at or past the
appended events' position. If the configured timeout elapses,
dispatch returns a `ConsistencyTimeoutError` — the events
remain durably appended; only the wait failed.

The list is explicit. There is no "wait for everything"
shorthand because there is no in-store registry of which
subscriptions exist for which application.

## Concurrency model summary

| Scenario | Mechanism |
|---|---|
| Two appenders, same stream, same expected version | Unique constraint on `stream_events (stream_id, stream_version)`. One wins, the other gets `IS001`. |
| Two appenders, any streams, both targeting `'any_version'` | Row lock on `streams[target]` serialises same-stream; row lock on `streams[$all]` orders globally. |
| Two workers, same subscription | `claim_subscription` row lock — only one holds the lease at a time. |
| One worker, ack after another worker has taken over the lease | `advance_subscription` checks `claimed_by`; raises `IS022 subscription_lease_lost` if it doesn't match. |
| Reader and appender on the same stream | MVCC. Reads run outside any locks taken by the appender. |
| Dispatcher and worker, same Postgres | Lock sets disjoint: dispatch holds `streams` + events; worker ack holds `subscriptions` + `snapshots`. |

## What's outside the store

- **Aggregate state caching** — none. Every command reloads.
  Snapshots make reload cheap for long streams.
- **Subscription routing / handler invocation** — SDK concern.
  The store delivers a batch; the SDK invokes the handler.
- **Selector evaluation** — SDK-side. The SDK reads a batch and
  applies the selector before invoking the handler; the cursor
  advances past skipped events.
- **Snapshot policy** — application concern. `snapshot_every: N`
  is the SDK convention.
- **Strong-consistency polling** — SDK concern. The store
  exposes `read_subscription_position`; the SDK does the
  polling and timeout.
- **Dispatch wait orchestration, retries, backoff** — SDK
  concern.

## SDK structure

Each SDK splits into a **core** layer that drives the SQL
contract and a **conveniences** layer that offers idiomatic
APIs on top.

**Core** (every SDK must provide, in some form):

- Procedure wrappers with `SQLSTATE → typed-error` translation.
- The aggregate load-execute-append loop with OCC retry.
- A persistent-subscription worker loop with lease, heartbeat,
  and cursor advance.
- Snapshot read/write/delete primitives.
- A subscription-position read for strong-consistency waits.

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
  aggregate state schema should stamp a `snapshot_module_version`
  into metadata and reject mismatched snapshots on load. The
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
