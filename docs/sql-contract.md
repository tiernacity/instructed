# SQL contract — human-oriented reference

This document is a reading guide for [`sql/instructed.sql`](../sql/instructed.sql).
**The SQL file is the spec.** Anything authoritative — pre/post
conditions, lock ordering, the closed set of errors — lives in
docstrings on the schema objects themselves. This file is a quick
index, a catalogue of SQLSTATEs, and a small set of recommended
call patterns that compose multiple procedures.

If this document and `instructed.sql` disagree, `instructed.sql`
wins.

## How the contract is structured

`instructed.sql` installs the `instructed` schema and a fixed set of
procedures into an existing Postgres database. The schema is
**append-only** for events (`UPDATE`/`DELETE` on `events` and
`stream_events` raise `IS006`) and accepts mutating traffic only
through the procedures listed below.

Procedures follow three conventions, borrowed from absurd:

- **Caller-tunable knobs go through a `p_options jsonb` parameter**,
  not positional arguments. The set of recognised keys is documented
  in each procedure's docstring; unknown keys are silently ignored.
  This is the forward-compatibility lever for a future operator-facing
  partition dimension on `subscriptions` (ML-0013) and for ML-0002
  (`LISTEN`/`NOTIFY`).
- **Errors are a closed set per procedure**, raised with custom
  SQLSTATEs in class `IS` (table below). SDKs translate each
  SQLSTATE to a language-native error type.
- **Locks are documented in lock-acquisition order**, and procedure
  lock sets are deliberately disjoint where possible (per
  [D-0011](decisions.md#d-0011) / [D-0016](decisions.md#d-0016)).

## Schema at a glance

| Table             | Purpose | Invariants |
|-------------------|---------|------------|
| `streams`         | One row per logical event stream. Seeded row `(stream_id = 0, stream_uuid = '$all')` is the global stream. | INV-APPEND-002/003, INV-STREAM-001..003, [D-0012](decisions.md#d-0012) |
| `events`          | Caller-keyed event rows. Append-only. | INV-APPEND-001, INV-APPEND-030, INV-APPEND-040 |
| `stream_events`   | (event, stream) join. Carries per-stream + original-stream positions. Unique `(stream_id, stream_version)` is the OCC mechanism ([D-0005](decisions.md#d-0005)). | INV-APPEND-002/022, INV-READ-005..008 |
| `snapshots`       | At most one per `source_uuid`. Used by aggregates and PMs (PM-020..024). | INV-SNAP-001..004 |
| `subscriptions`   | Persistent routing cursor + lease per `(stream_id, name)`. Routing-worker hot path. | INV-SUB-P-001/002/010..012, [D-0002](decisions.md#d-0002), [D-0006](decisions.md#d-0006) |
| `subscription_work_items` | Per-subscription work queue. One row per routed event per partition; carries the per-item lease processing workers compete on. PK absorbs duplicate INSERTs on routing-worker re-run. ON DELETE CASCADE from `subscriptions`. | INV-SUB-W-001..030, [D-0002](decisions.md#d-0002) |

Triggers `events_no_update / events_no_delete /
stream_events_no_update / stream_events_no_delete` enforce
INV-APPEND-040 by raising `IS006` on any direct table mutation.
`IS006` is **internal-only**: no procedure in this contract ever
raises it. SDKs that see it have bypassed the contract.

## Procedures at a glance

### Append + read

| Procedure                       | Returns                  | Purpose |
|---------------------------------|--------------------------|---------|
| `append_to_stream`              | `setof (event_id, stream_version, event_number, created_at)` | Atomic N-event append; honours `any`/`no_stream`/`stream_exists`/`exact` |
| `read_stream`                   | `setof recorded_event`   | Forward read of a single stream from a `stream_version` |
| `read_all`                      | `setof recorded_event`   | Forward read of `$all` from an `event_number`; rows carry original-stream identity |

### Snapshots

| Procedure                       | Returns                  | Purpose |
|---------------------------------|--------------------------|---------|
| `record_snapshot`               | `void`                   | Upsert snapshot by `source_uuid` |
| `read_snapshot`                 | `(source_uuid, type, version, data, metadata, created_at)` | Fetch or raise `IS010` |
| `delete_snapshot`               | `void`                   | Idempotent delete |

### Subscription lifecycle

| Procedure                       | Returns                  | Purpose |
|---------------------------------|--------------------------|---------|
| `claim_subscription`            | `(result, last_seen, claimed_by, claim_expires_at)` | Acquire routing-side lease on first create or expiry; returns `already_claimed` (not an error) when a live lease is held by someone else or the row is being concurrently written. Uses MVCC pre-check + `FOR UPDATE SKIP LOCKED` internally per [D-0025](decisions.md#d-0025) so contended callers never queue on a row lock. In the `'already_claimed'` outcome `claimed_by` and `claim_expires_at` are diagnostic and **may be NULL** when the `FOR UPDATE SKIP LOCKED` step finds the row locked: the released-between-batches D-0025 state and the row-deleted-between-checks race both surface as NULL fields. SDK wrappers MUST type both fields as nullable in the `'already_claimed'` branch. |
| `release_subscription`          | `void`                   | Clean release; cursor and queue preserved |
| `delete_subscription`           | `void`                   | Removes row + cascaded work items; raises `IS020` on missing ([D-0009](decisions.md#d-0009)) |

### Routing-worker hot path

| Procedure                       | Returns                  | Purpose |
|---------------------------------|--------------------------|---------|
| `route_batch`                   | `void`                   | Atomic: INSERT N work items + advance `subscriptions.last_seen`. `ON CONFLICT DO NOTHING` on the work-items PK absorbs duplicate INSERTs on routing-worker re-run. Requires the caller's `worker_id` to match the current subscription `claimed_by` (raises `IS022` otherwise). |

### Processing-worker hot path

| Procedure                       | Returns                  | Purpose |
|---------------------------------|--------------------------|---------|
| `claim_work_item`               | `nullable (partition_key, event_number, was_takeover, prior_claimed_by)` | `FOR UPDATE SKIP LOCKED` + per-partition predicate. Stamps the row with the caller's `worker_id` + lease expiry. Returns null when nothing eligible; non-null with `was_takeover=true` on the lease-takeover branch. Does NOT take a subscription lease. |
| `extend_work_item_claim`        | `void`                   | Processing-worker heartbeat. Raises `IS030 work_item_lease_lost` on `claimed_by` mismatch. |
| `complete_work_item_projection` | `void`                   | Projection-side terminal success: DELETEs the row immediately (no `done` state persists for projections). Raises `IS030` on lease loss. |
| `complete_work_item_pm`         | `void`                   | PM-side non-terminal success: UPDATE row to `done` + UPSERT the snapshot in one tx. Raises `IS030` on lease loss. |
| `complete_pm_instance`          | `(snapshot_deleted, work_items_deleted)` | PM-side terminal success (`handle` returned `{ complete: true }`): DELETE the snapshot + every work item for the partition in one tx. Idempotent. |
| `fail_work_item`                | `void`                   | UPDATE the row to `failed` with `error_text`. Operator-only resolution thereafter; the default error policy never calls this. Raises `IS030` on lease loss. |

### Catch-up + PM-state rebuild

| Procedure                       | Returns                  | Purpose |
|---------------------------------|--------------------------|---------|
| `is_subscription_caught_up`     | `(caught_up boolean)`    | Two-conjunct catch-up predicate (routing cursor at-or-past target AND no in-flight work items at-or-below target). Polled by `waitForProjection`. See [INV-SUB-CATCHUP-001]. |
| `list_pm_rebuild_events`        | `setof recorded_event`   | Cold-path read: every `done` work-item event for `(subscription_name, partition_key)` with `event_number < exclusive_upper`, in event-number order. Used by PM-state rebuild after a snapshot miss / module-version mismatch. |

`recorded_event` is shorthand for the eleven columns: `event_id`,
`event_number`, `stream_uuid`, `stream_version`, `event_type`,
`causation_id`, `correlation_id`, `data`, `metadata`, `created_at`.
For reads through `$all` (via `read_all`, or via
`list_pm_rebuild_events` when the rebuild walks events sourced
from `$all`), `stream_uuid` / `stream_version` carry the
**original** stream identity (INV-READ-006/007).

## Error-code catalogue

Custom SQLSTATEs are in class `IS` ("Instructed Store"), which is
outside the ranges PostgreSQL reserves for itself (00..0Z, 20..2Z,
38..44, 53..58, 72, F0, HV, P0, XX) and outside its application-
reserved class `P0`. SDKs map each SQLSTATE to a language-native
error type.

| SQLSTATE | Name                          | Raised by                                                                 | Mapped invariant / decision |
|----------|-------------------------------|---------------------------------------------------------------------------|------------------------------|
| `IS001`  | `wrong_expected_version`      | `append_to_stream`                                                        | INV-APPEND-013, INV-APPEND-020 |
| `IS002`  | `stream_exists`               | `append_to_stream` (with `expected_version_type = 'no_stream'`)           | INV-APPEND-011 |
| `IS003`  | `stream_not_found`            | `append_to_stream` (with `'stream_exists'`), `read_stream`, `claim_subscription` | INV-APPEND-012, INV-READ-001 |
| `IS004`  | `duplicate_event`             | `append_to_stream`                                                        | INV-APPEND-030 |
| `IS005`  | `reserved_stream_uuid`        | `append_to_stream`, `read_stream` (on `'$all'`)                           | INV-STREAM-003 |
| `IS006`  | append-only violation         | triggers on `events` / `stream_events` direct UPDATE/DELETE only          | INV-APPEND-040 — **never** raised by a procedure |
| `IS010`  | `snapshot_not_found`          | `read_snapshot`                                                           | INV-SNAP-003 |
| `IS020`  | `subscription_not_found`      | `release_subscription`, `route_batch`, `delete_subscription`, `claim_work_item`, `extend_work_item_claim`, `complete_work_item_*`, `complete_pm_instance`, `fail_work_item`, `is_subscription_caught_up`, `list_pm_rebuild_events` | INV-SUB-P-062, [D-0009](decisions.md#d-0009) |
| `IS021`  | `subscription_already_claimed` | (reserved; v1 surfaces this case as a non-error `result = 'already_claimed'` row from `claim_subscription`. The SQLSTATE is retained in the catalogue for forward use if a future variant of `claim_subscription` chooses to raise.) | [D-0006](decisions.md#d-0006) |
| `IS022`  | `subscription_lease_lost`     | `release_subscription`, `route_batch` — the routing-worker surface | [D-0006](decisions.md#d-0006) |
| `IS030`  | `work_item_lease_lost`        | `extend_work_item_claim`, `complete_work_item_projection`, `complete_work_item_pm`, `fail_work_item` — the processing-worker terminal surface. Covers both "row gone" (taken over) and "`claimed_by` mismatch"; either way the worker should stop and let redelivery happen. | INV-SUB-W-012 |
| `22023`  | `invalid_parameter_value`     | every procedure, on null / malformed / out-of-range input                 | (standard SQLSTATE) |
| `0A000`  | `feature_not_supported`       | reserved; not raised by any v1 procedure                                  | (standard SQLSTATE) |

The error set per procedure is closed: a procedure either succeeds,
raises one of the codes listed in its docstring, or raises a standard
Postgres SQLSTATE that signals storage/serialisation failure
(connection loss, disk full, etc.) which the SDK propagates as an
infrastructure error.

**`IS021` is reserved, not raised.** `claim_subscription` reports
already-claimed status as a returned row with `result =
'already_claimed'`, **not** as a raised exception. The SQLSTATE slot
is held so we have room to add a stricter variant later without
renumbering. Mention this in any SDK error-translation table so
implementers know `IS021` is a known-empty code, not an oversight.

**`claim_subscription` does NOT raise `IS022`.** Lease-lost is a
distinction that matters to a worker currently holding a lease. A
worker calling `claim_subscription` is by definition not yet holding
one; the contract surface for that case is the `already_claimed`
return row.

## Lock-acquisition order, by procedure

(Repeats the summary in `sql/instructed.sql` Pass 2 for quick
reference. Authoritative copy lives in the SQL file.)

| Procedure                       | Locks held until commit                                                  |
|---------------------------------|--------------------------------------------------------------------------|
| `append_to_stream`              | `streams[target]` → `streams[$all]` → `events` → `stream_events`         |
| `read_stream`, `read_all`       | none (MVCC reads)                                                        |
| `record_snapshot`, `delete_snapshot` | `snapshots[source_uuid]`                                            |
| `read_snapshot`                 | none                                                                     |
| `claim_subscription`            | `subscriptions[stream,name]`                                             |
| `release_subscription`          | `subscriptions[stream,name]`                                             |
| `route_batch`                   | `subscriptions[stream,name]` → `subscription_work_items[*]` (PK)         |
| `delete_subscription`           | `subscriptions[stream,name]` → cascaded `subscription_work_items`        |
| `claim_work_item`               | `subscription_work_items[row]` (`FOR UPDATE SKIP LOCKED`)                |
| `extend_work_item_claim`        | `subscription_work_items[row]`                                           |
| `complete_work_item_projection` | `subscription_work_items[row]`                                           |
| `complete_work_item_pm`         | `subscription_work_items[row]` → `snapshots[source_uuid]`                |
| `complete_pm_instance`          | `subscription_work_items[*]` → `snapshots[source_uuid]`                  |
| `fail_work_item`                | `subscription_work_items[row]`                                           |
| `is_subscription_caught_up`     | none (MVCC reads)                                                        |
| `list_pm_rebuild_events`        | none (MVCC reads)                                                        |

The **dispatch** lock set (`streams`, `events`, `stream_events`),
the **routing** lock set (`subscriptions`,
`subscription_work_items`), and the **processing terminal** lock
set (`subscription_work_items`, `snapshots`) are pairwise
disjoint. A handler-side terminal step never blocks a dispatcher
in another session calling `append_to_stream`, and a routing
worker writing a batch never blocks a processing worker claiming
a previously-routed item. The disjointness is a property of the
per-procedure lock-acquisition orders documented above; it does
not require pool or client separation in the SDK (per
[D-0026](decisions.md#d-0026), the SDK uses one pool / one
`Client` for the entire application).

## Recommended call patterns

These are the canonical SDK shapes. The SQL file is agnostic to which
one the SDK adopts; both are supported.

### Aggregate command ([D-0004](decisions.md#d-0004), [D-0005](decisions.md#d-0005))

```text
load:
  read_snapshot(source_uuid)            -- optional; ignore IS010
  read_stream(stream_uuid,
              from = snapshot.source_version + 1,
              qty  = ...)               -- fold events to current state
execute:
  cmd_handler(state, command) -> [events]
append:
  append_to_stream(stream_uuid,
                   expected_version_type = 'exact',
                   expected_version       = current_state.version,
                   events)
  -- on IS001: re-load, re-execute, retry up to budget
```

Per D-0005, retry on `IS001` is the per-aggregate serialisation
mechanism; there is no advisory lock.

### Routing worker ([D-0002](decisions.md#d-0002), [D-0025](decisions.md#d-0025))

One routing worker per subscription at any single instant,
single-active via the subscription lease. The SDK's default loop
claims and releases the lease per batch ([D-0025](decisions.md#d-0025))
so the active worker identity rotates per batch across whichever
processes are polling. Reads events past the cursor, runs the
user's `routeFn` per event, atomically writes the new cursor +
work-item rows.

```text
loop:
  result = claim_subscription(stream, name, worker_id, lease_secs)
  if result.result == 'already_claimed':
    sleep poll_interval; continue
  events = read_all(result.last_seen + 1, batch_size)   -- or read_stream
  if events empty:
    release_subscription(stream, name, worker_id)
    sleep poll_interval; continue
  decisions = []
  for e in events:
    d = routeFn(e)                                -- pure; no I/O; fast
    if d != "ignore":
      decisions.append((e.event_number, d.partitionKey))
  BEGIN  -- single atomic tx
    route_batch(stream, name, worker_id, decisions,
                new_cursor = max(events).event_number)
      -- on IS022: drop batch, continue loop
  COMMIT
  release_subscription(stream, name, worker_id)
on graceful shutdown:
  release_subscription(stream, name, worker_id)   -- best-effort
```

`route_batch` is the only place the routing-cursor advance and
the work-item INSERTs commit together. This atomicity is
load-bearing for the catch-up predicate ([INV-SUB-CATCHUP-001]):
once `last_seen >= N` is observable, the work items for events
up to N are observable too. The work-items PK absorbs duplicate
INSERTs (`ON CONFLICT DO NOTHING`), so a crashed or
`IS022`-aborted mid-batch routing worker is recoverable: the
next worker re-reads from `last_seen` and the re-inserts are
no-ops.

Under D-0025 there is no routing-side heartbeat: the lease
covers one batch, `route_batch` re-verifies `claimed_by` on
each call, and the lease is released explicitly. A lost lease
surfaces as `IS022` rather than via a heartbeat.

### Processing worker ([D-0016](decisions.md#d-0016))

N processing workers per subscription run concurrently. Each
claims one item at a time, runs the handler, calls a
kind-specific terminal step. No subscription lease.

```text
loop:
  claim = claim_work_item(stream, name, worker_id, lease_secs)
  if claim is null: sleep poll_interval; continue
  event = read_all(claim.event_number, 1)
  -- heartbeat in parallel:
  extend_work_item_claim(...)                 -- on IS030: abort item
  try:
    handler(event)                            -- NO SDK transaction
  catch:
    error policy decides retry-in / stop
    -- 'retry-in' loops to retry handler on the SAME claim;
    -- 'stop' exits; lease expires; another worker takes over.
    continue
  on success:
    BEGIN  -- short terminal tx
      complete_work_item_projection(...)      -- projection: DELETE row
        -- OR
      complete_work_item_pm(...)              -- PM non-terminal:
                                              --   UPDATE row to done
                                              --   + UPSERT snapshot
        -- OR
      complete_pm_instance(...)               -- PM terminal:
                                              --   DELETE snapshot
                                              --   + DELETE all items
    COMMIT
```

The handler runs with no row lock held. If the worker crashes
between handler-return and the terminal call, the per-item lease
expires and another processing worker takes the item over; the
first worker's eventual terminal call raises `IS030
work_item_lease_lost`.

The handler is application-domain; it may write to Postgres,
Elasticsearch, Redis, an external API, or anywhere else. The SDK
does not pass it a connection. Idempotency on redelivery is the
handler's concern.

### PM-state load + dispatch ([D-0011](decisions.md#d-0011), [D-0017](decisions.md#d-0017))

A PM processing worker adds a state-load step before `handle`
and a dispatch step between `handle` and the terminal call:

```text
claim = claim_work_item(...)
event = read_all(claim.event_number, 1)

-- State load:
snap = read_snapshot("{pm_name}-{partition_key}")   -- IS010 = miss
if snap exists AND snap.metadata["$instructed.snapshot_module_version"] == def.version:
  state = snap.data
else:
  -- Rebuild via apply, from initialState():
  state = initialState()
  for e in list_pm_rebuild_events(stream, name, partition_key,
                                  exclusive_upper = claim.event_number):
    state = apply(state, e)

staged_state = apply(state, event)
result = handle(staged_state, event)                -- { commands?, complete? }

-- Dispatch on the same client (per D-0026; lock-set disjointness
-- is a property of the SQL contract, not of client identity):
for c in result.commands:
  client.append_to_stream(c.streamUuid, ..., events_from(c))
     -- causation_id = event.event_id
     -- correlation_id = event.correlation_id

if result.complete == true:
  complete_pm_instance(stream, name, partition_key,
                       source_uuid = "{pm_name}-{partition_key}")
else:
  complete_work_item_pm(stream, name, worker_id, partition_key,
                        event.event_number,
                        snapshot = { ..., data: staged_state,
                                     source_version: event.event_number,
                                     metadata: { "$instructed.snapshot_module_version": def.version } })
```

Under [D-0026](decisions.md#d-0026) the same `Client` is used
for both dispatch and persist-and-ack. The dispatch path locks
`streams` + the events tables; the terminal step locks
`subscription_work_items` + `snapshots`. The two lock sets are
disjoint by construction, so a dispatched aggregate's
`append_to_stream` cannot deadlock against the same worker's
`complete_work_item_pm` — each is its own short transaction with
a documented lock-acquisition order, and the sets share no rows.

### Strong-consistency-on-dispatch wait ([D-0010](decisions.md#d-0010))

```text
result = append_to_stream(...)
for sub in consistency_list:
  poll until is_subscription_caught_up(sub.stream, sub.name,
                                       target = result.last_event_number)
            returns true
  or until consistency_timeout elapses
```

`consistency_list` is the explicit list per [D-0010](decisions.md#d-0010)
(no `:strong` shorthand); polling cadence is the SDK's
responsibility. The predicate has two conjuncts (routing cursor
at-or-past target AND no in-flight work items at-or-below
target); both are evaluated server-side in one round-trip per
poll.

## Implementation notes

Three small Postgres-specific points discovered while filling in the
bodies that future maintainers will hit:

1. **`#variable_conflict use_column` is required on every plpgsql
   function whose `RETURNS TABLE` column names overlap with table
   columns.** Without it, identifiers like `stream_version`,
   `event_id`, `stream_uuid` inside `INSERT ... RETURNING` or
   `UPDATE ... RETURNING ... INTO` clauses are interpreted as the
   output-table variables, not the table columns, and queries fail
   with `column reference is ambiguous`. The pragma is set in every
   plpgsql function in the contract; do not remove it casually.
2. **Multiple modifying CTEs run in undefined order.** A duplicate
   `event_id` on `append_to_stream` may surface as a
   `unique_violation` on either `events_pkey` *or*
   `stream_events_pkey` (`(event_id, stream_id)`) depending on which
   CTE Postgres executed first. Both map to `IS004
   duplicate_event`. Only the unique on `stream_events
   (stream_id, stream_version)` is mapped to `IS001
   wrong_expected_version`.
3. **[D-0012](decisions.md#d-0012) holds at runtime.** Two sessions doing
   concurrent `'any_version'` appends to the same fresh stream are
   serialised by the row lock taken by `INSERT ... ON CONFLICT DO UPDATE`
   on the target `streams` row, and the `$all` row lock guarantees
   contiguous global numbers. Validated by running two parallel
   psql sessions and inspecting the final `stream_events` ordering.

## Migration story

`sql/instructed.sql` is the canonical schema. `sql/migrations/`
contains a linear sequence of delta scripts, named
`<from>-<to>.sql`, applied in order on top of a previous installed
version. `instructed.get_schema_version()` returns the installed
release tag (`'main'` during development; rewritten by release
automation).

No migrations exist yet — v1 has not shipped. The first migration
will be cut alongside the first tagged release.

## Cross-references

- [`docs/invariants.md`](invariants.md) — the formal catalogue this
  schema realises (closed error sets per procedure).
- [`docs/architecture.md`](architecture.md) — how the procedures fit
  together; lock-set disjointness; worker loops.
- [`docs/decisions.md`](decisions.md) — the ADRs the contract
  reflects, particularly D-0005, D-0006, D-0009, D-0010, D-0011,
  D-0012, D-0016.
- [`docs/non-goals.md`](non-goals.md) — positioning statements on
  hard-delete, JSONB-only payloads, the reserved `$all` name, etc.
- [`docs/maybe-later.md`](maybe-later.md) — ML-0002
  (`LISTEN`/`NOTIFY` wake-up), ML-0004 (`bindToConnection`),
  ML-0010 (post-success retention for projection work-items),
  ML-0011 (less-opinionated `PartitionBy`), ML-0012 (routing
  worker flush-vs-drop).
