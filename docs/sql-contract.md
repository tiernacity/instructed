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
  This is the forward-compatibility lever for ML-0001 (partitioned
  consumers) and ML-0002 (`LISTEN`/`NOTIFY`).
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
| `subscriptions`   | Persistent leased cursors. Identity `(stream_id, name, shard)`; `shard` reserved at v1 default 0 for ML-0001. | INV-SUB-P-001/002, [D-0006](decisions.md#d-0006) |

Triggers `events_no_update / events_no_delete /
stream_events_no_update / stream_events_no_delete` enforce
INV-APPEND-040 by raising `IS006` on any direct table mutation.
`IS006` is **internal-only**: no procedure in this contract ever
raises it. SDKs that see it have bypassed the contract.

## Procedures at a glance

| Procedure                       | Returns                  | Purpose |
|---------------------------------|--------------------------|---------|
| `append_to_stream`              | `setof (event_id, stream_version, event_number, created_at)` | Atomic N-event append; honours `any`/`no_stream`/`stream_exists`/`exact` |
| `read_stream`                   | `setof recorded_event`   | Forward read of a single stream from a `stream_version` |
| `read_all`                      | `setof recorded_event`   | Forward read of `$all` from an `event_number`; rows carry original-stream identity |
| `record_snapshot`               | `void`                   | Upsert snapshot by `source_uuid` |
| `read_snapshot`                 | `(source_uuid, type, version, data, metadata, created_at)` | Fetch or raise `IS010` |
| `delete_snapshot`               | `void`                   | Idempotent delete |
| `claim_subscription`            | `(result, last_seen, claimed_by, claim_expires_at)` | Acquire lease on first create or expiry; returns `already_claimed` (not an error) when a live lease is held by someone else |
| `extend_subscription_claim`     | `(claim_expires_at)`     | Heartbeat |
| `release_subscription`          | `void`                   | Clean release; cursor preserved |
| `read_subscription_batch`       | `setof recorded_event`   | Lock + read batch from cursor; does NOT advance |
| `advance_subscription`          | `(last_seen)`            | Monotone cursor advance; called in SDK's own short tx after the handler returns ([D-0016](decisions.md#d-0016)) |
| `read_subscription_position`    | `(last_seen)`            | Read cursor for strong-consistency-on-dispatch polling ([D-0010](decisions.md#d-0010)) |
| `delete_subscription`           | `void`                   | Removes row; raises `IS020` on missing ([D-0009](decisions.md#d-0009)) |

`recorded_event` is shorthand for the eleven columns: `event_id`,
`event_number`, `stream_uuid`, `stream_version`, `event_type`,
`causation_id`, `correlation_id`, `data`, `metadata`, `created_at`.
For reads through `$all` (both `read_all` and `read_subscription_batch`
on `$all`), `stream_uuid` / `stream_version` carry the
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
| `IS020`  | `subscription_not_found`      | `extend_subscription_claim`, `release_subscription`, `read_subscription_batch`, `advance_subscription`, `read_subscription_position`, `delete_subscription` | INV-SUB-P-062, [D-0009](decisions.md#d-0009) |
| `IS021`  | `subscription_already_claimed` | (reserved; v1 surfaces this case as a non-error `result = 'already_claimed'` row from `claim_subscription`. The SQLSTATE is retained in the catalogue for forward use if a future variant of `claim_subscription` chooses to raise.) | [D-0006](decisions.md#d-0006) |
| `IS022`  | `subscription_lease_lost`     | `extend_subscription_claim`, `release_subscription`, `read_subscription_batch`, `advance_subscription` | [D-0006](decisions.md#d-0006) |
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
| `claim_subscription`            | `subscriptions[stream,name,shard]`                                       |
| `extend_subscription_claim`     | `subscriptions[stream,name,shard]`                                       |
| `release_subscription`          | `subscriptions[stream,name,shard]`                                       |
| `read_subscription_batch`       | `subscriptions[stream,name,shard]` (`FOR UPDATE`), `events` (MVCC)       |
| `advance_subscription`          | `subscriptions[stream,name,shard]`                                       |
| `read_subscription_position`    | none                                                                     |
| `delete_subscription`           | `subscriptions[stream,name,shard]`                                       |

The dispatch lock set (`streams`, `events`, `stream_events`) and the
persist-and-ack lock set (`snapshots`, `subscriptions`) are
**disjoint**. A handler that opens one transaction to persist its
projection and advance its cursor (`record_snapshot` +
`advance_subscription`) never blocks a dispatcher in another session
calling `append_to_stream` and vice versa.

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

### Subscription worker ([D-0016](decisions.md#d-0016))

The handler is opaque to the SDK; it runs outside any SDK
transaction. The SDK opens two short transactions per batch —
one to read, one to advance — with the handler call between
them:

```text
claim_subscription(stream, name, worker_id, lease_secs)
loop:
  BEGIN  -- short read tx
    events = read_subscription_batch(stream, name, worker_id, batch_size)
  COMMIT
  if events empty: sleep poll_interval; continue
  for e in events: handler(e)              -- NO SDK transaction
  BEGIN  -- short ack tx
    advance_subscription(stream, name, worker_id, last_position)
  COMMIT
  -- heartbeat in parallel:
  extend_subscription_claim(...)           -- on IS022: stop the worker
on graceful shutdown:
  release_subscription(stream, name, worker_id)
```

The `subscriptions` row lock acquired by `read_subscription_batch` is
released when the read tx commits; `advance_subscription` re-acquires
it briefly in the ack tx. The handler runs with no row lock held, so
it cannot contend with another worker that has taken over the lease
(though such a worker will observe a different `last_seen` and the
old worker's eventual `advance_subscription` call will raise IS022).

The handler is application-domain; it may write to Postgres,
Elasticsearch, Redis, an external API, or anywhere else. The SDK does
not pass it a connection. Idempotency on redelivery is the handler's
concern. An SDK that genuinely wants a co-transactional
pattern — same Postgres database, projection write atomic with
cursor advance — may still call `advance_subscription` inside
its own transaction; the SQL contract supports it. v1 SDKs do
not exercise that capability (see [ML-0004](maybe-later.md#ml-0004)).

### Process manager worker (PM-020..024, [D-0011](decisions.md#d-0011), [D-0016](decisions.md#d-0016))

Same shape as the subscription worker, but the SDK runs an
additional persist-and-ack pair (snapshot + cursor advance) in
the ack tx after the user's `handle` returns. The PM snapshot is
SDK-owned bookkeeping (PM-024 absorption depends on its
`source_version` advancing in lock-step with `last_seen` on
every routed-event ack), so it stays inside the SDK's ack tx
alongside `advance_subscription`. Ignored events advance only
the cursor, leaving `source_version` unchanged. Dispatch happens in a separate
session via `append_to_stream` per the lock-set disjointness above.

```text
claim_subscription($all, pm_name, worker_id, lease_secs)
loop:
  BEGIN; events = read_subscription_batch(...); COMMIT  -- short read tx
  if events empty: sleep poll_interval; continue
  for e in events:
    state = read_snapshot(pm_source_uuid)  -- or empty
    (state, commands) = pm_handle(state, e)              -- NO SDK transaction
    for c in commands: dispatch(c)                       -- separate session
    BEGIN  -- short snapshot+ack tx, SDK-internal
      record_snapshot(pm_source_uuid, pm_type, e.event_number, state)
      advance_subscription($all, pm_name, worker_id, e.event_number)
    COMMIT
```

`dispatch(c)` is whatever the SDK exposes to call `append_to_stream`
on the target aggregate. It opens its own connection (and thus its own
transaction); the PM's snapshot+ack transaction does not nest inside
it. This is the lock-set disjointness [D-0011](decisions.md#d-0011)
relies on.

### Strong-consistency-on-dispatch wait ([D-0010](decisions.md#d-0010))

```text
result = append_to_stream(...)
for sub_name in consistency_list:
  poll until read_subscription_position(stream, sub_name).last_seen
            >= result.last_event_number
  or until consistency_timeout elapses
```

`consistency_list` is the explicit list per [D-0010](decisions.md#d-0010)
(no `:strong` shorthand); polling cadence is the SDK's responsibility.

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
- [`docs/maybe-later.md`](maybe-later.md) — ML-0001 (`shard`
  column), ML-0002 (`LISTEN`/`NOTIFY` wake-up), ML-0003
  (server-side selectors), ML-0004 (`bindToConnection`).
