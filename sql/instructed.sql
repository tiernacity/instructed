-- instructed installs a Postgres-native CQRS/event-store contract that can be
-- dropped into an existing database. It bootstraps the `instructed` schema and
-- the required extensions so that streams, events, snapshots, and persistent
-- subscriptions all live alongside application data without external services.
--
-- The SQL file is the spec. Schema constraints, stored procedure pre/post
-- conditions, error SQLSTATEs, and lock-acquisition order live in comments in
-- this file. `docs/sql-contract.md` is a human-oriented reference; this file
-- is the source of truth.
--
-- Tables in the `instructed` schema:
--   streams         -- one row per logical event stream, including `$all`
--                      (stream_id = 0) which is the global stream all events
--                      are linked into. Carries the current per-stream version
--                      and acts as the per-stream + global serialisation
--                      point (per D-0012 / OQ-0001 resolution).
--   events          -- one row per recorded event. The row is identified by a
--                      caller-supplied UUID (INV-APPEND-001). Append-only:
--                      triggers raise on UPDATE / DELETE (NG-0008).
--   stream_events   -- many-to-(stream) join table: one row per (event,
--                      stream-it-is-linked-into) pair. For every event there
--                      is exactly one row with `stream_id` = the event's
--                      origin stream and one row with `stream_id = 0` linking
--                      it into `$all`. Carries the gapless per-stream
--                      `stream_version` (INV-APPEND-002) and \"original
--                      stream\" projection columns required by INV-READ-005..008.
--                      Unique constraint on (stream_id, stream_version) is the
--                      mechanism realising optimistic concurrency control
--                      (INV-APPEND-022, D-0005).
--   snapshots       -- at most one snapshot per `source_uuid`
--                      (INV-SNAP-001). Reused for process-manager state
--                      (PM-020..024); the snapshot's `source_version` doubles
--                      as the PM instance's `last_seen_event` (PM-024).
--   subscriptions   -- persistent leased cursors (D-0006). One row per
--                      `(stream_id, subscription_name, shard)`. The `shard`
--                      column is reserved at v1 with default 0 so that
--                      ML-0013 (partitioned consumers) can be added without
--                      breaking v1 callers. Under SUB-A the `last_seen`
--                      column is conceptually the *routing cursor*: it is
--                      advanced by the routing worker as it inserts work
--                      items, not by per-event ack.
--   subscription_work_items
--                   -- SUB-A work queue. One row per `(subscription, partition,
--                      event_number)` decision emitted by the routing worker.
--                      Processing workers claim, complete, or fail rows here;
--                      per-partition ordering is enforced by a NOT EXISTS
--                      predicate against the same table. See the table
--                      docstring below for the full lifecycle contract.
--
-- Procedures (full docstrings on each):
--   append_to_stream, read_stream, read_all,
--   record_snapshot, read_snapshot, delete_snapshot,
--   claim_subscription, extend_subscription_claim, release_subscription,
--   read_subscription_batch, advance_subscription,
--   read_subscription_position, delete_subscription,
--   route_batch, claim_work_item, extend_work_item_claim,
--   complete_work_item_projection, complete_work_item_pm,
--   complete_pm_instance, fail_work_item,
--   is_subscription_caught_up, list_pm_rebuild_events.
--
-- All procedures that accept caller-tunable knobs do so via a `p_options jsonb`
-- parameter rather than positional arguments, so that ML-0013 / ML-0002 can
-- grow the surface without breaking callers. (Following absurd's convention.)

create extension if not exists "uuid-ossp";

create schema if not exists instructed;

-- Returns the instructed schema release version baked into this SQL file.
-- During development this is "main"; release automation replaces it with the
-- actual tag version. Mirrors `absurd.get_schema_version`.
create or replace function instructed.get_schema_version ()
  returns text
  language sql
as $$
  select 'main'::text;
$$;

-- ----------------------------------------------------------------------------
-- Custom SQLSTATE catalogue
--
-- Error codes raised by the procedures in this file. Codes are in class 'IS'
-- (Instructed Store). The Postgres documentation reserves classes in the
-- ranges 00..0Z, 20..2Z, 38..44, 53..58, 72, F0, HV, P0, XX; the class 'IS'
-- is outside those ranges and will not collide with future PostgreSQL error
-- codes. SDKs translate each SQLSTATE to a language-native error type.
--
--   IS001  wrong_expected_version          (INV-APPEND-013, INV-APPEND-020)
--   IS002  stream_exists                   (INV-APPEND-011)
--   IS003  stream_not_found                (INV-APPEND-012, INV-READ-001)
--   IS004  duplicate_event                 (INV-APPEND-030, D-0011 Phase 7 input #3)
--   IS005  reserved_stream_uuid            (INV-STREAM-003 / NG-0011)
--   IS010  snapshot_not_found              (INV-SNAP-003)
--   IS020  subscription_not_found          (INV-SUB-P-062 / D-0009)
--   IS021  subscription_already_claimed    (D-0006; claim attempted on live lease)
--   IS022  subscription_lease_lost         (D-0006; worker no longer holds claim)
--   IS030  work_item_lease_lost            (SUB-A; complete/fail by a worker
--                                              that no longer holds the row's
--                                              claim, or the row was already
--                                              terminal-deleted by a takeover
--                                              worker. Either way: stop.)
--
-- The full closed error set per procedure is documented in each procedure's
-- docstring below.
-- ----------------------------------------------------------------------------

-- ============================================================================
-- Schema
-- ============================================================================

-- streams
--
-- One row per logical event stream. Carries the current per-stream version
-- (`stream_version`) and serves as the per-stream serialisation point: every
-- `append_to_stream` call UPDATEs the row, taking a row-level lock for the
-- rest of the transaction.
--
-- The seed row (stream_id = 0, stream_uuid = '$all') represents the global
-- stream. Its `stream_version` is the latest globally-assigned `event_number`
-- (INV-APPEND-003). Every append also UPDATEs this row, taking its row-level
-- lock and reserving a contiguous block of global numbers. Per D-0012 this is
-- the mechanism that realises INV-APPEND-003's gaplessness under concurrent
-- writers.
--
-- The CHECK constraint enforces INV-STREAM-003 (NG-0011): the literal string
-- `$all` is reserved for the seed row only; user appends with that
-- `stream_uuid` raise IS005 (reserved_stream_uuid) before reaching the unique
-- constraint.
create table instructed.streams (
  stream_id      bigint generated by default as identity primary key,
  stream_uuid    text not null unique,
  stream_version bigint not null default 0,
  created_at     timestamptz not null default now(),
  check (stream_uuid <> '$all' or stream_id = 0)
);

-- Seed the `$all` stream. Insert with explicit stream_id = 0 to bypass the
-- identity default; subsequent user inserts let the identity assign the next
-- positive id.
insert into instructed.streams (stream_id, stream_uuid, stream_version)
values (0, '$all', 0)
on conflict do nothing;

-- Bump the identity sequence past 0 so the first user-created stream gets
-- stream_id = 1, not 0. (Without this, the next nextval() would be 1 anyway,
-- but if a future migration ever pre-populates streams the explicit setval
-- documents intent.)
select setval(
  pg_get_serial_sequence('instructed.streams', 'stream_id'),
  1,
  false
);


-- events
--
-- One row per recorded event. The `event_id` is supplied by the caller
-- (INV-APPEND-001, D-0011 Phase 7 input #2): the SDK generates a fresh UUID
-- for normal commands; an absurd-bridge task derives the id deterministically
-- from `(task_id, step_name)` so a re-run gets `duplicate_event` (IS004)
-- instead of a second insert. The primary-key uniqueness check is the
-- mechanism that realises INV-APPEND-030.
--
-- `data` and `metadata` are `jsonb` per INV-META-011 mapping (NG-0010). No
-- bytea payloads in v1.
--
-- Append-only: the no_update_events / no_delete_events triggers raise on any
-- UPDATE or DELETE (INV-APPEND-040, NG-0008).
create table instructed.events (
  event_id        uuid primary key,
  event_type      text not null,
  causation_id    uuid,
  correlation_id  uuid,
  data            jsonb not null,
  metadata        jsonb,
  created_at      timestamptz not null default now()
);


-- stream_events
--
-- Many-to-(stream) join. Each event has exactly two rows: one with
-- `stream_id` = its origin stream (and `original_stream_id` equal to the
-- same), and one with `stream_id = 0` linking it into `$all` (with
-- `original_stream_id` still the event's origin). The `original_*` columns
-- realise INV-READ-005..008: a `$all` read projects events with their
-- *original* stream identity, not their position within `$all`.
--
-- The UNIQUE constraint on (stream_id, stream_version) is the optimistic-
-- locking mechanism (INV-APPEND-022 / D-0005): two concurrent appenders that
-- both compute "next version = V" both try to insert the same (stream_id, V)
-- pair; one wins, the other gets unique_violation, which `append_to_stream`
-- catches and translates to IS001 (wrong_expected_version).
--
-- Append-only: triggers raise on UPDATE / DELETE.
create table instructed.stream_events (
  event_id                uuid not null references instructed.events (event_id),
  stream_id               bigint not null references instructed.streams (stream_id),
  stream_version          bigint not null,
  original_stream_id      bigint not null references instructed.streams (stream_id),
  original_stream_version bigint not null,
  primary key (event_id, stream_id),
  unique (stream_id, stream_version)
);


-- snapshots
--
-- INV-SNAP-001: at most one snapshot per `source_uuid`. Used by aggregates
-- (`source_uuid = aggregate_uuid`, `source_type = aggregate_module`) and by
-- process managers (`source_uuid = "<pm_name>-<process_uuid>"`,
-- `source_type = pm_module`); see PM-020..024.
--
-- `record_snapshot` is a full-row upsert (INV-SNAP-002); `read_snapshot` on a
-- missing source_uuid raises IS010; `delete_snapshot` is idempotent
-- (INV-SNAP-004) -- contrast `delete_subscription` per D-0009.
create table instructed.snapshots (
  source_uuid    text primary key,
  source_type    text not null,
  source_version bigint not null,
  data           jsonb not null,
  metadata       jsonb,
  created_at     timestamptz not null default now()
);


-- subscriptions
--
-- One row per persistent subscription. Identity is
-- `(stream_id, subscription_name, shard)`. The `shard` column is reserved at
-- v1 with default 0 per ML-0013 so that partitioned-consumer support can be
-- added without a v1-breaking migration.
--
-- Leased ownership (D-0006): `claimed_by` records the current worker (or
-- NULL); `claim_expires_at` is when the lease becomes reclaimable.
-- `last_seen` is the cursor position. For single-stream subscriptions this is
-- the stream_version of the last delivered-or-skipped event; for `$all`
-- subscriptions it is the `event_number` (which is `stream_events.stream_version`
-- with `stream_id = 0`). Initialised by `start_from` on first create
-- (INV-SUB-P-020); ignored on subsequent (re)claims (INV-SUB-P-021).
--
-- `created_at` is the moment the subscription row was created; it does not
-- move on re-claim.
create table instructed.subscriptions (
  stream_id          bigint not null references instructed.streams (stream_id),
  subscription_name  text not null,
  shard              smallint not null default 0,
  last_seen          bigint not null default 0,
  claimed_by         text,
  claim_expires_at   timestamptz,
  created_at         timestamptz not null default now(),
  primary key (stream_id, subscription_name, shard)
);

-- Reclaim sweep helper: workers calling claim_subscription will scan rows
-- whose claim has expired. The index keeps that scan cheap as the
-- subscriptions table grows.
create index subscriptions_claim_expires_idx
  on instructed.subscriptions (claim_expires_at)
  where claimed_by is not null;


-- subscription_work_items
--
-- SUB-A work queue. One row per routing decision: the routing worker reads
-- the next batch from `$all`, runs the user-supplied `RouteFn` per event,
-- and inserts one row here for every event that routed to a partition
-- (`"ignore"` decisions produce no row). The cursor advance on
-- `subscriptions.last_seen` and the work-item INSERTs commit together --
-- that same-tx atomicity is what makes the SUB-A catch-up predicate
-- (`waitForProjection`) race-safe at the start: once `last_seen >= N` is
-- observable, the corresponding work-item rows (if any) are observable too.
--
-- Processing workers claim rows here via `claim_work_item`, run the
-- user-supplied handler, and transition the row to a terminal state. The
-- partial index below excludes `done` rows from the hot claim path; see the
-- SUB-A "Processing worker -- claim and complete" section for the claim
-- query shape.
--
-- Per-partition ordering: a row is claimable only if no earlier row for the
-- same `(subscription_id, partition_key)` is still in a non-terminal state
-- (`pending` / `claimed` / `failed`). This gives concurrent claims *across*
-- partitions and serial claims *within* a partition. A `failed` row blocks
-- subsequent work for its partition only; other partitions are unaffected.
-- `failed` rows are never auto-skipped or auto-deleted: operator action
-- (deferred to `instructedctl`) is required to clear them.
--
-- Lifecycle by subscription kind (SUB-A "Work-item lifecycle by subscription
-- kind"):
--
--   Projection (PRJ-E): on handler success the SDK calls
--     `complete_work_item_projection`, which DELETEs the row. The DELETE
--     runs as its own short SDK-owned tx *after* the handler returns; the
--     handler is opaque to the SDK and may target any store (Postgres,
--     Elasticsearch, Redis, an HTTP API, ...). See D-0016 in
--     `docs/decisions.md`. `done` rows do not exist for projections.
--
--   Process manager (PM-C / PM-F): on non-terminal success the row is
--     UPDATEd to `state = 'done'` in the same tx as the snapshot upsert.
--     `done` rows accumulate per partition for the life of the PM instance
--     because PM-C's snapshot rebuild via `apply` reads them. When `handle`
--     returns `{ complete: true }` the snapshot and every work-item for the
--     partition (including the triggering one) are DELETEd in one tx.
--
-- Columns:
--   subscription_id   FK into `subscriptions`; identifies the owning
--                       subscription. Composite FK on
--                       `(stream_id, subscription_name, shard)` because that
--                       is `subscriptions`' PK (no synthetic id at v1; see
--                       "Note on `subscription_id`" in the table body).
--   partition_key     opaque string chosen by `RouteFn`. For projections the
--                       three `PartitionBy` modes (PRJ-A) reduce to:
--                         sequential  -> the literal `'_default'`
--                         per-event   -> the event_number as text
--                         per-key     -> the user's key(event)
--                       For PMs it is the PM instance identifier.
--   event_number      the global `event_number` from `$all` that this work
--                       item refers to. The event payload is fetched by
--                       processing workers via primary-key lookup on
--                       `stream_events` (stream_id = 0, stream_version = N).
--   state             one of:
--                         'pending' -- routed, not yet claimed.
--                         'claimed' -- a worker holds a lease (see
--                                      `lease_expires_at`). On lease expiry
--                                      another worker may take it (the claim
--                                      query has a takeover branch).
--                         'failed'  -- handler raised; SUB-B error policy
--                                      requested no further retries (or the
--                                      core gave up). Blocks the partition
--                                      until an operator clears it.
--                         'done'    -- terminal-success for PMs only.
--                                      Projections DELETE on success, so
--                                      `done` rows are PM-only in practice;
--                                      the CHECK still admits the value
--                                      uniformly across kinds.
--   claimed_by        opaque worker identifier supplied by the SDK; NULL
--                       when state is not `'claimed'`.
--   lease_expires_at  wall-clock expiry of the current claim; NULL when
--                       state is not `'claimed'`. The claim query's takeover
--                       branch treats `state = 'claimed' AND lease_expires_at
--                       < now()` as eligible.
--   failed_at         set when the row transitions to `'failed'`; NULL
--                       otherwise. Diagnostic only.
--   error_text        SUB-B error message; NULL when state is not `'failed'`.
--                       Diagnostic only; not parsed by the framework.
--
-- Identity / PK: `(subscription_id_components..., partition_key,
-- event_number)` is the natural key. Routing is crash-safe via this PK: a
-- routing worker that crashes mid-batch and retries hits `ON CONFLICT DO
-- NOTHING` on the INSERT, then re-attempts the cursor advance.
--
-- Note on `subscription_id`: the SUB-A design sketch uses a synthetic
-- `subscription_id INT` for brevity. At v1 the `subscriptions` table has a
-- composite PK `(stream_id, subscription_name, shard)` and no surrogate id
-- column. We expand the FK / PK accordingly here rather than retrofit a
-- surrogate id onto `subscriptions` in this slice. If a surrogate id is
-- introduced later (e.g. for cross-table joins or ML-0013 ergonomics) this
-- table's PK shrinks to `(subscription_id, partition_key, event_number)`
-- with no semantic change.
create table instructed.subscription_work_items (
  stream_id         bigint   not null,
  subscription_name text     not null,
  shard             smallint not null,
  partition_key     text     not null,
  event_number      bigint   not null,
  state             text     not null
    check (state in ('pending','claimed','failed','done')),
  claimed_by        text,
  lease_expires_at  timestamptz,
  failed_at         timestamptz,
  error_text        text,
  primary key (stream_id, subscription_name, shard, partition_key, event_number),
  foreign key (stream_id, subscription_name, shard)
    references instructed.subscriptions (stream_id, subscription_name, shard)
    on delete cascade,
  -- Per-state column invariants. These are mechanism-level; the procedure
  -- contract is the user-facing surface, but the CHECKs document the shape
  -- the procedures will maintain and catch direct-SQL bugs early.
  check (
    (state = 'claimed') = (claimed_by is not null and lease_expires_at is not null)
  ),
  check (
    (state = 'failed') = (failed_at is not null)
  ),
  check (
    error_text is null or state = 'failed'
  )
);

-- Hot-path partial index for the claim query (SUB-A "Processing worker --
-- claim and complete"). Excludes `done` rows so the per-partition NOT
-- EXISTS subquery stays cheap as PMs accumulate completed work items.
create index subscription_work_items_claimable
  on instructed.subscription_work_items
     (stream_id, subscription_name, shard, event_number)
  where state in ('pending','claimed','failed');


-- ============================================================================
-- Immutability triggers
--
-- INV-APPEND-040 / NG-0008: events and stream_events are append-only. We
-- reject UPDATE and DELETE outright. Operators with extraordinary GDPR-style
-- needs must use external tooling and own the consequences.
-- ============================================================================

create or replace function instructed.raise_append_only ()
  returns trigger
  language plpgsql
as $$
begin
  raise exception 'instructed: % on % is not permitted (append-only)',
    tg_op, tg_table_name
    using errcode = 'IS006', -- not in the documented procedure-facing set;
                              -- this fires only for direct table manipulation,
                              -- never via a procedure
          hint = 'instructed events and stream_events are append-only by design (NG-0008).';
end;
$$;

create trigger events_no_update
  before update on instructed.events
  for each row execute function instructed.raise_append_only();

create trigger events_no_delete
  before delete on instructed.events
  for each row execute function instructed.raise_append_only();

create trigger stream_events_no_update
  before update on instructed.stream_events
  for each row execute function instructed.raise_append_only();

create trigger stream_events_no_delete
  before delete on instructed.stream_events
  for each row execute function instructed.raise_append_only();


-- ============================================================================
-- Procedure contract (Pass 2)
--
-- Each procedure has a docstring covering:
--   * inputs (positional + options-jsonb shape)
--   * outputs (return table or scalar)
--   * the closed set of errors with SQLSTATEs
--   * lock-acquisition order
--
-- Lock-set disjointness, per D-0011 Phase 7 input #6:
--
--   append_to_stream            holds  { streams[target], streams[$all],
--                                        events, stream_events }
--   read_stream / read_all      holds  { } (MVCC reads only)
--   record_snapshot             holds  { snapshots[source_uuid] }
--   read_snapshot               holds  { } (MVCC read)
--   delete_snapshot             holds  { snapshots[source_uuid] }
--   claim_subscription          holds  { subscriptions[stream,name,shard] }
--   extend_subscription_claim   holds  { subscriptions[stream,name,shard] }
--   release_subscription        holds  { subscriptions[stream,name,shard] }
--   read_subscription_batch     holds  { subscriptions[stream,name,shard]
--                                        (FOR UPDATE), events (MVCC) }
--   advance_subscription        holds  { subscriptions[stream,name,shard] }
--   read_subscription_position  holds  { } (MVCC read)
--   delete_subscription         holds  { subscriptions[stream,name,shard] }
--   route_batch                 holds  { subscriptions[stream,name,shard]
--                                        (FOR UPDATE),
--                                        subscription_work_items (INSERTs) }
--   claim_work_item             holds  { subscription_work_items[one row]
--                                        (FOR UPDATE SKIP LOCKED),
--                                        events / stream_events (MVCC) }
--   extend_work_item_claim      holds  { subscription_work_items[one row] }
--   complete_work_item_projection  holds  { subscription_work_items[one row] }
--                                  (own short SDK tx, after the handler
--                                  returns; no read-model locks -- see
--                                  D-0016 in `docs/decisions.md`)
--   complete_work_item_pm       holds  { subscription_work_items[one row],
--                                        snapshots[source_uuid] }
--   complete_pm_instance        holds  { subscription_work_items[partition
--                                        slice], snapshots[source_uuid] }
--   fail_work_item              holds  { subscription_work_items[one row] }
--   is_subscription_caught_up   holds  { } (MVCC read)
--   list_pm_rebuild_events      holds  { } (MVCC read; cold path)
--
-- The persist-and-ack transaction the PM worker opens (record_snapshot then
-- advance_subscription in one tx, per D-0008/PM-023) holds
--   { snapshots, subscriptions }
-- which is disjoint from the dispatch transaction
--   { streams, events, stream_events }.
-- A different SDK binary or language (the absurd-bridge task, per D-0011
-- Phase 7 input #4) calling append_to_stream from a fresh session never
-- competes for those locks because it only touches the dispatch set.
--
-- Forward-compat: every procedure that takes caller-tunable knobs accepts
-- them via a `p_options jsonb default '{}'::jsonb` parameter so ML-0013 and
-- ML-0002 can grow the surface without breaking v1 callers. Recognised keys
-- per procedure are documented in each docstring; unknown keys are
-- silently ignored.
--
-- Bodies in this file are stubs (raise feature_not_supported, 0A000) so the
-- file installs and the signatures lock in. Pass 3 fills them in.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- append_to_stream
--
-- Atomically append N events to a single stream. Realises Part B of
-- `docs/invariants.md` (INV-APPEND-001..041) and the optimistic-locking
-- mechanism of D-0005 / INV-APPEND-022.
--
-- Inputs:
--   p_stream_uuid              text, the target stream's external identity.
--                                MUST NOT be the reserved literal '$all'
--                                (INV-STREAM-003 / NG-0011); raises IS005.
--   p_expected_version_type    text, one of:
--                                'any'           -- :any_version (INV-APPEND-010)
--                                'no_stream'     -- :no_stream   (INV-APPEND-011)
--                                'stream_exists' -- :stream_exists (INV-APPEND-012)
--                                'exact'         -- integer V    (INV-APPEND-013)
--                              Any other value raises invalid_parameter_value
--                              (22023).
--   p_expected_version         bigint, required iff p_expected_version_type =
--                              'exact'; the current stream version that MUST
--                              hold at append time. Ignored for other types.
--   p_events                   jsonb, a non-empty JSON array of event objects.
--                              Each element MUST have:
--                                event_id        uuid    (caller-supplied,
--                                                          INV-APPEND-001,
--                                                          D-0011 Phase 7 #2)
--                                event_type      text
--                                data            jsonb   (INV-META-011)
--                              and MAY have:
--                                metadata        jsonb   (default null)
--                                causation_id    uuid    (default null)
--                                correlation_id  uuid    (default null)
--                              An empty or null array raises
--                              invalid_parameter_value (22023).
--   p_options                  jsonb, default '{}'. Recognised keys: none in
--                              v1. ML-0002 reserves room for a future
--                              'notify': bool key to suppress an end-of-
--                              transaction pg_notify; that key MUST default
--                              to true when added so v1 callers do not see
--                              behaviour change.
--
-- Output: one row per appended event, in append order:
--   event_id          uuid          echoed from input
--   stream_version    bigint        per-stream contiguous version, starting at
--                                    (current_version + 1) (INV-APPEND-002)
--   event_number      bigint        gapless global position (INV-APPEND-003)
--   created_at        timestamptz   the row's stored timestamp
--
-- Errors (closed set):
--   IS001  wrong_expected_version   the current stream version did not match
--                                    `p_expected_version` ('exact'), or a
--                                    concurrent append took the same version
--                                    slot (INV-APPEND-020).
--   IS002  stream_exists            p_expected_version_type = 'no_stream'
--                                    but the stream already exists
--                                    (INV-APPEND-011).
--   IS003  stream_not_found         p_expected_version_type = 'stream_exists'
--                                    but the stream does not exist
--                                    (INV-APPEND-012).
--   IS004  duplicate_event          one of the supplied event_ids already
--                                    exists in the store. The append is
--                                    rolled back wholesale; no partial
--                                    persistence (INV-APPEND-030, D-0011
--                                    Phase 7 #3).
--   IS005  reserved_stream_uuid     p_stream_uuid = '$all'.
--   22023  invalid_parameter_value  malformed input (unknown
--                                    expected_version_type, empty events
--                                    array, missing required event fields,
--                                    or bad UUID).
--
-- Atomicity (INV-APPEND-006/007): all N events succeed or none do. The
-- procedure runs in a single (implicit) transaction; any error rolls back
-- the per-stream and `$all` version bumps along with the event rows.
--
-- Lock-acquisition order (D-0012, the OQ-0001 resolution):
--   1. The target stream's `streams` row: either upserted (INV-APPEND-010
--      'any' / 'no_stream') or selected and version-checked
--      ('stream_exists' / 'exact'). The row-level lock acquired here
--      serialises concurrent appends to the same stream.
--   2. The `$all` row in `streams` (stream_id = 0):
--        UPDATE streams SET stream_version = stream_version + N
--          WHERE stream_id = 0
--          RETURNING stream_version - N AS initial_event_number
--      The row lock taken here is the global serialisation point that
--      gives INV-APPEND-003 its gaplessness.
--   3. N new rows in `events`. INV-APPEND-030 fires here if any event_id
--      collides with a prior row (events_pkey unique violation translated
--      to IS004).
--   4. 2N new rows in `stream_events`: one linking each event into its
--      origin stream and one into `$all`. INV-APPEND-022 / D-0005 fires
--      here on the origin-stream link if a concurrent appender beat us to
--      the same (stream_id, stream_version) (stream_events_(stream_id,
--      stream_version) unique violation translated to IS001).
--
-- Session / transaction:
--   Callable from any session (including a different SDK binary or
--   programming language, per D-0011 Phase 7 #4). The procedure makes no
--   assumption about session GUCs, prior transactions, or held leases. The
--   caller MAY wrap append_to_stream in a larger transaction; in that case
--   the locks above are held until the caller's commit.
-- ----------------------------------------------------------------------------
create or replace function instructed.append_to_stream (
  p_stream_uuid           text,
  p_expected_version_type text,
  p_expected_version      bigint,
  p_events                jsonb,
  p_options               jsonb default '{}'::jsonb
)
  returns table (
    event_id        uuid,
    stream_version  bigint,
    event_number    bigint,
    created_at      timestamptz
  )
  language plpgsql
as $$
#variable_conflict use_column
declare
  v_n          integer;
  v_stream_id  bigint;
  v_base_sv    bigint;  -- per-stream version before this batch
  v_base_en    bigint;  -- global event_number before this batch
  v_current    bigint;
  v_constraint text;
begin
  -- ----- input validation -----------------------------------------------
  if p_stream_uuid is null then
    raise exception 'append_to_stream: p_stream_uuid is null'
      using errcode = '22023';
  end if;
  if p_stream_uuid = '$all' then
    raise exception 'append_to_stream: stream_uuid ''$all'' is reserved'
      using errcode = 'IS005';
  end if;
  if p_expected_version_type is null
     or p_expected_version_type not in ('any','no_stream','stream_exists','exact')
  then
    raise exception 'append_to_stream: invalid p_expected_version_type: %',
      coalesce(p_expected_version_type, '<null>')
      using errcode = '22023';
  end if;
  if p_expected_version_type = 'exact'
     and (p_expected_version is null or p_expected_version < 0)
  then
    raise exception 'append_to_stream: p_expected_version must be a non-negative integer for ''exact'''
      using errcode = '22023';
  end if;
  if p_events is null
     or jsonb_typeof(p_events) <> 'array'
     or jsonb_array_length(p_events) = 0
  then
    raise exception 'append_to_stream: p_events must be a non-empty JSON array'
      using errcode = '22023';
  end if;
  v_n := jsonb_array_length(p_events);

  -- Per-event shape check. Each element must have event_id (uuid),
  -- event_type (text), data (jsonb of any type). causation_id /
  -- correlation_id may be present-and-uuid or absent/null.
  if exists (
    select 1
    from jsonb_array_elements(p_events) as evt
    where not (evt ? 'event_id')
       or not (evt ? 'event_type')
       or not (evt ? 'data')
       or jsonb_typeof(evt->'event_type') <> 'string'
       or jsonb_typeof(evt->'event_id')   <> 'string'
  ) then
    raise exception 'append_to_stream: each event must have event_id, event_type, data'
      using errcode = '22023';
  end if;
  begin
    perform (evt->>'event_id')::uuid
      from jsonb_array_elements(p_events) as evt;
  exception when invalid_text_representation then
    raise exception 'append_to_stream: malformed event_id (must be UUID)'
      using errcode = '22023';
  end;

  -- ----- (1) resolve / lock the target stream's row ---------------------
  case p_expected_version_type
    when 'any' then
      -- Upsert: create the stream on first append, otherwise bump version.
      -- The ON CONFLICT path takes a row-level lock on the existing row
      -- (per D-0005); the INSERT path takes a lock on the new row.
      insert into instructed.streams as s (stream_uuid, stream_version)
      values (p_stream_uuid, v_n)
      on conflict (stream_uuid) do update
        set stream_version = s.stream_version + v_n
      returning s.stream_id, s.stream_version - v_n
        into v_stream_id, v_base_sv;

    when 'no_stream' then
      begin
        insert into instructed.streams (stream_uuid, stream_version)
        values (p_stream_uuid, v_n)
        returning stream_id, 0::bigint
          into v_stream_id, v_base_sv;
      exception when unique_violation then
        raise exception 'append_to_stream: stream % already exists', p_stream_uuid
          using errcode = 'IS002';
      end;

    when 'stream_exists' then
      update instructed.streams s
         set stream_version = s.stream_version + v_n
       where s.stream_uuid = p_stream_uuid
      returning s.stream_id, s.stream_version - v_n
        into v_stream_id, v_base_sv;
      if not found then
        raise exception 'append_to_stream: stream % does not exist', p_stream_uuid
          using errcode = 'IS003';
      end if;

    when 'exact' then
      select stream_id, stream_version
        into v_stream_id, v_current
        from instructed.streams
       where stream_uuid = p_stream_uuid
       for update;
      if not found then
        if p_expected_version = 0 then
          -- INV-APPEND-014: V=0 against a missing stream creates it.
          -- Concurrent first-time appenders both pass the SELECT FOR
          -- UPDATE above (no row exists yet, so no lock is acquired);
          -- the loser's INSERT here trips streams_stream_uuid_key. We
          -- translate that to IS001 because both callers asserted
          -- "stream is at version 0" and the loser was wrong: the
          -- actual version is now N. The SDK's runCommand OCC loop
          -- handles IS001 idiomatically.
          begin
            insert into instructed.streams (stream_uuid, stream_version)
            values (p_stream_uuid, v_n)
            returning stream_id, 0::bigint
              into v_stream_id, v_base_sv;
          exception when unique_violation then
            raise exception 'append_to_stream: stream % was created concurrently (expected version 0)',
              p_stream_uuid
              using errcode = 'IS001';
          end;
        else
          raise exception 'append_to_stream: stream % does not exist (expected version %)',
            p_stream_uuid, p_expected_version
            using errcode = 'IS001';
        end if;
      else
        if v_current <> p_expected_version then
          raise exception 'append_to_stream: wrong expected version: actual %, expected %',
            v_current, p_expected_version
            using errcode = 'IS001';
        end if;
        update instructed.streams s
           set stream_version = s.stream_version + v_n
         where s.stream_id = v_stream_id
        returning s.stream_version - v_n into v_base_sv;
      end if;
  end case;

  -- ----- (2) lock and bump the $all row ---------------------------------
  -- D-0012: the row lock taken here is the global serialisation point
  -- that gives INV-APPEND-003 its gaplessness.
  update instructed.streams s
     set stream_version = s.stream_version + v_n
   where s.stream_id = 0
  returning s.stream_version - v_n into v_base_en;

  -- ----- (3) insert events and (4) link into origin + $all --------------
  -- A single CTE chain so the exception block can distinguish events_pkey
  -- (IS004) from stream_events_stream_id_stream_version_key (IS001).
  begin
    return query
    with
      new_events as (
        select
          (evt->>'event_id')::uuid                       as event_id,
          (evt->>'event_type')::text                     as event_type,
          nullif(evt->>'causation_id','')::uuid          as causation_id,
          nullif(evt->>'correlation_id','')::uuid        as correlation_id,
          coalesce(evt->'data', 'null'::jsonb)           as data,
          evt->'metadata'                                as metadata,
          idx                                            as idx,
          (v_base_sv + idx)::bigint                      as sv,
          (v_base_en + idx)::bigint                      as en
        from jsonb_array_elements(p_events) with ordinality as t(evt, idx)
      ),
      ins_events as (
        insert into instructed.events
          (event_id, event_type, causation_id, correlation_id, data, metadata)
        select event_id, event_type, causation_id, correlation_id, data, metadata
          from new_events
        returning event_id, created_at
      ),
      ins_origin as (
        insert into instructed.stream_events
          (event_id, stream_id, stream_version,
           original_stream_id, original_stream_version)
        select event_id, v_stream_id, sv, v_stream_id, sv from new_events
        returning 1
      ),
      ins_all as (
        insert into instructed.stream_events
          (event_id, stream_id, stream_version,
           original_stream_id, original_stream_version)
        select event_id, 0::bigint, en, v_stream_id, sv from new_events
        returning 1
      )
    select n.event_id, n.sv, n.en, i.created_at
      from new_events n
      join ins_events i using (event_id)
      -- force the linking CTEs to materialise
     where (select count(*) from ins_origin) is not null
       and (select count(*) from ins_all)    is not null
     order by n.idx;
  exception when unique_violation then
    get stacked diagnostics v_constraint = constraint_name;
    if v_constraint in ('events_pkey', 'stream_events_pkey') then
      -- events_pkey: duplicate event_id reached the events table.
      -- stream_events_pkey (event_id, stream_id): duplicate event_id
      -- reached the $all link (or the origin link). In a multi-CTE
      -- statement these fire in undefined order; both signal the same
      -- semantic condition -- a caller re-used an event_id.
      raise exception 'append_to_stream: duplicate event_id'
        using errcode = 'IS004';
    elsif v_constraint = 'stream_events_stream_id_stream_version_key' then
      raise exception 'append_to_stream: wrong expected version (concurrent append)'
        using errcode = 'IS001';
    else
      raise;
    end if;
  end;
end;
$$;


-- ----------------------------------------------------------------------------
-- read_stream
--
-- Read events from a single stream, in stream_version order. Realises Part
-- C of `docs/invariants.md` (INV-READ-001..004, INV-READ-020).
--
-- Inputs:
--   p_stream_uuid          text, the stream to read.
--                            MUST NOT be '$all'; for the global stream use
--                            read_all. A '$all' argument raises IS005.
--   p_from_stream_version  bigint, inclusive lower bound on stream_version
--                            (INV-READ-004). 0 returns from the start of
--                            the stream.
--   p_qty                  integer, maximum rows to return. NULL or <= 0
--                            raises invalid_parameter_value (22023). The
--                            SDK is responsible for paging (calling this
--                            in a loop with `from = last + 1` until empty).
--   p_options              jsonb, default '{}'. Recognised keys: none in v1.
--
-- Output: zero or more rows in strictly increasing stream_version order:
--   event_id        uuid
--   event_number    bigint        global position (INV-APPEND-003)
--   stream_uuid     text          echoed (always = p_stream_uuid)
--   stream_version  bigint        per-stream position (INV-APPEND-002)
--   event_type      text
--   causation_id    uuid
--   correlation_id  uuid
--   data            jsonb
--   metadata        jsonb
--   created_at      timestamptz
--
-- Errors (closed set):
--   IS003  stream_not_found         the stream has never been appended to
--                                    (INV-READ-001).
--   IS005  reserved_stream_uuid     p_stream_uuid = '$all'.
--   22023  invalid_parameter_value  malformed input.
--
-- Concurrency: MVCC snapshot read; INV-READ-020 applies (paged reads do
-- not promise to lazily pick up later appends).
--
-- Lock-acquisition order: none. Pure read.
--
-- Session / transaction: callable from any session.
-- ----------------------------------------------------------------------------
create or replace function instructed.read_stream (
  p_stream_uuid         text,
  p_from_stream_version bigint,
  p_qty                 integer,
  p_options             jsonb default '{}'::jsonb
)
  returns table (
    event_id        uuid,
    event_number    bigint,
    stream_uuid     text,
    stream_version  bigint,
    event_type      text,
    causation_id    uuid,
    correlation_id  uuid,
    data            jsonb,
    metadata        jsonb,
    created_at      timestamptz
  )
  language plpgsql
as $$
#variable_conflict use_column
declare
  v_stream_id bigint;
begin
  if p_stream_uuid is null then
    raise exception 'read_stream: p_stream_uuid is null'
      using errcode = '22023';
  end if;
  if p_stream_uuid = '$all' then
    raise exception 'read_stream: stream_uuid ''$all'' is reserved; use read_all'
      using errcode = 'IS005';
  end if;
  if p_from_stream_version is null or p_from_stream_version < 0 then
    raise exception 'read_stream: p_from_stream_version must be a non-negative integer'
      using errcode = '22023';
  end if;
  if p_qty is null or p_qty <= 0 then
    raise exception 'read_stream: p_qty must be a positive integer'
      using errcode = '22023';
  end if;

  select stream_id into v_stream_id
    from instructed.streams
   where stream_uuid = p_stream_uuid;
  if not found then
    raise exception 'read_stream: stream % not found', p_stream_uuid
      using errcode = 'IS003';
  end if;

  return query
  select
    e.event_id,
    all_se.stream_version           as event_number,
    p_stream_uuid                   as stream_uuid,
    se.stream_version               as stream_version,
    e.event_type,
    e.causation_id,
    e.correlation_id,
    e.data,
    e.metadata,
    e.created_at
  from instructed.stream_events se
  join instructed.events e
    on e.event_id = se.event_id
  join instructed.stream_events all_se
    on all_se.event_id = se.event_id and all_se.stream_id = 0
  where se.stream_id = v_stream_id
    and se.stream_version >= p_from_stream_version
  order by se.stream_version
  limit p_qty;
end;
$$;


-- ----------------------------------------------------------------------------
-- read_all
--
-- Read events from the global `$all` stream, in event_number order. Realises
-- INV-READ-005..008. The returned `stream_uuid` / `stream_version` carry the
-- *original* stream identity (INV-READ-006/007), not `$all` / event_number.
--
-- Inputs:
--   p_from_event_number  bigint, inclusive lower bound on event_number.
--                          0 returns from the beginning of the store.
--   p_qty                integer, maximum rows. NULL or <= 0 raises
--                          invalid_parameter_value (22023).
--   p_options            jsonb, default '{}'. Recognised keys: none in v1.
--
-- Output: zero or more rows in strictly increasing event_number order:
--   event_id        uuid
--   event_number    bigint        position in `$all`
--   stream_uuid     text          original stream's uuid (INV-READ-006)
--   stream_version  bigint        original stream's version (INV-READ-007)
--   event_type      text
--   causation_id    uuid
--   correlation_id  uuid
--   data            jsonb
--   metadata        jsonb
--   created_at      timestamptz
--
-- Errors (closed set):
--   22023  invalid_parameter_value  malformed input.
--
-- Note: `read_all` never returns IS003. The `$all` stream is guaranteed to
-- exist by the install-time seed in `instructed.streams`; an empty store is
-- a valid state and returns zero rows.
--
-- Lock-acquisition order: none. Pure read.
-- ----------------------------------------------------------------------------
create or replace function instructed.read_all (
  p_from_event_number bigint,
  p_qty               integer,
  p_options           jsonb default '{}'::jsonb
)
  returns table (
    event_id        uuid,
    event_number    bigint,
    stream_uuid     text,
    stream_version  bigint,
    event_type      text,
    causation_id    uuid,
    correlation_id  uuid,
    data            jsonb,
    metadata        jsonb,
    created_at      timestamptz
  )
  language plpgsql
as $$
#variable_conflict use_column
begin
  if p_from_event_number is null or p_from_event_number < 0 then
    raise exception 'read_all: p_from_event_number must be a non-negative integer'
      using errcode = '22023';
  end if;
  if p_qty is null or p_qty <= 0 then
    raise exception 'read_all: p_qty must be a positive integer'
      using errcode = '22023';
  end if;

  return query
  select
    e.event_id,
    se.stream_version                  as event_number,
    orig.stream_uuid                   as stream_uuid,
    se.original_stream_version         as stream_version,
    e.event_type,
    e.causation_id,
    e.correlation_id,
    e.data,
    e.metadata,
    e.created_at
  from instructed.stream_events se
  join instructed.events e
    on e.event_id = se.event_id
  join instructed.streams orig
    on orig.stream_id = se.original_stream_id
  where se.stream_id = 0
    and se.stream_version >= p_from_event_number
  order by se.stream_version
  limit p_qty;
end;
$$;


-- ----------------------------------------------------------------------------
-- record_snapshot
--
-- Full-row upsert of a snapshot keyed by `source_uuid`. Realises INV-SNAP-001
-- (at-most-one) and INV-SNAP-002 (wholesale replace). Used by aggregates and
-- by process managers (PM-020..024; PM state is just a snapshot whose
-- `source_version` doubles as `last_seen_event_number`).
--
-- Inputs:
--   p_source_uuid     text, the snapshot key. For aggregates this is the
--                       aggregate's uuid; for PMs this is
--                       "<pm_name>-<process_uuid>" per PM-020 (the
--                       namespacing convention is the SDK's responsibility;
--                       the store treats it as an opaque key).
--   p_source_type     text, informational (typically the aggregate / PM
--                       module name); echoed on read. SNAP-005.
--   p_source_version  bigint, the version this snapshot represents. SNAP-004.
--   p_data            jsonb, the serialised state (INV-META-011).
--   p_metadata        jsonb, may be null. The SDK reserves the metadata key
--                       `snapshot_module_version` per SNAP-002, but the
--                       store assigns no meaning.
--   p_options         jsonb, default '{}'. Recognised keys: none in v1.
--
-- Output: void.
--
-- Errors (closed set):
--   22023  invalid_parameter_value  null p_source_uuid / p_source_type /
--                                    p_data, or negative p_source_version.
--
-- Lock-acquisition order:
--   1. The `snapshots` row keyed by p_source_uuid (insert-or-update). This
--      is the only lock taken.
--
-- Lock-set disjointness: holds `snapshots`; does not touch `streams`,
-- `events`, `stream_events`, or `subscriptions`. Safe to call from inside
-- the PM's SDK-internal snapshot+advance transaction alongside
-- advance_subscription (D-0016 / PM-023; D-0016 limits the
-- co-transactional pattern to SDK-owned bookkeeping like the PM
-- snapshot, not to user-facing handlers).
-- ----------------------------------------------------------------------------
create or replace function instructed.record_snapshot (
  p_source_uuid    text,
  p_source_type    text,
  p_source_version bigint,
  p_data           jsonb,
  p_metadata       jsonb default null,
  p_options        jsonb default '{}'::jsonb
)
  returns void
  language plpgsql
as $$
#variable_conflict use_column
begin
  if p_source_uuid is null or p_source_uuid = '' then
    raise exception 'record_snapshot: p_source_uuid is null/empty'
      using errcode = '22023';
  end if;
  if p_source_type is null or p_source_type = '' then
    raise exception 'record_snapshot: p_source_type is null/empty'
      using errcode = '22023';
  end if;
  if p_source_version is null or p_source_version < 0 then
    raise exception 'record_snapshot: p_source_version must be a non-negative integer'
      using errcode = '22023';
  end if;
  if p_data is null then
    raise exception 'record_snapshot: p_data is null'
      using errcode = '22023';
  end if;

  insert into instructed.snapshots
    (source_uuid, source_type, source_version, data, metadata, created_at)
  values
    (p_source_uuid, p_source_type, p_source_version, p_data, p_metadata, now())
  on conflict (source_uuid) do update
    set source_type    = excluded.source_type,
        source_version = excluded.source_version,
        data           = excluded.data,
        metadata       = excluded.metadata,
        created_at     = excluded.created_at;
end;
$$;


-- ----------------------------------------------------------------------------
-- read_snapshot
--
-- Fetch the snapshot for `source_uuid`. Realises INV-SNAP-003.
--
-- Inputs:
--   p_source_uuid  text
--   p_options      jsonb, default '{}'. Recognised keys: none in v1.
--
-- Output: exactly one row (or the procedure raises):
--   source_uuid     text
--   source_type     text
--   source_version  bigint
--   data            jsonb
--   metadata        jsonb
--   created_at      timestamptz
--
-- Errors (closed set):
--   IS010  snapshot_not_found       no snapshot exists for p_source_uuid.
--   22023  invalid_parameter_value  null p_source_uuid.
--
-- Lock-acquisition order: none. Pure MVCC read.
-- ----------------------------------------------------------------------------
create or replace function instructed.read_snapshot (
  p_source_uuid text,
  p_options     jsonb default '{}'::jsonb
)
  returns table (
    source_uuid    text,
    source_type    text,
    source_version bigint,
    data           jsonb,
    metadata       jsonb,
    created_at     timestamptz
  )
  language plpgsql
as $$
#variable_conflict use_column
begin
  if p_source_uuid is null then
    raise exception 'read_snapshot: p_source_uuid is null'
      using errcode = '22023';
  end if;

  return query
  select s.source_uuid, s.source_type, s.source_version, s.data, s.metadata, s.created_at
    from instructed.snapshots s
   where s.source_uuid = p_source_uuid;
  if not found then
    raise exception 'read_snapshot: snapshot % not found', p_source_uuid
      using errcode = 'IS010';
  end if;
end;
$$;


-- ----------------------------------------------------------------------------
-- delete_snapshot
--
-- Remove the snapshot for `source_uuid`. Idempotent per INV-SNAP-004:
-- deleting a missing snapshot succeeds silently (no error). Contrast
-- delete_subscription (D-0009), which raises on missing.
--
-- Inputs:
--   p_source_uuid  text
--   p_options      jsonb, default '{}'. Recognised keys: none in v1.
--
-- Output: void.
--
-- Errors (closed set):
--   22023  invalid_parameter_value  null p_source_uuid.
--
-- Lock-acquisition order:
--   1. The `snapshots` row keyed by p_source_uuid (DELETE; no-op if
--      absent).
-- ----------------------------------------------------------------------------
create or replace function instructed.delete_snapshot (
  p_source_uuid text,
  p_options     jsonb default '{}'::jsonb
)
  returns void
  language plpgsql
as $$
#variable_conflict use_column
begin
  if p_source_uuid is null then
    raise exception 'delete_snapshot: p_source_uuid is null'
      using errcode = '22023';
  end if;

  delete from instructed.snapshots where source_uuid = p_source_uuid;
  -- INV-SNAP-004: idempotent. No error if no row was deleted.
end;
$$;


-- ----------------------------------------------------------------------------
-- claim_subscription
--
-- Acquire (or re-acquire) the lease on a persistent subscription. Realises
-- INV-SUB-P-001/002, INV-SUB-P-010..012, INV-SUB-P-020/021. Lease-based
-- single-active-subscriber per D-0006 (no pg_advisory_lock).
--
-- If the subscription row does not yet exist, it is created with the
-- initial cursor determined by p_options->>'start_from' and immediately
-- claimed by p_worker_id. If it exists and is either unclaimed or has an
-- expired lease, the lease is transferred to p_worker_id; the cursor is
-- preserved (INV-SUB-P-021: start_from is ignored on subsequent claims).
-- If it exists and a different worker holds a live lease, the call returns
-- with result = 'already_claimed' (NOT an error; the caller may retry or
-- back off; the current holder is reported for diagnostics).
--
-- Inputs:
--   p_stream_uuid       text, the subscription's stream scope. Use '$all'
--                         to subscribe to the global stream.
--   p_subscription_name text, the subscription's name (the human-readable
--                         half of INV-SUB-P-001's identity pair).
--   p_worker_id         text, an opaque identifier for the claiming
--                         worker. The SDK supplies this; the store treats
--                         it as an opaque string.
--   p_lease_seconds     integer, lease duration in seconds. MUST be > 0;
--                         null or <= 0 raises invalid_parameter_value
--                         (22023). The new claim_expires_at is set to
--                         now() + p_lease_seconds.
--   p_options           jsonb, default '{}'. Recognised keys:
--                         'start_from' :: 'origin' (default) | 'current'
--                                      | non-negative integer.
--                                      Used only on first create per
--                                      INV-SUB-P-020; ignored on
--                                      subsequent claims (INV-SUB-P-021).
--                                      'origin' -> last_seen = 0.
--                                      'current' -> last_seen = current
--                                        head (stream_version for a
--                                        single-stream subscription;
--                                        event_number for '$all').
--                                      integer N -> last_seen = N.
--                         'shard'      :: smallint (default 0). Reserved
--                                      for ML-0013; v1 callers should
--                                      omit. Unknown shard values raise
--                                      invalid_parameter_value.
--
-- Output: exactly one row:
--   result              text, 'claimed' | 'already_claimed'
--   last_seen           bigint, the subscription's cursor
--   claimed_by          text, the current holder (= p_worker_id on
--                         'claimed', else the existing holder).
--   claim_expires_at    timestamptz, when the current lease expires.
--
-- Errors (closed set):
--   IS003  stream_not_found         p_stream_uuid does not name an
--                                    existing stream (and is not '$all').
--                                    The store does not auto-create the
--                                    underlying stream on subscribe.
--   IS005  reserved_stream_uuid     not raised here -- '$all' is permitted
--                                    for subscriptions and resolves to
--                                    stream_id = 0.
--   22023  invalid_parameter_value  null/empty p_subscription_name or
--                                    p_worker_id; non-positive
--                                    p_lease_seconds; malformed
--                                    p_options.start_from; negative
--                                    shard.
--
-- Lock-acquisition order (per D-0025):
--   1. The target stream's `streams` row, read-only (lookup of
--      stream_id, no lock).
--   2. MVCC-snapshot existence + lease check on the `subscriptions` row
--      (no lock). Three outcomes:
--        a. Row missing  -> proceed to first-create branch.
--        b. Row exists, lease live, held by another worker -> return
--           'already_claimed' immediately, without taking a row lock.
--           This is the fast path for the per-batch routing loop's
--           steady-state miss: M-1 of M concurrent claimers per
--           subscription return through here every tick with zero
--           lock contention.
--        c. Row exists, lease free / expired / held by self -> proceed
--           to the locked branch below to re-verify and commit.
--   3. Locked branch:
--        - First-create: INSERT ... ON CONFLICT DO NOTHING. If the
--          INSERT conflicted (because another worker raced us in
--          between the MVCC check and the INSERT), fall through to
--          the SKIP LOCKED branch on the now-existing row.
--        - Re-claim: SELECT ... FOR UPDATE SKIP LOCKED. If 0 rows
--          (someone else is mid-write on the row), return
--          'already_claimed' -- the contention is observationally
--          equivalent to a live lease held by that writer. If 1 row,
--          re-verify the lease state (the MVCC snapshot may have been
--          stale) and either UPDATE to claim or return
--          'already_claimed'.
--
-- Lock-set disjointness: holds the subscriptions row only, briefly.
-- Does not contend with append_to_stream's lock set.
-- ----------------------------------------------------------------------------
create or replace function instructed.claim_subscription (
  p_stream_uuid       text,
  p_subscription_name text,
  p_worker_id         text,
  p_lease_seconds     integer,
  p_options           jsonb default '{}'::jsonb
)
  returns table (
    result           text,
    last_seen        bigint,
    claimed_by       text,
    claim_expires_at timestamptz
  )
  language plpgsql
as $$
#variable_conflict use_column
declare
  v_stream_id  bigint;
  v_shard      smallint;
  v_start_from text;
  v_initial    bigint;
  v_now        timestamptz := now();
  v_expires    timestamptz;
  v_row        instructed.subscriptions%rowtype;
begin
  -- ----- input validation -----------------------------------------------
  if p_stream_uuid is null then
    raise exception 'claim_subscription: p_stream_uuid is null'
      using errcode = '22023';
  end if;
  if p_subscription_name is null or p_subscription_name = '' then
    raise exception 'claim_subscription: p_subscription_name is null/empty'
      using errcode = '22023';
  end if;
  if p_worker_id is null or p_worker_id = '' then
    raise exception 'claim_subscription: p_worker_id is null/empty'
      using errcode = '22023';
  end if;
  if p_lease_seconds is null or p_lease_seconds <= 0 then
    raise exception 'claim_subscription: p_lease_seconds must be a positive integer'
      using errcode = '22023';
  end if;

  v_shard := coalesce((p_options->>'shard')::smallint, 0);
  if v_shard < 0 then
    raise exception 'claim_subscription: shard must be non-negative'
      using errcode = '22023';
  end if;

  v_start_from := coalesce(p_options->>'start_from', 'origin');
  v_expires    := v_now + make_interval(secs => p_lease_seconds);

  -- (the rest of the procedure body — stream resolution, MVCC pre-check,
  -- locked branch — is below; the MVCC pre-check fast-path was added per
  -- D-0025.)
  -- Resolve target stream_id ('$all' resolves to 0 via the seed row).
  select stream_id into v_stream_id
    from instructed.streams
   where stream_uuid = p_stream_uuid;
  if not found then
    raise exception 'claim_subscription: stream % not found', p_stream_uuid
      using errcode = 'IS003';
  end if;

  -- ----- Step 2: MVCC pre-check (no lock) per D-0025 ------------------
  -- The common case for the per-batch routing loop is "someone else
  -- holds a live lease right now". An unlocked snapshot read lets
  -- M-1 of M concurrent claimers exit through here every tick
  -- without queueing on a row lock.
  select * into v_row
    from instructed.subscriptions
   where stream_id = v_stream_id
     and subscription_name = p_subscription_name
     and shard = v_shard;

  if found
     and v_row.claimed_by is not null
     and v_row.claimed_by <> p_worker_id
     and v_row.claim_expires_at is not null
     and v_row.claim_expires_at > v_now
  then
    -- Live lease held by another worker; surface that without locking.
    -- The snapshot can be stale by the time the caller reads it, but
    -- that staleness is benign: the worst case is the caller retries
    -- and wins on a subsequent tick.
    return query
    select 'already_claimed'::text,
           v_row.last_seen,
           v_row.claimed_by,
           v_row.claim_expires_at;
    return;
  end if;

  -- ----- Step 3: locked branch ----------------------------------------
  if not found then
    -- First-create path. Compute initial cursor from start_from.
    if v_start_from = 'origin' then
      v_initial := 0;
    elsif v_start_from = 'current' then
      -- For $all this is current event_number; for single-stream this is
      -- the current stream_version. Both are the streams row's
      -- stream_version column.
      select stream_version into v_initial
        from instructed.streams
       where stream_id = v_stream_id;
    else
      begin
        v_initial := v_start_from::bigint;
      exception when invalid_text_representation then
        raise exception 'claim_subscription: start_from must be ''origin'', ''current'', or a non-negative integer'
          using errcode = '22023';
      end;
      if v_initial < 0 then
        raise exception 'claim_subscription: start_from must be non-negative'
          using errcode = '22023';
      end if;
    end if;

    -- Race: another worker may have inserted between our MVCC check
    -- and this INSERT. ON CONFLICT DO NOTHING lets us fall through to
    -- the locked re-claim path on the now-existing row.
    insert into instructed.subscriptions
      (stream_id, subscription_name, shard, last_seen,
       claimed_by, claim_expires_at)
    values
      (v_stream_id, p_subscription_name, v_shard, v_initial,
       p_worker_id, v_expires)
    on conflict (stream_id, subscription_name, shard) do nothing
    returning last_seen into v_initial;

    if found then
      return query
      select 'claimed'::text, v_initial, p_worker_id, v_expires;
      return;
    end if;
    -- INSERT lost the race; another worker created the row. Fall
    -- through to the locked re-claim path.
  end if;

  -- Take the lock with SKIP LOCKED so we never queue on a concurrent
  -- writer. Zero rows means "another transaction is mid-write"; we
  -- treat that observationally as "already_claimed" and let the caller
  -- retry on its next poll. The cursor value reported back uses the
  -- MVCC snapshot if we have one; otherwise NULL claimed_by /
  -- claim_expires_at carry the "unknown" signal forward.
  select * into v_row
    from instructed.subscriptions
   where stream_id = v_stream_id
     and subscription_name = p_subscription_name
     and shard = v_shard
   for update skip locked;

  if not found then
    -- Row exists (we either just observed it in step 2, or another
    -- worker just inserted) but it's locked by someone else right now.
    -- Report 'already_claimed'. Re-read the row without a lock so we
    -- can carry useful diagnostics; if even that fails, return nulls.
    select * into v_row
      from instructed.subscriptions
     where stream_id = v_stream_id
       and subscription_name = p_subscription_name
       and shard = v_shard;
    if found then
      return query
      select 'already_claimed'::text,
             v_row.last_seen,
             v_row.claimed_by,
             v_row.claim_expires_at;
    else
      -- The row was deleted between our checks. Treat as a transient
      -- contention signal; the caller will retry and either create or
      -- claim on the next poll.
      return query
      select 'already_claimed'::text,
             0::bigint,
             null::text,
             null::timestamptz;
    end if;
    return;
  end if;

  -- We hold the row lock. Re-verify the lease state under the lock
  -- (the MVCC snapshot may have been stale: the lease could have
  -- expired, or the same-worker case may apply). INV-SUB-P-021:
  -- start_from is ignored on subsequent claims.
  if v_row.claimed_by is null
     or v_row.claim_expires_at is null
     or v_row.claim_expires_at <= v_now
     or v_row.claimed_by = p_worker_id
  then
    update instructed.subscriptions
       set claimed_by       = p_worker_id,
           claim_expires_at = v_expires
     where stream_id = v_stream_id
       and subscription_name = p_subscription_name
       and shard = v_shard;

    return query
    select 'claimed'::text, v_row.last_seen, p_worker_id, v_expires;
    return;
  end if;

  -- Under-lock re-verify says another worker took the lease between
  -- our MVCC pre-check and our lock. Surface that as 'already_claimed'.
  return query
  select 'already_claimed'::text,
         v_row.last_seen,
         v_row.claimed_by,
         v_row.claim_expires_at;
end;
$$;


-- ----------------------------------------------------------------------------
-- extend_subscription_claim
--
-- Heartbeat: extend the lease on a subscription the caller already holds.
-- Per D-0006, if this fails (because the lease has been lost to another
-- worker, or the subscription was deleted) the worker MUST stop processing
-- immediately; continuing risks double-delivery against the new holder.
--
-- Inputs:
--   p_stream_uuid       text
--   p_subscription_name text
--   p_worker_id         text, the caller's worker_id. Must match the row's
--                         current claimed_by.
--   p_lease_seconds     integer, the new lease duration (> 0).
--   p_options           jsonb, default '{}'. Recognised keys:
--                         'shard' :: smallint (default 0; ML-0013).
--
-- Output: exactly one row:
--   claim_expires_at    timestamptz, the new lease expiry (= now() +
--                         p_lease_seconds).
--
-- Errors (closed set):
--   IS020  subscription_not_found   no subscription row for
--                                    (stream, name, shard).
--   IS022  subscription_lease_lost  the row exists but claimed_by !=
--                                    p_worker_id (another worker took
--                                    over after a lease expiry).
--   22023  invalid_parameter_value  null inputs, non-positive lease.
--
-- Lock-acquisition order:
--   1. The `subscriptions` row keyed by (stream_id, name, shard):
--      SELECT ... FOR UPDATE; verify claimed_by; UPDATE
--      claim_expires_at.
-- ----------------------------------------------------------------------------
create or replace function instructed.extend_subscription_claim (
  p_stream_uuid       text,
  p_subscription_name text,
  p_worker_id         text,
  p_lease_seconds     integer,
  p_options           jsonb default '{}'::jsonb
)
  returns table (
    claim_expires_at timestamptz
  )
  language plpgsql
as $$
#variable_conflict use_column
declare
  v_stream_id   bigint;
  v_shard       smallint;
  v_expires     timestamptz;
  v_holder      text;
begin
  if p_stream_uuid is null then
    raise exception 'extend_subscription_claim: p_stream_uuid is null'
      using errcode = '22023';
  end if;
  if p_subscription_name is null or p_subscription_name = '' then
    raise exception 'extend_subscription_claim: p_subscription_name is null/empty'
      using errcode = '22023';
  end if;
  if p_worker_id is null or p_worker_id = '' then
    raise exception 'extend_subscription_claim: p_worker_id is null/empty'
      using errcode = '22023';
  end if;
  if p_lease_seconds is null or p_lease_seconds <= 0 then
    raise exception 'extend_subscription_claim: p_lease_seconds must be a positive integer'
      using errcode = '22023';
  end if;

  v_shard   := coalesce((p_options->>'shard')::smallint, 0);
  v_expires := now() + make_interval(secs => p_lease_seconds);

  select stream_id into v_stream_id
    from instructed.streams
   where stream_uuid = p_stream_uuid;
  if not found then
    raise exception 'extend_subscription_claim: subscription not found (no such stream)'
      using errcode = 'IS020';
  end if;

  select claimed_by into v_holder
    from instructed.subscriptions
   where stream_id = v_stream_id
     and subscription_name = p_subscription_name
     and shard = v_shard
   for update;

  if not found then
    raise exception 'extend_subscription_claim: subscription % on % (shard %) not found',
      p_subscription_name, p_stream_uuid, v_shard
      using errcode = 'IS020';
  end if;
  if v_holder is distinct from p_worker_id then
    raise exception 'extend_subscription_claim: lease lost (holder=%, caller=%)',
      coalesce(v_holder,'<none>'), p_worker_id
      using errcode = 'IS022';
  end if;

  update instructed.subscriptions
     set claim_expires_at = v_expires
   where stream_id = v_stream_id
     and subscription_name = p_subscription_name
     and shard = v_shard;

  return query select v_expires;
end;
$$;


-- ----------------------------------------------------------------------------
-- release_subscription
--
-- Clean release of a held lease (INV-SUB-P-060). The cursor is preserved;
-- the row remains; only claimed_by and claim_expires_at are cleared. A
-- subsequent claim_subscription resumes from last_seen.
--
-- Inputs:
--   p_stream_uuid       text
--   p_subscription_name text
--   p_worker_id         text, the caller's worker_id. Must match
--                         claimed_by; releasing someone else's lease is an
--                         error.
--   p_options           jsonb, default '{}'. Recognised keys:
--                         'shard' :: smallint (default 0; ML-0013).
--
-- Output: void.
--
-- Errors (closed set):
--   IS020  subscription_not_found   no subscription row for
--                                    (stream, name, shard).
--   IS022  subscription_lease_lost  row exists but claimed_by !=
--                                    p_worker_id, or the lease has
--                                    already expired and been picked up
--                                    by another worker. This is
--                                    informational; the application may
--                                    safely ignore it on graceful
--                                    shutdown.
--   22023  invalid_parameter_value  null inputs.
--
-- Lock-acquisition order:
--   1. The `subscriptions` row keyed by (stream_id, name, shard):
--      SELECT ... FOR UPDATE; verify claimed_by; UPDATE
--      (claimed_by := NULL, claim_expires_at := NULL).
-- ----------------------------------------------------------------------------
create or replace function instructed.release_subscription (
  p_stream_uuid       text,
  p_subscription_name text,
  p_worker_id         text,
  p_options           jsonb default '{}'::jsonb
)
  returns void
  language plpgsql
as $$
#variable_conflict use_column
declare
  v_stream_id bigint;
  v_shard     smallint;
  v_holder    text;
begin
  if p_stream_uuid is null then
    raise exception 'release_subscription: p_stream_uuid is null'
      using errcode = '22023';
  end if;
  if p_subscription_name is null or p_subscription_name = '' then
    raise exception 'release_subscription: p_subscription_name is null/empty'
      using errcode = '22023';
  end if;
  if p_worker_id is null or p_worker_id = '' then
    raise exception 'release_subscription: p_worker_id is null/empty'
      using errcode = '22023';
  end if;

  v_shard := coalesce((p_options->>'shard')::smallint, 0);

  select stream_id into v_stream_id
    from instructed.streams
   where stream_uuid = p_stream_uuid;
  if not found then
    raise exception 'release_subscription: subscription not found (no such stream)'
      using errcode = 'IS020';
  end if;

  select claimed_by into v_holder
    from instructed.subscriptions
   where stream_id = v_stream_id
     and subscription_name = p_subscription_name
     and shard = v_shard
   for update;

  if not found then
    raise exception 'release_subscription: subscription % on % (shard %) not found',
      p_subscription_name, p_stream_uuid, v_shard
      using errcode = 'IS020';
  end if;
  if v_holder is distinct from p_worker_id then
    raise exception 'release_subscription: lease lost (holder=%, caller=%)',
      coalesce(v_holder,'<none>'), p_worker_id
      using errcode = 'IS022';
  end if;

  update instructed.subscriptions
     set claimed_by       = null,
         claim_expires_at = null
   where stream_id = v_stream_id
     and subscription_name = p_subscription_name
     and shard = v_shard;
end;
$$;


-- ----------------------------------------------------------------------------
-- read_subscription_batch
--
-- Fetch the next batch of events for a held subscription, in delivery order.
-- Realises INV-SUB-P-030 (strictly increasing order) and INV-SUB-P-031
-- (at-least-once: this call does NOT advance the cursor; only
-- advance_subscription does).
--
-- For single-stream subscriptions, events are ordered by stream_version
-- starting at last_seen + 1. For '$all' subscriptions, events are ordered
-- by event_number starting at last_seen + 1, and each row's `stream_uuid`
-- and `stream_version` carry the *original* stream identity
-- (INV-READ-006/007).
--
-- Inputs:
--   p_stream_uuid       text, the subscription's stream scope.
--   p_subscription_name text
--   p_worker_id         text, must match the row's claimed_by.
--   p_qty               integer, batch size cap. MUST be > 0.
--   p_options           jsonb, default '{}'. Recognised keys:
--                         'shard' :: smallint (default 0; ML-0013).
--                       OQ-0003 (selector evaluation locus) is deferred to
--                       Phase 8; if server-side selector evaluation is
--                       chosen there, it lands here as an additive
--                       'selector' key, with no change to the v1 default
--                       semantics.
--
-- Output: zero or more rows in delivery order:
--   event_id        uuid
--   event_number    bigint
--   stream_uuid     text          original-stream identity (always for
--                                  $all; equal to p_stream_uuid for
--                                  single-stream)
--   stream_version  bigint        original-stream version
--   event_type      text
--   causation_id    uuid
--   correlation_id  uuid
--   data            jsonb
--   metadata        jsonb
--   created_at      timestamptz
--
-- Errors (closed set):
--   IS020  subscription_not_found   no subscription row for
--                                    (stream, name, shard).
--   IS022  subscription_lease_lost  claimed_by != p_worker_id.
--   22023  invalid_parameter_value  null inputs or non-positive p_qty.
--
-- Lock-acquisition order:
--   1. The `subscriptions` row keyed by (stream_id, name, shard):
--      SELECT ... FOR UPDATE; verify claimed_by. The row lock is held
--      until the caller's transaction commits. This serves the SDK's
--      "BEGIN; read_batch; handler; advance; COMMIT" pattern (D-0008):
--      the subsequent advance_subscription call from the same
--      transaction reuses the row lock without re-acquiring; the
--      handler's projection writes happen in between without contending
--      with the cursor.
--   2. `events` and `stream_events` reads (MVCC; no row locks).
--
-- Note: holding the row lock across the handler means a second worker
-- attempting to take the lease (claim_subscription on an expired lease)
-- will block on the SELECT FOR UPDATE rather than racing. This is
-- deliberate -- it keeps the at-most-one-live-handler invariant intact
-- for the duration of an in-flight batch.
-- ----------------------------------------------------------------------------
create or replace function instructed.read_subscription_batch (
  p_stream_uuid       text,
  p_subscription_name text,
  p_worker_id         text,
  p_qty               integer,
  p_options           jsonb default '{}'::jsonb
)
  returns table (
    event_id        uuid,
    event_number    bigint,
    stream_uuid     text,
    stream_version  bigint,
    event_type      text,
    causation_id    uuid,
    correlation_id  uuid,
    data            jsonb,
    metadata        jsonb,
    created_at      timestamptz
  )
  language plpgsql
as $$
#variable_conflict use_column
declare
  v_stream_id bigint;
  v_shard     smallint;
  v_holder    text;
  v_last_seen bigint;
begin
  if p_stream_uuid is null then
    raise exception 'read_subscription_batch: p_stream_uuid is null'
      using errcode = '22023';
  end if;
  if p_subscription_name is null or p_subscription_name = '' then
    raise exception 'read_subscription_batch: p_subscription_name is null/empty'
      using errcode = '22023';
  end if;
  if p_worker_id is null or p_worker_id = '' then
    raise exception 'read_subscription_batch: p_worker_id is null/empty'
      using errcode = '22023';
  end if;
  if p_qty is null or p_qty <= 0 then
    raise exception 'read_subscription_batch: p_qty must be a positive integer'
      using errcode = '22023';
  end if;

  v_shard := coalesce((p_options->>'shard')::smallint, 0);

  select stream_id into v_stream_id
    from instructed.streams
   where stream_uuid = p_stream_uuid;
  if not found then
    raise exception 'read_subscription_batch: subscription not found (no such stream)'
      using errcode = 'IS020';
  end if;

  select claimed_by, last_seen into v_holder, v_last_seen
    from instructed.subscriptions
   where stream_id = v_stream_id
     and subscription_name = p_subscription_name
     and shard = v_shard
   for update;

  if not found then
    raise exception 'read_subscription_batch: subscription % on % (shard %) not found',
      p_subscription_name, p_stream_uuid, v_shard
      using errcode = 'IS020';
  end if;
  if v_holder is distinct from p_worker_id then
    raise exception 'read_subscription_batch: lease lost (holder=%, caller=%)',
      coalesce(v_holder,'<none>'), p_worker_id
      using errcode = 'IS022';
  end if;

  -- Unified delivery query. For v_stream_id = 0 ($all): se.stream_version
  -- is the event_number; original_stream_version is the per-stream
  -- version; orig.stream_uuid is the event's original stream.
  -- For v_stream_id > 0: se.stream_version = original_stream_version,
  -- and all_se.stream_version is the event_number.
  return query
  select
    e.event_id,
    case when v_stream_id = 0
         then se.stream_version
         else all_se.stream_version
    end                                  as event_number,
    orig.stream_uuid                     as stream_uuid,
    se.original_stream_version           as stream_version,
    e.event_type,
    e.causation_id,
    e.correlation_id,
    e.data,
    e.metadata,
    e.created_at
  from instructed.stream_events se
  join instructed.events e
    on e.event_id = se.event_id
  join instructed.streams orig
    on orig.stream_id = se.original_stream_id
  left join instructed.stream_events all_se
    on v_stream_id <> 0
   and all_se.event_id = se.event_id
   and all_se.stream_id = 0
  where se.stream_id = v_stream_id
    and se.stream_version > v_last_seen
  order by se.stream_version
  limit p_qty;
end;
$$;


-- ----------------------------------------------------------------------------
-- advance_subscription
--
-- Advance the persistent cursor for a held subscription. Realises
-- INV-SUB-P-032/033 (ack monotonic, up-to-and-including N).
--
-- Per D-0016 (supersedes D-0008), the recommended SDK pattern is:
--   BEGIN; read_subscription_batch(...); COMMIT;
--   <handler(s) run OUTSIDE any SDK transaction>
--   BEGIN; advance_subscription(..., last_position); COMMIT;
-- The two short transactions release and re-acquire the subscriptions
-- row lock; lease loss in the gap between them is signalled by IS022
-- on the advance call (the new lease holder will redeliver).
--
-- The cursor advances to max(last_seen, p_up_to_position): out-of-order
-- or duplicate acks are absorbed without error (INV-SUB-P-034).
--
-- Selectors that skip events (INV-SUB-P-050) call this with the highest
-- *fetched* event_number: per D-0016 the SDK advances past every event
-- returned by read_subscription_batch regardless of whether the
-- application's selector matched (the selector decides only whether
-- `handler` is called, not whether the cursor moves past the event).
--
-- Inputs:
--   p_stream_uuid       text
--   p_subscription_name text
--   p_worker_id         text, must match claimed_by.
--   p_up_to_position    bigint, the cursor target. For single-stream
--                         subscriptions this is a stream_version; for
--                         '$all' subscriptions an event_number. MUST be
--                         >= 0.
--   p_options           jsonb, default '{}'. Recognised keys:
--                         'shard' :: smallint (default 0; ML-0013).
--
-- Output: exactly one row:
--   last_seen  bigint, the cursor's value after the update.
--
-- Errors (closed set):
--   IS020  subscription_not_found   no subscription row for
--                                    (stream, name, shard).
--   IS022  subscription_lease_lost  claimed_by != p_worker_id. The
--                                    SDK MUST stop processing on this
--                                    error per D-0006.
--   22023  invalid_parameter_value  null inputs or negative position.
--
-- Lock-acquisition order:
--   1. The `subscriptions` row keyed by (stream_id, name, shard):
--      UPDATE ... WHERE claimed_by = p_worker_id RETURNING last_seen.
--      Under the D-0016 two-short-tx pattern this is the only lock
--      taken by this procedure; the lock acquired by the earlier
--      read_subscription_batch was released at that tx's COMMIT.
--
-- D-0016 / D-0008 lock-set constraint: advance_subscription MUST NOT
-- take any lock that the SDK could plausibly hold from earlier
-- statements outside its own short ack transaction. The subscriptions
-- row IS such a lock -- but it is the *only* one, and no other store
-- procedure touches subscriptions rows, so the SDK cannot accidentally
-- acquire it through some other path. The SQL contract still permits
-- an SDK that wants the historical D-0008 co-transactional behaviour
-- (projection write atomic with cursor advance, same Postgres database)
-- to call advance_subscription inside its own larger transaction; v1
-- SDKs do not exercise that capability (NG-0015).
-- ----------------------------------------------------------------------------
create or replace function instructed.advance_subscription (
  p_stream_uuid       text,
  p_subscription_name text,
  p_worker_id         text,
  p_up_to_position    bigint,
  p_options           jsonb default '{}'::jsonb
)
  returns table (
    last_seen bigint
  )
  language plpgsql
as $$
#variable_conflict use_column
declare
  v_stream_id bigint;
  v_shard     smallint;
  v_holder    text;
  v_new       bigint;
begin
  if p_stream_uuid is null then
    raise exception 'advance_subscription: p_stream_uuid is null'
      using errcode = '22023';
  end if;
  if p_subscription_name is null or p_subscription_name = '' then
    raise exception 'advance_subscription: p_subscription_name is null/empty'
      using errcode = '22023';
  end if;
  if p_worker_id is null or p_worker_id = '' then
    raise exception 'advance_subscription: p_worker_id is null/empty'
      using errcode = '22023';
  end if;
  if p_up_to_position is null or p_up_to_position < 0 then
    raise exception 'advance_subscription: p_up_to_position must be non-negative'
      using errcode = '22023';
  end if;

  v_shard := coalesce((p_options->>'shard')::smallint, 0);

  select stream_id into v_stream_id
    from instructed.streams
   where stream_uuid = p_stream_uuid;
  if not found then
    raise exception 'advance_subscription: subscription not found (no such stream)'
      using errcode = 'IS020';
  end if;

  select claimed_by into v_holder
    from instructed.subscriptions
   where stream_id = v_stream_id
     and subscription_name = p_subscription_name
     and shard = v_shard
   for update;

  if not found then
    raise exception 'advance_subscription: subscription % on % (shard %) not found',
      p_subscription_name, p_stream_uuid, v_shard
      using errcode = 'IS020';
  end if;
  if v_holder is distinct from p_worker_id then
    raise exception 'advance_subscription: lease lost (holder=%, caller=%)',
      coalesce(v_holder,'<none>'), p_worker_id
      using errcode = 'IS022';
  end if;

  -- INV-SUB-P-034: monotone advance only; out-of-order or duplicate acks
  -- are absorbed without error.
  update instructed.subscriptions s
     set last_seen = greatest(s.last_seen, p_up_to_position)
   where s.stream_id = v_stream_id
     and s.subscription_name = p_subscription_name
     and s.shard = v_shard
  returning s.last_seen into v_new;

  return query select v_new;
end;
$$;


-- ----------------------------------------------------------------------------
-- read_subscription_position
--
-- Return a subscription's current cursor (`last_seen`) without claiming or
-- modifying anything. Used by the strong-consistency-on-dispatch wait
-- helper per D-0010 / CON-010: after appending events, the dispatcher
-- polls this for each name in `consistency: [...]` until each returned
-- `last_seen` is >= the appended event's position (stream_version for
-- single-stream subscriptions; event_number for '$all'), or the
-- `consistency_timeout` elapses.
--
-- Inputs:
--   p_stream_uuid       text
--   p_subscription_name text
--   p_options           jsonb, default '{}'. Recognised keys:
--                         'shard' :: smallint (default 0; ML-0013).
--
-- Output: exactly one row:
--   last_seen  bigint
--
-- Errors (closed set):
--   IS020  subscription_not_found   no subscription row for
--                                    (stream, name, shard). The strong-
--                                    consistency wait helper treats this
--                                    as a configuration error (the named
--                                    subscription does not exist) and
--                                    surfaces it; it does not retry.
--   22023  invalid_parameter_value  null inputs.
--
-- Lock-acquisition order: none. Pure MVCC read.
-- ----------------------------------------------------------------------------
create or replace function instructed.read_subscription_position (
  p_stream_uuid       text,
  p_subscription_name text,
  p_options           jsonb default '{}'::jsonb
)
  returns table (
    last_seen bigint
  )
  language plpgsql
as $$
#variable_conflict use_column
declare
  v_stream_id bigint;
  v_shard     smallint;
  v_last      bigint;
begin
  if p_stream_uuid is null then
    raise exception 'read_subscription_position: p_stream_uuid is null'
      using errcode = '22023';
  end if;
  if p_subscription_name is null or p_subscription_name = '' then
    raise exception 'read_subscription_position: p_subscription_name is null/empty'
      using errcode = '22023';
  end if;

  v_shard := coalesce((p_options->>'shard')::smallint, 0);

  select stream_id into v_stream_id
    from instructed.streams
   where stream_uuid = p_stream_uuid;
  if not found then
    raise exception 'read_subscription_position: subscription not found (no such stream)'
      using errcode = 'IS020';
  end if;

  select s.last_seen into v_last
    from instructed.subscriptions s
   where s.stream_id = v_stream_id
     and s.subscription_name = p_subscription_name
     and s.shard = v_shard;
  if not found then
    raise exception 'read_subscription_position: subscription % on % (shard %) not found',
      p_subscription_name, p_stream_uuid, v_shard
      using errcode = 'IS020';
  end if;

  return query select v_last;
end;
$$;


-- ----------------------------------------------------------------------------
-- delete_subscription
--
-- Remove a subscription row entirely. Realises INV-SUB-P-061. A subsequent
-- claim_subscription on the same identity behaves as a first-create and
-- honours `start_from`.
--
-- Per D-0009, deleting a missing subscription is an *error*
-- (subscription_not_found), not a silent success. This is the abstract
-- Commanded contract (INV-SUB-P-062); the reference adapter is lenient,
-- and we deliberately are not.
--
-- delete_subscription does NOT check claimed_by. The administrator (or
-- any caller) can delete a subscription even while another worker holds
-- its lease; that worker's next call (extend_subscription_claim,
-- read_subscription_batch, or advance_subscription) will fail with
-- IS020, which is the contract's signal to stop.
--
-- Inputs:
--   p_stream_uuid       text
--   p_subscription_name text
--   p_options           jsonb, default '{}'. Recognised keys:
--                         'shard' :: smallint (default 0; ML-0013).
--
-- Output: void.
--
-- Errors (closed set):
--   IS020  subscription_not_found   no subscription row for
--                                    (stream, name, shard).
--   22023  invalid_parameter_value  null inputs.
--
-- Lock-acquisition order:
--   1. The `subscriptions` row keyed by (stream_id, name, shard):
--      DELETE returning 1; if 0 rows deleted, raise IS020.
-- ----------------------------------------------------------------------------
create or replace function instructed.delete_subscription (
  p_stream_uuid       text,
  p_subscription_name text,
  p_options           jsonb default '{}'::jsonb
)
  returns void
  language plpgsql
as $$
#variable_conflict use_column
declare
  v_stream_id bigint;
  v_shard     smallint;
  v_deleted   integer;
begin
  if p_stream_uuid is null then
    raise exception 'delete_subscription: p_stream_uuid is null'
      using errcode = '22023';
  end if;
  if p_subscription_name is null or p_subscription_name = '' then
    raise exception 'delete_subscription: p_subscription_name is null/empty'
      using errcode = '22023';
  end if;

  v_shard := coalesce((p_options->>'shard')::smallint, 0);

  select stream_id into v_stream_id
    from instructed.streams
   where stream_uuid = p_stream_uuid;
  if not found then
    raise exception 'delete_subscription: subscription not found (no such stream)'
      using errcode = 'IS020';
  end if;

  delete from instructed.subscriptions
   where stream_id = v_stream_id
     and subscription_name = p_subscription_name
     and shard = v_shard;
  get diagnostics v_deleted = row_count;
  if v_deleted = 0 then
    raise exception 'delete_subscription: subscription % on % (shard %) not found',
      p_subscription_name, p_stream_uuid, v_shard
      using errcode = 'IS020';
  end if;
end;
$$;


-- ============================================================================
-- SUB-A work-queue procedures
--
-- These maintain `subscription_work_items` per the contract documented on
-- that table. Two worker roles touch them:
--
--   Routing worker (one per subscription, holds the subscription's lease):
--     calls `route_batch` to atomically advance the routing cursor and
--     insert the per-event work items produced by RouteFn.
--
--   Processing workers (any number per subscription, no subscription-lease
--     dependency): call `claim_work_item` to pick a row, then one of
--     `complete_work_item_projection` / `complete_work_item_pm` /
--     `complete_pm_instance` / `fail_work_item` to transition it. The
--     per-work-item lease (`claimed_by`, `lease_expires_at`) gates these
--     terminal calls; a worker that has lost its lease (because another
--     worker took over after expiry) sees IS030.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- route_batch
--
-- Atomically: insert N work items, advance the subscription's routing
-- cursor. One tx. Crash-safe: re-running with the same decisions hits the
-- work-items PK and the redundant inserts are absorbed by ON CONFLICT DO
-- NOTHING; the cursor advance is the commit point so a crash before commit
-- leaves both the cursor and the work-items table untouched.
--
-- The caller MUST hold the subscription's lease (claim_subscription).
-- route_batch verifies this with a SELECT ... FOR UPDATE on the
-- subscriptions row before any INSERT; lease loss raises IS022.
--
-- Inputs:
--   p_stream_uuid       text, the subscription's stream scope (typically
--                         '$all' under SUB-A, but the procedure is
--                         scope-agnostic).
--   p_subscription_name text
--   p_worker_id         text, must match subscriptions.claimed_by.
--   p_new_cursor        bigint, the cursor target after the batch.
--                         Monotone: the procedure UPDATEs last_seen to
--                         greatest(last_seen, p_new_cursor). A caller that
--                         re-runs after a partial crash thus cannot move
--                         the cursor backwards.
--   p_decisions         jsonb, an array (possibly empty) of objects:
--                           [{ "partition_key": text, "event_number": int }, ...]
--                         An empty array means "advance the cursor past
--                         events that all routed to 'ignore'". Per-event
--                         routing decisions equal in (partition_key,
--                         event_number) to an already-present row are
--                         absorbed by ON CONFLICT DO NOTHING; this is the
--                         crash-safety mechanism, not a duplicate-routing
--                         feature (the routing worker is single-active per
--                         subscription via the subscription lease).
--   p_options           jsonb, default '{}'. Recognised keys:
--                         'shard' :: smallint (default 0; ML-0013).
--
-- Output: exactly one row:
--   inserted_count  bigint, the number of rows actually inserted
--                     (excludes ON CONFLICT-absorbed rows).
--   new_last_seen   bigint, the cursor value after the update.
--
-- Errors (closed set):
--   IS020  subscription_not_found   no subscription row.
--   IS022  subscription_lease_lost  caller does not hold the subscription
--                                    lease.
--   22023  invalid_parameter_value  malformed inputs (null, negative
--                                    cursor, non-array decisions,
--                                    malformed decision element).
--
-- Lock-acquisition order:
--   1. The subscriptions row keyed by (stream_id, name, shard) -- SELECT
--      FOR UPDATE; verify lease; then UPDATE last_seen.
--   2. INSERT into subscription_work_items (PK locks per inserted row).
--      The same-tx atomicity here is load-bearing for the SUB-A catch-up
--      predicate: once last_seen >= N is visible to a reader, the
--      corresponding work-item rows are too.
-- ----------------------------------------------------------------------------
create or replace function instructed.route_batch (
  p_stream_uuid       text,
  p_subscription_name text,
  p_worker_id         text,
  p_new_cursor        bigint,
  p_decisions         jsonb,
  p_options           jsonb default '{}'::jsonb
)
  returns table (
    inserted_count bigint,
    new_last_seen  bigint
  )
  language plpgsql
as $$
#variable_conflict use_column
declare
  v_stream_id bigint;
  v_shard     smallint;
  v_holder    text;
  v_inserted  bigint;
  v_new_last  bigint;
begin
  if p_stream_uuid is null then
    raise exception 'route_batch: p_stream_uuid is null'
      using errcode = '22023';
  end if;
  if p_subscription_name is null or p_subscription_name = '' then
    raise exception 'route_batch: p_subscription_name is null/empty'
      using errcode = '22023';
  end if;
  if p_worker_id is null or p_worker_id = '' then
    raise exception 'route_batch: p_worker_id is null/empty'
      using errcode = '22023';
  end if;
  if p_new_cursor is null or p_new_cursor < 0 then
    raise exception 'route_batch: p_new_cursor must be non-negative'
      using errcode = '22023';
  end if;
  if p_decisions is null or jsonb_typeof(p_decisions) <> 'array' then
    raise exception 'route_batch: p_decisions must be a JSON array (possibly empty)'
      using errcode = '22023';
  end if;
  -- Per-element shape check.
  if exists (
    select 1
    from jsonb_array_elements(p_decisions) as d
    where not (d ? 'partition_key')
       or not (d ? 'event_number')
       or jsonb_typeof(d->'partition_key') <> 'string'
       or jsonb_typeof(d->'event_number') <> 'number'
  ) then
    raise exception 'route_batch: each decision must have partition_key:text and event_number:int'
      using errcode = '22023';
  end if;

  v_shard := coalesce((p_options->>'shard')::smallint, 0);

  select stream_id into v_stream_id
    from instructed.streams
   where stream_uuid = p_stream_uuid;
  if not found then
    raise exception 'route_batch: subscription not found (no such stream)'
      using errcode = 'IS020';
  end if;

  -- (1) Lock the subscription row; verify lease.
  select claimed_by into v_holder
    from instructed.subscriptions
   where stream_id = v_stream_id
     and subscription_name = p_subscription_name
     and shard = v_shard
   for update;

  if not found then
    raise exception 'route_batch: subscription % on % (shard %) not found',
      p_subscription_name, p_stream_uuid, v_shard
      using errcode = 'IS020';
  end if;
  if v_holder is distinct from p_worker_id then
    raise exception 'route_batch: lease lost (holder=%, caller=%)',
      coalesce(v_holder,'<none>'), p_worker_id
      using errcode = 'IS022';
  end if;

  -- (2) Insert the work items. ON CONFLICT DO NOTHING absorbs crash-replay.
  with ins as (
    insert into instructed.subscription_work_items
      (stream_id, subscription_name, shard, partition_key, event_number, state)
    select
      v_stream_id,
      p_subscription_name,
      v_shard,
      (d->>'partition_key')::text,
      (d->>'event_number')::bigint,
      'pending'
    from jsonb_array_elements(p_decisions) as d
    on conflict do nothing
    returning 1
  )
  select count(*) into v_inserted from ins;

  -- (3) Advance the cursor (monotone).
  update instructed.subscriptions s
     set last_seen = greatest(s.last_seen, p_new_cursor)
   where s.stream_id = v_stream_id
     and s.subscription_name = p_subscription_name
     and s.shard = v_shard
  returning s.last_seen into v_new_last;

  return query select v_inserted, v_new_last;
end;
$$;


-- ----------------------------------------------------------------------------
-- claim_work_item
--
-- Claim the next claimable work item for a subscription, enforcing
-- per-partition ordering. Returns 0 or 1 row.
--
-- A row is claimable iff:
--   * state = 'pending', OR
--   * state = 'claimed' AND lease_expires_at < now()  (takeover branch)
--   AND no earlier row for the same (subscription, partition_key) is
--   still non-terminal (i.e. in pending/claimed/failed).
--
-- The NOT EXISTS subquery is the per-partition ordering enforcement.
-- Combined with FOR UPDATE SKIP LOCKED on the candidate row, this yields
-- concurrent claims *across* partitions and serial claims *within* a
-- partition. The partial index `subscription_work_items_claimable`
-- excludes 'done' rows from the subquery scan.
--
-- This procedure does NOT verify any subscription-level lease: any worker
-- may claim. (The subscription-level lease is held by the routing worker.)
--
-- Inputs:
--   p_stream_uuid       text
--   p_subscription_name text
--   p_worker_id         text, identifies the claimant. Stored in
--                         claimed_by; later complete_*/fail_* calls verify
--                         it.
--   p_lease_seconds     integer, > 0. The claim's lease window. On expiry
--                         the row becomes eligible to a takeover claim.
--   p_options           jsonb, default '{}'. Recognised keys:
--                         'shard' :: smallint (default 0; ML-0013).
--
-- Output: zero or one row:
--   partition_key     text
--   event_number      bigint
--   claimed_by        text  (= p_worker_id)
--   lease_expires_at  timestamptz
--   was_takeover      boolean, true iff the row was previously 'claimed'
--                       by a different worker whose lease had expired.
--                       Informational only; the SDK may log it.
--   prior_claimed_by  text, the previous holder on a takeover; NULL
--                       otherwise.
--
-- Returning zero rows is the normal "queue empty" outcome; the SDK polls.
--
-- Errors (closed set):
--   IS020  subscription_not_found   no subscription row for
--                                    (stream, name, shard). The work-items
--                                    table is FK-cascaded; a missing
--                                    subscription means a missing queue.
--   22023  invalid_parameter_value  null inputs or non-positive lease.
--
-- Lock-acquisition order:
--   1. The candidate subscription_work_items row, via the inner SELECT
--      ... FOR UPDATE SKIP LOCKED, then UPDATE on the same row.
--      No subscriptions-row lock is taken.
-- ----------------------------------------------------------------------------
create or replace function instructed.claim_work_item (
  p_stream_uuid       text,
  p_subscription_name text,
  p_worker_id         text,
  p_lease_seconds     integer,
  p_options           jsonb default '{}'::jsonb
)
  returns table (
    partition_key    text,
    event_number     bigint,
    claimed_by       text,
    lease_expires_at timestamptz,
    was_takeover     boolean,
    prior_claimed_by text
  )
  language plpgsql
as $$
#variable_conflict use_column
declare
  v_stream_id bigint;
  v_shard     smallint;
  v_now       timestamptz := now();
  v_expires   timestamptz;
begin
  if p_stream_uuid is null then
    raise exception 'claim_work_item: p_stream_uuid is null'
      using errcode = '22023';
  end if;
  if p_subscription_name is null or p_subscription_name = '' then
    raise exception 'claim_work_item: p_subscription_name is null/empty'
      using errcode = '22023';
  end if;
  if p_worker_id is null or p_worker_id = '' then
    raise exception 'claim_work_item: p_worker_id is null/empty'
      using errcode = '22023';
  end if;
  if p_lease_seconds is null or p_lease_seconds <= 0 then
    raise exception 'claim_work_item: p_lease_seconds must be a positive integer'
      using errcode = '22023';
  end if;

  v_shard   := coalesce((p_options->>'shard')::smallint, 0);
  v_expires := v_now + make_interval(secs => p_lease_seconds);

  select stream_id into v_stream_id
    from instructed.streams
   where stream_uuid = p_stream_uuid;
  if not found then
    raise exception 'claim_work_item: subscription not found (no such stream)'
      using errcode = 'IS020';
  end if;

  -- Existence check on the subscription itself. We do not lock it; we
  -- only need to surface IS020 distinctly from "queue empty".
  if not exists (
    select 1 from instructed.subscriptions
     where stream_id = v_stream_id
       and subscription_name = p_subscription_name
       and shard = v_shard
  ) then
    raise exception 'claim_work_item: subscription % on % (shard %) not found',
      p_subscription_name, p_stream_uuid, v_shard
      using errcode = 'IS020';
  end if;

  return query
  with candidate as (
    select wi.partition_key, wi.event_number, wi.claimed_by as prior
      from instructed.subscription_work_items wi
     where wi.stream_id = v_stream_id
       and wi.subscription_name = p_subscription_name
       and wi.shard = v_shard
       and (
         wi.state = 'pending'
         or (wi.state = 'claimed' and wi.lease_expires_at < v_now)
       )
       and not exists (
         select 1
           from instructed.subscription_work_items earlier
          where earlier.stream_id = wi.stream_id
            and earlier.subscription_name = wi.subscription_name
            and earlier.shard = wi.shard
            and earlier.partition_key = wi.partition_key
            and earlier.event_number  < wi.event_number
            and earlier.state in ('pending','claimed','failed')
       )
     order by wi.event_number asc
     for update skip locked
     limit 1
  ),
  updated as (
    update instructed.subscription_work_items w
       set state            = 'claimed',
           claimed_by       = p_worker_id,
           lease_expires_at = v_expires
      from candidate c
     where w.stream_id = v_stream_id
       and w.subscription_name = p_subscription_name
       and w.shard = v_shard
       and w.partition_key = c.partition_key
       and w.event_number  = c.event_number
    returning
      w.partition_key,
      w.event_number,
      w.claimed_by,
      w.lease_expires_at,
      c.prior is not null and c.prior <> p_worker_id as was_takeover,
      case when c.prior is not null and c.prior <> p_worker_id then c.prior
           else null end as prior_claimed_by
  )
  select * from updated;
end;
$$;


-- ----------------------------------------------------------------------------
-- complete_work_item_projection
--
-- Terminal success for a projection work item (PRJ-E). DELETEs the row.
-- The SDK calls this in its own short tx *after* the handler returns; the
-- handler is opaque to the SDK (D-0016 in `docs/decisions.md`) and may
-- target any store. The procedure takes no read-model locks.
--
-- The worker MUST be the row's current claimant. A mismatch (because
-- another worker took over after a lease expiry, or completed first)
-- raises IS030; the calling worker MUST stop processing on that error.
--
-- Inputs:
--   p_stream_uuid       text
--   p_subscription_name text
--   p_worker_id         text, must match the row's claimed_by.
--   p_partition_key     text
--   p_event_number      bigint
--   p_options           jsonb, default '{}'. Recognised keys:
--                         'shard' :: smallint (default 0; ML-0013).
--
-- Output: void.
--
-- Errors (closed set):
--   IS020  subscription_not_found   no subscription row (stream / name /
--                                    shard).
--   IS030  work_item_lease_lost     the row no longer exists, or exists
--                                    but claimed_by != p_worker_id.
--   22023  invalid_parameter_value  null inputs or negative event_number.
--
-- Lock-acquisition order:
--   1. The subscription_work_items row keyed by
--      (stream_id, name, shard, partition_key, event_number):
--      SELECT ... FOR UPDATE; verify claimed_by; DELETE.
-- ----------------------------------------------------------------------------
create or replace function instructed.complete_work_item_projection (
  p_stream_uuid       text,
  p_subscription_name text,
  p_worker_id         text,
  p_partition_key     text,
  p_event_number      bigint,
  p_options           jsonb default '{}'::jsonb
)
  returns void
  language plpgsql
as $$
#variable_conflict use_column
declare
  v_stream_id bigint;
  v_shard     smallint;
  v_holder    text;
begin
  if p_stream_uuid is null then
    raise exception 'complete_work_item_projection: p_stream_uuid is null'
      using errcode = '22023';
  end if;
  if p_subscription_name is null or p_subscription_name = '' then
    raise exception 'complete_work_item_projection: p_subscription_name is null/empty'
      using errcode = '22023';
  end if;
  if p_worker_id is null or p_worker_id = '' then
    raise exception 'complete_work_item_projection: p_worker_id is null/empty'
      using errcode = '22023';
  end if;
  if p_partition_key is null then
    raise exception 'complete_work_item_projection: p_partition_key is null'
      using errcode = '22023';
  end if;
  if p_event_number is null or p_event_number < 0 then
    raise exception 'complete_work_item_projection: p_event_number must be non-negative'
      using errcode = '22023';
  end if;

  v_shard := coalesce((p_options->>'shard')::smallint, 0);

  select stream_id into v_stream_id
    from instructed.streams
   where stream_uuid = p_stream_uuid;
  if not found then
    raise exception 'complete_work_item_projection: subscription not found (no such stream)'
      using errcode = 'IS020';
  end if;

  if not exists (
    select 1 from instructed.subscriptions
     where stream_id = v_stream_id
       and subscription_name = p_subscription_name
       and shard = v_shard
  ) then
    raise exception 'complete_work_item_projection: subscription % on % (shard %) not found',
      p_subscription_name, p_stream_uuid, v_shard
      using errcode = 'IS020';
  end if;

  select claimed_by into v_holder
    from instructed.subscription_work_items
   where stream_id = v_stream_id
     and subscription_name = p_subscription_name
     and shard = v_shard
     and partition_key = p_partition_key
     and event_number  = p_event_number
   for update;

  if not found then
    raise exception 'complete_work_item_projection: work item (%, %) gone (takeover)',
      p_partition_key, p_event_number
      using errcode = 'IS030';
  end if;
  if v_holder is distinct from p_worker_id then
    raise exception 'complete_work_item_projection: lease lost (holder=%, caller=%)',
      coalesce(v_holder,'<none>'), p_worker_id
      using errcode = 'IS030';
  end if;

  delete from instructed.subscription_work_items
   where stream_id = v_stream_id
     and subscription_name = p_subscription_name
     and shard = v_shard
     and partition_key = p_partition_key
     and event_number  = p_event_number;
end;
$$;


-- ----------------------------------------------------------------------------
-- complete_work_item_pm
--
-- Terminal-success-non-terminal for a PM work item (PM-C / PM-F): UPDATE
-- the row to 'done' AND UPSERT the PM's snapshot in one tx. The retained
-- 'done' row is what PM-C's rebuild-via-apply path reads when a future
-- snapshot mismatch forces a state rebuild.
--
-- The worker MUST be the row's current claimant; mismatch raises IS030.
--
-- Inputs:
--   p_stream_uuid             text
--   p_subscription_name       text
--   p_worker_id               text
--   p_partition_key           text
--   p_event_number            bigint
--   p_snapshot_uuid           text, snapshots.source_uuid for the PM
--                               instance (per PM-020 the SDK builds this as
--                               '<pm_name>-<process_uuid>'; opaque to the
--                               store).
--   p_snapshot_type           text, snapshots.source_type (the PM module
--                               name, typically).
--   p_snapshot_version        bigint, snapshots.source_version. Per
--                               PM-024 / SUB-A this MUST equal the
--                               just-claimed work item's event_number; the
--                               procedure does NOT enforce that equality
--                               (it's an SDK invariant, validated in
--                               higher-layer tests).
--   p_snapshot_data           jsonb, the staged PM state after apply.
--   p_snapshot_metadata       jsonb, may be null. The SDK encodes
--                               `snapshot_module_version` here per SNAP-002.
--   p_options                 jsonb, default '{}'. Recognised keys:
--                               'shard' :: smallint (default 0; ML-0013).
--
-- Output: void.
--
-- Errors (closed set):
--   IS020  subscription_not_found
--   IS030  work_item_lease_lost
--   22023  invalid_parameter_value
--
-- Lock-acquisition order:
--   1. The subscription_work_items row -- SELECT FOR UPDATE; verify
--      claimed_by; UPDATE to 'done'.
--   2. The snapshots row -- UPSERT (full-row replace).
-- ----------------------------------------------------------------------------
create or replace function instructed.complete_work_item_pm (
  p_stream_uuid       text,
  p_subscription_name text,
  p_worker_id         text,
  p_partition_key     text,
  p_event_number      bigint,
  p_snapshot_uuid     text,
  p_snapshot_type     text,
  p_snapshot_version  bigint,
  p_snapshot_data     jsonb,
  p_snapshot_metadata jsonb default null,
  p_options           jsonb default '{}'::jsonb
)
  returns void
  language plpgsql
as $$
#variable_conflict use_column
declare
  v_stream_id bigint;
  v_shard     smallint;
  v_holder    text;
begin
  if p_stream_uuid is null then
    raise exception 'complete_work_item_pm: p_stream_uuid is null'
      using errcode = '22023';
  end if;
  if p_subscription_name is null or p_subscription_name = '' then
    raise exception 'complete_work_item_pm: p_subscription_name is null/empty'
      using errcode = '22023';
  end if;
  if p_worker_id is null or p_worker_id = '' then
    raise exception 'complete_work_item_pm: p_worker_id is null/empty'
      using errcode = '22023';
  end if;
  if p_partition_key is null then
    raise exception 'complete_work_item_pm: p_partition_key is null'
      using errcode = '22023';
  end if;
  if p_event_number is null or p_event_number < 0 then
    raise exception 'complete_work_item_pm: p_event_number must be non-negative'
      using errcode = '22023';
  end if;
  if p_snapshot_uuid is null or p_snapshot_uuid = '' then
    raise exception 'complete_work_item_pm: p_snapshot_uuid is null/empty'
      using errcode = '22023';
  end if;
  if p_snapshot_type is null or p_snapshot_type = '' then
    raise exception 'complete_work_item_pm: p_snapshot_type is null/empty'
      using errcode = '22023';
  end if;
  if p_snapshot_version is null or p_snapshot_version < 0 then
    raise exception 'complete_work_item_pm: p_snapshot_version must be non-negative'
      using errcode = '22023';
  end if;
  if p_snapshot_data is null then
    raise exception 'complete_work_item_pm: p_snapshot_data is null'
      using errcode = '22023';
  end if;

  v_shard := coalesce((p_options->>'shard')::smallint, 0);

  select stream_id into v_stream_id
    from instructed.streams
   where stream_uuid = p_stream_uuid;
  if not found then
    raise exception 'complete_work_item_pm: subscription not found (no such stream)'
      using errcode = 'IS020';
  end if;

  if not exists (
    select 1 from instructed.subscriptions
     where stream_id = v_stream_id
       and subscription_name = p_subscription_name
       and shard = v_shard
  ) then
    raise exception 'complete_work_item_pm: subscription % on % (shard %) not found',
      p_subscription_name, p_stream_uuid, v_shard
      using errcode = 'IS020';
  end if;

  select claimed_by into v_holder
    from instructed.subscription_work_items
   where stream_id = v_stream_id
     and subscription_name = p_subscription_name
     and shard = v_shard
     and partition_key = p_partition_key
     and event_number  = p_event_number
   for update;

  if not found then
    raise exception 'complete_work_item_pm: work item (%, %) gone (takeover)',
      p_partition_key, p_event_number
      using errcode = 'IS030';
  end if;
  if v_holder is distinct from p_worker_id then
    raise exception 'complete_work_item_pm: lease lost (holder=%, caller=%)',
      coalesce(v_holder,'<none>'), p_worker_id
      using errcode = 'IS030';
  end if;

  -- (1) Mark the work item done. Clear claim metadata for cleanliness;
  -- the per-state CHECK requires claimed_by/lease_expires_at to be NULL
  -- when state != 'claimed'.
  update instructed.subscription_work_items
     set state            = 'done',
         claimed_by       = null,
         lease_expires_at = null
   where stream_id = v_stream_id
     and subscription_name = p_subscription_name
     and shard = v_shard
     and partition_key = p_partition_key
     and event_number  = p_event_number;

  -- (2) Upsert the snapshot. Mirrors record_snapshot semantics, inlined so
  -- both writes commit together.
  insert into instructed.snapshots
    (source_uuid, source_type, source_version, data, metadata, created_at)
  values
    (p_snapshot_uuid, p_snapshot_type, p_snapshot_version,
     p_snapshot_data, p_snapshot_metadata, now())
  on conflict (source_uuid) do update
    set source_type    = excluded.source_type,
        source_version = excluded.source_version,
        data           = excluded.data,
        metadata       = excluded.metadata,
        created_at     = excluded.created_at;
end;
$$;


-- ----------------------------------------------------------------------------
-- complete_pm_instance
--
-- Terminal success for a whole PM instance (`handle` returned
-- `{ complete: true }`, PM-F): in one tx, DELETE the snapshot AND every
-- work-item (any state) for the partition. The promise to the application
-- is: once you say a PM instance is complete, we discard everything for
-- the instance.
--
-- Idempotent: a missing snapshot is fine (matches delete_snapshot per
-- INV-SNAP-004); a partition with zero work items is fine.
--
-- Per the slice 2 spec this procedure takes no worker_id and no
-- event_number: a takeover worker that also reaches `complete: true` is
-- safe to call this again. The triggering work item was the one the worker
-- just held a lease on; by the time complete_pm_instance fires, the SDK
-- has already finished dispatching the terminal commands.
--
-- Inputs:
--   p_stream_uuid       text
--   p_subscription_name text
--   p_partition_key     text
--   p_snapshot_uuid     text, the PM instance's snapshot source_uuid.
--   p_options           jsonb, default '{}'. Recognised keys:
--                         'shard' :: smallint (default 0; ML-0013).
--
-- Output: exactly one row:
--   work_items_deleted bigint
--   snapshot_deleted   boolean
--
-- Errors (closed set):
--   IS020  subscription_not_found
--   22023  invalid_parameter_value
--
-- Lock-acquisition order:
--   1. subscription_work_items rows for the partition -- DELETE.
--   2. snapshots row -- DELETE.
-- ----------------------------------------------------------------------------
create or replace function instructed.complete_pm_instance (
  p_stream_uuid       text,
  p_subscription_name text,
  p_partition_key     text,
  p_snapshot_uuid     text,
  p_options           jsonb default '{}'::jsonb
)
  returns table (
    work_items_deleted bigint,
    snapshot_deleted   boolean
  )
  language plpgsql
as $$
#variable_conflict use_column
declare
  v_stream_id  bigint;
  v_shard      smallint;
  v_wi_deleted bigint;
  v_snap_del   integer;
begin
  if p_stream_uuid is null then
    raise exception 'complete_pm_instance: p_stream_uuid is null'
      using errcode = '22023';
  end if;
  if p_subscription_name is null or p_subscription_name = '' then
    raise exception 'complete_pm_instance: p_subscription_name is null/empty'
      using errcode = '22023';
  end if;
  if p_partition_key is null then
    raise exception 'complete_pm_instance: p_partition_key is null'
      using errcode = '22023';
  end if;
  if p_snapshot_uuid is null or p_snapshot_uuid = '' then
    raise exception 'complete_pm_instance: p_snapshot_uuid is null/empty'
      using errcode = '22023';
  end if;

  v_shard := coalesce((p_options->>'shard')::smallint, 0);

  select stream_id into v_stream_id
    from instructed.streams
   where stream_uuid = p_stream_uuid;
  if not found then
    raise exception 'complete_pm_instance: subscription not found (no such stream)'
      using errcode = 'IS020';
  end if;

  if not exists (
    select 1 from instructed.subscriptions
     where stream_id = v_stream_id
       and subscription_name = p_subscription_name
       and shard = v_shard
  ) then
    raise exception 'complete_pm_instance: subscription % on % (shard %) not found',
      p_subscription_name, p_stream_uuid, v_shard
      using errcode = 'IS020';
  end if;

  with del as (
    delete from instructed.subscription_work_items
     where stream_id = v_stream_id
       and subscription_name = p_subscription_name
       and shard = v_shard
       and partition_key = p_partition_key
    returning 1
  )
  select count(*) into v_wi_deleted from del;

  delete from instructed.snapshots where source_uuid = p_snapshot_uuid;
  get diagnostics v_snap_del = row_count;

  return query select v_wi_deleted, v_snap_del > 0;
end;
$$;


-- ----------------------------------------------------------------------------
-- fail_work_item
--
-- Move a claimed work item to 'failed'. Sets failed_at / error_text;
-- clears claimed_by / lease_expires_at. The 'failed' row blocks subsequent
-- work items for its partition only (via the per-partition NOT EXISTS in
-- the claim query); other partitions are unaffected. 'failed' rows are
-- never auto-skipped or auto-deleted by any code path; operator action
-- (deferred to `instructedctl`) is required.
--
-- The worker MUST be the row's current claimant; mismatch raises IS030.
--
-- Inputs:
--   p_stream_uuid       text
--   p_subscription_name text
--   p_worker_id         text
--   p_partition_key     text
--   p_event_number      bigint
--   p_error_text        text, may be NULL (diagnostic only; not parsed
--                         by the framework).
--   p_options           jsonb, default '{}'. Recognised keys:
--                         'shard' :: smallint (default 0; ML-0013).
--
-- Output: void.
--
-- Errors (closed set):
--   IS020  subscription_not_found
--   IS030  work_item_lease_lost
--   22023  invalid_parameter_value
--
-- Lock-acquisition order:
--   1. subscription_work_items row -- SELECT FOR UPDATE; verify
--      claimed_by; UPDATE.
-- ----------------------------------------------------------------------------
create or replace function instructed.fail_work_item (
  p_stream_uuid       text,
  p_subscription_name text,
  p_worker_id         text,
  p_partition_key     text,
  p_event_number      bigint,
  p_error_text        text,
  p_options           jsonb default '{}'::jsonb
)
  returns void
  language plpgsql
as $$
#variable_conflict use_column
declare
  v_stream_id bigint;
  v_shard     smallint;
  v_holder    text;
begin
  if p_stream_uuid is null then
    raise exception 'fail_work_item: p_stream_uuid is null'
      using errcode = '22023';
  end if;
  if p_subscription_name is null or p_subscription_name = '' then
    raise exception 'fail_work_item: p_subscription_name is null/empty'
      using errcode = '22023';
  end if;
  if p_worker_id is null or p_worker_id = '' then
    raise exception 'fail_work_item: p_worker_id is null/empty'
      using errcode = '22023';
  end if;
  if p_partition_key is null then
    raise exception 'fail_work_item: p_partition_key is null'
      using errcode = '22023';
  end if;
  if p_event_number is null or p_event_number < 0 then
    raise exception 'fail_work_item: p_event_number must be non-negative'
      using errcode = '22023';
  end if;

  v_shard := coalesce((p_options->>'shard')::smallint, 0);

  select stream_id into v_stream_id
    from instructed.streams
   where stream_uuid = p_stream_uuid;
  if not found then
    raise exception 'fail_work_item: subscription not found (no such stream)'
      using errcode = 'IS020';
  end if;

  if not exists (
    select 1 from instructed.subscriptions
     where stream_id = v_stream_id
       and subscription_name = p_subscription_name
       and shard = v_shard
  ) then
    raise exception 'fail_work_item: subscription % on % (shard %) not found',
      p_subscription_name, p_stream_uuid, v_shard
      using errcode = 'IS020';
  end if;

  select claimed_by into v_holder
    from instructed.subscription_work_items
   where stream_id = v_stream_id
     and subscription_name = p_subscription_name
     and shard = v_shard
     and partition_key = p_partition_key
     and event_number  = p_event_number
   for update;

  if not found then
    raise exception 'fail_work_item: work item (%, %) gone (takeover)',
      p_partition_key, p_event_number
      using errcode = 'IS030';
  end if;
  if v_holder is distinct from p_worker_id then
    raise exception 'fail_work_item: lease lost (holder=%, caller=%)',
      coalesce(v_holder,'<none>'), p_worker_id
      using errcode = 'IS030';
  end if;

  update instructed.subscription_work_items
     set state            = 'failed',
         failed_at        = now(),
         error_text       = p_error_text,
         claimed_by       = null,
         lease_expires_at = null
   where stream_id = v_stream_id
     and subscription_name = p_subscription_name
     and shard = v_shard
     and partition_key = p_partition_key
     and event_number  = p_event_number;
end;
$$;


-- ----------------------------------------------------------------------------
-- is_subscription_caught_up
--
-- The SUB-A catch-up predicate, used by waitForProjection (slice 8) and
-- equivalents. Returns TRUE iff subscription S is caught up to event_number
-- T: both
--
--   * the routing cursor (subscriptions.last_seen) is >= T, AND
--   * no work-item row for S with event_number <= T is in a non-terminal
--     state (pending / claimed / failed).
--
-- The routing cursor disambiguates "no rows" from "routing hasn't reached T
-- yet"; the work-items check guarantees that what was routed has been
-- processed. The state filter is logically redundant for projections (which
-- DELETE on success and so have no 'done' rows) but harmless; for PMs it
-- correctly excludes retained 'done' rows from blocking catch-up.
--
-- Race safety at the start of waitForProjection depends on route_batch's
-- single-tx commit of (cursor advance + work-item INSERTs); see the
-- subscription_work_items docstring and the SUB-A "Catch-up predicate"
-- subsection.
--
-- Inputs:
--   p_stream_uuid       text
--   p_subscription_name text
--   p_target            bigint, the target event_number.
--   p_options           jsonb, default '{}'. Recognised keys:
--                         'shard' :: smallint (default 0; ML-0013).
--
-- Output: exactly one row:
--   caught_up  boolean
--
-- Errors (closed set):
--   IS020  subscription_not_found
--   22023  invalid_parameter_value
--
-- Lock-acquisition order: none. Pure MVCC read.
-- ----------------------------------------------------------------------------
create or replace function instructed.is_subscription_caught_up (
  p_stream_uuid       text,
  p_subscription_name text,
  p_target            bigint,
  p_options           jsonb default '{}'::jsonb
)
  returns table (
    caught_up boolean
  )
  language plpgsql
as $$
#variable_conflict use_column
declare
  v_stream_id bigint;
  v_shard     smallint;
  v_last_seen bigint;
begin
  if p_stream_uuid is null then
    raise exception 'is_subscription_caught_up: p_stream_uuid is null'
      using errcode = '22023';
  end if;
  if p_subscription_name is null or p_subscription_name = '' then
    raise exception 'is_subscription_caught_up: p_subscription_name is null/empty'
      using errcode = '22023';
  end if;
  if p_target is null or p_target < 0 then
    raise exception 'is_subscription_caught_up: p_target must be non-negative'
      using errcode = '22023';
  end if;

  v_shard := coalesce((p_options->>'shard')::smallint, 0);

  select stream_id into v_stream_id
    from instructed.streams
   where stream_uuid = p_stream_uuid;
  if not found then
    raise exception 'is_subscription_caught_up: subscription not found (no such stream)'
      using errcode = 'IS020';
  end if;

  select s.last_seen into v_last_seen
    from instructed.subscriptions s
   where s.stream_id = v_stream_id
     and s.subscription_name = p_subscription_name
     and s.shard = v_shard;
  if not found then
    raise exception 'is_subscription_caught_up: subscription % on % (shard %) not found',
      p_subscription_name, p_stream_uuid, v_shard
      using errcode = 'IS020';
  end if;

  return query
  select (v_last_seen >= p_target)
     and not exists (
       select 1
         from instructed.subscription_work_items wi
        where wi.stream_id = v_stream_id
          and wi.subscription_name = p_subscription_name
          and wi.shard = v_shard
          and wi.event_number <= p_target
          and wi.state in ('pending','claimed','failed')
     );
end;
$$;


-- ----------------------------------------------------------------------------
-- extend_work_item_claim
--
-- Heartbeat for a work-item claim. The processing worker calls this
-- periodically while a long-running handler is in progress so the work
-- item's lease does not expire and trigger a takeover by another worker.
--
-- Mirrors extend_subscription_claim in shape. If this call fails with
-- IS030 the worker MUST stop processing the item; the lease has been
-- taken over and continuing risks double-execution.
--
-- Inputs:
--   p_stream_uuid       text
--   p_subscription_name text
--   p_worker_id         text, must match the row's claimed_by.
--   p_partition_key     text
--   p_event_number      bigint
--   p_lease_seconds     integer, the new lease duration (> 0).
--   p_options           jsonb, default '{}'. Recognised keys:
--                         'shard' :: smallint (default 0; ML-0013).
--
-- Output: exactly one row:
--   lease_expires_at    timestamptz, the new lease expiry (= now() +
--                         p_lease_seconds).
--
-- Errors (closed set):
--   IS020  subscription_not_found
--   IS030  work_item_lease_lost     row missing, row is not in 'claimed'
--                                    state, or claimed_by != p_worker_id.
--   22023  invalid_parameter_value  null inputs or non-positive lease.
--
-- Lock-acquisition order:
--   1. The subscription_work_items row keyed by
--      (stream_id, name, shard, partition_key, event_number):
--      SELECT ... FOR UPDATE; verify state='claimed' AND claimed_by;
--      UPDATE lease_expires_at.
-- ----------------------------------------------------------------------------
create or replace function instructed.extend_work_item_claim (
  p_stream_uuid       text,
  p_subscription_name text,
  p_worker_id         text,
  p_partition_key     text,
  p_event_number      bigint,
  p_lease_seconds     integer,
  p_options           jsonb default '{}'::jsonb
)
  returns table (
    lease_expires_at timestamptz
  )
  language plpgsql
as $$
#variable_conflict use_column
declare
  v_stream_id bigint;
  v_shard     smallint;
  v_expires   timestamptz;
  v_state     text;
  v_holder    text;
begin
  if p_stream_uuid is null then
    raise exception 'extend_work_item_claim: p_stream_uuid is null'
      using errcode = '22023';
  end if;
  if p_subscription_name is null or p_subscription_name = '' then
    raise exception 'extend_work_item_claim: p_subscription_name is null/empty'
      using errcode = '22023';
  end if;
  if p_worker_id is null or p_worker_id = '' then
    raise exception 'extend_work_item_claim: p_worker_id is null/empty'
      using errcode = '22023';
  end if;
  if p_partition_key is null then
    raise exception 'extend_work_item_claim: p_partition_key is null'
      using errcode = '22023';
  end if;
  if p_event_number is null or p_event_number < 0 then
    raise exception 'extend_work_item_claim: p_event_number must be non-negative'
      using errcode = '22023';
  end if;
  if p_lease_seconds is null or p_lease_seconds <= 0 then
    raise exception 'extend_work_item_claim: p_lease_seconds must be a positive integer'
      using errcode = '22023';
  end if;

  v_shard   := coalesce((p_options->>'shard')::smallint, 0);
  v_expires := now() + make_interval(secs => p_lease_seconds);

  select stream_id into v_stream_id
    from instructed.streams
   where stream_uuid = p_stream_uuid;
  if not found then
    raise exception 'extend_work_item_claim: subscription not found (no such stream)'
      using errcode = 'IS020';
  end if;

  if not exists (
    select 1 from instructed.subscriptions
     where stream_id = v_stream_id
       and subscription_name = p_subscription_name
       and shard = v_shard
  ) then
    raise exception 'extend_work_item_claim: subscription % on % (shard %) not found',
      p_subscription_name, p_stream_uuid, v_shard
      using errcode = 'IS020';
  end if;

  select state, claimed_by into v_state, v_holder
    from instructed.subscription_work_items
   where stream_id = v_stream_id
     and subscription_name = p_subscription_name
     and shard = v_shard
     and partition_key = p_partition_key
     and event_number  = p_event_number
   for update;

  if not found then
    raise exception 'extend_work_item_claim: work item (%, %) gone (takeover)',
      p_partition_key, p_event_number
      using errcode = 'IS030';
  end if;
  if v_state <> 'claimed' or v_holder is distinct from p_worker_id then
    raise exception 'extend_work_item_claim: lease lost (state=%, holder=%, caller=%)',
      v_state, coalesce(v_holder,'<none>'), p_worker_id
      using errcode = 'IS030';
  end if;

  update instructed.subscription_work_items
     set lease_expires_at = v_expires
   where stream_id = v_stream_id
     and subscription_name = p_subscription_name
     and shard = v_shard
     and partition_key = p_partition_key
     and event_number  = p_event_number;

  return query select v_expires;
end;
$$;


-- ----------------------------------------------------------------------------
-- list_pm_rebuild_events
--
-- Cold-path read for PM state rebuild (PM-C / SUB-A slice 7). When a PM
-- processing worker claims a work item and the partition's snapshot is
-- missing (IS010) or carries a `snapshot_module_version` (in metadata)
-- that no longer matches the SDK's compiled-in version (SNAP-002), the
-- worker rebuilds state by folding every previously-`done` event for
-- the partition through the PM's `apply` callback. This function
-- returns those events, ordered by event_number, in the
-- `read_all`-compatible shape so the SDK can fold directly.
--
-- Only rows in state 'done' are returned. 'pending' / 'claimed' /
-- 'failed' rows by definition haven't yet been seen by `apply` (the
-- claim query's per-partition NOT EXISTS guarantees this: a row is
-- only claimable once every earlier row for its partition is terminal,
-- and on the PM path the terminal-success step is the
-- UPDATE-to-'done'). The claimed event itself is excluded by the
-- exclusive upper bound; the SDK runs `apply` on it after the rebuild
-- to produce the staged state handed to user `handle`.
--
-- Inputs:
--   p_stream_uuid       text
--   p_subscription_name text
--   p_partition_key     text
--   p_event_number      bigint, exclusive upper bound (typically the
--                         claimed event's event_number).
--   p_options           jsonb, default '{}'. Recognised keys:
--                         'shard' :: smallint (default 0; ML-0013).
--
-- Output: a set of rows in the read_all shape, ordered by event_number
-- ascending. Empty set if the partition has no 'done' rows below the
-- cutoff (e.g. claimed event is the first ever for the partition).
--
-- Errors (closed set):
--   IS020  subscription_not_found
--   22023  invalid_parameter_value
--
-- Lock-acquisition order: none. Pure MVCC read.
-- ----------------------------------------------------------------------------
create or replace function instructed.list_pm_rebuild_events (
  p_stream_uuid       text,
  p_subscription_name text,
  p_partition_key     text,
  p_event_number      bigint,
  p_options           jsonb default '{}'::jsonb
)
  returns table (
    event_id        uuid,
    event_number    bigint,
    stream_uuid     text,
    stream_version  bigint,
    event_type      text,
    causation_id    uuid,
    correlation_id  uuid,
    data            jsonb,
    metadata        jsonb,
    created_at      timestamptz
  )
  language plpgsql
as $$
#variable_conflict use_column
declare
  v_stream_id bigint;
  v_shard     smallint;
begin
  if p_stream_uuid is null then
    raise exception 'list_pm_rebuild_events: p_stream_uuid is null'
      using errcode = '22023';
  end if;
  if p_subscription_name is null or p_subscription_name = '' then
    raise exception 'list_pm_rebuild_events: p_subscription_name is null/empty'
      using errcode = '22023';
  end if;
  if p_partition_key is null then
    raise exception 'list_pm_rebuild_events: p_partition_key is null'
      using errcode = '22023';
  end if;
  if p_event_number is null or p_event_number < 0 then
    raise exception 'list_pm_rebuild_events: p_event_number must be non-negative'
      using errcode = '22023';
  end if;

  v_shard := coalesce((p_options->>'shard')::smallint, 0);

  select stream_id into v_stream_id
    from instructed.streams
   where stream_uuid = p_stream_uuid;
  if not found then
    raise exception 'list_pm_rebuild_events: subscription not found (no such stream)'
      using errcode = 'IS020';
  end if;

  if not exists (
    select 1 from instructed.subscriptions
     where stream_id = v_stream_id
       and subscription_name = p_subscription_name
       and shard = v_shard
  ) then
    raise exception 'list_pm_rebuild_events: subscription % on % (shard %) not found',
      p_subscription_name, p_stream_uuid, v_shard
      using errcode = 'IS020';
  end if;

  return query
  select
    e.event_id,
    se.stream_version                  as event_number,
    orig.stream_uuid                   as stream_uuid,
    se.original_stream_version         as stream_version,
    e.event_type,
    e.causation_id,
    e.correlation_id,
    e.data,
    e.metadata,
    e.created_at
  from instructed.subscription_work_items wi
  join instructed.stream_events se
    on se.stream_id = 0
   and se.stream_version = wi.event_number
  join instructed.events e
    on e.event_id = se.event_id
  join instructed.streams orig
    on orig.stream_id = se.original_stream_id
  where wi.stream_id        = v_stream_id
    and wi.subscription_name = p_subscription_name
    and wi.shard             = v_shard
    and wi.partition_key     = p_partition_key
    and wi.state             = 'done'
    and wi.event_number      < p_event_number
  order by wi.event_number asc;
end;
$$;
