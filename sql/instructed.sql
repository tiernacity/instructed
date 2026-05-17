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
--                      ML-0001 (partitioned consumers) can be added without
--                      breaking v1 callers.
--
-- Procedures (full docstrings on each):
--   append_to_stream, read_stream, read_all,
--   record_snapshot, read_snapshot, delete_snapshot,
--   claim_subscription, extend_subscription_claim, release_subscription,
--   read_subscription_batch, advance_subscription,
--   read_subscription_position, delete_subscription.
--
-- All procedures that accept caller-tunable knobs do so via a `p_options jsonb`
-- parameter rather than positional arguments, so that ML-0001 / ML-0002 can
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
-- v1 with default 0 per ML-0001 so that partitioned-consumer support can be
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
-- Procedure stubs (Pass 1)
--
-- Pass 1 only lays out the schema and resolves OQ-0001 (now D-0012). Pass 2
-- adds full docstrings (inputs, outputs, error sets, lock-acquisition order)
-- to each procedure listed above. Pass 3 fills the bodies.
--
-- The roadmap procedure set, for reference:
--   append_to_stream, read_stream, read_all,
--   record_snapshot, read_snapshot, delete_snapshot,
--   claim_subscription, extend_subscription_claim, release_subscription,
--   read_subscription_batch, advance_subscription,
--   read_subscription_position, delete_subscription.
-- ============================================================================
