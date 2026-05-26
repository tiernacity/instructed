-- Read-store schema for the bank-account example.
--
-- Lives in its own `bank_account` schema in the same Postgres
-- database as the `instructed` event store. Two tables:
--
--   * balances  -- one row per account stream
--   * transfers -- one row per Transfer aggregate (transfer_id)
--
-- Both carry `last_event_number` so the projection handlers are
-- idempotent under at-least-once redelivery: UPDATEs guard with
-- `where $new_event_number > last_event_number` and the
-- AccountOpened / TransferRequested UPSERTs use the same guard
-- in their ON CONFLICT branch. This is the read-side equivalent
-- of the lastEventByAccount Map the in-memory version used.

create schema if not exists bank_account;

create table if not exists bank_account.balances (
  stream_uuid       text primary key,
  owner             text,
  balance           bigint not null default 0,
  last_event_number bigint not null
);

create table if not exists bank_account.transfers (
  transfer_id       text primary key,
  from_account      text not null,
  to_account        text not null,
  amount            bigint not null,
  status            text not null,     -- 'requested' | 'completed' | 'failed'
  reason            text,
  requested_at      timestamptz not null,
  last_event_number bigint not null
);

-- Index on requested_at desc so the recent-transfers listing
-- doesn't have to scan the whole table.
create index if not exists transfers_requested_at_idx
  on bank_account.transfers (requested_at desc);
