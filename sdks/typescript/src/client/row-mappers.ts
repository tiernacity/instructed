/**
 * Row-mapping helpers shared by the `Client` read methods (A6).
 *
 * Postgres `int8` columns arrive as strings (sometimes numbers) and
 * `timestamptz` may arrive as a string depending on the driver/types
 * config, so the read path coerces every row through these. The
 * recorded-event SELECT column list lives here too, so the three read
 * procedures (`read_stream`, `read_all`, `list_pm_rebuild_events`) cannot
 * drift in column order/shape.
 */

import type { Event, RecordedEvent } from '../types/index.ts'

/** Some Postgres ints (int8) come back as strings. Coerce to bigint. */
export function toBigInt(v: unknown): bigint {
  if (typeof v === 'bigint') return v
  if (typeof v === 'number') return BigInt(v)
  if (typeof v === 'string') return BigInt(v)
  throw new Error(`expected bigint-like, got ${typeof v}: ${String(v)}`)
}

export function toDate(v: unknown): Date {
  if (v instanceof Date) return v
  if (typeof v === 'string') return new Date(v)
  throw new Error(`expected Date, got ${typeof v}`)
}

/**
 * The recorded-event projection column list, shared verbatim by the
 * three read procedures so they stay in lockstep with {@link RawEventRow}
 * and {@link mapRecordedEvent}.
 */
export const READ_EVENT_COLUMNS =
  'event_id, event_number, stream_uuid, stream_version, ' +
  'event_type, causation_id, correlation_id, data, metadata, created_at'

export interface RawEventRow {
  event_id: string
  event_number: string | number
  stream_uuid: string
  stream_version: string | number
  event_type: string
  causation_id: string | null
  correlation_id: string | null
  data: unknown
  metadata: unknown
  created_at: Date | string
}

export function mapRecordedEvent<E extends Event>(row: RawEventRow): RecordedEvent<E> {
  // L1 → L2 boundary: SQL `event_type` becomes TS `type`. See
  // `packEvent` (pack-event.ts) for the inverse direction.
  //
  // The cast at the return is unavoidable: `RecordedEvent<E>` is a
  // distributive conditional over the user's event union, so TS
  // cannot prove that the runtime shape (built from an opaque SQL
  // row whose `event_type` is just `string`) matches the specific
  // union member. The constructed shape IS structurally compatible;
  // the caller's `E` selects which branch the consumer sees.
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- DB-row boundary: opaque SQL row asserted to the caller's event union; shape guaranteed by the SELECT.
  return {
    event_id: row.event_id,
    event_number: toBigInt(row.event_number),
    stream_uuid: row.stream_uuid,
    stream_version: toBigInt(row.stream_version),
    type: row.event_type,
    causation_id: row.causation_id,
    correlation_id: row.correlation_id,
    data: row.data,
    metadata: row.metadata,
    created_at: toDate(row.created_at),
  } as RecordedEvent<E>
}
