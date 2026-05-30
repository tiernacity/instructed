// Core: stream inspection and event reads.

import type { Db } from "./db.ts";
import type { EventRecord, StreamSummary } from "./types.ts";
import { sqlstateOf, StreamNotFound } from "./errors.ts";

interface StreamRow {
  stream_uuid: string;
  head: string;
  event_count: string;
}

function toSummary(r: StreamRow): StreamSummary {
  return {
    streamUuid: r.stream_uuid,
    head: Number(r.head),
    eventCount: Number(r.event_count),
  };
}

const STREAM_SELECT = `
  select
    st.stream_uuid,
    st.stream_version as head,
    (select count(*) from instructed.stream_events se
       where se.stream_id = st.stream_id) as event_count
  from instructed.streams st
  where st.stream_id <> 0`;

// List every user stream (the global $all stream is excluded).
export async function listStreams(db: Db): Promise<StreamSummary[]> {
  const rows = await db.query<StreamRow>(
    `${STREAM_SELECT} order by st.stream_uuid`,
  );
  return rows.map(toSummary);
}

// One stream by uuid, or null when it does not exist.
export async function getStream(
  db: Db,
  streamUuid: string,
): Promise<StreamSummary | null> {
  const rows = await db.query<StreamRow>(
    `${STREAM_SELECT} and st.stream_uuid = $1`,
    [streamUuid],
  );
  return rows.length ? toSummary(rows[0]) : null;
}

interface EventRow {
  event_id: string;
  event_number: string;
  stream_uuid: string;
  stream_version: string;
  event_type: string;
  causation_id: string | null;
  correlation_id: string | null;
  data: unknown;
  metadata: unknown;
  created_at: Date;
}

function toEvent(r: EventRow): EventRecord {
  return {
    eventId: r.event_id,
    eventNumber: Number(r.event_number),
    streamUuid: r.stream_uuid,
    streamVersion: Number(r.stream_version),
    eventType: r.event_type,
    causationId: r.causation_id,
    correlationId: r.correlation_id,
    data: r.data,
    metadata: r.metadata,
    createdAt: r.created_at,
  };
}

// Read a range of events from a single stream, starting at stream version
// `from` (inclusive), up to `count` events. Raises StreamNotFound when the
// stream does not exist.
export async function readStream(
  db: Db,
  opts: { streamUuid: string; from: number; count: number },
): Promise<EventRecord[]> {
  try {
    const rows = await db.query<EventRow>(
      "select * from instructed.read_stream($1, $2, $3)",
      [opts.streamUuid, opts.from, opts.count],
    );
    return rows.map(toEvent);
  } catch (err) {
    if (sqlstateOf(err) === "IS003") throw new StreamNotFound(opts.streamUuid);
    throw err;
  }
}

// Read a range of events from the global $all stream, starting at event_number
// `from` (inclusive), up to `count` events. Events carry their original stream
// identity, not $all.
export async function readAll(
  db: Db,
  opts: { from: number; count: number },
): Promise<EventRecord[]> {
  const rows = await db.query<EventRow>(
    "select * from instructed.read_all($1, $2)",
    [opts.from, opts.count],
  );
  return rows.map(toEvent);
}
