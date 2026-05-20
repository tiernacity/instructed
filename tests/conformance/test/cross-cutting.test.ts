/**
 * Part F — Cross-cutting conformance (Phase 9, step 7/8).
 *
 * Covers the catch-all invariants that don't belong to a single
 * procedure:
 *
 *   - INV-META-001        causation / correlation persisted + echoed
 *   - INV-META-010..011   event_type opaque; data + metadata round-trip
 *   - INV-STREAM-001      stream_uuid is the stable external identity
 *   - INV-STREAM-002      [reference-only mechanism] internal stream_id
 *   - INV-STREAM-003      '$all' is reserved (schema-level CHECK)
 *   - INV-LINK-001        dropped — no user-facing event linking
 *   - INV-DELETE-001      dropped — no hard-delete path
 */

import { after, before, beforeEach, describe, test } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import type pg from "pg";
import { closePool, getPool, truncateAll } from "./fixtures.ts";
import { rejectsWithCode } from "./_helpers.ts";

let pool: pg.Pool;

before(async () => {
  pool = await getPool();
});

after(async () => {
  await closePool();
});

beforeEach(async () => {
  await truncateAll(pool);
});

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

interface AppendOptions {
  event_id?: string;
  event_type: string;
  data?: unknown;
  metadata?: unknown;
  causation_id?: string | null;
  correlation_id?: string | null;
}

async function appendOne(
  streamUuid: string,
  e: AppendOptions,
): Promise<{ event_id: string }> {
  const eventId = e.event_id ?? randomUUID();
  const payload = [{
    event_id: eventId,
    event_type: e.event_type,
    data: e.data ?? {},
    ...(e.metadata !== undefined ? { metadata: e.metadata } : {}),
    ...(e.causation_id !== undefined ? { causation_id: e.causation_id } : {}),
    ...(e.correlation_id !== undefined
      ? { correlation_id: e.correlation_id }
      : {}),
  }];
  await pool.query(
    `SELECT * FROM instructed.append_to_stream($1, 'any', NULL, $2::jsonb)`,
    [streamUuid, JSON.stringify(payload)],
  );
  return { event_id: eventId };
}

interface ReadRow {
  event_id: string;
  event_type: string;
  causation_id: string | null;
  correlation_id: string | null;
  data: unknown;
  metadata: unknown;
  stream_uuid: string;
  stream_version: bigint;
  event_number: bigint;
}

async function readFirst(streamUuid: string): Promise<ReadRow> {
  const r = await pool.query<{
    event_id: string;
    event_type: string;
    causation_id: string | null;
    correlation_id: string | null;
    data: unknown;
    metadata: unknown;
    stream_uuid: string;
    stream_version: string;
    event_number: string;
  }>(
    `SELECT * FROM instructed.read_stream($1, 0, 100)`,
    [streamUuid],
  );
  assert.ok(r.rowCount && r.rowCount > 0, "expected at least one row");
  const row = r.rows[0];
  return {
    event_id: row.event_id,
    event_type: row.event_type,
    causation_id: row.causation_id,
    correlation_id: row.correlation_id,
    data: row.data,
    metadata: row.metadata,
    stream_uuid: row.stream_uuid,
    stream_version: BigInt(row.stream_version),
    event_number: BigInt(row.event_number),
  };
}

async function readAllRows(): Promise<ReadRow[]> {
  const r = await pool.query<{
    event_id: string;
    event_type: string;
    causation_id: string | null;
    correlation_id: string | null;
    data: unknown;
    metadata: unknown;
    stream_uuid: string;
    stream_version: string;
    event_number: string;
  }>(`SELECT * FROM instructed.read_all(0, 100)`);
  return r.rows.map((row) => ({
    event_id: row.event_id,
    event_type: row.event_type,
    causation_id: row.causation_id,
    correlation_id: row.correlation_id,
    data: row.data,
    metadata: row.metadata,
    stream_uuid: row.stream_uuid,
    stream_version: BigInt(row.stream_version),
    event_number: BigInt(row.event_number),
  }));
}

// =============================================================================
// Causation and correlation (INV-META-001)
// =============================================================================

describe("metadata — causation and correlation", () => {
  // INV-META-001: causation_id / correlation_id, when set on input, MUST be
  //   persisted and echoed on the corresponding RecordedEvent.
  test("causation_id and correlation_id round-trip via read_stream", async () => {
    const s = randomUUID();
    const causation = randomUUID();
    const correlation = randomUUID();
    await appendOne(s, {
      event_type: "Created",
      causation_id: causation,
      correlation_id: correlation,
    });
    const got = await readFirst(s);
    assert.equal(got.causation_id, causation);
    assert.equal(got.correlation_id, correlation);
  });

  // INV-META-001: same echo via read_all
  test("causation_id and correlation_id round-trip via read_all", async () => {
    const s = randomUUID();
    const causation = randomUUID();
    const correlation = randomUUID();
    await appendOne(s, {
      event_type: "Created",
      causation_id: causation,
      correlation_id: correlation,
    });
    const all = await readAllRows();
    assert.equal(all.length, 1);
    assert.equal(all[0].causation_id, causation);
    assert.equal(all[0].correlation_id, correlation);
  });

  // INV-META-001: absent causation/correlation come back as null,
  //   not as an empty string or a defaulted UUID
  test("absent causation_id / correlation_id come back as null", async () => {
    const s = randomUUID();
    await appendOne(s, { event_type: "A" });
    const got = await readFirst(s);
    assert.equal(got.causation_id, null);
    assert.equal(got.correlation_id, null);
  });
});

// =============================================================================
// event_type and JSONB payloads (INV-META-010, INV-META-011)
// =============================================================================

describe("metadata — event_type and data/metadata payloads", () => {
  // INV-META-010: event_type is a string chosen by the caller and is
  //   opaque to the store. Try a few non-trivial shapes.
  test("event_type round-trips verbatim for several unusual shapes", async () => {
    const cases = [
      "PascalCase",
      "snake_case_event",
      "Event.With.Dots",
      "Event-With-Dashes",
      "イベント", // non-ASCII
      "x".repeat(200), // long string
    ];
    for (const t of cases) {
      const s = randomUUID();
      await appendOne(s, { event_type: t });
      const got = await readFirst(s);
      assert.equal(got.event_type, t);
    }
  });

  // INV-META-011: data round-trips through JSONB faithfully
  test("data payload round-trips through JSONB (nested, arrays, primitives)", async () => {
    const s = randomUUID();
    const data = {
      balance: 1234,
      name: "alice",
      flags: [true, false, null],
      nested: { a: { b: { c: [1, 2, 3] } } },
      empty: {},
      arr: [{ x: 1 }, { x: 2 }],
    };
    await appendOne(s, { event_type: "X", data });
    const got = await readFirst(s);
    assert.deepEqual(got.data, data);
  });

  // INV-META-011: metadata round-trips similarly
  test("metadata payload round-trips through JSONB", async () => {
    const s = randomUUID();
    const meta = { tracing_id: "abc-123", attempts: 3, tags: ["a", "b"] };
    await appendOne(s, { event_type: "X", metadata: meta });
    const got = await readFirst(s);
    assert.deepEqual(got.metadata, meta);
  });

  // INV-META-011: absent metadata comes back as null
  test("absent metadata comes back as null", async () => {
    const s = randomUUID();
    await appendOne(s, { event_type: "X" });
    const got = await readFirst(s);
    assert.equal(got.metadata, null);
  });

  // INV-META-011: top-level data may be any JSON type (array, scalar, null);
  //   the store treats it as opaque. (The procedure default-coalesces a
  //   missing 'data' key to JSON null; an explicit non-object value
  //   round-trips verbatim.)
  test("data may be a JSON array or scalar (treated opaquely)", async () => {
    const s1 = randomUUID();
    await appendOne(s1, { event_type: "X", data: [1, 2, 3] });
    assert.deepEqual((await readFirst(s1)).data, [1, 2, 3]);

    const s2 = randomUUID();
    await appendOne(s2, { event_type: "X", data: "just-a-string" });
    assert.equal((await readFirst(s2)).data, "just-a-string");

    const s3 = randomUUID();
    await appendOne(s3, { event_type: "X", data: 42 });
    assert.equal((await readFirst(s3)).data, 42);
  });
});

// =============================================================================
// Streams as a first-class concept (INV-STREAM-001, 002, 003)
// =============================================================================

describe("streams — first-class identity", () => {
  // INV-STREAM-001: stream_uuid is the stable external identity. Two
  //   distinct stream_uuids never share state; same stream_uuid is
  //   always the same stream.
  test("two distinct stream_uuids carry independent events and versions", async () => {
    const a = randomUUID();
    const b = randomUUID();
    await appendOne(a, { event_type: "A1" });
    await appendOne(a, { event_type: "A2" });
    await appendOne(b, { event_type: "B1" });

    const fromA = await readFirst(a);
    assert.equal(fromA.stream_uuid, a);
    assert.equal(fromA.event_type, "A1");
    // A's stream is at version 2; B's is at version 1 — checked via $all.
    const all = await readAllRows();
    assert.equal(all.length, 3);
    assert.deepEqual(all.map((r) => r.stream_uuid), [a, a, b]);
    assert.deepEqual(all.map((r) => r.stream_version), [1n, 2n, 1n]);
  });

  // INV-STREAM-001: re-appending under the same stream_uuid hits the
  //   same stream (no shadowing, no collision); per-stream versions
  //   stay contiguous.
  test("the same stream_uuid resolves to the same stream across calls", async () => {
    const s = randomUUID();
    await appendOne(s, { event_type: "A" });
    await appendOne(s, { event_type: "B" });
    await appendOne(s, { event_type: "C" });
    const all = await readAllRows();
    assert.deepEqual(
      all.filter((r) => r.stream_uuid === s).map((r) => r.stream_version),
      [1n, 2n, 3n],
    );
  });

  // INV-STREAM-002: [reference-only mechanism]
  //   The internal numeric stream_id is an implementation detail; it
  //   is NOT exposed across the procedure contract. None of the
  //   returned-row signatures of read_stream / read_all /
  //   read_subscription_batch / read_snapshot mention stream_id.
  //   The smoke test in step 1/8 already pinned the function names;
  //   this case pins the column-shape contract.
  test("no procedure exposes internal stream_id in its return columns", async () => {
    // We inspect pg_proc for the column shape of each "read" function
    // and assert none of them name 'stream_id'.
    const r = await pool.query<{ proname: string; argname: string }>(
      `SELECT p.proname,
              unnest(p.proallargtypes) AS atype,
              unnest(p.proargnames)    AS argname
         FROM pg_proc p
         JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'instructed'
          AND p.proname IN (
            'read_stream', 'read_all', 'read_subscription_batch',
            'read_snapshot', 'read_subscription_position'
          )`,
    );
    const offenders = r.rows.filter((row) => row.argname === "stream_id");
    assert.equal(
      offenders.length,
      0,
      `procedures exposing internal stream_id: ${
        offenders.map((o) => o.proname).join(", ")
      }`,
    );
  });

  // INV-STREAM-003: '$all' is reserved at the schema level — not by
  //   convention. The CHECK constraint on streams (stream_uuid <> '$all'
  //   OR stream_id = 0) rejects user inserts of '$all' as a fresh
  //   stream. The procedure path raises IS005 first (see append.test.ts);
  //   the schema-level CHECK is the belt-and-braces enforcement.
  test("a direct INSERT of a non-seed '$all' row is rejected by the CHECK constraint", async () => {
    // Postgres raises check_violation (23514) on the constraint named
    // streams_check (or similar — pg generates the name from the table
    // and column order). We assert on the SQLSTATE, not the constraint
    // name (which is schema-internal).
    await rejectsWithCode(
      () =>
        pool.query(
          `INSERT INTO instructed.streams (stream_uuid, stream_version)
             VALUES ('$all', 0)`,
        ),
      "23514",
    );
  });

  // INV-STREAM-003: read_stream on '$all' is rejected as a reserved name
  //   (IS005), not transparently routed to read_all. (Already covered
  //   in read.test.ts; pinned here for the cross-cutting matrix.)
  test("read_stream against '$all' raises IS005", async () => {
    await rejectsWithCode(
      () => pool.query(`SELECT * FROM instructed.read_stream('$all', 0, 100)`),
      "IS005",
    );
  });
});

// =============================================================================
// Dropped invariants (INV-LINK-001, INV-DELETE-001)
// =============================================================================

describe("dropped invariants — no user-facing linking, no hard delete", () => {
  // INV-LINK-001: dropped — see mapping.md Pass 1 (NG-0009)
  //
  // Commanded's reference adapter supports linking an event_id into
  // additional streams via the stream_events join table; it is not
  // exposed at the adapter contract. `instructed` does not expose any
  // such surface either. The omission shape: no procedure named
  // link_event / link_to_stream / similar exists in instructed.*.
  test("INV-LINK-001 (dropped, NG-0009): no link-event procedure exists", async () => {
    const r = await pool.query<{ proname: string }>(
      `SELECT proname
         FROM pg_proc p
         JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'instructed'
          AND (
            p.proname ILIKE '%link%'
            OR p.proname ILIKE 'category_%'
          )`,
    );
    assert.equal(
      r.rowCount,
      0,
      `expected no link-event procedure; found ${r.rows.map((row) => row.proname).join(", ")}`,
    );
  });

  // INV-DELETE-001: dropped — see NG-0008
  //
  // The reference adapter exposes a gated hard-delete; `instructed`
  // does not. Already covered as part of INV-APPEND-041 in
  // append.test.ts; pinned here for the cross-cutting INV-DELETE-001
  // identifier so the coverage matrix renders it as covered (dropped).
  test("INV-DELETE-001 (dropped, NG-0008): no hard-delete procedure exists", async () => {
    const r = await pool.query<{ proname: string }>(
      `SELECT proname
         FROM pg_proc p
         JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'instructed'
          AND (
            p.proname ILIKE '%hard_delete%'
            OR p.proname ILIKE '%delete_event%'
            OR p.proname ILIKE '%purge%'
          )`,
    );
    assert.equal(
      r.rowCount,
      0,
      `expected no hard-delete procedure; found ${r.rows.map((row) => row.proname).join(", ")}`,
    );
  });
});
