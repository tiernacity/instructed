/**
 * Part C — Read conformance (Phase 9, step 3/8).
 *
 * Drives `instructed.read_stream` and `instructed.read_all` directly
 * via `pg` (D-0021). Each `test(...)` carries one or more
 * `// INV-READ-NNN` annotations on the line above it.
 *
 * The procedures' full contracts live in `sql/instructed.sql`. This
 * file does not paraphrase them.
 */

import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { after, before, beforeEach, describe, test } from 'node:test'

import type pg from 'pg'

import { appendAny, type InputEvent, rejectsWithCode } from './_helpers.ts'
import { closePool, getPool, truncateAll } from './fixtures.ts'

let pool: pg.Pool

before(async () => {
  pool = await getPool()
})

after(async () => {
  await closePool()
})

beforeEach(async () => {
  await truncateAll(pool)
})

// -----------------------------------------------------------------------------
// Per-procedure helpers. Shared `appendAny` / `rejectsWithCode` come from
// ./_helpers.ts; only the read-row mapping stays local.
// -----------------------------------------------------------------------------

interface ReadRow {
  event_id: string
  event_number: bigint
  stream_uuid: string
  stream_version: bigint
  event_type: string
  causation_id: string | null
  correlation_id: string | null
  data: unknown
  metadata: unknown
  created_at: Date
}

function append(streamUuid: string, events: InputEvent[]): Promise<void> {
  return appendAny(pool, streamUuid, events)
}

function asReadRow(row: {
  event_id: string
  event_number: string
  stream_uuid: string
  stream_version: string
  event_type: string
  causation_id: string | null
  correlation_id: string | null
  data: unknown
  metadata: unknown
  created_at: Date
}): ReadRow {
  return {
    event_id: row.event_id,
    event_number: BigInt(row.event_number),
    stream_uuid: row.stream_uuid,
    stream_version: BigInt(row.stream_version),
    event_type: row.event_type,
    causation_id: row.causation_id,
    correlation_id: row.correlation_id,
    data: row.data,
    metadata: row.metadata,
    created_at: row.created_at,
  }
}

async function readStream(
  streamUuid: string,
  fromStreamVersion: bigint,
  qty: number,
): Promise<ReadRow[]> {
  const r = await pool.query(`SELECT * FROM instructed.read_stream($1, $2, $3)`, [
    streamUuid,
    fromStreamVersion,
    qty,
  ])
  // deno-lint-ignore no-explicit-any
  return r.rows.map((row: any) => asReadRow(row))
}

async function readAll(fromEventNumber: bigint, qty: number): Promise<ReadRow[]> {
  const r = await pool.query(`SELECT * FROM instructed.read_all($1, $2)`, [fromEventNumber, qty])
  // deno-lint-ignore no-explicit-any
  return r.rows.map((row: any) => asReadRow(row))
}

// =============================================================================
// read_stream — single-stream reads (INV-READ-001..004)
// =============================================================================

void describe('read_stream — stream-not-found and basics', () => {
  // INV-READ-001: reading a never-appended-to stream MUST return IS003
  void test('reading a never-appended-to stream raises IS003 stream_not_found', async () => {
    await rejectsWithCode(() => readStream(randomUUID(), 0n, 100), 'IS003')
  })

  // INV-READ-001: the existence check is on the streams row, not on the
  //   presence of events. (Today there is no way to create a stream row
  //   without also appending; the test below pins the contract anyway.)
  void test('reading an empty page from a present stream returns zero rows, not IS003', async () => {
    const s = randomUUID()
    await append(s, [{ event_type: 'A' }])
    // Read past the end: from_stream_version = 999 — empty page, no error.
    const rows = await readStream(s, 999n, 100)
    assert.equal(rows.length, 0)
  })

  // INV-READ-002: events come back in strictly increasing stream_version order
  // INV-READ-008: event_number reflects global position (NOT per-stream)
  void test('returns events in strictly increasing stream_version order; event_number is global', async () => {
    const a = randomUUID()
    const b = randomUUID()
    await append(a, [{ event_type: 'A1' }])
    await append(b, [{ event_type: 'B1' }, { event_type: 'B2' }])
    await append(a, [{ event_type: 'A2' }])

    const rows = await readStream(a, 0n, 100)
    assert.equal(rows.length, 2)
    assert.deepEqual(
      rows.map((r) => r.event_type),
      ['A1', 'A2'],
    )
    assert.deepEqual(
      rows.map((r) => r.stream_version),
      [1n, 2n],
    )
    // event_number is the global position: A1 was global #1, A2 was global #4.
    assert.deepEqual(
      rows.map((r) => r.event_number),
      [1n, 4n],
    )
  })

  // INV-READ-004: p_from_stream_version is INCLUSIVE
  void test('from_stream_version is inclusive (=N returns event at version N)', async () => {
    const s = randomUUID()
    await append(s, [{ event_type: 'A' }, { event_type: 'B' }, { event_type: 'C' }])
    const fromTwo = await readStream(s, 2n, 100)
    assert.deepEqual(
      fromTwo.map((r) => r.event_type),
      ['B', 'C'],
    )
    assert.deepEqual(
      fromTwo.map((r) => r.stream_version),
      [2n, 3n],
    )
  })

  // INV-READ-004: from_stream_version = 0 returns from the start
  void test('from_stream_version = 0 returns from the start of the stream', async () => {
    const s = randomUUID()
    await append(s, [{ event_type: 'A' }, { event_type: 'B' }])
    const rows = await readStream(s, 0n, 100)
    assert.deepEqual(
      rows.map((r) => r.stream_version),
      [1n, 2n],
    )
  })

  // INV-READ-003: paged reads cover the full requested range
  void test('paged reads (small qty, loop until empty) cover every event >= start_version', async () => {
    const s = randomUUID()
    const types = Array.from({ length: 10 }, (_, i) => `E${i + 1}`)
    await append(
      s,
      types.map((t) => ({ event_type: t })),
    )

    // Page through with qty = 3.
    const seen: ReadRow[] = []
    let cursor = 0n
    for (let safety = 0; safety < 20; safety++) {
      const page = await readStream(s, cursor + 1n, 3)
      if (page.length === 0) break
      seen.push(...page)
      cursor = page[page.length - 1].stream_version
    }
    assert.deepEqual(
      seen.map((r) => r.event_type),
      types,
    )
    assert.deepEqual(
      seen.map((r) => r.stream_version),
      [1n, 2n, 3n, 4n, 5n, 6n, 7n, 8n, 9n, 10n],
    )
  })

  // INV-READ-006: stream_uuid on each returned row echoes the requested stream
  void test('returned stream_uuid echoes the requested stream identity', async () => {
    const s = randomUUID()
    await append(s, [{ event_type: 'A' }])
    const rows = await readStream(s, 0n, 100)
    assert.equal(rows[0].stream_uuid, s)
  })

  // INV-STREAM-003 / NG-0011: reads on '$all' must use read_all
  void test("read_stream against '$all' raises IS005 reserved_stream_uuid", async () => {
    await rejectsWithCode(() => readStream('$all', 0n, 100), 'IS005')
  })
})

// =============================================================================
// read_all — global reads (INV-READ-005..008)
// =============================================================================

void describe('read_all — global ordering and original-identity echo', () => {
  // INV-READ-005: read_all returns ALL events ordered by strictly
  //   increasing event_number, regardless of origin stream
  void test('returns every event in the store in strictly increasing event_number order', async () => {
    const a = randomUUID()
    const b = randomUUID()
    const c = randomUUID()
    await append(a, [{ event_type: 'A1' }])
    await append(b, [{ event_type: 'B1' }, { event_type: 'B2' }])
    await append(c, [{ event_type: 'C1' }])
    await append(a, [{ event_type: 'A2' }])

    const rows = await readAll(0n, 100)
    assert.equal(rows.length, 5)
    assert.deepEqual(
      rows.map((r) => r.event_number),
      [1n, 2n, 3n, 4n, 5n],
    )
    assert.deepEqual(
      rows.map((r) => r.event_type),
      ['A1', 'B1', 'B2', 'C1', 'A2'],
    )
  })

  // INV-READ-006: stream_uuid carries the *original* stream identity,
  //   not '$all'.
  void test("stream_uuid on each row is the original stream's uuid, not '$all'", async () => {
    const a = randomUUID()
    const b = randomUUID()
    await append(a, [{ event_type: 'A1' }])
    await append(b, [{ event_type: 'B1' }])
    await append(a, [{ event_type: 'A2' }])

    const rows = await readAll(0n, 100)
    assert.deepEqual(
      rows.map((r) => r.stream_uuid),
      [a, b, a],
    )
    // None of them say '$all'.
    for (const r of rows) {
      assert.notEqual(r.stream_uuid, '$all')
    }
  })

  // INV-READ-007: stream_version returned via read_all is the event's
  //   *per-stream* version in its origin stream, NOT its position in $all
  void test('stream_version is the per-stream version, not the $all position', async () => {
    const a = randomUUID()
    const b = randomUUID()
    await append(a, [{ event_type: 'A1' }, { event_type: 'A2' }])
    await append(b, [{ event_type: 'B1' }])
    await append(a, [{ event_type: 'A3' }])

    const rows = await readAll(0n, 100)
    // event_number is the global position
    assert.deepEqual(
      rows.map((r) => r.event_number),
      [1n, 2n, 3n, 4n],
    )
    // stream_version is per-origin-stream:
    //   A1=1, A2=2, B1=1, A3=3
    assert.deepEqual(
      rows.map((r) => ({
        stream: r.stream_uuid,
        sv: r.stream_version,
      })),
      [
        { stream: a, sv: 1n },
        { stream: a, sv: 2n },
        { stream: b, sv: 1n },
        { stream: a, sv: 3n },
      ],
    )
  })

  // INV-READ-008: event_number is the position in $all, full stop.
  //   (Already exercised in the test above; pinned here as a per-INV
  //   marker so the coverage report renders 008 distinctly.)
  void test('event_number is the global $all position', async () => {
    const s = randomUUID()
    await append(s, [{ event_type: 'A' }, { event_type: 'B' }])
    const rows = await readAll(0n, 100)
    assert.deepEqual(
      rows.map((r) => r.event_number),
      [1n, 2n],
    )
  })

  // INV-READ-004 (mirrored for read_all): from_event_number is inclusive.
  void test('from_event_number is inclusive (= N returns event at #N)', async () => {
    const s = randomUUID()
    await append(s, [{ event_type: 'A' }, { event_type: 'B' }, { event_type: 'C' }])
    const rows = await readAll(2n, 100)
    assert.deepEqual(
      rows.map((r) => r.event_number),
      [2n, 3n],
    )
  })

  // INV-READ-005: read_all on an empty store returns zero rows, NOT IS003
  //   (the $all stream always exists; an empty store is a valid state).
  void test('read_all on an empty store returns zero rows, not an error', async () => {
    const rows = await readAll(0n, 100)
    assert.equal(rows.length, 0)
  })

  // INV-READ-003 (mirrored for read_all): paged reads cover the full range.
  void test('paged reads via read_all cover every event in order', async () => {
    const streams = [randomUUID(), randomUUID(), randomUUID()]
    for (let i = 0; i < 9; i++) {
      await append(streams[i % 3], [{ event_type: `E${i + 1}` }])
    }
    const seen: ReadRow[] = []
    let cursor = 0n
    for (let safety = 0; safety < 20; safety++) {
      const page = await readAll(cursor + 1n, 4)
      if (page.length === 0) break
      seen.push(...page)
      cursor = page[page.length - 1].event_number
    }
    assert.equal(seen.length, 9)
    assert.deepEqual(
      seen.map((r) => r.event_number),
      [1n, 2n, 3n, 4n, 5n, 6n, 7n, 8n, 9n],
    )
  })
})

// =============================================================================
// Reader / append concurrency (INV-READ-020)
// =============================================================================

void describe('read_stream / read_all — concurrency with appends', () => {
  // INV-READ-020: a reader started AFTER an append A observes A.
  //   (The other half of the invariant — whether a started-before reader
  //   lazily picks up A on a later page — is implementation-defined and
  //   Commanded does not rely on either behaviour, so we don't assert it.)
  void test('a read started after an append observes the appended events', async () => {
    const s = randomUUID()
    await append(s, [{ event_type: 'A' }])
    // Append B, then read.
    await append(s, [{ event_type: 'B' }])
    const rows = await readStream(s, 0n, 100)
    assert.deepEqual(
      rows.map((r) => r.event_type),
      ['A', 'B'],
    )
  })

  // INV-READ-020 (continued): same for read_all.
  void test('read_all started after an append observes the appended events', async () => {
    const s = randomUUID()
    await append(s, [{ event_type: 'A' }])
    await append(s, [{ event_type: 'B' }])
    const rows = await readAll(0n, 100)
    assert.deepEqual(
      rows.map((r) => r.event_type),
      ['A', 'B'],
    )
  })

  // INV-READ-020 (continued): a paged read's snapshot semantics are
  //   implementation-defined under Postgres MVCC. We document this by
  //   asserting only that (a) each page is internally consistent and
  //   (b) any rows the reader DOES see are in valid order. We do NOT
  //   assert that an append committed mid-paginate will or will not
  //   appear in a later page; Commanded relies on neither behaviour.
  void test('paged read interleaved with appends is internally consistent (order preserved within and across pages it sees)', async () => {
    const s = randomUUID()
    for (let i = 0; i < 5; i++) {
      await append(s, [{ event_type: `E${i + 1}` }])
    }
    // Read page 1.
    const page1 = await readStream(s, 1n, 3)
    assert.equal(page1.length, 3)
    // Append more while "paginating".
    await append(s, [{ event_type: 'Late1' }, { event_type: 'Late2' }])
    // Read page 2.
    const page2 = await readStream(s, page1[page1.length - 1].stream_version + 1n, 100)
    // Whatever page2 contains, it must be strictly-increasing in
    // stream_version, and must not overlap page1.
    for (let i = 1; i < page2.length; i++) {
      assert.ok(page2[i].stream_version > page2[i - 1].stream_version)
    }
    if (page2.length > 0) {
      assert.ok(
        page2[0].stream_version > page1[page1.length - 1].stream_version,
        "page2 must start strictly after page1's last event",
      )
    }
  })
})
