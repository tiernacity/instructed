/**
 * Part E — Persistent subscriptions conformance (Phase 9, step 5/8).
 *
 * Drives `instructed.claim_subscription`,
 * `instructed.extend_subscription_claim`,
 * `instructed.release_subscription`,
 * `instructed.read_subscription_batch`,
 * `instructed.advance_subscription`,
 * `instructed.read_subscription_position`, and
 * `instructed.delete_subscription` directly via `pg` (D-0021).
 *
 * Each `test(...)` carries one or more `// INV-SUB-P-NNN` annotations
 * on the line above it. The procedures' full contracts live in
 * `sql/instructed.sql`.
 *
 * Lease-expiry cases simulate expiry via a direct UPDATE on the
 * `subscriptions` table (lease_seconds is an integer ≥ 1; waiting
 * for real expiry would slow the suite without exercising any
 * different invariant). This is a SQL-only harness: bypassing the
 * procedure surface for setup is fair game as long as the
 * post-condition under test still goes through a procedure.
 */

import { after, before, beforeEach, describe, test } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import type pg from "pg";
import { closePool, getPool, truncateAll } from "./fixtures.ts";
import { appendAny, rejectsWithCode } from "./_helpers.ts";

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
// Per-procedure thin wrappers
// -----------------------------------------------------------------------------

interface ClaimRow {
  result: "claimed" | "already_claimed";
  last_seen: bigint;
  claimed_by: string;
  claim_expires_at: Date;
}

async function claim(
  streamUuid: string,
  name: string,
  workerId: string,
  leaseSeconds = 30,
  options: Record<string, unknown> = {},
): Promise<ClaimRow> {
  const r = await pool.query<{
    result: string;
    last_seen: string;
    claimed_by: string;
    claim_expires_at: Date;
  }>(
    `SELECT * FROM instructed.claim_subscription($1, $2, $3, $4, $5::jsonb)`,
    [streamUuid, name, workerId, leaseSeconds, JSON.stringify(options)],
  );
  const row = r.rows[0];
  return {
    result: row.result as "claimed" | "already_claimed",
    last_seen: BigInt(row.last_seen),
    claimed_by: row.claimed_by,
    claim_expires_at: row.claim_expires_at,
  };
}

async function extend(
  streamUuid: string,
  name: string,
  workerId: string,
  leaseSeconds = 30,
): Promise<Date> {
  const r = await pool.query<{ claim_expires_at: Date }>(
    `SELECT * FROM instructed.extend_subscription_claim($1, $2, $3, $4)`,
    [streamUuid, name, workerId, leaseSeconds],
  );
  return r.rows[0].claim_expires_at;
}

async function release(
  streamUuid: string,
  name: string,
  workerId: string,
): Promise<void> {
  await pool.query(
    `SELECT instructed.release_subscription($1, $2, $3)`,
    [streamUuid, name, workerId],
  );
}

interface BatchRow {
  event_id: string;
  event_number: bigint;
  stream_uuid: string;
  stream_version: bigint;
  event_type: string;
  data: unknown;
}

async function readBatch(
  streamUuid: string,
  name: string,
  workerId: string,
  qty: number,
): Promise<BatchRow[]> {
  const r = await pool.query<{
    event_id: string;
    event_number: string;
    stream_uuid: string;
    stream_version: string;
    event_type: string;
    data: unknown;
  }>(
    `SELECT event_id, event_number, stream_uuid, stream_version, event_type, data
       FROM instructed.read_subscription_batch($1, $2, $3, $4)`,
    [streamUuid, name, workerId, qty],
  );
  return r.rows.map((row) => ({
    event_id: row.event_id,
    event_number: BigInt(row.event_number),
    stream_uuid: row.stream_uuid,
    stream_version: BigInt(row.stream_version),
    event_type: row.event_type,
    data: row.data,
  }));
}

async function advance(
  streamUuid: string,
  name: string,
  workerId: string,
  upToPosition: bigint,
): Promise<bigint> {
  const r = await pool.query<{ last_seen: string }>(
    `SELECT * FROM instructed.advance_subscription($1, $2, $3, $4)`,
    [streamUuid, name, workerId, upToPosition],
  );
  return BigInt(r.rows[0].last_seen);
}

async function position(streamUuid: string, name: string): Promise<bigint> {
  const r = await pool.query<{ last_seen: string }>(
    `SELECT * FROM instructed.read_subscription_position($1, $2)`,
    [streamUuid, name],
  );
  return BigInt(r.rows[0].last_seen);
}

async function deleteSub(streamUuid: string, name: string): Promise<void> {
  await pool.query(
    `SELECT instructed.delete_subscription($1, $2)`,
    [streamUuid, name],
  );
}

/** Force the lease to be expired without sleeping. */
async function expireLease(streamUuid: string, name: string): Promise<void> {
  await pool.query(
    `UPDATE instructed.subscriptions s
        SET claim_expires_at = now() - interval '1 second'
       FROM instructed.streams str
      WHERE s.stream_id = str.stream_id
        AND str.stream_uuid = $1
        AND s.subscription_name = $2`,
    [streamUuid, name],
  );
}

/** Seed N events on a fresh stream; return the stream uuid. */
async function seedStream(n: number): Promise<string> {
  const s = randomUUID();
  const events = Array.from({ length: n }, (_, i) => ({
    event_type: `E${i + 1}`,
    data: { i: i + 1 },
  }));
  await appendAny(pool, s, events);
  return s;
}

// =============================================================================
// Identity and idempotent re-subscribe (INV-SUB-P-001, 002)
// =============================================================================

describe("subscriptions — identity and re-subscribe", () => {
  // INV-SUB-P-001: identity is (stream_uuid, subscription_name)
  test("first claim on a fresh (stream, name) creates the row and returns last_seen = 0", async () => {
    const s = await seedStream(3);
    const r = await claim(s, "handler-a", "worker-1");
    assert.equal(r.result, "claimed");
    assert.equal(r.last_seen, 0n);
    assert.equal(r.claimed_by, "worker-1");
    assert.ok(r.claim_expires_at instanceof Date);
  });

  // INV-SUB-P-002: re-claim by the same worker is idempotent — cursor preserved
  test("re-claim by the same worker preserves last_seen and extends the lease", async () => {
    const s = await seedStream(3);
    const first = await claim(s, "handler-a", "worker-1");
    // Advance the cursor so the second claim's last_seen is observably non-zero.
    await advance(s, "handler-a", "worker-1", 2n);
    const second = await claim(s, "handler-a", "worker-1");
    assert.equal(second.result, "claimed");
    assert.equal(second.last_seen, 2n);
    assert.equal(second.claimed_by, "worker-1");
    assert.ok(
      second.claim_expires_at.getTime() >= first.claim_expires_at.getTime(),
      "lease must not move backwards across re-claim",
    );
  });

  // INV-SUB-P-001: subscription on '$all' is permitted (stream_id = 0)
  test("a subscription on '$all' is permitted and starts at last_seen = 0", async () => {
    const r = await claim("$all", "all-handler", "worker-1");
    assert.equal(r.result, "claimed");
    assert.equal(r.last_seen, 0n);
  });

  // INV-SUB-P-001 (negative): the target stream must exist (and is not $all)
  test("claim on a non-existent stream raises IS003 stream_not_found", async () => {
    await rejectsWithCode(
      () => claim(randomUUID(), "handler-a", "worker-1"),
      "IS003",
    );
  });
});

// =============================================================================
// start_from semantics (INV-SUB-P-020, 021)
// =============================================================================

describe("subscriptions — start_from on first vs subsequent claim", () => {
  // INV-SUB-P-020: default start_from = 'origin' → last_seen = 0
  test("default start_from is 'origin' (last_seen = 0)", async () => {
    const s = await seedStream(5);
    const r = await claim(s, "h", "w1");
    assert.equal(r.last_seen, 0n);
  });

  // INV-SUB-P-020: start_from = 'current' → last_seen = current head
  test("start_from 'current' on '$all' starts at the current event_number head", async () => {
    await seedStream(3);
    await seedStream(2); // 5 events total
    const r = await claim("$all", "h", "w1", 30, { start_from: "current" });
    assert.equal(r.last_seen, 5n);
  });

  // INV-SUB-P-020: start_from = 'current' on single stream → last_seen = its current version
  test("start_from 'current' on a single stream starts at that stream's version", async () => {
    const s = await seedStream(7);
    const r = await claim(s, "h", "w1", 30, { start_from: "current" });
    assert.equal(r.last_seen, 7n);
  });

  // INV-SUB-P-020: start_from = N → last_seen = N
  test("start_from integer N sets last_seen = N", async () => {
    const s = await seedStream(10);
    const r = await claim(s, "h", "w1", 30, { start_from: "3" });
    assert.equal(r.last_seen, 3n);
  });

  // INV-SUB-P-021: start_from is IGNORED on subsequent claims
  test("start_from is ignored on subsequent claims (cursor resumes from last_seen)", async () => {
    const s = await seedStream(10);
    await claim(s, "h", "w1", 30, { start_from: "0" });
    await advance(s, "h", "w1", 4n);
    // Now re-claim with start_from = '0' (which would reset to 0 if honoured) —
    // the call must instead resume from last_seen = 4.
    const r = await claim(s, "h", "w1", 30, { start_from: "0" });
    assert.equal(r.last_seen, 4n);
  });

  // INV-SUB-P-020 (input validation): malformed start_from → 22023
  test("malformed start_from raises 22023", async () => {
    const s = await seedStream(1);
    await rejectsWithCode(
      () => claim(s, "h", "w1", 30, { start_from: "not-a-number" }),
      "22023",
    );
  });
});

// =============================================================================
// Single-active-subscriber and failover (INV-SUB-P-010, 011, 012)
// =============================================================================

describe("subscriptions — single-active-subscriber and failover", () => {
  // INV-SUB-P-010: only one live subscriber per (stream, name).
  //   In `instructed`'s lease model, a competing claim returns
  //   `result = 'already_claimed'` with the current holder reported
  //   in `claimed_by` (NOT a raised exception — this is the row-result
  //   shape per claim_subscription's contract).
  // INV-SUB-P-011 [reference-only mechanism]: Commanded uses
  //   pg_advisory_lock; `instructed` uses lease rows per D-0006.
  //   Both realise INV-SUB-P-010; only the mechanism differs.
  test("a second claim on a live lease returns 'already_claimed' with the current holder", async () => {
    const s = await seedStream(2);
    const first = await claim(s, "h", "worker-A");
    assert.equal(first.result, "claimed");

    const second = await claim(s, "h", "worker-B");
    assert.equal(second.result, "already_claimed");
    assert.equal(second.claimed_by, "worker-A"); // diagnostics
    assert.equal(second.last_seen, 0n); // cursor reported, unmodified
  });

  // INV-SUB-P-012: when the holder disconnects, its slot becomes
  //   available without administrative action — realised by lease TTL.
  test("after lease expiry, a different worker claims successfully", async () => {
    const s = await seedStream(2);
    await claim(s, "h", "worker-A", 30);
    // Simulate worker-A disconnecting and its lease timing out:
    await expireLease(s, "h");
    const r = await claim(s, "h", "worker-B", 30);
    assert.equal(r.result, "claimed");
    assert.equal(r.claimed_by, "worker-B");
  });

  // INV-SUB-P-010 (idempotence under same worker): re-claim by the
  //   current holder always succeeds (used as a heartbeat fallback).
  test("re-claim by the current holder succeeds (no 'already_claimed' against self)", async () => {
    const s = await seedStream(1);
    await claim(s, "h", "w1");
    const r = await claim(s, "h", "w1");
    assert.equal(r.result, "claimed");
    assert.equal(r.claimed_by, "w1");
  });

  // INV-SUB-P-012 (mechanism): extend_subscription_claim is the heartbeat.
  //   The holder extends; a different worker cannot.
  test("extend_subscription_claim updates expiry for the holder", async () => {
    const s = await seedStream(1);
    const first = await claim(s, "h", "w1", 30);
    // Wait a tick so timestamps are observably different.
    await new Promise((r) => setTimeout(r, 10));
    const newExpiry = await extend(s, "h", "w1", 60);
    assert.ok(
      newExpiry.getTime() > first.claim_expires_at.getTime(),
      "heartbeat must move the expiry forward",
    );
  });

  // INV-SUB-P-012 (mechanism): a non-holder heartbeat fails with IS022.
  test("extend_subscription_claim by a non-holder raises IS022 lease_lost", async () => {
    const s = await seedStream(1);
    await claim(s, "h", "w1");
    await rejectsWithCode(() => extend(s, "h", "w2"), "IS022");
  });

  // INV-SUB-P-012 (mechanism): heartbeat on a missing subscription → IS020.
  test("extend_subscription_claim on a missing subscription raises IS020", async () => {
    const s = await seedStream(1);
    await rejectsWithCode(() => extend(s, "no-such", "w1"), "IS020");
  });
});

// =============================================================================
// Delivery order, at-least-once (INV-SUB-P-030, 031)
// =============================================================================

describe("subscriptions — delivery", () => {
  // INV-SUB-P-030: single-stream subscriptions deliver in stream_version order
  test("single-stream subscription delivers events in stream_version order", async () => {
    const s = await seedStream(5);
    await claim(s, "h", "w1");
    const batch = await readBatch(s, "h", "w1", 100);
    assert.deepEqual(
      batch.map((b) => b.stream_version),
      [1n, 2n, 3n, 4n, 5n],
    );
    assert.deepEqual(
      batch.map((b) => b.event_type),
      ["E1", "E2", "E3", "E4", "E5"],
    );
  });

  // INV-SUB-P-030: '$all' subscriptions deliver in event_number order
  test("'$all' subscription delivers events in event_number order across streams", async () => {
    const a = await seedStream(2); // events 1,2
    const b = await seedStream(1); // event 3
    await appendAny(pool, a, [{ event_type: "A3" }]); // event 4
    await claim("$all", "h", "w1");
    const batch = await readBatch("$all", "h", "w1", 100);
    assert.deepEqual(
      batch.map((b) => b.event_number),
      [1n, 2n, 3n, 4n],
    );
    // And the original stream identities are echoed (INV-READ-006/007
    // applied to subscriptions).
    assert.deepEqual(batch.map((b) => b.stream_uuid), [a, a, b, a]);
    assert.deepEqual(batch.map((b) => b.stream_version), [1n, 2n, 1n, 3n]);
  });

  // INV-SUB-P-031: read_subscription_batch does NOT advance the cursor;
  //   repeated calls without advance return the same events.
  test("read_subscription_batch does NOT advance the cursor (at-least-once contract)", async () => {
    const s = await seedStream(3);
    await claim(s, "h", "w1");
    const first = await readBatch(s, "h", "w1", 100);
    const second = await readBatch(s, "h", "w1", 100);
    assert.deepEqual(
      first.map((b) => b.event_id),
      second.map((b) => b.event_id),
    );
    assert.equal(await position(s, "h"), 0n);
  });

  // INV-SUB-P-031 (continued): after advance, the cursor moves and
  //   subsequent reads return only the unacked tail.
  test("after advance, subsequent reads return only events past last_seen", async () => {
    const s = await seedStream(5);
    await claim(s, "h", "w1");
    await advance(s, "h", "w1", 3n);
    const batch = await readBatch(s, "h", "w1", 100);
    assert.deepEqual(batch.map((b) => b.stream_version), [4n, 5n]);
  });

  // INV-SUB-P-031: a non-holder read raises IS022.
  test("read_subscription_batch by a non-holder raises IS022", async () => {
    const s = await seedStream(1);
    await claim(s, "h", "w1");
    await rejectsWithCode(() => readBatch(s, "h", "w2", 100), "IS022");
  });

  // INV-SUB-P-031: a read against a non-existent subscription raises IS020.
  test("read_subscription_batch on a missing subscription raises IS020", async () => {
    const s = await seedStream(1);
    await rejectsWithCode(() => readBatch(s, "no-such", "w1", 100), "IS020");
  });

  // INV-SUB-P-030: a read of an empty tail returns zero rows.
  test("read_subscription_batch on a caught-up cursor returns zero rows", async () => {
    const s = await seedStream(2);
    await claim(s, "h", "w1");
    await advance(s, "h", "w1", 2n);
    const batch = await readBatch(s, "h", "w1", 100);
    assert.equal(batch.length, 0);
  });

  // INV-SUB-P-030: batch qty caps the page size; the cursor still hasn't moved
  //   so a second call returns the next page.
  test("p_qty caps the batch size; further events appear on the next read after advance", async () => {
    const s = await seedStream(6);
    await claim(s, "h", "w1");
    const first = await readBatch(s, "h", "w1", 3);
    assert.deepEqual(first.map((b) => b.stream_version), [1n, 2n, 3n]);
    // Without advance, the next call returns the same first 3.
    const second = await readBatch(s, "h", "w1", 3);
    assert.deepEqual(second.map((b) => b.stream_version), [1n, 2n, 3n]);
    // Advance, then read the rest.
    await advance(s, "h", "w1", 3n);
    const third = await readBatch(s, "h", "w1", 3);
    assert.deepEqual(third.map((b) => b.stream_version), [4n, 5n, 6n]);
  });
});

// =============================================================================
// Cursor advance and monotonicity (INV-SUB-P-032, 033, 034)
// =============================================================================

describe("subscriptions — advance and monotonicity", () => {
  // INV-SUB-P-032: advance to N records "all events up to and including N"
  test("advance(N) sets last_seen = N", async () => {
    const s = await seedStream(5);
    await claim(s, "h", "w1");
    const after = await advance(s, "h", "w1", 3n);
    assert.equal(after, 3n);
    assert.equal(await position(s, "h"), 3n);
  });

  // INV-SUB-P-033: cursor does not advance past unacked events.
  //   Reading without calling advance leaves last_seen at its prior value.
  test("read without advance leaves the cursor unmoved (no auto-ack)", async () => {
    const s = await seedStream(3);
    await claim(s, "h", "w1");
    await readBatch(s, "h", "w1", 100); // reads but does not advance
    assert.equal(await position(s, "h"), 0n);
  });

  // INV-SUB-P-034: out-of-order / duplicate ack is absorbed via
  //   max(last_seen, p_up_to_position). Lower values don't move the
  //   cursor backwards.
  test("advance with a lower position is a no-op (monotone)", async () => {
    const s = await seedStream(5);
    await claim(s, "h", "w1");
    await advance(s, "h", "w1", 3n);
    const after = await advance(s, "h", "w1", 1n); // lower
    assert.equal(after, 3n);
    assert.equal(await position(s, "h"), 3n);
  });

  // INV-SUB-P-034: repeated ack at the same position is a no-op.
  test("advance with the same position is a no-op", async () => {
    const s = await seedStream(3);
    await claim(s, "h", "w1");
    await advance(s, "h", "w1", 2n);
    const after = await advance(s, "h", "w1", 2n);
    assert.equal(after, 2n);
  });

  // INV-SUB-P-032 (defensive): advance by a non-holder raises IS022.
  test("advance by a non-holder raises IS022", async () => {
    const s = await seedStream(1);
    await claim(s, "h", "w1");
    await rejectsWithCode(() => advance(s, "h", "w2", 1n), "IS022");
  });

  // INV-SUB-P-032 (defensive): advance on a missing subscription → IS020.
  test("advance on a missing subscription raises IS020", async () => {
    const s = await seedStream(1);
    await rejectsWithCode(() => advance(s, "no-such", "w1", 1n), "IS020");
  });

  // INV-SUB-P-031 (redelivery): if a worker reads, processes, but crashes
  //   before advance, the next claim re-reads the same events.
  test("redelivery: crash-before-advance is recovered by re-claim", async () => {
    const s = await seedStream(3);
    await claim(s, "h", "w1");
    const first = await readBatch(s, "h", "w1", 100);
    // Simulate worker-1 crashing — its lease expires.
    await expireLease(s, "h");
    // Worker-2 takes over.
    const claim2 = await claim(s, "h", "w2");
    assert.equal(claim2.last_seen, 0n);
    const redelivered = await readBatch(s, "h", "w2", 100);
    assert.deepEqual(
      first.map((b) => b.event_id),
      redelivered.map((b) => b.event_id),
    );
  });
});

// =============================================================================
// Lifecycle — release / delete / position (INV-SUB-P-060, 061, 062)
// =============================================================================

describe("subscriptions — lifecycle", () => {
  // INV-SUB-P-060: release detaches the holder but preserves the cursor
  test("release_subscription clears the holder and preserves last_seen", async () => {
    const s = await seedStream(5);
    await claim(s, "h", "w1");
    await advance(s, "h", "w1", 3n);
    await release(s, "h", "w1");
    // Cursor is still 3.
    assert.equal(await position(s, "h"), 3n);
    // A new worker can claim and resume from there.
    const r = await claim(s, "h", "w2");
    assert.equal(r.result, "claimed");
    assert.equal(r.last_seen, 3n);
  });

  // INV-SUB-P-060: release by a non-holder raises IS022.
  test("release_subscription by a non-holder raises IS022", async () => {
    const s = await seedStream(1);
    await claim(s, "h", "w1");
    await rejectsWithCode(() => release(s, "h", "w2"), "IS022");
  });

  // INV-SUB-P-060: release on a missing subscription raises IS020.
  test("release_subscription on a missing subscription raises IS020", async () => {
    const s = await seedStream(1);
    await rejectsWithCode(() => release(s, "no-such", "w1"), "IS020");
  });

  // INV-SUB-P-061: delete removes the row entirely; subsequent claim
  //   behaves as a first-create (honours start_from).
  test("delete_subscription removes the row; next claim treats it as fresh", async () => {
    const s = await seedStream(5);
    await claim(s, "h", "w1");
    await advance(s, "h", "w1", 3n);
    await deleteSub(s, "h");
    // The cursor is gone — a position read raises IS020.
    await rejectsWithCode(() => position(s, "h"), "IS020");
    // A subsequent claim honours start_from (here: explicit '4').
    const r = await claim(s, "h", "w1", 30, { start_from: "4" });
    assert.equal(r.last_seen, 4n);
  });

  // INV-SUB-P-062 / D-0009: delete on a missing subscription raises IS020.
  //   This is tighter than Commanded's reference adapter (which is
  //   silent); `instructed` follows the abstract contract.
  test("delete_subscription on a missing subscription raises IS020 (D-0009)", async () => {
    const s = await seedStream(1);
    await rejectsWithCode(() => deleteSub(s, "no-such"), "IS020");
  });

  // INV-SUB-P-062: delete on a stream that doesn't even exist → IS020.
  test("delete_subscription on a non-existent stream raises IS020", async () => {
    await rejectsWithCode(
      () => deleteSub(randomUUID(), "h"),
      "IS020",
    );
  });

  // read_subscription_position (supporting CON-010 strong-consistency-on-
  //   dispatch per D-0010) — reads the cursor without claiming.
  test("read_subscription_position returns last_seen without claiming the lease", async () => {
    const s = await seedStream(2);
    await claim(s, "h", "w1");
    await advance(s, "h", "w1", 1n);
    // Position read does NOT require a worker_id; anyone may call.
    assert.equal(await position(s, "h"), 1n);
    // And the lease is still held by w1.
    const reclaim = await claim(s, "h", "w2");
    assert.equal(reclaim.result, "already_claimed");
    assert.equal(reclaim.claimed_by, "w1");
  });

  // read_subscription_position on a missing subscription raises IS020.
  test("read_subscription_position on a missing subscription raises IS020", async () => {
    const s = await seedStream(1);
    await rejectsWithCode(() => position(s, "no-such"), "IS020");
  });
});
