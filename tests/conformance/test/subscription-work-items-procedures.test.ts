/**
 * Slice 2 (SUB-A) — procedure coverage for the work-queue.
 *
 * Per-procedure contracts:
 *   - route_batch
 *   - claim_work_item
 *   - complete_work_item_projection
 *   - complete_work_item_pm
 *   - complete_pm_instance
 *   - fail_work_item
 *   - is_subscription_caught_up
 *
 * Plus the cross-cutting acceptance items from the slice brief:
 *   - per-partition ordering under concurrent claimants
 *   - SKIP LOCKED distribution across partitions
 *   - failed row blocks its partition only
 *   - complete_pm_instance is atomic across snapshot + work-items
 */

import { after, before, beforeEach, describe, test } from "node:test";
import assert from "node:assert/strict";
import type pg from "pg";
import { closePool, getPool, truncateAll } from "./fixtures.ts";
import { appendAny, rejectsWithCode } from "./_helpers.ts";

const WORKER = "test-worker-1";
const SUB = "sub-A";
const ALL = "$all";

/** Claim the subscription for WORKER so route_batch may run. */
async function claimSubscription(
  q: pg.ClientBase | pg.Pool,
  worker = WORKER,
  leaseSeconds = 30,
): Promise<void> {
  await q.query(
    `SELECT * FROM instructed.claim_subscription($1, $2, $3, $4)`,
    [ALL, SUB, worker, leaseSeconds],
  );
}

/** Append N events to a stream and return their event_numbers. */
async function appendN(
  pool: pg.Pool,
  stream: string,
  n: number,
  typePrefix = "E",
): Promise<bigint[]> {
  await appendAny(
    pool,
    stream,
    Array.from({ length: n }, (_, i) => ({
      event_type: `${typePrefix}${i}`,
      data: { i },
    })),
  );
  const r = await pool.query<{ en: string }>(
    `SELECT stream_version AS en
       FROM instructed.stream_events
      WHERE stream_id = 0
      ORDER BY stream_version DESC
      LIMIT $1`,
    [n],
  );
  return r.rows
    .map((row) => BigInt(row.en))
    .sort((a, b) => Number(a - b));
}

interface ClaimRow {
  partition_key: string;
  event_number: string;
  claimed_by: string;
  lease_expires_at: string;
  was_takeover: boolean;
  prior_claimed_by: string | null;
}

async function claim(
  q: pg.ClientBase | pg.Pool,
  worker = WORKER,
  leaseSeconds = 30,
): Promise<ClaimRow | null> {
  const r = await q.query<ClaimRow>(
    `SELECT * FROM instructed.claim_work_item($1, $2, $3, $4)`,
    [ALL, SUB, worker, leaseSeconds],
  );
  return r.rowCount === 0 ? null : r.rows[0];
}

describe("SUB-A slice 2 — route_batch", () => {
  let pool: pg.Pool;
  before(async () => {
    pool = await getPool();
  });
  beforeEach(async () => {
    await truncateAll(pool);
    await claimSubscription(pool);
  });
  after(async () => {
    await closePool();
  });

  test("inserts decisions and advances the cursor atomically", async () => {
    const [e1, e2, e3] = await appendN(pool, "s1", 3);
    const r = await pool.query<{ inserted_count: string; new_last_seen: string }>(
      `SELECT * FROM instructed.route_batch($1, $2, $3, $4, $5::jsonb)`,
      [
        ALL,
        SUB,
        WORKER,
        e3.toString(),
        JSON.stringify([
          { partition_key: "p1", event_number: Number(e1) },
          { partition_key: "p2", event_number: Number(e2) },
          { partition_key: "p1", event_number: Number(e3) },
        ]),
      ],
    );
    assert.equal(r.rows[0].inserted_count, "3");
    assert.equal(r.rows[0].new_last_seen, e3.toString());

    const items = await pool.query<{ partition_key: string; event_number: string }>(
      `SELECT partition_key, event_number FROM instructed.subscription_work_items
        ORDER BY event_number`,
    );
    assert.equal(items.rowCount, 3);
    assert.deepEqual(
      items.rows.map((r) => [r.partition_key, r.event_number]),
      [
        ["p1", e1.toString()],
        ["p2", e2.toString()],
        ["p1", e3.toString()],
      ],
    );
  });

  test("empty decisions array still advances the cursor", async () => {
    const [e1, e2] = await appendN(pool, "s1", 2);
    const r = await pool.query<{ inserted_count: string; new_last_seen: string }>(
      `SELECT * FROM instructed.route_batch($1, $2, $3, $4, '[]'::jsonb)`,
      [ALL, SUB, WORKER, e2.toString()],
    );
    assert.equal(r.rows[0].inserted_count, "0");
    assert.equal(r.rows[0].new_last_seen, e2.toString());
    void e1;
  });

  test("ON CONFLICT DO NOTHING absorbs crash-replay (idempotent)", async () => {
    const [e1] = await appendN(pool, "s1", 1);
    const decisions = JSON.stringify([
      { partition_key: "p1", event_number: Number(e1) },
    ]);
    const r1 = await pool.query<{ inserted_count: string }>(
      `SELECT * FROM instructed.route_batch($1, $2, $3, $4, $5::jsonb)`,
      [ALL, SUB, WORKER, e1.toString(), decisions],
    );
    assert.equal(r1.rows[0].inserted_count, "1");
    const r2 = await pool.query<{ inserted_count: string; new_last_seen: string }>(
      `SELECT * FROM instructed.route_batch($1, $2, $3, $4, $5::jsonb)`,
      [ALL, SUB, WORKER, e1.toString(), decisions],
    );
    assert.equal(r2.rows[0].inserted_count, "0");
    assert.equal(r2.rows[0].new_last_seen, e1.toString());
  });

  test("cursor advance is monotone (lower target is a no-op)", async () => {
    const [e1, e2] = await appendN(pool, "s1", 2);
    await pool.query(
      `SELECT * FROM instructed.route_batch($1, $2, $3, $4, '[]'::jsonb)`,
      [ALL, SUB, WORKER, e2.toString()],
    );
    const r = await pool.query<{ new_last_seen: string }>(
      `SELECT * FROM instructed.route_batch($1, $2, $3, $4, '[]'::jsonb)`,
      [ALL, SUB, WORKER, e1.toString()],
    );
    assert.equal(r.rows[0].new_last_seen, e2.toString());
  });

  test("atomicity: a failing batch leaves cursor and inserts unchanged", async () => {
    // Force a failure mid-batch by including a malformed decision.
    const [e1] = await appendN(pool, "s1", 1);
    const before = await pool.query<{ last_seen: string }>(
      `SELECT last_seen FROM instructed.subscriptions
        WHERE stream_id = 0 AND subscription_name = $1`,
      [SUB],
    );
    await rejectsWithCode(
      () =>
        pool.query(
          `SELECT * FROM instructed.route_batch($1, $2, $3, $4, $5::jsonb)`,
          [
            ALL,
            SUB,
            WORKER,
            e1.toString(),
            JSON.stringify([{ partition_key: "p1" /* missing event_number */ }]),
          ],
        ),
      "22023",
    );
    const after = await pool.query<{ last_seen: string }>(
      `SELECT last_seen FROM instructed.subscriptions
        WHERE stream_id = 0 AND subscription_name = $1`,
      [SUB],
    );
    assert.equal(after.rows[0].last_seen, before.rows[0].last_seen);
    const items = await pool.query(
      `SELECT 1 FROM instructed.subscription_work_items`,
    );
    assert.equal(items.rowCount, 0);
  });

  test("lease lost: non-holder raises IS022", async () => {
    const [e1] = await appendN(pool, "s1", 1);
    await rejectsWithCode(
      () =>
        pool.query(
          `SELECT * FROM instructed.route_batch($1, $2, $3, $4, $5::jsonb)`,
          [
            ALL,
            SUB,
            "intruder",
            e1.toString(),
            JSON.stringify([{ partition_key: "p", event_number: Number(e1) }]),
          ],
        ),
      "IS022",
    );
  });

  test("subscription-not-found raises IS020", async () => {
    await rejectsWithCode(
      () =>
        pool.query(
          `SELECT * FROM instructed.route_batch($1, $2, $3, $4, '[]'::jsonb)`,
          [ALL, "nope", WORKER, 0],
        ),
      "IS020",
    );
  });

  test("malformed inputs raise 22023", async () => {
    await rejectsWithCode(
      () =>
        pool.query(
          `SELECT * FROM instructed.route_batch($1, $2, $3, -1, '[]'::jsonb)`,
          [ALL, SUB, WORKER],
        ),
      "22023",
    );
    await rejectsWithCode(
      () =>
        pool.query(
          `SELECT * FROM instructed.route_batch($1, $2, $3, 0, 'null'::jsonb)`,
          [ALL, SUB, WORKER],
        ),
      "22023",
    );
  });
});

describe("SUB-A slice 2 — claim_work_item", () => {
  let pool: pg.Pool;
  before(async () => {
    pool = await getPool();
  });
  beforeEach(async () => {
    await truncateAll(pool);
    await claimSubscription(pool);
  });
  after(async () => {
    await closePool();
  });

  /** Append N events and route each to (partition_key, event_number). */
  async function seedRouted(
    decisions: Array<{ pk: string }>,
  ): Promise<bigint[]> {
    const ens = await appendN(pool, "s1", decisions.length);
    const json = decisions.map((d, i) => ({
      partition_key: d.pk,
      event_number: Number(ens[i]),
    }));
    await pool.query(
      `SELECT * FROM instructed.route_batch($1, $2, $3, $4, $5::jsonb)`,
      [ALL, SUB, WORKER, ens[ens.length - 1].toString(), JSON.stringify(json)],
    );
    return ens;
  }

  test("empty queue returns zero rows", async () => {
    const r = await claim(pool);
    assert.equal(r, null);
  });

  test("claim transitions pending -> claimed with lease metadata", async () => {
    const [e1] = await seedRouted([{ pk: "p1" }]);
    const r = await claim(pool);
    assert.ok(r);
    assert.equal(r.partition_key, "p1");
    assert.equal(r.event_number, e1.toString());
    assert.equal(r.claimed_by, WORKER);
    assert.equal(r.was_takeover, false);
    assert.equal(r.prior_claimed_by, null);

    const row = await pool.query<{ state: string; claimed_by: string }>(
      `SELECT state, claimed_by FROM instructed.subscription_work_items
        WHERE event_number = $1`,
      [e1.toString()],
    );
    assert.equal(row.rows[0].state, "claimed");
    assert.equal(row.rows[0].claimed_by, WORKER);
  });

  test("per-partition ordering: serial within a partition", async () => {
    const [e1, e2] = await seedRouted([{ pk: "p1" }, { pk: "p1" }]);
    const c1 = await claim(pool, "wA");
    assert.equal(c1?.event_number, e1.toString());
    // e2 must NOT be claimable while e1 is in flight.
    const blocked = await claim(pool, "wB");
    assert.equal(blocked, null);
    // Complete e1 (projection-style DELETE).
    await pool.query(
      `SELECT instructed.complete_work_item_projection($1, $2, $3, $4, $5)`,
      [ALL, SUB, "wA", "p1", Number(e1)],
    );
    // Now e2 is claimable.
    const c2 = await claim(pool, "wB");
    assert.equal(c2?.event_number, e2.toString());
  });

  test("parallel across partitions: two claimants get disjoint partitions (SKIP LOCKED)", async () => {
    await seedRouted([{ pk: "p1" }, { pk: "p2" }, { pk: "p3" }]);
    // Use two concurrent transactions so SKIP LOCKED is exercised.
    const c1 = await pool.connect();
    const c2 = await pool.connect();
    try {
      await c1.query("BEGIN");
      await c2.query("BEGIN");
      const r1 = await claim(c1, "wA");
      const r2 = await claim(c2, "wB");
      const r3 = await claim(c2, "wB");
      assert.ok(r1);
      assert.ok(r2);
      assert.ok(r3);
      const pks = [r1.partition_key, r2.partition_key, r3.partition_key].sort();
      assert.deepEqual(pks, ["p1", "p2", "p3"]);
      await c1.query("COMMIT");
      await c2.query("COMMIT");
    } finally {
      c1.release();
      c2.release();
    }
  });

  test("failed row blocks its partition only", async () => {
    const [e1, e2, e3] = await seedRouted([
      { pk: "p1" },
      { pk: "p1" },
      { pk: "p2" },
    ]);
    const c1 = await claim(pool, "wA");
    assert.equal(c1?.event_number, e1.toString());
    await pool.query(
      `SELECT instructed.fail_work_item($1, $2, $3, $4, $5, $6)`,
      [ALL, SUB, "wA", "p1", Number(e1), "boom"],
    );
    // e2 (same partition) is blocked.
    const blocked = await claim(pool, "wB");
    assert.ok(blocked, "another partition should still be claimable");
    assert.equal(blocked.event_number, e3.toString());
    assert.equal(blocked.partition_key, "p2");
    // No more p1 work claimable.
    const none = await claim(pool, "wC");
    assert.equal(none, null);
    void e2;
  });

  test("lease takeover: expired 'claimed' row is re-claimable", async () => {
    const [e1] = await seedRouted([{ pk: "p1" }]);
    // Claim with a 1-second lease then back-date it so it's already expired.
    await claim(pool, "wDead", 1);
    await pool.query(
      `UPDATE instructed.subscription_work_items
          SET lease_expires_at = now() - interval '5 seconds'
        WHERE event_number = $1`,
      [e1.toString()],
    );
    const taken = await claim(pool, "wAlive");
    assert.ok(taken);
    assert.equal(taken.was_takeover, true);
    assert.equal(taken.prior_claimed_by, "wDead");
    assert.equal(taken.claimed_by, "wAlive");
  });

  test("subscription-not-found raises IS020", async () => {
    await rejectsWithCode(
      () =>
        pool.query(
          `SELECT * FROM instructed.claim_work_item($1, 'nope', $2, 30)`,
          [ALL, WORKER],
        ),
      "IS020",
    );
  });
});

describe("SUB-A slice 5 — extend_work_item_claim", () => {
  let pool: pg.Pool;
  before(async () => {
    pool = await getPool();
  });
  beforeEach(async () => {
    await truncateAll(pool);
    await claimSubscription(pool);
  });
  after(async () => {
    await closePool();
  });

  async function seedAndClaim(
    worker = WORKER,
  ): Promise<{ en: bigint; expiresBefore: Date }> {
    const [en] = await appendN(pool, "s1", 1);
    await pool.query(
      `SELECT instructed.route_batch($1, $2, $3, $4, $5::jsonb)`,
      [
        ALL,
        SUB,
        WORKER,
        en.toString(),
        JSON.stringify([{ partition_key: "p", event_number: Number(en) }]),
      ],
    );
    const claimed = await claim(pool, worker);
    return { en, expiresBefore: new Date(claimed!.lease_expires_at) };
  }

  test("extends the lease for the claimant", async () => {
    const { en, expiresBefore } = await seedAndClaim();
    await new Promise((r) => setTimeout(r, 50));
    const r = await pool.query<{ lease_expires_at: string }>(
      `SELECT * FROM instructed.extend_work_item_claim(
         $1, $2, $3, $4, $5, $6)`,
      [ALL, SUB, WORKER, "p", Number(en), 60],
    );
    const expiresAfter = new Date(r.rows[0].lease_expires_at);
    assert.ok(
      expiresAfter.getTime() > expiresBefore.getTime(),
      `lease should have been extended (before=${expiresBefore.toISOString()}, after=${expiresAfter.toISOString()})`,
    );
  });

  test("non-claimant raises IS030", async () => {
    const { en } = await seedAndClaim();
    await rejectsWithCode(
      () =>
        pool.query(
          `SELECT * FROM instructed.extend_work_item_claim(
             $1, $2, $3, $4, $5, $6)`,
          [ALL, SUB, "intruder", "p", Number(en), 30],
        ),
      "IS030",
    );
  });

  test("missing row raises IS030", async () => {
    await rejectsWithCode(
      () =>
        pool.query(
          `SELECT * FROM instructed.extend_work_item_claim(
             $1, $2, $3, $4, $5, $6)`,
          [ALL, SUB, WORKER, "nope", 999, 30],
        ),
      "IS030",
    );
  });

  test("row no longer in 'claimed' state raises IS030", async () => {
    const { en } = await seedAndClaim();
    // Move the row to 'failed'.
    await pool.query(
      `SELECT instructed.fail_work_item($1, $2, $3, $4, $5, $6)`,
      [ALL, SUB, WORKER, "p", Number(en), "boom"],
    );
    await rejectsWithCode(
      () =>
        pool.query(
          `SELECT * FROM instructed.extend_work_item_claim(
             $1, $2, $3, $4, $5, $6)`,
          [ALL, SUB, WORKER, "p", Number(en), 30],
        ),
      "IS030",
    );
  });
});

describe("SUB-A slice 2 — complete_work_item_projection", () => {
  let pool: pg.Pool;
  before(async () => {
    pool = await getPool();
  });
  beforeEach(async () => {
    await truncateAll(pool);
    await claimSubscription(pool);
  });
  after(async () => {
    await closePool();
  });

  async function seedAndClaim(pk = "p1"): Promise<bigint> {
    const [en] = await appendN(pool, "s1", 1);
    await pool.query(
      `SELECT instructed.route_batch($1, $2, $3, $4, $5::jsonb)`,
      [
        ALL,
        SUB,
        WORKER,
        en.toString(),
        JSON.stringify([{ partition_key: pk, event_number: Number(en) }]),
      ],
    );
    await claim(pool);
    return en;
  }

  test("happy path: DELETEs the row", async () => {
    const en = await seedAndClaim();
    await pool.query(
      `SELECT instructed.complete_work_item_projection($1, $2, $3, $4, $5)`,
      [ALL, SUB, WORKER, "p1", Number(en)],
    );
    const r = await pool.query(
      `SELECT 1 FROM instructed.subscription_work_items WHERE event_number = $1`,
      [en.toString()],
    );
    assert.equal(r.rowCount, 0);
  });

  test("missing row raises IS030 (takeover already completed)", async () => {
    const en = await seedAndClaim();
    await pool.query(
      `SELECT instructed.complete_work_item_projection($1, $2, $3, $4, $5)`,
      [ALL, SUB, WORKER, "p1", Number(en)],
    );
    await rejectsWithCode(
      () =>
        pool.query(
          `SELECT instructed.complete_work_item_projection($1, $2, $3, $4, $5)`,
          [ALL, SUB, WORKER, "p1", Number(en)],
        ),
      "IS030",
    );
  });

  test("non-claimant raises IS030", async () => {
    const en = await seedAndClaim();
    await rejectsWithCode(
      () =>
        pool.query(
          `SELECT instructed.complete_work_item_projection($1, $2, $3, $4, $5)`,
          [ALL, SUB, "other-worker", "p1", Number(en)],
        ),
      "IS030",
    );
  });
});

describe("SUB-A slice 2 — complete_work_item_pm", () => {
  let pool: pg.Pool;
  before(async () => {
    pool = await getPool();
  });
  beforeEach(async () => {
    await truncateAll(pool);
    await claimSubscription(pool);
  });
  after(async () => {
    await closePool();
  });

  test("UPDATEs row to 'done' and UPSERTs snapshot in one tx", async () => {
    const [en] = await appendN(pool, "s1", 1);
    await pool.query(
      `SELECT instructed.route_batch($1, $2, $3, $4, $5::jsonb)`,
      [
        ALL,
        SUB,
        WORKER,
        en.toString(),
        JSON.stringify([{ partition_key: "pm-1", event_number: Number(en) }]),
      ],
    );
    await claim(pool);
    await pool.query(
      `SELECT instructed.complete_work_item_pm(
         $1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb)`,
      [
        ALL,
        SUB,
        WORKER,
        "pm-1",
        Number(en),
        "MyPM-instance-1",
        "MyPM",
        Number(en),
        JSON.stringify({ counter: 1 }),
        JSON.stringify({ snapshot_module_version: "v1" }),
      ],
    );
    const row = await pool.query<{
      state: string;
      claimed_by: string | null;
    }>(
      `SELECT state, claimed_by FROM instructed.subscription_work_items
        WHERE event_number = $1`,
      [en.toString()],
    );
    assert.equal(row.rows[0].state, "done");
    assert.equal(row.rows[0].claimed_by, null);
    const snap = await pool.query<{
      source_version: string;
      data: { counter: number };
    }>(
      `SELECT source_version, data FROM instructed.snapshots
        WHERE source_uuid = 'MyPM-instance-1'`,
    );
    assert.equal(snap.rows[0].source_version, en.toString());
    assert.equal(snap.rows[0].data.counter, 1);
  });

  test("non-claimant raises IS030 and leaves both row and snapshot unchanged", async () => {
    const [en] = await appendN(pool, "s1", 1);
    await pool.query(
      `SELECT instructed.route_batch($1, $2, $3, $4, $5::jsonb)`,
      [
        ALL,
        SUB,
        WORKER,
        en.toString(),
        JSON.stringify([{ partition_key: "pm-1", event_number: Number(en) }]),
      ],
    );
    await claim(pool, WORKER);
    await rejectsWithCode(
      () =>
        pool.query(
          `SELECT instructed.complete_work_item_pm(
             $1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, NULL)`,
          [
            ALL,
            SUB,
            "intruder",
            "pm-1",
            Number(en),
            "MyPM-instance-1",
            "MyPM",
            Number(en),
            JSON.stringify({ counter: 1 }),
          ],
        ),
      "IS030",
    );
    const row = await pool.query<{ state: string }>(
      `SELECT state FROM instructed.subscription_work_items
        WHERE event_number = $1`,
      [en.toString()],
    );
    assert.equal(row.rows[0].state, "claimed");
    const snap = await pool.query(
      `SELECT 1 FROM instructed.snapshots WHERE source_uuid = 'MyPM-instance-1'`,
    );
    assert.equal(snap.rowCount, 0);
  });
});

describe("SUB-A slice 2 — complete_pm_instance", () => {
  let pool: pg.Pool;
  before(async () => {
    pool = await getPool();
  });
  beforeEach(async () => {
    await truncateAll(pool);
    await claimSubscription(pool);
  });
  after(async () => {
    await closePool();
  });

  test("DELETEs snapshot AND all work items for the partition in one tx", async () => {
    const ens = await appendN(pool, "s1", 3);
    await pool.query(
      `SELECT instructed.route_batch($1, $2, $3, $4, $5::jsonb)`,
      [
        ALL,
        SUB,
        WORKER,
        ens[2].toString(),
        JSON.stringify([
          { partition_key: "pm-A", event_number: Number(ens[0]) },
          { partition_key: "pm-A", event_number: Number(ens[1]) },
          { partition_key: "pm-B", event_number: Number(ens[2]) },
        ]),
      ],
    );
    // Walk pm-A to completion through complete_work_item_pm so it has
    // a snapshot AND a 'done' row plus one 'pending'.
    await claim(pool);
    await pool.query(
      `SELECT instructed.complete_work_item_pm(
         $1, $2, $3, $4, $5, 'PM-A', 'PM', $5, '{"v":1}'::jsonb, NULL)`,
      [ALL, SUB, WORKER, "pm-A", Number(ens[0])],
    );

    const r = await pool.query<{
      work_items_deleted: string;
      snapshot_deleted: boolean;
    }>(
      `SELECT * FROM instructed.complete_pm_instance($1, $2, $3, $4)`,
      [ALL, SUB, "pm-A", "PM-A"],
    );
    assert.equal(r.rows[0].work_items_deleted, "2"); // done + pending
    assert.equal(r.rows[0].snapshot_deleted, true);

    // pm-A is fully gone.
    const a = await pool.query(
      `SELECT 1 FROM instructed.subscription_work_items WHERE partition_key = 'pm-A'`,
    );
    assert.equal(a.rowCount, 0);
    const snap = await pool.query(
      `SELECT 1 FROM instructed.snapshots WHERE source_uuid = 'PM-A'`,
    );
    assert.equal(snap.rowCount, 0);
    // pm-B is untouched.
    const b = await pool.query(
      `SELECT 1 FROM instructed.subscription_work_items WHERE partition_key = 'pm-B'`,
    );
    assert.equal(b.rowCount, 1);
  });

  test("idempotent: a second call returns zero counts and does not raise", async () => {
    const r = await pool.query<{
      work_items_deleted: string;
      snapshot_deleted: boolean;
    }>(
      `SELECT * FROM instructed.complete_pm_instance($1, $2, $3, $4)`,
      [ALL, SUB, "no-such-pm", "no-such-snapshot"],
    );
    assert.equal(r.rows[0].work_items_deleted, "0");
    assert.equal(r.rows[0].snapshot_deleted, false);
  });

  test("atomicity: a failing call rolls back both deletes", async () => {
    // Seed: pm-A with a snapshot and a work item.
    const [en] = await appendN(pool, "s1", 1);
    await pool.query(
      `SELECT instructed.route_batch($1, $2, $3, $4, $5::jsonb)`,
      [
        ALL,
        SUB,
        WORKER,
        en.toString(),
        JSON.stringify([{ partition_key: "pm-A", event_number: Number(en) }]),
      ],
    );
    await claim(pool);
    await pool.query(
      `SELECT instructed.complete_work_item_pm(
         $1, $2, $3, $4, $5, 'PM-A', 'PM', $5, '{}'::jsonb, NULL)`,
      [ALL, SUB, WORKER, "pm-A", Number(en)],
    );

    // Force a failure by passing a bad subscription name -> IS020.
    await rejectsWithCode(
      () =>
        pool.query(
          `SELECT * FROM instructed.complete_pm_instance($1, 'no-such', $2, $3)`,
          [ALL, "pm-A", "PM-A"],
        ),
      "IS020",
    );
    // Snapshot and work item still there.
    const snap = await pool.query(
      `SELECT 1 FROM instructed.snapshots WHERE source_uuid = 'PM-A'`,
    );
    assert.equal(snap.rowCount, 1);
    const wi = await pool.query(
      `SELECT 1 FROM instructed.subscription_work_items WHERE partition_key = 'pm-A'`,
    );
    assert.equal(wi.rowCount, 1);
  });
});

describe("SUB-A slice 2 — fail_work_item", () => {
  let pool: pg.Pool;
  before(async () => {
    pool = await getPool();
  });
  beforeEach(async () => {
    await truncateAll(pool);
    await claimSubscription(pool);
  });
  after(async () => {
    await closePool();
  });

  test("transitions claimed -> failed; clears lease; records error_text", async () => {
    const [en] = await appendN(pool, "s1", 1);
    await pool.query(
      `SELECT instructed.route_batch($1, $2, $3, $4, $5::jsonb)`,
      [
        ALL,
        SUB,
        WORKER,
        en.toString(),
        JSON.stringify([{ partition_key: "p1", event_number: Number(en) }]),
      ],
    );
    await claim(pool);
    await pool.query(
      `SELECT instructed.fail_work_item($1, $2, $3, $4, $5, $6)`,
      [ALL, SUB, WORKER, "p1", Number(en), "kaboom"],
    );
    const row = await pool.query<{
      state: string;
      claimed_by: string | null;
      lease_expires_at: string | null;
      failed_at: string | null;
      error_text: string | null;
    }>(
      `SELECT state, claimed_by, lease_expires_at, failed_at, error_text
         FROM instructed.subscription_work_items WHERE event_number = $1`,
      [en.toString()],
    );
    assert.equal(row.rows[0].state, "failed");
    assert.equal(row.rows[0].claimed_by, null);
    assert.equal(row.rows[0].lease_expires_at, null);
    assert.ok(row.rows[0].failed_at);
    assert.equal(row.rows[0].error_text, "kaboom");
  });

  test("non-claimant raises IS030", async () => {
    const [en] = await appendN(pool, "s1", 1);
    await pool.query(
      `SELECT instructed.route_batch($1, $2, $3, $4, $5::jsonb)`,
      [
        ALL,
        SUB,
        WORKER,
        en.toString(),
        JSON.stringify([{ partition_key: "p1", event_number: Number(en) }]),
      ],
    );
    await claim(pool, WORKER);
    await rejectsWithCode(
      () =>
        pool.query(
          `SELECT instructed.fail_work_item($1, $2, $3, $4, $5, NULL)`,
          [ALL, SUB, "intruder", "p1", Number(en)],
        ),
      "IS030",
    );
  });
});

describe("SUB-A slice 2 — is_subscription_caught_up", () => {
  let pool: pg.Pool;
  before(async () => {
    pool = await getPool();
  });
  beforeEach(async () => {
    await truncateAll(pool);
    await claimSubscription(pool);
  });
  after(async () => {
    await closePool();
  });

  async function caughtUp(target: bigint | number): Promise<boolean> {
    const r = await pool.query<{ caught_up: boolean }>(
      `SELECT * FROM instructed.is_subscription_caught_up($1, $2, $3)`,
      [ALL, SUB, Number(target)],
    );
    return r.rows[0].caught_up;
  }

  test("routing-not-yet-reached-target => false", async () => {
    const [e1] = await appendN(pool, "s1", 1);
    // last_seen still 0; e1 not routed yet.
    assert.equal(await caughtUp(e1), false);
  });

  test("routed-and-completed => true", async () => {
    const [e1] = await appendN(pool, "s1", 1);
    await pool.query(
      `SELECT instructed.route_batch($1, $2, $3, $4, $5::jsonb)`,
      [
        ALL,
        SUB,
        WORKER,
        e1.toString(),
        JSON.stringify([{ partition_key: "p", event_number: Number(e1) }]),
      ],
    );
    await claim(pool);
    await pool.query(
      `SELECT instructed.complete_work_item_projection($1, $2, $3, $4, $5)`,
      [ALL, SUB, WORKER, "p", Number(e1)],
    );
    assert.equal(await caughtUp(e1), true);
  });

  test("routed-but-pending => false even though cursor passed target", async () => {
    const [e1] = await appendN(pool, "s1", 1);
    await pool.query(
      `SELECT instructed.route_batch($1, $2, $3, $4, $5::jsonb)`,
      [
        ALL,
        SUB,
        WORKER,
        e1.toString(),
        JSON.stringify([{ partition_key: "p", event_number: Number(e1) }]),
      ],
    );
    assert.equal(await caughtUp(e1), false);
  });

  test("routed-then-failed => false (failed blocks catch-up)", async () => {
    const [e1] = await appendN(pool, "s1", 1);
    await pool.query(
      `SELECT instructed.route_batch($1, $2, $3, $4, $5::jsonb)`,
      [
        ALL,
        SUB,
        WORKER,
        e1.toString(),
        JSON.stringify([{ partition_key: "p", event_number: Number(e1) }]),
      ],
    );
    await claim(pool);
    await pool.query(
      `SELECT instructed.fail_work_item($1, $2, $3, $4, $5, NULL)`,
      [ALL, SUB, WORKER, "p", Number(e1)],
    );
    assert.equal(await caughtUp(e1), false);
  });

  test("PM 'done' rows do NOT block catch-up", async () => {
    const [e1] = await appendN(pool, "s1", 1);
    await pool.query(
      `SELECT instructed.route_batch($1, $2, $3, $4, $5::jsonb)`,
      [
        ALL,
        SUB,
        WORKER,
        e1.toString(),
        JSON.stringify([{ partition_key: "pm-1", event_number: Number(e1) }]),
      ],
    );
    await claim(pool);
    await pool.query(
      `SELECT instructed.complete_work_item_pm(
         $1, $2, $3, $4, $5, 'PM-A', 'PM', $5, '{}'::jsonb, NULL)`,
      [ALL, SUB, WORKER, "pm-1", Number(e1)],
    );
    assert.equal(await caughtUp(e1), true);
  });

  test("target=0 on a never-touched subscription is true (cursor>=0, no rows)", async () => {
    assert.equal(await caughtUp(0), true);
  });

  test("subscription-not-found raises IS020", async () => {
    await rejectsWithCode(
      () =>
        pool.query(
          `SELECT * FROM instructed.is_subscription_caught_up($1, 'nope', 0)`,
          [ALL],
        ),
      "IS020",
    );
  });
});

describe("SUB-A slice 7 — list_pm_rebuild_events", () => {
  let pool: pg.Pool;
  before(async () => {
    pool = await getPool();
  });
  beforeEach(async () => {
    await truncateAll(pool);
    await claimSubscription(pool);
  });
  after(async () => {
    await closePool();
  });

  async function routeAndDone(
    partitionKey: string,
    eventNumber: bigint,
    snapshotUuid: string,
  ): Promise<void> {
    await pool.query(
      `SELECT instructed.route_batch($1, $2, $3, $4, $5::jsonb)`,
      [
        ALL,
        SUB,
        WORKER,
        eventNumber.toString(),
        JSON.stringify([
          { partition_key: partitionKey, event_number: Number(eventNumber) },
        ]),
      ],
    );
    await claim(pool);
    await pool.query(
      `SELECT instructed.complete_work_item_pm(
         $1, $2, $3, $4, $5, $6, 'PM', $5, '{}'::jsonb, NULL)`,
      [ALL, SUB, WORKER, partitionKey, Number(eventNumber), snapshotUuid],
    );
  }

  test("returns only 'done' rows for the partition, ordered by event_number", async () => {
    const ens = await appendN(pool, "s1", 5);
    // pm-A: 3 done; pm-B: 1 done (should not appear).
    await routeAndDone("pm-A", ens[0], "PM-A");
    await routeAndDone("pm-A", ens[1], "PM-A");
    await routeAndDone("pm-A", ens[2], "PM-A");
    await routeAndDone("pm-B", ens[3], "PM-B");
    // route a 'pending' for pm-A and a 'failed' for pm-A; both must
    // be excluded from the rebuild result.
    await pool.query(
      `SELECT instructed.route_batch($1, $2, $3, $4, $5::jsonb)`,
      [
        ALL,
        SUB,
        WORKER,
        ens[4].toString(),
        JSON.stringify([
          { partition_key: "pm-A", event_number: Number(ens[4]) },
        ]),
      ],
    );

    const r = await pool.query<{ event_number: string }>(
      `SELECT event_number::text AS event_number
         FROM instructed.list_pm_rebuild_events($1, $2, $3, $4)`,
      [ALL, SUB, "pm-A", Number(ens[4]) + 1],
    );
    assert.deepEqual(
      r.rows.map((x) => x.event_number),
      [ens[0], ens[1], ens[2]].map((n) => n.toString()),
    );
  });

  test("exclusive upper bound: returns rows strictly less than p_event_number", async () => {
    const ens = await appendN(pool, "s1", 3);
    await routeAndDone("pm-A", ens[0], "PM-A");
    await routeAndDone("pm-A", ens[1], "PM-A");
    await routeAndDone("pm-A", ens[2], "PM-A");

    const r = await pool.query<{ event_number: string }>(
      `SELECT event_number::text AS event_number
         FROM instructed.list_pm_rebuild_events($1, $2, $3, $4)`,
      [ALL, SUB, "pm-A", Number(ens[2])],
    );
    // Only ens[0] and ens[1]; ens[2] is the cutoff.
    assert.deepEqual(
      r.rows.map((x) => x.event_number),
      [ens[0], ens[1]].map((n) => n.toString()),
    );
  });

  test("returns the read_all-compatible event payload", async () => {
    const [e1] = await appendN(pool, "s1", 1);
    await routeAndDone("pm-A", e1, "PM-A");

    const r = await pool.query(
      `SELECT event_id, event_number::text AS event_number,
              stream_uuid, stream_version::text AS stream_version,
              event_type, causation_id, correlation_id, data, metadata,
              created_at
         FROM instructed.list_pm_rebuild_events($1, $2, $3, $4)`,
      [ALL, SUB, "pm-A", Number(e1) + 1],
    );
    assert.equal(r.rowCount, 1);
    const row = r.rows[0];
    assert.equal(row.event_number, e1.toString());
    assert.equal(typeof row.event_id, "string");
    assert.ok(row.event_type);
    assert.ok(row.created_at instanceof Date);
  });

  test("empty result when partition has no 'done' rows below the cutoff", async () => {
    const r = await pool.query(
      `SELECT * FROM instructed.list_pm_rebuild_events($1, $2, $3, $4)`,
      [ALL, SUB, "no-such-partition", 9999],
    );
    assert.equal(r.rowCount, 0);
  });

  test("subscription-not-found raises IS020", async () => {
    await rejectsWithCode(
      () =>
        pool.query(
          `SELECT * FROM instructed.list_pm_rebuild_events($1, 'no-such', $2, 0)`,
          [ALL, "pm-A"],
        ),
      "IS020",
    );
  });

  test("rejects invalid parameters with 22023", async () => {
    await rejectsWithCode(
      () =>
        pool.query(
          `SELECT * FROM instructed.list_pm_rebuild_events($1, $2, $3, -1)`,
          [ALL, SUB, "pm-A"],
        ),
      "22023",
    );
    await rejectsWithCode(
      () =>
        pool.query(
          `SELECT * FROM instructed.list_pm_rebuild_events($1, $2, NULL, 0)`,
          [ALL, SUB],
        ),
      "22023",
    );
  });
});
