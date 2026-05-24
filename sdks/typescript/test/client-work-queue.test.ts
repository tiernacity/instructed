/**
 * Layer 0 round-trip tests for the SUB-A work-queue procedures.
 *
 * Mirrors `client.test.ts` style: exercises each Client wrapper plus
 * its closed-error-set translation against the live Postgres provided
 * by `docker compose up -d postgres`.
 */

import { after, before, beforeEach, describe, test } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { closePool, getPool, truncateAll } from "./fixtures.ts";
import {
  Client,
  expected,
  InvalidParameterValue,
  SubscriptionLeaseLost,
  SubscriptionNotFound,
  WorkItemLeaseLost,
} from "../src/index.ts";
import type pg from "pg";

const ALL = "$all";
const SUB = "client-sub";
const WORKER = "client-worker";

let pool: pg.Pool;
let client: Client;

before(async () => {
  pool = await getPool();
  client = new Client(pool);
});

after(async () => {
  await closePool();
});

beforeEach(async () => {
  await truncateAll(pool);
  await client.claimSubscription(ALL, SUB, WORKER, 60);
});

async function appendN(streamPrefix: string, n: number): Promise<bigint[]> {
  const rows = await client.appendToStream(
    `${streamPrefix}-${randomUUID()}`,
    expected.any,
    Array.from({ length: n }, (_, i) => ({
      event_type: `E${i}`,
      data: { i },
    })),
  );
  return rows.map((r) => r.event_number);
}

describe("Client.routeBatch", () => {
  test("happy path: returns insertedCount and newLastSeen as bigints", async () => {
    const ens = await appendN("rb", 3);
    const r = await client.routeBatch(ALL, SUB, WORKER, ens[2], [
      { partitionKey: "p1", eventNumber: ens[0] },
      { partitionKey: "p2", eventNumber: ens[1] },
      { partitionKey: "p1", eventNumber: ens[2] },
    ]);
    assert.equal(r.insertedCount, 3n);
    assert.equal(r.newLastSeen, ens[2]);
  });

  test("empty decisions array still advances cursor", async () => {
    const ens = await appendN("rb", 2);
    const r = await client.routeBatch(ALL, SUB, WORKER, ens[1], []);
    assert.equal(r.insertedCount, 0n);
    assert.equal(r.newLastSeen, ens[1]);
  });

  test("crash-replay safe: re-sending the same decisions inserts zero", async () => {
    const [e1] = await appendN("rb", 1);
    const decisions = [{ partitionKey: "p", eventNumber: e1 }];
    await client.routeBatch(ALL, SUB, WORKER, e1, decisions);
    const r2 = await client.routeBatch(ALL, SUB, WORKER, e1, decisions);
    assert.equal(r2.insertedCount, 0n);
    assert.equal(r2.newLastSeen, e1);
  });

  test("non-holder raises SubscriptionLeaseLost (IS022)", async () => {
    const [e1] = await appendN("rb", 1);
    await assert.rejects(
      () =>
        client.routeBatch(ALL, SUB, "intruder", e1, [
          { partitionKey: "p", eventNumber: e1 },
        ]),
      (err) => err instanceof SubscriptionLeaseLost && err.code === "IS022",
    );
  });

  test("missing subscription raises SubscriptionNotFound (IS020)", async () => {
    await assert.rejects(
      () => client.routeBatch(ALL, "no-such", WORKER, 0n, []),
      (err) => err instanceof SubscriptionNotFound && err.code === "IS020",
    );
  });
});

describe("Client.claimWorkItem", () => {
  async function route(
    decisions: Array<{ pk: string }>,
  ): Promise<bigint[]> {
    const ens = await appendN("cl", decisions.length);
    await client.routeBatch(
      ALL,
      SUB,
      WORKER,
      ens[ens.length - 1],
      decisions.map((d, i) => ({ partitionKey: d.pk, eventNumber: ens[i] })),
    );
    return ens;
  }

  test("returns null on empty queue", async () => {
    const r = await client.claimWorkItem(ALL, SUB, WORKER, 30);
    assert.equal(r, null);
  });

  test("returns typed row with bigint eventNumber and Date lease expiry", async () => {
    const [e1] = await route([{ pk: "p1" }]);
    const r = await client.claimWorkItem(ALL, SUB, WORKER, 30);
    assert.ok(r);
    assert.equal(r.partitionKey, "p1");
    assert.equal(r.eventNumber, e1);
    assert.equal(r.claimedBy, WORKER);
    assert.ok(r.leaseExpiresAt instanceof Date);
    assert.equal(r.wasTakeover, false);
    assert.equal(r.priorClaimedBy, null);
  });

  test("surfaces takeover metadata when displacing an expired claim", async () => {
    const [e1] = await route([{ pk: "p1" }]);
    await client.claimWorkItem(ALL, SUB, "wDead", 1);
    await pool.query(
      `UPDATE instructed.subscription_work_items
          SET lease_expires_at = now() - interval '5 seconds'
        WHERE event_number = $1`,
      [e1.toString()],
    );
    const r = await client.claimWorkItem(ALL, SUB, "wAlive", 30);
    assert.ok(r);
    assert.equal(r.wasTakeover, true);
    assert.equal(r.priorClaimedBy, "wDead");
    assert.equal(r.claimedBy, "wAlive");
  });

  test("missing subscription raises SubscriptionNotFound (IS020)", async () => {
    await assert.rejects(
      () => client.claimWorkItem(ALL, "no-such", WORKER, 30),
      (err) => err instanceof SubscriptionNotFound && err.code === "IS020",
    );
  });
});

describe("Client.completeWorkItemProjection", () => {
  async function routeAndClaim(): Promise<bigint> {
    const [e1] = await appendN("cp", 1);
    await client.routeBatch(ALL, SUB, WORKER, e1, [
      { partitionKey: "p1", eventNumber: e1 },
    ]);
    await client.claimWorkItem(ALL, SUB, WORKER, 30);
    return e1;
  }

  test("DELETEs the work item", async () => {
    const e1 = await routeAndClaim();
    await client.completeWorkItemProjection(ALL, SUB, WORKER, "p1", e1);
    const r = await pool.query(
      `SELECT 1 FROM instructed.subscription_work_items WHERE event_number = $1`,
      [e1.toString()],
    );
    assert.equal(r.rowCount, 0);
  });

  test("missing row raises WorkItemLeaseLost (IS030) with context", async () => {
    const e1 = await routeAndClaim();
    await client.completeWorkItemProjection(ALL, SUB, WORKER, "p1", e1);
    await assert.rejects(
      () => client.completeWorkItemProjection(ALL, SUB, WORKER, "p1", e1),
      (err) => {
        if (!(err instanceof WorkItemLeaseLost)) return false;
        return (
          err.code === "IS030" &&
          err.partitionKey === "p1" &&
          err.eventNumber === e1
        );
      },
    );
  });

  test("non-claimant raises WorkItemLeaseLost (IS030)", async () => {
    const e1 = await routeAndClaim();
    await assert.rejects(
      () =>
        client.completeWorkItemProjection(ALL, SUB, "intruder", "p1", e1),
      (err) => err instanceof WorkItemLeaseLost && err.code === "IS030",
    );
  });
});

describe("Client.completeWorkItemPm", () => {
  test("UPDATEs row to 'done' and UPSERTs the snapshot", async () => {
    const [e1] = await appendN("pm", 1);
    await client.routeBatch(ALL, SUB, WORKER, e1, [
      { partitionKey: "pm-1", eventNumber: e1 },
    ]);
    await client.claimWorkItem(ALL, SUB, WORKER, 30);
    await client.completeWorkItemPm(ALL, SUB, WORKER, "pm-1", e1, {
      sourceUuid: "MyPM-instance-1",
      sourceType: "MyPM",
      sourceVersion: e1,
      data: { counter: 1 },
      metadata: { snapshot_module_version: "v1" },
    });
    const row = await pool.query<{ state: string }>(
      `SELECT state FROM instructed.subscription_work_items
        WHERE event_number = $1`,
      [e1.toString()],
    );
    assert.equal(row.rows[0].state, "done");
    const snap = await client.readSnapshot<{ counter: number }>(
      "MyPM-instance-1",
    );
    assert.equal(snap.sourceVersion, e1);
    assert.equal(snap.data.counter, 1);
  });

  test("non-claimant raises WorkItemLeaseLost (IS030)", async () => {
    const [e1] = await appendN("pm", 1);
    await client.routeBatch(ALL, SUB, WORKER, e1, [
      { partitionKey: "pm-1", eventNumber: e1 },
    ]);
    await client.claimWorkItem(ALL, SUB, WORKER, 30);
    await assert.rejects(
      () =>
        client.completeWorkItemPm(ALL, SUB, "intruder", "pm-1", e1, {
          sourceUuid: "MyPM-x",
          sourceType: "MyPM",
          sourceVersion: e1,
          data: {},
        }),
      (err) => err instanceof WorkItemLeaseLost && err.code === "IS030",
    );
  });
});

describe("Client.completePmInstance", () => {
  test("returns counts and is idempotent", async () => {
    const [e1] = await appendN("pmi", 1);
    await client.routeBatch(ALL, SUB, WORKER, e1, [
      { partitionKey: "pm-A", eventNumber: e1 },
    ]);
    await client.claimWorkItem(ALL, SUB, WORKER, 30);
    await client.completeWorkItemPm(ALL, SUB, WORKER, "pm-A", e1, {
      sourceUuid: "PM-A",
      sourceType: "PM",
      sourceVersion: e1,
      data: {},
    });
    const r1 = await client.completePmInstance(ALL, SUB, "pm-A", "PM-A");
    assert.equal(r1.workItemsDeleted, 1n);
    assert.equal(r1.snapshotDeleted, true);
    const r2 = await client.completePmInstance(ALL, SUB, "pm-A", "PM-A");
    assert.equal(r2.workItemsDeleted, 0n);
    assert.equal(r2.snapshotDeleted, false);
  });
});

describe("Client.failWorkItem", () => {
  test("transitions claimed -> failed and records error_text", async () => {
    const [e1] = await appendN("fw", 1);
    await client.routeBatch(ALL, SUB, WORKER, e1, [
      { partitionKey: "p1", eventNumber: e1 },
    ]);
    await client.claimWorkItem(ALL, SUB, WORKER, 30);
    await client.failWorkItem(ALL, SUB, WORKER, "p1", e1, "boom");
    const r = await pool.query<{
      state: string;
      error_text: string;
      claimed_by: string | null;
    }>(
      `SELECT state, error_text, claimed_by
         FROM instructed.subscription_work_items WHERE event_number = $1`,
      [e1.toString()],
    );
    assert.equal(r.rows[0].state, "failed");
    assert.equal(r.rows[0].error_text, "boom");
    assert.equal(r.rows[0].claimed_by, null);
  });

  test("null errorText is accepted", async () => {
    const [e1] = await appendN("fw", 1);
    await client.routeBatch(ALL, SUB, WORKER, e1, [
      { partitionKey: "p1", eventNumber: e1 },
    ]);
    await client.claimWorkItem(ALL, SUB, WORKER, 30);
    await client.failWorkItem(ALL, SUB, WORKER, "p1", e1, null);
    const r = await pool.query<{ error_text: string | null }>(
      `SELECT error_text FROM instructed.subscription_work_items
        WHERE event_number = $1`,
      [e1.toString()],
    );
    assert.equal(r.rows[0].error_text, null);
  });

  test("non-claimant raises WorkItemLeaseLost (IS030)", async () => {
    const [e1] = await appendN("fw", 1);
    await client.routeBatch(ALL, SUB, WORKER, e1, [
      { partitionKey: "p1", eventNumber: e1 },
    ]);
    await client.claimWorkItem(ALL, SUB, WORKER, 30);
    await assert.rejects(
      () => client.failWorkItem(ALL, SUB, "intruder", "p1", e1, null),
      (err) => err instanceof WorkItemLeaseLost && err.code === "IS030",
    );
  });
});

describe("Client.isSubscriptionCaughtUp", () => {
  test("true on a fresh subscription with target 0", async () => {
    assert.equal(await client.isSubscriptionCaughtUp(ALL, SUB, 0n), true);
  });

  test("false while a routed item is still pending; true after completion", async () => {
    const [e1] = await appendN("cu", 1);
    await client.routeBatch(ALL, SUB, WORKER, e1, [
      { partitionKey: "p", eventNumber: e1 },
    ]);
    assert.equal(await client.isSubscriptionCaughtUp(ALL, SUB, e1), false);
    await client.claimWorkItem(ALL, SUB, WORKER, 30);
    await client.completeWorkItemProjection(ALL, SUB, WORKER, "p", e1);
    assert.equal(await client.isSubscriptionCaughtUp(ALL, SUB, e1), true);
  });

  test("missing subscription raises SubscriptionNotFound (IS020)", async () => {
    await assert.rejects(
      () => client.isSubscriptionCaughtUp(ALL, "no-such", 0n),
      (err) => err instanceof SubscriptionNotFound && err.code === "IS020",
    );
  });
});

describe("Client work-queue — InvalidParameterValue (22023) sampling", () => {
  // One 22023 per procedure is enough to exercise the translation path
  // for this slice; the SQL contract already covers each input.
  test("routeBatch: negative cursor", async () => {
    await assert.rejects(
      () => client.routeBatch(ALL, SUB, WORKER, -1n, []),
      (err) => err instanceof InvalidParameterValue && err.code === "22023",
    );
  });

  test("claimWorkItem: non-positive lease", async () => {
    await assert.rejects(
      () => client.claimWorkItem(ALL, SUB, WORKER, 0),
      (err) => err instanceof InvalidParameterValue && err.code === "22023",
    );
  });

  test("failWorkItem: negative event_number", async () => {
    await assert.rejects(
      () => client.failWorkItem(ALL, SUB, WORKER, "p", -1n, null),
      (err) => err instanceof InvalidParameterValue && err.code === "22023",
    );
  });

  test("isSubscriptionCaughtUp: negative target", async () => {
    await assert.rejects(
      () => client.isSubscriptionCaughtUp(ALL, SUB, -1n),
      (err) => err instanceof InvalidParameterValue && err.code === "22023",
    );
  });
});
