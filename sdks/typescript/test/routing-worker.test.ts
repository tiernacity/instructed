/**
 * SUB-A slice 4 — routing worker tests.
 *
 * Slice acceptance items:
 *   - batch atomicity (cursor and inserts commit together)
 *   - routing-decision determinism (same input -> same rows)
 *   - "ignore" decisions produce no rows
 *   - crash mid-batch leaves cursor un-advanced
 *   - re-run after crash re-routes the same events (idempotent via PK)
 *   - race safety: last_seen >= N implies work-item rows for events <= N
 *     are visible
 *
 * Plus lifecycle: lease-loss aborts the worker; close() releases the lease.
 */

import { after, before, beforeEach, describe, test } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { closePool, getPool, truncateAll } from "./fixtures.ts";
import { Client, expected, SubscriptionLeaseLost } from "../src/index.ts";
import {
  startRoutingWorker,
  type RoutingDecision,
} from "../src/routing-worker.ts";
import type pg from "pg";
import type { RunningWorker } from "../src/subscription.ts";

const ALL = "$all";

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
});

async function appendN(
  streamPrefix: string,
  n: number,
): Promise<{ stream: string; ens: bigint[] }> {
  const stream = `${streamPrefix}-${randomUUID()}`;
  const rows = await client.appendToStream(
    stream,
    expected.any,
    Array.from({ length: n }, (_, i) => ({
      event_type: `E${i}`,
      data: { i },
    })),
  );
  return { stream, ens: rows.map((r) => r.event_number) };
}

/** Poll until `predicate()` returns truthy or the deadline elapses. */
async function waitFor<T>(
  predicate: () => Promise<T | null | undefined>,
  timeoutMs = 3000,
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const v = await predicate();
    if (v) return v;
    await new Promise((r) => setTimeout(r, 25));
  }
  throw new Error(`waitFor: timed out after ${timeoutMs}ms`);
}

async function workItems(
  subscriptionName: string,
): Promise<
  Array<{ partition_key: string; event_number: string; state: string }>
> {
  const r = await pool.query<{
    partition_key: string;
    event_number: string;
    state: string;
  }>(
    `SELECT partition_key, event_number, state
       FROM instructed.subscription_work_items
      WHERE subscription_name = $1
      ORDER BY event_number`,
    [subscriptionName],
  );
  return r.rows;
}

async function lastSeen(subscriptionName: string): Promise<bigint | null> {
  const r = await pool.query<{ last_seen: string }>(
    `SELECT last_seen FROM instructed.subscriptions
      WHERE subscription_name = $1`,
    [subscriptionName],
  );
  if (r.rowCount === 0) return null;
  return BigInt(r.rows[0].last_seen);
}

async function waitForSubscription(name: string): Promise<void> {
  await waitFor(async () => {
    const r = await pool.query(
      `SELECT 1 FROM instructed.subscriptions WHERE subscription_name = $1`,
      [name],
    );
    return (r.rowCount ?? 0) > 0 ? true : null;
  });
}

describe("routing worker — happy path", () => {
  test("routes appended events; advances cursor; inserts work items", async () => {
    const name = `routing-happy-${randomUUID().slice(0, 8)}`;
    await appendN("h", 2);
    const w = startRoutingWorker(client, {
      name,
      routeFn: (e) => ({ partitionKey: `p-${e.event_type}` }),
    });
    try {
      await waitFor(async () => {
        const items = await workItems(name);
        return items.length >= 2 ? items : null;
      });
      const items = await workItems(name);
      assert.equal(items.length, 2);
      assert.equal(items[0].partition_key, "p-E0");
      assert.equal(items[1].partition_key, "p-E1");
      assert.equal(items[0].state, "pending");
      // Cursor at or past the last routed event_number.
      const ls = await lastSeen(name);
      assert.ok(
        ls !== null && ls >= BigInt(items[items.length - 1].event_number),
      );
    } finally {
      await w.close();
    }
  });

  test('"ignore" decisions produce no rows but cursor still advances', async () => {
    const name = `routing-ignore-${randomUUID().slice(0, 8)}`;
    const { ens } = await appendN("i", 3);
    const w = startRoutingWorker<unknown>(client, {
      name,
      routeFn: (e): RoutingDecision =>
        e.event_type === "E1" ? { partitionKey: "p" } : "ignore",
    });
    try {
      await waitFor(async () => {
        const ls = await lastSeen(name);
        return ls !== null && ls >= ens[2] ? ls : null;
      });
      const items = await workItems(name);
      assert.equal(items.length, 1);
      assert.equal(items[0].partition_key, "p");
      assert.equal(items[0].event_number, ens[1].toString());
      const ls = await lastSeen(name);
      assert.equal(ls, ens[2]);
    } finally {
      await w.close();
    }
  });

  test("picks up events appended after the worker is running", async () => {
    const name = `routing-stream-${randomUUID().slice(0, 8)}`;
    const w = startRoutingWorker(client, {
      name,
      routeFn: () => ({ partitionKey: "p" }),
    });
    try {
      // Wait for the worker to claim the subscription.
      await waitForSubscription(name);
      const { ens } = await appendN("late", 2);
      await waitFor(async () => {
        const items = await workItems(name);
        return items.length >= 2 ? true : null;
      });
      const items = await workItems(name);
      assert.equal(items.length, 2);
      assert.deepEqual(
        items.map((i) => i.event_number),
        ens.map((e) => e.toString()),
      );
    } finally {
      await w.close();
    }
  });
});

describe("routing worker — determinism / idempotency", () => {
  test("restart after partial run re-routes nothing new (PK absorbs)", async () => {
    const name = `routing-restart-${randomUUID().slice(0, 8)}`;
    await appendN("r", 3);
    const w1 = startRoutingWorker(client, {
      name,
      routeFn: (e) => ({ partitionKey: `p-${e.event_type}` }),
    });
    await waitFor(async () => {
      const items = await workItems(name);
      return items.length >= 3 ? true : null;
    });
    const itemsBefore = await workItems(name);
    const lsBefore = await lastSeen(name);
    await w1.close();

    // Second worker, same definition, same data — should be a no-op.
    const w2 = startRoutingWorker(client, {
      name,
      routeFn: (e) => ({ partitionKey: `p-${e.event_type}` }),
    });
    try {
      // Give it time to claim, poll once, and idle.
      await new Promise((r) => setTimeout(r, 400));
      const itemsAfter = await workItems(name);
      const lsAfter = await lastSeen(name);
      assert.deepEqual(itemsAfter, itemsBefore);
      assert.equal(lsAfter, lsBefore);
    } finally {
      await w2.close();
    }
  });
});

describe("routing worker — crash safety", () => {
  test("close mid-batch leaves cursor un-advanced for un-routed events", async () => {
    // Append more events than the batch will fit; close immediately
    // after the first routeBatch round-trip. We don't have a hook for
    // 'after first batch'; instead we kill before the first routeBatch
    // commits by closing very quickly. To make the test deterministic
    // we use a very small batchSize and a slow routeFn to force the
    // worker to be inside routeFn when close() fires.
    const name = `routing-crash-${randomUUID().slice(0, 8)}`;
    const { ens } = await appendN("c", 10);
    let routeFnCalls = 0;
    const blockEvent = 5; // the 6th event (0-indexed)
    let release!: () => void;
    const block = new Promise<void>((r) => {
      release = r;
    });
    const w = startRoutingWorker<unknown>(
      client,
      {
        name,
        routeFn: async (e): Promise<RoutingDecision> => {
          routeFnCalls += 1;
          if (e.event_type === `E${blockEvent}`) {
            await block;
          }
          return { partitionKey: "p" };
        },
      },
      { batchSize: 100 }, // single big batch
    );
    try {
      // Wait until we've reached the blocking event mid-batch.
      await waitFor(async () => (routeFnCalls > blockEvent ? true : null));
      // Now close while we're stuck inside routeFn for event #5.
      // close() aborts the signal; the in-flight batch is dropped
      // without route_batch being called.
      const closing = w.close();
      release();
      await closing;

      // Cursor is still 0 because route_batch was never called for
      // this batch (atomic: cursor + INSERTs commit together).
      const ls = await lastSeen(name);
      assert.equal(ls, 0n, `expected cursor un-advanced; got ${ls}`);
      const items = await workItems(name);
      assert.equal(items.length, 0);
    } finally {
      // Make sure block is released even on assertion failure.
      try {
        release();
      } catch {
        /* ignore */
      }
      void ens;
    }
  });

  test("re-run after crash routes the events on the next worker", async () => {
    // Build on the previous scenario: leave the store with N events
    // and no work items, then start a fresh worker and observe it
    // routes them.
    const name = `routing-resume-${randomUUID().slice(0, 8)}`;
    const { ens } = await appendN("re", 3);

    // First worker: close immediately so it never gets a chance.
    const w1 = startRoutingWorker(client, {
      name,
      routeFn: () => ({ partitionKey: "p" }),
    });
    await w1.close();

    // Fresh worker resumes from cursor=0 and routes all 3.
    const w2 = startRoutingWorker(client, {
      name,
      routeFn: () => ({ partitionKey: "p" }),
    });
    try {
      await waitFor(async () => {
        const items = await workItems(name);
        return items.length >= 3 ? true : null;
      });
      const items = await workItems(name);
      assert.equal(items.length, 3);
      assert.deepEqual(
        items.map((i) => i.event_number),
        ens.map((e) => e.toString()),
      );
    } finally {
      await w2.close();
    }
  });
});

describe("routing worker — race safety", () => {
  test("last_seen >= N never observed without the corresponding work items", async () => {
    // We can't introduce arbitrary scheduling, but we can hammer the
    // store with concurrent appends + polling reads of (last_seen,
    // work_items) and check the invariant on every snapshot. If
    // route_batch were non-atomic this would eventually fire.
    const name = `routing-race-${randomUUID().slice(0, 8)}`;
    const w = startRoutingWorker(client, {
      name,
      routeFn: (e) => ({ partitionKey: `p-${Number(e.event_number) % 3}` }),
    });

    let violationsAt: bigint[] = [];
    let polling = true;
    const poller = (async () => {
      while (polling) {
        // Read cursor and routed event_numbers; snapshot must satisfy
        // "every event_number <= last_seen that routed has a row".
        const r = await pool.query<{ last_seen: string }>(
          `SELECT last_seen FROM instructed.subscriptions
            WHERE subscription_name = $1`,
          [name],
        );
        if (r.rowCount === 0) {
          await new Promise((res) => setTimeout(res, 5));
          continue;
        }
        const ls = BigInt(r.rows[0].last_seen);
        if (ls === 0n) {
          await new Promise((res) => setTimeout(res, 5));
          continue;
        }
        // For each event in $all with event_number <= ls, if the
        // routeFn would have routed it (which it always does here),
        // there MUST be a work_items row.
        const all = await pool.query<{ en: string }>(
          `SELECT stream_version AS en FROM instructed.stream_events
             WHERE stream_id = 0 AND stream_version <= $1`,
          [ls.toString()],
        );
        const wi = await pool.query<{ en: string }>(
          `SELECT event_number AS en FROM instructed.subscription_work_items
             WHERE subscription_name = $1`,
          [name],
        );
        const routed = new Set(wi.rows.map((r) => r.en));
        for (const row of all.rows) {
          if (!routed.has(row.en)) {
            violationsAt.push(BigInt(row.en));
            break;
          }
        }
        await new Promise((res) => setTimeout(res, 2));
      }
    })();

    try {
      // Concurrent appenders for ~500ms.
      const writers = Array.from({ length: 3 }, async (_, i) => {
        const s = `race-${i}-${randomUUID().slice(0, 8)}`;
        for (let k = 0; k < 30; k++) {
          await client.appendToStream(s, expected.any, [
            { event_type: "X", data: { i, k } },
          ]);
        }
      });
      await Promise.all(writers);
      // Let the worker drain.
      await waitFor(async () => {
        const r = await pool.query<{ last_seen: string }>(
          `SELECT last_seen FROM instructed.subscriptions
            WHERE subscription_name = $1`,
          [name],
        );
        if (r.rowCount === 0) return null;
        const ls = BigInt(r.rows[0].last_seen);
        return ls >= 90n ? true : null;
      }, 5000);
    } finally {
      polling = false;
      await poller;
      await w.close();
    }

    assert.deepEqual(
      violationsAt,
      [],
      `race violations at event_numbers: ${violationsAt.join(",")}`,
    );
  });
});

describe("routing worker — lifecycle", () => {
  test("external lease takeover aborts the worker via onError", async () => {
    const name = `routing-lease-${randomUUID().slice(0, 8)}`;
    const errors: Error[] = [];
    const w = startRoutingWorker(
      client,
      {
        name,
        routeFn: () => "ignore",
      },
      {
        leaseSeconds: 1,
        heartbeatInterval: 200,
        onError: (e) => errors.push(e),
      },
    );
    // Give the worker time to claim.
    await waitFor(async () => {
      const r = await pool.query(
        `SELECT 1 FROM instructed.subscriptions WHERE subscription_name = $1`,
        [name],
      );
      return r.rowCount! > 0 ? true : null;
    });
    // Steal the lease: force-expire it, then claim as another worker.
    await pool.query(
      `UPDATE instructed.subscriptions
          SET claim_expires_at = now() - interval '5 seconds'
        WHERE subscription_name = $1`,
      [name],
    );
    await client.claimSubscription(ALL, name, "thief", 30);
    await w.stopped;
    assert.ok(
      errors.some((e) => e instanceof SubscriptionLeaseLost),
      `expected SubscriptionLeaseLost, got ${errors.map((e) => e.constructor.name).join(",")}`,
    );
  });

  test("close() releases the lease so a fresh worker can claim", async () => {
    const name = `routing-release-${randomUUID().slice(0, 8)}`;
    const w1 = startRoutingWorker(client, {
      name,
      routeFn: () => "ignore",
    });
    await waitFor(async () => {
      const r = await pool.query<{ claimed_by: string | null }>(
        `SELECT claimed_by FROM instructed.subscriptions
          WHERE subscription_name = $1`,
        [name],
      );
      return r.rowCount! > 0 && r.rows[0].claimed_by ? true : null;
    });
    await w1.close();
    const r = await pool.query<{ claimed_by: string | null }>(
      `SELECT claimed_by FROM instructed.subscriptions
        WHERE subscription_name = $1`,
      [name],
    );
    assert.equal(r.rows[0].claimed_by, null);
  });

  test("routeFn throw stops the worker without advancing cursor", async () => {
    const name = `routing-throw-${randomUUID().slice(0, 8)}`;
    await appendN("t", 2);
    const errors: Error[] = [];
    const w: RunningWorker = startRoutingWorker(
      client,
      {
        name,
        routeFn: () => {
          throw new Error("routeFn deliberately broken");
        },
      },
      { onError: (e) => errors.push(e) },
    );
    await w.stopped;
    assert.ok(
      errors.some((e) => /deliberately broken/.test(e.message)),
      `expected the user-thrown error to surface; got: ${errors.map((e) => e.message).join("; ")}`,
    );
    const ls = await lastSeen(name);
    // Either 0n (worker stopped before claiming) or a value if it
    // claimed first; in either case it must be < any event_number
    // that would imply a successful route_batch (we appended 2, so
    // anything other than 0n indicates a routed advance, which must
    // not happen on a routeFn throw).
    assert.equal(ls ?? 0n, 0n);
  });
});
