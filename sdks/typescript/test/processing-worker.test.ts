/**
 * SUB-A slice 5 — processing worker (common claim mechanics).
 *
 * Slice acceptance items:
 *   - per-partition ordering under concurrent claimants
 *   - parallel-across-partitions throughput
 *   - lease-takeover after worker death
 *   - `failed` row blocks its partition only
 *   - default error policy back-off
 *
 * Plus: lease renewal during long handlers; SUB-B `retry-in` and
 * `stop` decision shapes; complete() called once per work item.
 *
 * Tests use trivial stub adapters (handle that records events, a
 * complete that DELETEs the work item) — projection / PM specifics
 * land in slices 6 and 7.
 */

import { after, before, beforeEach, describe, test } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { closePool, getPool, truncateAll } from "./fixtures.ts";
import { Client, expected } from "../src/index.ts";
import {
  DEFAULT_ERROR_POLICY,
  startProcessingWorker,
  type ErrorPolicy,
  type ProcessingWorkerDefinition,
} from "../src/processing-worker.ts";
import type pg from "pg";
import type { RunningWorker } from "../src/internal/running-worker.ts";

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

// -- helpers -----------------------------------------------------------------

async function ensureSubscription(name: string): Promise<void> {
  // The processing worker doesn't claim the subscription itself; we
  // create the row by claiming and releasing as a setup actor so that
  // claim_work_item doesn't see IS020.
  await client.claimSubscription(ALL, name, "setup", 30);
  await client.releaseSubscription(ALL, name, "setup");
}

async function append(streamPrefix: string, n: number): Promise<bigint[]> {
  const stream = `${streamPrefix}-${randomUUID().slice(0, 8)}`;
  const rows = await client.appendToStream(
    stream,
    expected.any,
    Array.from({ length: n }, (_, i) => ({
      event_type: `E${i}`,
      data: { i },
    })),
  );
  return rows.map((r) => r.event_number);
}

/** Route a list of (partitionKey, eventNumber) decisions atomically. */
async function route(
  name: string,
  decisions: Array<{ pk: string; en: bigint }>,
): Promise<void> {
  // We piggy-back on claim_subscription so route_batch's lease check
  // passes. A short claim + immediate release after route_batch.
  await client.claimSubscription(ALL, name, "router", 30);
  try {
    const en = decisions.reduce(
      (m, d) => (d.en > m ? d.en : m),
      decisions[0]?.en ?? 0n,
    );
    await client.routeBatch(
      ALL,
      name,
      "router",
      en,
      decisions.map((d) => ({ partitionKey: d.pk, eventNumber: d.en })),
    );
  } finally {
    await client.releaseSubscription(ALL, name, "router");
  }
}

async function workItemState(
  name: string,
  partitionKey: string,
  eventNumber: bigint,
): Promise<{ state: string; claimed_by: string | null } | null> {
  const r = await pool.query<{ state: string; claimed_by: string | null }>(
    `SELECT state, claimed_by FROM instructed.subscription_work_items
      WHERE subscription_name = $1 AND partition_key = $2 AND event_number = $3`,
    [name, partitionKey, eventNumber.toString()],
  );
  if (r.rowCount === 0) return null;
  return r.rows[0];
}

async function waitFor<T>(
  predicate: () => Promise<T | null | undefined>,
  timeoutMs = 5000,
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const v = await predicate();
    if (v) return v;
    await new Promise((r) => setTimeout(r, 25));
  }
  throw new Error(`waitFor: timed out after ${timeoutMs}ms`);
}

// A stub "completer" that DELETEs the work item. Stands in for the
// real projection / PM completion paths until slices 6 / 7 land.
function deleteCompleter(name: string) {
  return async (_event: any, ctx: { workerId: string; partitionKey: string; eventNumber: bigint }) => {
    await client.completeWorkItemProjection(
      ALL,
      name,
      ctx.workerId,
      ctx.partitionKey,
      ctx.eventNumber,
    );
  };
}

// -- per-partition ordering ---------------------------------------------------

describe("processing worker — per-partition ordering", () => {
  test("serial within a partition under concurrent claimants", async () => {
    const name = `pw-order-${randomUUID().slice(0, 8)}`;
    await ensureSubscription(name);
    const [e1, e2, e3] = await append("o", 3);
    await route(name, [
      { pk: "p1", en: e1 },
      { pk: "p1", en: e2 },
      { pk: "p1", en: e3 },
    ]);

    const seen: bigint[] = [];
    const def: ProcessingWorkerDefinition = {
      name,
      handle: async (event) => {
        seen.push(event.event_number);
        // small delay so a parallel worker would have a chance to
        // race if ordering were broken
        await new Promise((r) => setTimeout(r, 20));
      },
      complete: deleteCompleter(name),
    };

    const w1 = startProcessingWorker(client, def);
    const w2 = startProcessingWorker(client, def);
    try {
      await waitFor(async () => (seen.length >= 3 ? true : null));
      // Must be in event_number order regardless of which worker handled which.
      assert.deepEqual(seen, [e1, e2, e3]);
    } finally {
      await Promise.all([w1.close(), w2.close()]);
    }
  });
});

describe("processing worker — parallel across partitions", () => {
  test("multiple workers drain disjoint partitions concurrently", async () => {
    const name = `pw-par-${randomUUID().slice(0, 8)}`;
    await ensureSubscription(name);
    const ens = await append("par", 6);
    // 3 partitions, 2 events each.
    await route(name, [
      { pk: "p1", en: ens[0] },
      { pk: "p2", en: ens[1] },
      { pk: "p3", en: ens[2] },
      { pk: "p1", en: ens[3] },
      { pk: "p2", en: ens[4] },
      { pk: "p3", en: ens[5] },
    ]);

    const handled: Array<{ pk: string; en: bigint; t: number }> = [];
    const def: ProcessingWorkerDefinition = {
      name,
      handle: async (event, ctx) => {
        handled.push({
          pk: ctx.partitionKey,
          en: event.event_number,
          t: Date.now(),
        });
        await new Promise((r) => setTimeout(r, 50));
      },
      complete: deleteCompleter(name),
    };

    const workers = Array.from({ length: 3 }, () =>
      startProcessingWorker(client, def),
    );
    try {
      await waitFor(async () => (handled.length >= 6 ? true : null));

      // Within each partition: strict event_number order.
      for (const pk of ["p1", "p2", "p3"]) {
        const ens = handled.filter((h) => h.pk === pk).map((h) => h.en);
        assert.deepEqual(
          ens,
          [...ens].sort((a, b) => Number(a - b)),
          `partition ${pk} out of order`,
        );
      }

      // Across partitions: at some moment we expected at least two
      // distinct partitions to have an in-flight handle running
      // concurrently. We can't observe `claimed` directly here, but
      // the timing data approximates it: the first event of two
      // different partitions started within the same 50ms window.
      const firstByPk = new Map<string, number>();
      for (const h of handled) {
        if (!firstByPk.has(h.pk)) firstByPk.set(h.pk, h.t);
      }
      const starts = [...firstByPk.values()].sort((a, b) => a - b);
      assert.ok(
        starts.length >= 3 && starts[2] - starts[0] < 200,
        `expected three partitions to begin near-concurrently; spread=${
          starts[starts.length - 1] - starts[0]
        }ms`,
      );
    } finally {
      await Promise.all(workers.map((w) => w.close()));
    }
  });
});

// -- lease takeover -----------------------------------------------------------

describe("processing worker — lease takeover", () => {
  test("dead worker's claim is taken over after lease expiry", async () => {
    const name = `pw-tako-${randomUUID().slice(0, 8)}`;
    await ensureSubscription(name);
    const [e1] = await append("tk", 1);
    await route(name, [{ pk: "p1", en: e1 }]);

    // Worker A claims with a tiny lease, then "dies" mid-handle by
    // blocking forever (we'll close it without ever releasing).
    let release!: () => void;
    const block = new Promise<void>((r) => {
      release = r;
    });
    const handledByB: bigint[] = [];
    const takeoverEvents: string[] = [];

    const wA: RunningWorker = startProcessingWorker(
      client,
      {
        name,
        handle: async () => {
          await block;
        },
        complete: deleteCompleter(name),
      },
      { leaseSeconds: 1, heartbeatInterval: 99_999_999 /* effectively off */ },
    );

    // Wait until A has claimed the item.
    await waitFor(async () => {
      const s = await workItemState(name, "p1", e1);
      return s?.state === "claimed" ? true : null;
    });

    // Force-expire A's lease so B can take over on its next poll.
    await pool.query(
      `UPDATE instructed.subscription_work_items
          SET lease_expires_at = now() - interval '5 seconds'
        WHERE subscription_name = $1`,
      [name],
    );

    const wB: RunningWorker = startProcessingWorker(
      client,
      {
        name,
        handle: async (event) => {
          handledByB.push(event.event_number);
        },
        complete: deleteCompleter(name),
      },
      {
        onError: (e) => {
          if (/took over/.test(e.message)) takeoverEvents.push(e.message);
        },
      },
    );

    try {
      await waitFor(async () =>
        handledByB.length > 0 && handledByB[0] === e1 ? true : null,
      );
      assert.ok(takeoverEvents.length >= 1, "expected a takeover surface");
    } finally {
      release();
      await wA.close();
      await wB.close();
    }
  });

  test("lease renewal keeps a long-running handler alive past one lease window", async () => {
    const name = `pw-renew-${randomUUID().slice(0, 8)}`;
    await ensureSubscription(name);
    const [e1] = await append("rn", 1);
    await route(name, [{ pk: "p1", en: e1 }]);

    let completedCount = 0;
    const w = startProcessingWorker(
      client,
      {
        name,
        handle: async () => {
          // Sleep longer than one lease window; heartbeat must
          // renew or another worker would steal this item.
          await new Promise((r) => setTimeout(r, 1500));
        },
        complete: async (_e, ctx) => {
          await client.completeWorkItemProjection(
            ALL,
            name,
            ctx.workerId,
            ctx.partitionKey,
            ctx.eventNumber,
          );
          completedCount += 1;
        },
      },
      { leaseSeconds: 1, heartbeatInterval: 300 },
    );

    try {
      await waitFor(async () => (completedCount > 0 ? true : null), 4000);
      assert.equal(completedCount, 1);
    } finally {
      await w.close();
    }
  });
});

// -- failed row blocks its partition only -------------------------------------

describe("processing worker — `failed` blocks the partition only", () => {
  test("a failed predecessor blocks p1; p2 still drains", async () => {
    const name = `pw-fail-${randomUUID().slice(0, 8)}`;
    await ensureSubscription(name);
    const [e1, e2, e3] = await append("fb", 3);
    await route(name, [
      { pk: "p1", en: e1 },
      { pk: "p1", en: e2 },
      { pk: "p2", en: e3 },
    ]);

    // Pre-fail the first p1 item directly via SQL so the worker sees
    // a partition that has no claimable head row.
    await client.claimSubscription(ALL, name, "tmp", 30);
    await client.claimWorkItem(ALL, name, "tmp", 30);
    await client.failWorkItem(ALL, name, "tmp", "p1", e1, "boom");
    await client.releaseSubscription(ALL, name, "tmp");

    const seen: Array<{ pk: string; en: bigint }> = [];
    const w = startProcessingWorker(client, {
      name,
      handle: async (event, ctx) => {
        seen.push({ pk: ctx.partitionKey, en: event.event_number });
      },
      complete: deleteCompleter(name),
    });

    try {
      await waitFor(async () =>
        seen.some((s) => s.en === e3) ? true : null,
      );
      // We must NOT have processed e2 (p1 is blocked) or e1 (already failed).
      assert.ok(
        !seen.some((s) => s.en === e1 || s.en === e2),
        `expected p1 to stay blocked; saw ${seen.map((s) => `${s.pk}/${s.en}`).join(",")}`,
      );
      // e3 (p2) must have been processed.
      assert.ok(seen.some((s) => s.en === e3));
    } finally {
      await w.close();
    }
  });
});

// -- default error policy back-off --------------------------------------------

describe("processing worker — default error policy back-off", () => {
  test("handler keeps retrying with exponential backoff (default policy)", async () => {
    const name = `pw-backoff-${randomUUID().slice(0, 8)}`;
    await ensureSubscription(name);
    const [e1] = await append("bo", 1);
    await route(name, [{ pk: "p1", en: e1 }]);

    let attempts = 0;
    const w = startProcessingWorker(client, {
      name,
      handle: async (_event, ctx) => {
        attempts = ctx.attempt;
        if (ctx.attempt < 3) throw new Error("transient");
        // 3rd attempt succeeds.
      },
      complete: deleteCompleter(name),
    });

    try {
      await waitFor(async () => {
        const s = await workItemState(name, "p1", e1);
        return s === null ? true : null; // deleted by completer
      });
      assert.equal(attempts, 3, `expected 3 attempts; got ${attempts}`);
    } finally {
      await w.close();
    }
  });

  test("default policy returns exponential, capped retry-in", async () => {
    const ctx = { workerId: "w", partitionKey: "p", eventNumber: 1n };
    const d1 = await DEFAULT_ERROR_POLICY(new Error("e"), {
      ...ctx,
      attempt: 1,
    });
    const d2 = await DEFAULT_ERROR_POLICY(new Error("e"), {
      ...ctx,
      attempt: 2,
    });
    const d10 = await DEFAULT_ERROR_POLICY(new Error("e"), {
      ...ctx,
      attempt: 10,
    });
    assert.equal(d1.kind, "retry-in");
    assert.equal(d2.kind, "retry-in");
    assert.equal(d10.kind, "retry-in");
    // attempt=1 -> 100; attempt=2 -> 200; growing; capped at 30000.
    if (d1.kind === "retry-in" && d2.kind === "retry-in" && d10.kind === "retry-in") {
      assert.equal(d1.delayMs, 100);
      assert.equal(d2.delayMs, 200);
      assert.ok(d10.delayMs <= 30_000);
    }
  });

  test("'stop' decision exits the worker without moving the item to failed", async () => {
    const name = `pw-stop-${randomUUID().slice(0, 8)}`;
    await ensureSubscription(name);
    const [e1] = await append("st", 1);
    await route(name, [{ pk: "p1", en: e1 }]);

    const stopPolicy: ErrorPolicy = () => ({ kind: "stop" });
    const w = startProcessingWorker(client, {
      name,
      handle: async () => {
        throw new Error("nope");
      },
      complete: deleteCompleter(name),
      errorPolicy: stopPolicy,
    });

    await w.stopped;
    const s = await workItemState(name, "p1", e1);
    assert.ok(s, "work item should still exist");
    // The slice-5 contract: `stop` leaves the row claimed; lease will
    // expire and another worker may pick it up. State must NOT be
    // 'failed' (that's reserved for the convenience-wrapper future).
    assert.notEqual(
      s.state,
      "failed",
      `'stop' must not transition to 'failed'; got state=${s.state}`,
    );
  });
});
