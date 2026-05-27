/**
 * Layer 4: consistency-on-dispatch wait tests.
 *
 * The required case for step 5 (sdk-design.md §10): a subscription
 * that never advances past the target produces ConsistencyTimeout
 * with the missing-subscriptions list. Surrounding cases: the happy
 * path (a running projection drains the wait), per-stream targets,
 * and the empty-input no-op.
 */

import { after, before, beforeEach, describe, test } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { closePool, getPool, truncateAll } from "./fixtures.ts";
import {
  Client,
  ConsistencyTargetError,
  ConsistencyTimeout,
  expected,
  routingFnForPartitionBy,
  startProjectionWorker,
  startRoutingWorker,
  waitForProjection,
} from "../src/index.ts";
import type {
  RunningWorker,
  SubscriptionRef,
} from "../src/index.ts";

/**
 * Wire a routing+processing pair for `name` against `stream`.
 * Returns a composite RunningWorker so the test bodies stay short.
 */
function startProjPair(
  client: Client,
  name: string,
  stream: string,
  handler: () => Promise<void>,
  startFrom?: "origin" | "current",
): RunningWorker {
  const router = startRoutingWorker(
    client,
    {
      name,
      stream,
      routeFn: routingFnForPartitionBy({ kind: "sequential" }),
      ...(startFrom !== undefined ? { startFrom } : {}),
    },
    { pollInterval: 25 },
  );
  const proc = startProjectionWorker(
    client,
    { name, stream, handler },
    { pollInterval: 25, heartbeatInterval: 1_000 },
  );
  return {
    stopped: Promise.all([router.stopped, proc.stopped]).then(() => {}),
    close: async () => {
      await Promise.all([router.close(), proc.close()]);
    },
  };
}
import type pg from "pg";

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

// ---------------------------------------------------------------------------

describe("waitForProjection -- happy path", () => {
  test("returns once a running $all projection catches up", async () => {
    const stream = randomUUID();
    const name = `p-${randomUUID().slice(0, 8)}`;

    let handled = 0;
    const worker = startProjPair(
      client,
      name,
      "$all",
      async () => {
        handled++;
      },
    );
    try {
      const appended = await client.appendToStream(stream, expected.noStream, [
        { type: "A", data: {} },
        { type: "B", data: {} },
      ]);
      const subs: SubscriptionRef[] = [{ stream: "$all", name }];
      const start = Date.now();
      await waitForProjection(client, appended, subs, {
        timeout: 5_000,
        pollInterval: 10,
      });
      assert.ok(Date.now() - start < 5_000);
      assert.ok(handled >= 2, `expected handler to have run >= 2, got ${handled}`);
      // cursor is at or past the last event.
      const pos = await client.readSubscriptionPosition("$all", name);
      assert.ok(pos.lastSeen >= appended[appended.length - 1].event_number);
    } finally {
      await worker.close();
    }
  });

  test("per-stream subscription waits in event_number space (SUB-A)", async () => {
    // Under SUB-A all work-items carry the global event_number and
    // the catch-up predicate compares in that space for both `$all`
    // and per-stream subscriptions. The legacy stream_version-based
    // target is gone; the same wall-clock moment is reached either
    // way because each AppendedEvent carries both numbers.
    const stream = randomUUID();
    const name = `p-${randomUUID().slice(0, 8)}`;

    const appended = await client.appendToStream(stream, expected.noStream, [
      { type: "X", data: {} },
    ]);
    const worker = startProjPair(
      client,
      name,
      stream,
      async () => {
        /* no-op */
      },
      "origin",
    );
    try {
      await waitForProjection(
        client,
        appended,
        [{ stream, name }],
        { timeout: 5_000, pollInterval: 10 },
      );
      // Predicate-true guarantees both conjuncts; the cursor reached
      // the event_number target.
      const pos = await client.readSubscriptionPosition(stream, name);
      assert.ok(pos.lastSeen >= appended[0].event_number);
    } finally {
      await worker.close();
    }
  });

  test("empty appended / empty subscriptions are no-ops", async () => {
    await waitForProjection(client, [], [{ stream: "$all", name: "irrelevant" }]);
    const stream = randomUUID();
    const appended = await client.appendToStream(stream, expected.noStream, [
      { type: "A", data: {} },
    ]);
    // Empty subscription list returns immediately even if there's
    // appended events.
    await waitForProjection(client, appended, []);
  });
});

// ---------------------------------------------------------------------------

describe("waitForProjection — timeout", () => {
  test("throws ConsistencyTimeout listing missing subscriptions", async () => {
    const stream = randomUUID();
    // Subscription that nobody is running — claim it ourselves so the
    // row exists, then never advance it.
    const stuck = `stuck-${randomUUID().slice(0, 8)}`;
    await client.claimSubscription(
      "$all",
      stuck,
      "test-worker",
      60,
      { startFrom: "origin" },
    );

    const appended = await client.appendToStream(stream, expected.noStream, [
      { type: "A", data: {} },
      { type: "B", data: {} },
    ]);

    const start = Date.now();
    await assert.rejects(
      () =>
        waitForProjection(
          client,
          appended,
          [{ stream: "$all", name: stuck }],
          { timeout: 150, pollInterval: 25 },
        ),
      (err: unknown) => {
        assert.ok(err instanceof ConsistencyTimeout);
        const e = err as ConsistencyTimeout;
        assert.equal(e.waitedMs, 150);
        assert.deepEqual(e.missing, [`$all::${stuck}`]);
        return true;
      },
    );
    // Sanity: at least the timeout elapsed (no early throw).
    assert.ok(Date.now() - start >= 100);

    await client.releaseSubscription("$all", stuck, "test-worker");
  });

  test("non-existent subscription times out (treated as not caught up)", async () => {
    const stream = randomUUID();
    const appended = await client.appendToStream(stream, expected.noStream, [
      { type: "A", data: {} },
    ]);
    await assert.rejects(
      () =>
        waitForProjection(
          client,
          appended,
          [{ stream: "$all", name: "never-created" }],
          { timeout: 100, pollInterval: 20 },
        ),
      (err: unknown) =>
        err instanceof ConsistencyTimeout &&
        (err as ConsistencyTimeout).missing.includes("$all::never-created"),
    );
  });
});

// ---------------------------------------------------------------------------
// SUB-A slice 8 — work-item conjunct
//
// The legacy single-cursor tests above exercise the routing-cursor
// conjunct only (the legacy worker writes no work-items, so the
// predicate's NOT EXISTS is vacuously true). The cases below exercise
// the second conjunct under the real SUB-A routing + processing path.
// ---------------------------------------------------------------------------

describe("waitForProjection — SUB-A work-item conjunct", () => {
  test("routed-but-pending blocks; predicate flips to true once handler completes", async () => {
    const { startRoutingWorker } = await import("../src/routing-worker.ts");
    const { startProjectionWorker } = await import(
      "../src/projection-worker.ts"
    );
    const { routingFnForPartitionBy } = await import(
      "../src/partition-by.ts"
    );

    const stream = randomUUID();
    const name = `subA-wait-${randomUUID().slice(0, 8)}`;
    const appended = await client.appendToStream(stream, expected.noStream, [
      { type: "A", data: {} },
      { type: "B", data: {} },
    ]);

    // Block the handler so work-items stay `claimed` and the
    // predicate's second conjunct is false even though routing
    // catches up immediately. Source from $all (per-stream sources
    // require the stream to exist before claim_subscription; we use
    // $all uniformly across these tests so workers can start in any
    // order).
    let release!: () => void;
    const block = new Promise<void>((r) => {
      release = r;
    });
    const router = startRoutingWorker(client, {
      name,
      stream: "$all",
      routeFn: routingFnForPartitionBy({ kind: "sequential" }),
      startFrom: "origin",
    });
    const proj = startProjectionWorker(client, {
      name,
      stream: "$all",
      handler: async () => {
        await block;
      },
    });

    try {
      // Phase 1: wait should NOT return while handler is blocked.
      const racer = waitForProjection(
        client,
        appended,
        [{ stream: "$all", name }],
        { timeout: 2_000, pollInterval: 10 },
      );
      let racerResolved = false;
      racer.then(
        () => {
          racerResolved = true;
        },
        () => {
          racerResolved = true;
        },
      );
      await new Promise((r) => setTimeout(r, 250));
      assert.equal(
        racerResolved,
        false,
        "waitForProjection must not return while work-items are in flight",
      );

      // Phase 2: unblock handlers; wait should return cleanly.
      release();
      await racer;
    } finally {
      release();
      await Promise.all([router.close(), proj.close()]);
    }
  });

  test("failed work-item keeps the predicate false (operator-only resolution)", async () => {
    const { startRoutingWorker } = await import("../src/routing-worker.ts");
    const stream = randomUUID();
    const name = `subA-fail-${randomUUID().slice(0, 8)}`;
    const appended = await client.appendToStream(stream, expected.noStream, [
      { type: "A", data: {} },
    ]);

    // Just routing -- no processing worker; we'll set the work-item
    // to `failed` directly via SQL after routing fires. Source from
    // $all uniformly with the other SUB-A tests in this describe.
    const router = startRoutingWorker(client, {
      name,
      stream: "$all",
      routeFn: () => ({ partitionKey: "p1" }),
      startFrom: "origin",
    });

    try {
      // Wait for routing to insert the work-item, then claim+fail it
      // directly to put it in `failed` state.
      const deadline = Date.now() + 2_000;
      while (Date.now() < deadline) {
        const r = await pool.query(
          `SELECT 1 FROM instructed.subscription_work_items
            WHERE subscription_name = $1`,
          [name],
        );
        if ((r.rowCount ?? 0) > 0) break;
        await new Promise((r) => setTimeout(r, 25));
      }
      // Claim then fail via the same worker id.
      const claimed = await client.claimWorkItem(
        "$all",
        name,
        "test-w",
        30,
      );
      assert.ok(claimed, "expected a claimable work item");
      await client.failWorkItem(
        "$all",
        name,
        "test-w",
        claimed.partitionKey,
        claimed.eventNumber,
        "synthetic-failure-for-test",
      );

      // Predicate must report not-caught-up due to the `failed` row.
      await assert.rejects(
        () =>
          waitForProjection(
            client,
            appended,
            [{ stream: "$all", name }],
            { timeout: 150, pollInterval: 25 },
          ),
        (err: unknown) =>
          err instanceof ConsistencyTimeout &&
          (err as ConsistencyTimeout).missing.includes(`$all::${name}`),
      );
    } finally {
      await router.close();
    }
  });

  test("race-safety: append + immediate wait does not spuriously return caught-up", async () => {
    // Load-bearing on the routing worker's atomic route_batch: cursor
    // advance and work-item INSERTs commit in one tx. If they didn't,
    // there would be a window where last_seen >= N but the work-item
    // for N hasn't been inserted yet, and the predicate would
    // falsely return true.
    //
    // We source from $all so the routing worker can claim the
    // subscription before any user stream exists (per-stream sources
    // would raise IS003 at claim-time).
    const { startRoutingWorker } = await import("../src/routing-worker.ts");
    const { startProjectionWorker } = await import(
      "../src/projection-worker.ts"
    );
    const { routingFnForPartitionBy } = await import(
      "../src/partition-by.ts"
    );

    const stream = randomUUID();
    const name = `subA-race-${randomUUID().slice(0, 8)}`;

    const router = startRoutingWorker(client, {
      name,
      stream: "$all",
      routeFn: routingFnForPartitionBy({ kind: "sequential" }),
      startFrom: "origin",
    });
    let handled = 0;
    const proj = startProjectionWorker(client, {
      name,
      stream: "$all",
      handler: async () => {
        handled += 1;
      },
    });

    try {
      // Append-then-immediately-wait. The wait must block until both
      // routing and processing actually happen; it must not see a
      // stale "caught up" from before the append.
      const appended = await client.appendToStream(stream, expected.noStream, [
        { type: "R", data: {} },
      ]);
      await waitForProjection(
        client,
        appended,
        [{ stream: "$all", name }],
        { timeout: 5_000, pollInterval: 10 },
      );
      assert.ok(handled >= 1, "handler must have run before wait returned");
    } finally {
      await Promise.all([router.close(), proj.close()]);
    }
  });
});

// ---------------------------------------------------------------------------
// CON-B: cross-stream guard
//
// A per-stream `SubscriptionRef` whose `stream` does not match any
// appended event's `stream_uuid` is meaningless: the subscription's
// `last_seen` lives in its own stream's coordinate space, the target
// lives in the appended stream's coordinate space, and comparing
// them silently produces wrong answers (under SUB-A: vacuously true,
// the router never enqueued anything for the unrelated event).
// The guard rejects this synchronously.
//
// See `docs/todo/consistency.md` :: CON-B.
// ---------------------------------------------------------------------------

describe("waitForProjection \u2014 cross-stream guard (CON-B)", () => {
  test("per-stream ref matching an appended stream resolves normally", async () => {
    const stream = randomUUID();
    const name = `con-b-ok-${randomUUID().slice(0, 8)}`;
    // Per-stream sources require the stream to exist before
    // claim_subscription (IS003), so append first, then start the
    // workers (mirrors the pattern used by the timeout tests).
    const appended = await client.appendToStream(stream, expected.noStream, [
      { type: "A", data: {} },
    ]);
    const worker = startProjPair(client, name, stream, async () => {});
    try {
      // Per-stream ref pointing at the appended stream: must not
      // throw the guard, and must drain normally.
      await waitForProjection(
        client,
        appended,
        [{ stream, name }],
        { timeout: 5_000, pollInterval: 10 },
      );
    } finally {
      await worker.close();
    }
  });

  test("per-stream ref differing from every appended stream rejects fast (before pollInterval)", async () => {
    // `waitForProjection` is async, so the pre-await throw
    // surfaces as a rejected promise on the next microtask. The
    // "synchronous" intent in CON-B is fast-fail: the rejection
    // must materialise long before `pollInterval` would have
    // elapsed. We assert on error class and on elapsed time.
    const appendedStream = randomUUID();
    const otherStream = randomUUID();
    const appended = await client.appendToStream(
      appendedStream,
      expected.noStream,
      [{ type: "A", data: {} }],
    );

    const start = Date.now();
    await assert.rejects(
      () =>
        waitForProjection(
          client,
          appended,
          [{ stream: otherStream, name: "x" }],
          { timeout: 10_000, pollInterval: 250 },
        ),
      (err: unknown) => {
        assert.ok(
          err instanceof ConsistencyTargetError,
          `expected ConsistencyTargetError, got ${err}`,
        );
        const e = err as ConsistencyTargetError;
        assert.equal(e.subscriptionStream, otherStream);
        assert.equal(e.subscriptionName, "x");
        assert.deepEqual(e.appendedStreams, [appendedStream]);
        return true;
      },
    );
    const elapsed = Date.now() - start;
    assert.ok(
      elapsed < 100,
      `must reject before pollInterval (${elapsed}ms elapsed; pollInterval=250)`,
    );
  });

  test("mixed list: one valid ref + one invalid ref rejects (invalid prevents wait)", async () => {
    const appendedStream = randomUUID();
    const otherStream = randomUUID();
    const validName = `con-b-mixed-${randomUUID().slice(0, 8)}`;
    // Append first; per-stream source requires the stream to exist
    // before claim_subscription.
    const appended = await client.appendToStream(
      appendedStream,
      expected.noStream,
      [{ type: "A", data: {} }],
    );
    const worker = startProjPair(client, validName, appendedStream, async () => {});
    try {
      await assert.rejects(
        () =>
          waitForProjection(
            client,
            appended,
            [
              { stream: appendedStream, name: validName },
              { stream: otherStream, name: "bad" },
            ],
            { timeout: 10_000, pollInterval: 250 },
          ),
        (err: unknown) =>
          err instanceof ConsistencyTargetError &&
          (err as ConsistencyTargetError).subscriptionStream === otherStream,
      );
    } finally {
      await worker.close();
    }
  });

  test("$all refs are never rejected regardless of appended streams", async () => {
    const stream = randomUUID();
    const name = `con-b-all-${randomUUID().slice(0, 8)}`;
    const worker = startProjPair(client, name, "$all", async () => {});
    try {
      const appended = await client.appendToStream(stream, expected.noStream, [
        { type: "A", data: {} },
      ]);
      // $all ref is always valid; guard must not fire.
      await waitForProjection(
        client,
        appended,
        [{ stream: "$all", name }],
        { timeout: 5_000, pollInterval: 10 },
      );
    } finally {
      await worker.close();
    }
  });
});
