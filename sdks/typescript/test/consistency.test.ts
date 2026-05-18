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
  ConsistencyTimeout,
  expected,
  startProjection,
  waitForProjection,
} from "../src/index.ts";
import type {
  ProjectionDefinition,
  RunningWorker,
  SubscriptionRef,
} from "../src/index.ts";
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

describe("waitForProjection — happy path", () => {
  test("returns once a running $all projection catches up", async () => {
    const stream = randomUUID();
    const name = `p-${randomUUID().slice(0, 8)}`;

    let handled = 0;
    const def: ProjectionDefinition = {
      name,
      async handle() {
        handled++;
      },
    };
    const worker: RunningWorker = startProjection(client, def, {
      pollInterval: 25,
      heartbeatInterval: 1_000,
    });
    try {
      const appended = await client.appendToStream(stream, expected.noStream, [
        { event_type: "A", data: {} },
        { event_type: "B", data: {} },
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

  test("per-stream subscription uses stream_version as the target", async () => {
    const stream = randomUUID();
    const name = `p-${randomUUID().slice(0, 8)}`;

    // Per-stream subscription requires the stream to exist before
    // claim (claim_subscription resolves stream_id and raises IS003
    // otherwise). Seed the stream first.
    const appended = await client.appendToStream(stream, expected.noStream, [
      { event_type: "X", data: {} },
    ]);
    const def: ProjectionDefinition = {
      name,
      stream,
      startFrom: "origin",
      async handle() {
        /* no-op */
      },
    };
    const worker = startProjection(client, def, {
      pollInterval: 25,
      heartbeatInterval: 1_000,
    });
    try {
      await waitForProjection(
        client,
        appended,
        [{ stream, name }],
        { timeout: 5_000, pollInterval: 10 },
      );
      const pos = await client.readSubscriptionPosition(stream, name);
      assert.ok(pos.lastSeen >= appended[0].stream_version);
    } finally {
      await worker.close();
    }
  });

  test("empty appended / empty subscriptions are no-ops", async () => {
    await waitForProjection(client, [], [{ stream: "$all", name: "irrelevant" }]);
    const stream = randomUUID();
    const appended = await client.appendToStream(stream, expected.noStream, [
      { event_type: "A", data: {} },
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
      { event_type: "A", data: {} },
      { event_type: "B", data: {} },
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
      { event_type: "A", data: {} },
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
