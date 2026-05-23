/**
 * Layer 3: PM ignored-event ack coalescing (TODO #10 / ex-ML-0005).
 *
 * The PM no longer issues an `advance_subscription` per ignored event.
 * Instead:
 *   - An ignored event sets a pending pointer; no round-trip.
 *   - The next routed event's persist-and-ack tx covers the pending
 *     pointer implicitly (advance_subscription is monotone).
 *   - A trailing run of ignored events at the end of a batch is
 *     flushed with one `advance_subscription` after the loop.
 *
 * Each test spies on `client.advanceSubscription` to count round-trips
 * and asserts the new invariants. Final cursor positions are also
 * checked so we know the optimisation hasn't dropped any acks.
 */

import { after, before, beforeEach, describe, test } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { closePool, getPool, truncateAll } from "./fixtures.ts";
import {
  Client,
  expected,
  InstructedError,
  startProcessManager,
  SubscriptionNotFound,
} from "../src/index.ts";
import type {
  ProcessManagerDefinition,
} from "../src/index.ts";
import type pg from "pg";

let pool: pg.Pool;
let dispatchPool: pg.Pool;
let persistClient: Client;
let dispatchClient: Client;

before(async () => {
  pool = await getPool();
  dispatchPool = new (await import("pg")).default.Pool({
    host: process.env.PGHOST ?? "127.0.0.1",
    port: Number(process.env.PGPORT ?? 5432),
    user: process.env.PGUSER ?? "postgres",
    password: process.env.PGPASSWORD ?? "postgres",
    database: process.env.PGDATABASE ?? "instructed_test",
    max: 4,
  });
  persistClient = new Client(pool);
  dispatchClient = new Client(dispatchPool);
});
after(async () => {
  await dispatchPool.end();
  await closePool();
});
beforeEach(async () => {
  await truncateAll(pool);
});

async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 5_000,
  label = "condition",
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise((r) => setTimeout(r, 10));
  }
  throw new Error(`timeout waiting for ${label}`);
}

/**
 * Read a subscription's `lastSeen` position, returning `null` if the
 * subscription doesn't exist yet (the PM may not have claimed it in
 * the moment between worker start and the first poll). Used inside
 * `waitFor` predicates so they don't throw on the race.
 */
async function cursorOrNull(
  client: Client,
  stream: string,
  name: string,
): Promise<bigint | null> {
  try {
    const pos = await client.readSubscriptionPosition(stream, name);
    return pos.lastSeen;
  } catch (err) {
    if (err instanceof SubscriptionNotFound) return null;
    if (
      err instanceof InstructedError &&
      (err as { code?: string }).code === "IS020"
    ) {
      return null;
    }
    throw err;
  }
}

/**
 * Wrap `client.advanceSubscription` to count calls. Returns a restore
 * function. Counts are recorded into the supplied accumulator with the
 * `upToPosition` argument so tests can inspect what was acked.
 */
function spyAdvance(
  client: Client,
  recorded: { calls: { upTo: bigint; standalone: boolean }[] },
): () => void {
  const original = client.advanceSubscription.bind(client);
  // The persist-and-ack tx routes through a per-tx wrapper, so calls
  // from inside withTransaction don't hit this method on the base
  // client. That gives us a clean way to count *standalone* advance
  // calls (i.e. trailing flushes and any per-ignored-event acks if the
  // optimisation regressed).
  (client as unknown as { advanceSubscription: typeof client.advanceSubscription }).advanceSubscription =
    async function spy(
      streamUuid: string,
      subscriptionName: string,
      workerId: string,
      upToPosition: bigint,
      options?: Parameters<typeof client.advanceSubscription>[4],
    ) {
      recorded.calls.push({ upTo: upToPosition, standalone: true });
      return original(
        streamUuid,
        subscriptionName,
        workerId,
        upToPosition,
        options,
      );
    };
  return () => {
    (client as unknown as { advanceSubscription: typeof client.advanceSubscription }).advanceSubscription =
      original;
  };
}

// --- test PMs ------------------------------------------------------

/**
 * A PM that ignores everything via `{kind: 'ignore'}`. We use this
 * rather than an empty `routes` map because the existing tests use
 * the empty-routes path; this exercises the explicit-ignore branch.
 */
function ignoreAllPm(name: string): ProcessManagerDefinition<{}> {
  return {
    name,
    routes: {
      Tick: () => ({ kind: "ignore" }),
    },
    initialState: () => ({}),
    async handle(state) {
      return { state };
    },
  };
}

/**
 * A PM that routes `Triggered` and ignores `Tick`. It does NOT
 * dispatch any commands — dispatching from a PM on `$all` would
 * make the dispatched event flow back as a trailing ignored event
 * in subsequent batches, which would introduce extra advance calls
 * that have nothing to do with the coalescing logic these tests are
 * verifying. Dispatch coverage lives in `process-manager.test.ts`.
 */
function mixedPmNoDispatch(
  name: string,
): ProcessManagerDefinition<{ forwarded: number }> {
  return {
    name,
    routes: {
      Triggered(event) {
        const data = event.data as { processId: string };
        return { kind: "continue", processId: data.processId };
      },
      Tick: () => ({ kind: "ignore" }),
    },
    initialState: () => ({ forwarded: 0 }),
    async handle(state) {
      return { state: { forwarded: state.forwarded + 1 } };
    },
  };
}

// ---------------------------------------------------------------------------

describe("PM ack coalescing — all-ignored batch", () => {
  test("a batch of N ignored events advances the cursor to the last one with exactly one standalone advance call", async () => {
    const triggerStream = randomUUID();
    const pmName = `pm-${randomUUID().slice(0, 8)}`;

    const N = 5;
    // Append the ignored events BEFORE starting the PM so they all
    // land in one read batch.
    await persistClient.appendToStream(triggerStream, expected.noStream, [
      ...Array.from({ length: N }, () => ({
        event_type: "Tick",
        data: {},
      })),
    ]);
    const last = (await persistClient.readStream(triggerStream, BigInt(N), 1))[0];

    const recorded: { calls: { upTo: bigint; standalone: boolean }[] } = {
      calls: [],
    };
    const restore = spyAdvance(persistClient, recorded);

    const worker = startProcessManager(
      persistClient,
      dispatchClient,
      ignoreAllPm(pmName),
      {
        // Large enough that all N fit in one read batch.
        batchSize: 100,
        pollInterval: 50,
        heartbeatInterval: 1_000,
      },
    );
    try {
      await waitFor(async () => {
        const cur = await cursorOrNull(persistClient, "$all", pmName);
        return cur !== null && cur >= last.event_number;
      }, 5_000, "PM cursor to advance past trailing ignored run");

      // Exactly one standalone advance, to the last position.
      // (readSubscriptionPosition above doesn't touch advance.)
      assert.equal(
        recorded.calls.length,
        1,
        `expected exactly 1 standalone advance; got ${recorded.calls.length}: ${JSON.stringify(recorded.calls.map((c) => c.upTo.toString()))}`,
      );
      assert.equal(recorded.calls[0].upTo, last.event_number);
    } finally {
      restore();
      await worker.close();
    }
  });
});

describe("PM ack coalescing — routed event covers prior ignored", () => {
  test("[ignored, ignored, routed, ignored, ignored, routed] does zero standalone advances", async () => {
    const triggerStream = randomUUID();
    const pmName = `pm-${randomUUID().slice(0, 8)}`;
    const pid1 = randomUUID();
    const pid2 = randomUUID();

    await persistClient.appendToStream(triggerStream, expected.noStream, [
      { event_type: "Tick", data: {} },
      { event_type: "Tick", data: {} },
      { event_type: "Triggered", data: { processId: pid1, n: 1 } },
      { event_type: "Tick", data: {} },
      { event_type: "Tick", data: {} },
      { event_type: "Triggered", data: { processId: pid2, n: 2 } },
    ]);
    const last = (await persistClient.readStream(triggerStream, 6n, 1))[0];

    const recorded: { calls: { upTo: bigint; standalone: boolean }[] } = {
      calls: [],
    };
    const restore = spyAdvance(persistClient, recorded);

    const worker = startProcessManager(
      persistClient,
      dispatchClient,
      mixedPmNoDispatch(pmName),
      {
        batchSize: 100,
        pollInterval: 50,
        heartbeatInterval: 1_000,
      },
    );
    try {
      await waitFor(async () => {
        const cur = await cursorOrNull(persistClient, "$all", pmName);
        return cur !== null && cur >= last.event_number;
      }, 5_000, "PM cursor to advance past second routed event");

      // The two routed advances happen inside withTransaction, which
      // creates its own session-bound Client; that path doesn't go
      // through the base-client method we spied on. So we should see
      // zero standalone advances.
      assert.equal(
        recorded.calls.length,
        0,
        `expected zero standalone advances; got ${recorded.calls.length}: ${JSON.stringify(recorded.calls.map((c) => c.upTo.toString()))}`,
      );
    } finally {
      restore();
      await worker.close();
    }
  });
});

describe("PM ack coalescing — trailing ignored flush", () => {
  test("[routed, ignored, ignored] does one standalone advance to the trailing ignored", async () => {
    const triggerStream = randomUUID();
    const pmName = `pm-${randomUUID().slice(0, 8)}`;
    const pid = randomUUID();

    await persistClient.appendToStream(triggerStream, expected.noStream, [
      { event_type: "Triggered", data: { processId: pid, n: 3 } },
      { event_type: "Tick", data: {} },
      { event_type: "Tick", data: {} },
    ]);
    const last = (await persistClient.readStream(triggerStream, 3n, 1))[0];

    const recorded: { calls: { upTo: bigint; standalone: boolean }[] } = {
      calls: [],
    };
    const restore = spyAdvance(persistClient, recorded);

    const worker = startProcessManager(
      persistClient,
      dispatchClient,
      mixedPmNoDispatch(pmName),
      {
        batchSize: 100,
        pollInterval: 50,
        heartbeatInterval: 1_000,
      },
    );
    try {
      await waitFor(async () => {
        const cur = await cursorOrNull(persistClient, "$all", pmName);
        return cur !== null && cur >= last.event_number;
      }, 5_000, "PM cursor to advance past trailing ignored run");

      // Exactly one standalone advance, for the trailing-ignored flush,
      // targeting the last ignored event's event_number.
      assert.equal(
        recorded.calls.length,
        1,
        `expected exactly 1 standalone advance; got ${recorded.calls.length}: ${JSON.stringify(recorded.calls.map((c) => c.upTo.toString()))}`,
      );
      assert.equal(recorded.calls[0].upTo, last.event_number);
    } finally {
      restore();
      await worker.close();
    }
  });
});

describe("PM ack coalescing — ignored-then-routed (no trailing)", () => {
  test("[ignored, ignored, routed] does zero standalone advances; cursor reaches routed position", async () => {
    const triggerStream = randomUUID();
    const pmName = `pm-${randomUUID().slice(0, 8)}`;
    const pid = randomUUID();

    await persistClient.appendToStream(triggerStream, expected.noStream, [
      { event_type: "Tick", data: {} },
      { event_type: "Tick", data: {} },
      { event_type: "Triggered", data: { processId: pid, n: 4 } },
    ]);
    const last = (await persistClient.readStream(triggerStream, 3n, 1))[0];

    const recorded: { calls: { upTo: bigint; standalone: boolean }[] } = {
      calls: [],
    };
    const restore = spyAdvance(persistClient, recorded);

    const worker = startProcessManager(
      persistClient,
      dispatchClient,
      mixedPmNoDispatch(pmName),
      {
        batchSize: 100,
        pollInterval: 50,
        heartbeatInterval: 1_000,
      },
    );
    try {
      await waitFor(async () => {
        const cur = await cursorOrNull(persistClient, "$all", pmName);
        return cur !== null && cur >= last.event_number;
      }, 5_000, "PM cursor to advance past routed event");

      assert.equal(
        recorded.calls.length,
        0,
        `expected zero standalone advances; got ${recorded.calls.length}: ${JSON.stringify(recorded.calls.map((c) => c.upTo.toString()))}`,
      );
    } finally {
      restore();
      await worker.close();
    }
  });
});
