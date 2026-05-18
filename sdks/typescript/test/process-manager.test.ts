/**
 * Layer 3: process-manager worker — smoke test.
 *
 * The required case for step 4 (sdk-design.md §10): one event in → PM
 * routes and handles it → dispatches one command on the separate
 * client → cursor advances → snapshot persists. Surrounding cases:
 * `{kind: 'ignore'}` and unknown-route advance without snapshot, the
 * `{kind: 'start'}` leniency contract (§11.4), `{kind: 'stop'}` deletes
 * the snapshot, causation/correlation propagation (§11.8 / D-0017),
 * and the same-Client construction guard (D-0011 / D-0012).
 */

import { after, before, beforeEach, describe, test } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { closePool, getPool, truncateAll } from "./fixtures.ts";
import {
  Client,
  expected,
  InstructedError,
  runCommand,
  SnapshotNotFound,
  startProcessManager,
} from "../src/index.ts";
import type {
  AggregateDefinition,
  DomainEvent,
  DispatchedCommand,
  ProcessManagerDefinition,
  RecordedEvent,
  RunningWorker,
} from "../src/index.ts";
import type pg from "pg";

let pool: pg.Pool;
let dispatchPool: pg.Pool;
let persistClient: Client;
let dispatchClient: Client;

before(async () => {
  pool = await getPool();
  // PM requires a separate dispatch client; we use a second Pool
  // backed by the same database (D-0011 / D-0012 ask for session-
  // disjoint lock sets, not separate databases).
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

// --- a trivial target aggregate to receive PM-dispatched commands ----------

interface CounterState {
  value: number;
}
type CounterCommand = { kind: "add"; n: number };
interface CounterEvent extends DomainEvent {
  type: "Added";
  data: { n: number };
}

function counter(): AggregateDefinition<CounterState, CounterCommand, CounterEvent> {
  return {
    type: "Counter",
    initialState: () => ({ value: 0 }),
    execute(_state, command) {
      return { event_type: "Added", data: { n: command.n } };
    },
    apply(state, event) {
      if (event.type === "Added") {
        return { value: state.value + event.data.n };
      }
      return state;
    },
  };
}

// ---------------------------------------------------------------------------

describe("startProcessManager — construction", () => {
  test("throws when the same Client is passed for persist and dispatch (D-0011 / D-0012)", () => {
    const def: ProcessManagerDefinition<{ stage: string }> = {
      name: "same-client-pm",
      routes: {},
      initialState: () => ({ stage: "init" }),
      async handle(state) {
        return { state };
      },
    };
    assert.throws(
      () => startProcessManager(persistClient, persistClient, def),
      (err) => err instanceof InstructedError && /must be different/.test(err.message),
    );
  });
});

// ---------------------------------------------------------------------------

describe("startProcessManager — single-event smoke", () => {
  test("routes one event, dispatches one command, advances cursor, persists snapshot", async () => {
    const Counter = counter();
    const targetStream = randomUUID();
    const triggerStream = randomUUID();
    const pmName = `pm-${randomUUID().slice(0, 8)}`;

    // The PM listens on $all for one event type and starts an
    // instance per triggering event.
    interface PmState {
      forwarded: boolean;
      target: string | null;
      from: string | null;
    }
    const def: ProcessManagerDefinition<PmState> = {
      name: pmName,
      routes: {
        Triggered(event) {
          // The data carries a per-event processId so each trigger
          // creates a distinct PM instance.
          const pid = (event.data as { processId: string }).processId;
          return { kind: "start", processId: pid };
        },
      },
      initialState: () => ({ forwarded: false, target: null, from: null }),
      async handle(state, event) {
        const data = event.data as { processId: string; n: number };
        const commands: DispatchedCommand[] = [
          {
            streamUuid: targetStream,
            aggregate: Counter,
            command: { kind: "add", n: data.n } as CounterCommand,
          },
        ];
        return {
          state: { forwarded: true, target: targetStream, from: event.stream_uuid },
          commands,
        };
      },
    };

    const errors: Error[] = [];
    const worker: RunningWorker = startProcessManager(
      persistClient,
      dispatchClient,
      def,
      {
        leaseSeconds: 5,
        pollInterval: 50,
        heartbeatInterval: 1_000,
        onError: (err) => errors.push(err),
      },
    );

    try {
      const processId = randomUUID();
      // Append the trigger event AFTER the PM has claimed the
      // subscription, so {kind:'start'} is sensible. claim is fast;
      // we tolerate either order in practice.
      await persistClient.appendToStream(triggerStream, expected.noStream, [
        {
          event_type: "Triggered",
          data: { processId, n: 7 },
        },
      ]);

      // Wait for the target stream to see the dispatched command's event.
      await waitFor(async () => {
        try {
          const rows = await persistClient.readStream(targetStream, 1n, 10);
          return rows.length >= 1;
        } catch (err) {
          // The target stream doesn't exist until the PM dispatches.
          if (err instanceof InstructedError && (err as { code?: string }).code === "IS003") return false;
          throw err;
        }
      }, 5_000, "dispatched command to reach target stream");

      const rows = await persistClient.readStream<{ n: number }>(
        targetStream,
        1n,
        10,
      );
      assert.equal(rows.length, 1);
      assert.equal(rows[0].event_type, "Added");
      assert.deepEqual(rows[0].data, { n: 7 });

      // Snapshot exists for the PM instance.
      const sourceUuid = `${pmName}-${processId}`;
      await waitFor(async () => {
        try {
          await persistClient.readSnapshot(sourceUuid);
          return true;
        } catch (err) {
          if (err instanceof SnapshotNotFound) return false;
          throw err;
        }
      }, 5_000, "PM snapshot to be written");
      const snap = await persistClient.readSnapshot<PmState>(sourceUuid);
      assert.equal(snap.sourceType, pmName);
      assert.equal(snap.data.forwarded, true);
      assert.equal(snap.data.target, targetStream);
      assert.equal(snap.data.from, triggerStream);

      // Cursor advanced past the trigger event_number.
      const triggerEvent = (await persistClient.readStream(triggerStream, 1n, 1))[0];
      await waitFor(async () => {
        const pos = await persistClient.readSubscriptionPosition("$all", pmName);
        return pos.lastSeen >= triggerEvent.event_number;
      }, 5_000, "PM cursor to advance past trigger");

      // §11.8 / D-0017: the dispatched event's causation_id is the
      // triggering event's event_id; correlation flows through.
      assert.equal(rows[0].causation_id, triggerEvent.event_id);
      // correlation_id on the trigger was null; the dispatched event
      // therefore also has null correlation (undefined default).
      assert.equal(rows[0].correlation_id, null);

      // Sanity: no unexpected onError calls.
      assert.equal(errors.length, 0, `unexpected errors: ${errors.map(String).join("; ")}`);
    } finally {
      await worker.close();
    }
  });

  test("propagates correlation_id from the triggering event", async () => {
    const Counter = counter();
    const targetStream = randomUUID();
    const triggerStream = randomUUID();
    const pmName = `pm-${randomUUID().slice(0, 8)}`;
    const correlationId = randomUUID();

    const def: ProcessManagerDefinition<{ done: boolean }> = {
      name: pmName,
      routes: {
        Triggered(event) {
          return { kind: "start", processId: (event.data as { processId: string }).processId };
        },
      },
      initialState: () => ({ done: false }),
      async handle(_state, _event) {
        return {
          state: { done: true },
          commands: [
            {
              streamUuid: targetStream,
              aggregate: Counter,
              command: { kind: "add", n: 1 } as CounterCommand,
            },
          ],
        };
      },
    };
    const worker = startProcessManager(persistClient, dispatchClient, def, {
      pollInterval: 50,
      heartbeatInterval: 1_000,
    });

    try {
      await persistClient.appendToStream(triggerStream, expected.noStream, [
        {
          event_type: "Triggered",
          data: { processId: randomUUID() },
          correlation_id: correlationId,
        },
      ]);

      await waitFor(async () => {
        try {
          const rows = await persistClient.readStream(targetStream, 1n, 10);
          return rows.length >= 1;
        } catch (err) {
          if (err instanceof InstructedError && (err as { code?: string }).code === "IS003") return false;
          throw err;
        }
      }, 5_000, "dispatched event to land");
      const [evt] = await persistClient.readStream(targetStream, 1n, 1);
      assert.equal(evt.correlation_id, correlationId);
    } finally {
      await worker.close();
    }
  });
});

// ---------------------------------------------------------------------------

describe("startProcessManager — routing", () => {
  test("events with no route advance the cursor without snapshot", async () => {
    const triggerStream = randomUUID();
    const pmName = `pm-${randomUUID().slice(0, 8)}`;

    let handleCalled = false;
    const def: ProcessManagerDefinition<{}> = {
      name: pmName,
      routes: {
        // Empty: nothing is routed.
      },
      initialState: () => ({}),
      async handle(state) {
        handleCalled = true;
        return { state };
      },
    };
    const worker = startProcessManager(persistClient, dispatchClient, def, {
      pollInterval: 50,
      heartbeatInterval: 1_000,
    });
    try {
      await persistClient.appendToStream(triggerStream, expected.noStream, [
        { event_type: "Unrelated", data: {} },
        { event_type: "AlsoUnrelated", data: {} },
      ]);
      const last = (await persistClient.readStream(triggerStream, 2n, 1))[0];
      await waitFor(async () => {
        const pos = await persistClient.readSubscriptionPosition("$all", pmName);
        return pos.lastSeen >= last.event_number;
      }, 5_000, "cursor to advance past unrouted events");
      assert.equal(handleCalled, false);
    } finally {
      await worker.close();
    }
  });

  test("{kind:'ignore'} advances the cursor without calling handle", async () => {
    const triggerStream = randomUUID();
    const pmName = `pm-${randomUUID().slice(0, 8)}`;

    let handleCalled = false;
    const def: ProcessManagerDefinition<{}> = {
      name: pmName,
      routes: {
        Triggered() {
          return { kind: "ignore" };
        },
      },
      initialState: () => ({}),
      async handle(state) {
        handleCalled = true;
        return { state };
      },
    };
    const worker = startProcessManager(persistClient, dispatchClient, def, {
      pollInterval: 50,
      heartbeatInterval: 1_000,
    });
    try {
      await persistClient.appendToStream(triggerStream, expected.noStream, [
        { event_type: "Triggered", data: {} },
      ]);
      const evt = (await persistClient.readStream(triggerStream, 1n, 1))[0];
      await waitFor(async () => {
        const pos = await persistClient.readSubscriptionPosition("$all", pmName);
        return pos.lastSeen >= evt.event_number;
      }, 5_000, "cursor to advance past ignored event");
      assert.equal(handleCalled, false);
    } finally {
      await worker.close();
    }
  });
});

// ---------------------------------------------------------------------------

describe("startProcessManager — lifecycle (§11.4)", () => {
  test("{kind:'start'} discards an existing snapshot (lenient)", async () => {
    const triggerStream = randomUUID();
    const pmName = `pm-${randomUUID().slice(0, 8)}`;
    const processId = randomUUID();
    const sourceUuid = `${pmName}-${processId}`;

    // Pre-seed a snapshot that should be discarded on start.
    await persistClient.recordSnapshot({
      sourceUuid,
      sourceType: pmName,
      sourceVersion: 0n,
      data: { stage: "stale" },
    });

    interface PmState {
      stage: string;
    }
    let observed: string | null = null;
    const def: ProcessManagerDefinition<PmState> = {
      name: pmName,
      routes: {
        Triggered: () => ({ kind: "start", processId }),
      },
      initialState: () => ({ stage: "fresh" }),
      async handle(state, _event) {
        observed = state.stage;
        return { state: { stage: "finished" } };
      },
    };
    const worker = startProcessManager(persistClient, dispatchClient, def, {
      pollInterval: 50,
      heartbeatInterval: 1_000,
    });
    try {
      await persistClient.appendToStream(triggerStream, expected.noStream, [
        { event_type: "Triggered", data: {} },
      ]);
      await waitFor(async () => observed !== null, 5_000, "handle to run");
      assert.equal(observed, "fresh"); // not "stale"
      // The new snapshot is persisted.
      await waitFor(async () => {
        const s = await persistClient.readSnapshot<PmState>(sourceUuid);
        return s.data.stage === "finished";
      }, 5_000, "new snapshot to be written");
    } finally {
      await worker.close();
    }
  });

  test("{kind:'stop'} deletes the snapshot in the ack transaction", async () => {
    const triggerStream = randomUUID();
    const pmName = `pm-${randomUUID().slice(0, 8)}`;
    const processId = randomUUID();
    const sourceUuid = `${pmName}-${processId}`;

    // Seed the snapshot first so 'stop' has something to delete.
    await persistClient.recordSnapshot({
      sourceUuid,
      sourceType: pmName,
      sourceVersion: 0n,
      data: { done: false },
    });

    const def: ProcessManagerDefinition<{ done: boolean }> = {
      name: pmName,
      routes: {
        Done: () => ({ kind: "stop", processId }),
      },
      initialState: () => ({ done: false }),
      async handle(_s) {
        return { state: { done: true } };
      },
    };
    const worker = startProcessManager(persistClient, dispatchClient, def, {
      pollInterval: 50,
      heartbeatInterval: 1_000,
    });
    try {
      await persistClient.appendToStream(triggerStream, expected.noStream, [
        { event_type: "Done", data: {} },
      ]);
      // Wait until the snapshot is gone.
      await waitFor(async () => {
        try {
          await persistClient.readSnapshot(sourceUuid);
          return false;
        } catch (err) {
          return err instanceof SnapshotNotFound;
        }
      }, 5_000, "snapshot to be deleted on stop");
    } finally {
      await worker.close();
    }
  });
});
