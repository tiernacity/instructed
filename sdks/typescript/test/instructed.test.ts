/**
 * Layer 5: `Instructed` facade tests -- facade-specific behaviour only.
 *
 * Underlying correctness is covered by the layered tests (client,
 * aggregate, routing-worker, processing-worker, projection-worker,
 * pm-worker, consistency). Cases here:
 *
 *   - registerAggregate + dispatch (by-name lookup, UnknownAggregateType)
 *   - registerProjection: partitionBy / routeFn mutual exclusivity
 *   - poll fans out routing+processing workers for a
 *     projection and a PM under one handle; close() stops them all
 *   - dispatch( ... , { consistency: [...] }) waits for the
 *     projection to catch up
 */

import { after, before, beforeEach, describe, test } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { closePool, getPool, truncateAll } from "./fixtures.ts";
import pg from "pg";
import {
  commandRouter,
  expected,
  Instructed,
  UnknownAggregateType,
} from "../src/index.ts";
import type {
  AggregateDefinition,
  Command,
  CommandRouter,
  DispatchedCommand,
  DomainEvent,
  RecordedEvent,
} from "../src/index.ts";

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

// --- minimal counter aggregate ---------------------------------------------

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
    execute(_s, c) {
      return { type: "Added", data: { n: c.n } };
    },
    apply(state, event) {
      if (event.type === "Added") return { value: state.value + event.data.n };
      return state;
    },
  };
}

// ---------------------------------------------------------------------------

describe("Instructed -- dispatch (registry lookup)", () => {
  test("dispatches a registered aggregate by name; throws UnknownAggregateType otherwise", async () => {
    const app = new Instructed({ db: pool });
    const Counter = counter();
    app.register(Counter);

    const stream = randomUUID();
    const appended = await app.dispatch<CounterCommand>(
      "Counter",
      stream,
      { kind: "add", n: 3 },
    );
    assert.equal(appended.length, 1);
    assert.equal(appended[0].stream_version, 1n);

    await assert.rejects(
      () => app.dispatch("DoesNotExist", stream, {}),
      (err: unknown) =>
        err instanceof UnknownAggregateType &&
        (err as UnknownAggregateType).aggregateType === "DoesNotExist",
    );
  });

  test("does not own the user-supplied Pool; pool stays usable after dispatch", async () => {
    const app = new Instructed({ db: pool });
    app.register(counter());
    await app.dispatch<CounterCommand>("Counter", randomUUID(), { kind: "add", n: 1 });
    const r = await pool.query("SELECT 1 AS x");
    assert.equal(r.rows[0].x, 1);
  });
});

// ---------------------------------------------------------------------------

describe("Instructed -- registerProjection validation", () => {
  test("register rejects mutually-exclusive partitionBy + routeFn on a projection", () => {
    const app = new Instructed({ db: pool });
    assert.throws(
      () =>
        app.register({
          type: "p",
          partitionBy: { kind: "sequential" },
          routeFn: () => ({ partitionKey: "k" }),
          async handler() {},
        }),
      /mutually exclusive/,
    );
  });
});

// ---------------------------------------------------------------------------

describe("Instructed -- poll fan-out", () => {
  test("runs a registered projection and PM under one handle; close stops both", async () => {
    const app = new Instructed({ db: pool });

    const Counter = counter();
    app.register(Counter);

    // A projection on $all that counts events of type "Triggered".
    // SUB-A: legacy `selector` is recovered via a routeFn that
    // returns `"ignore"` for the would-be-skipped events.
    const projName = `proj-${randomUUID().slice(0, 8)}`;
    let projSeen = 0;
    app.register(
      {
        type: projName,
        routeFn: (e) =>
          e.type === "Triggered"
            ? { partitionKey: "_default" }
            : "ignore",
        async handler() {
          projSeen++;
        },
      },
      { pollInterval: 25, heartbeatInterval: 1_000 },
    );

    // A PM on $all that forwards each Triggered event into Counter.
    // PM-F routing: every "Triggered" event spins its own partition
    // (so the PM stops after one event per partition).
    const pmName = `pm-${randomUUID().slice(0, 8)}`;
    const targetStream = randomUUID();
    app.register(
      {
        type: pmName,
        routeFn: (e) =>
          e.type === "Triggered"
            ? {
                partitionKey:
                  (e.data as { processId: string }).processId,
              }
            : "ignore",
        initialState: () => ({ done: false }),
        apply: (state) => state,
        async handle() {
          const commands: DispatchedCommand[] = [
            {
              streamUuid: targetStream,
              aggregate: Counter,
              command: { kind: "add", n: 1 } as CounterCommand,
            },
          ];
          return { commands, complete: true };
        },
      },
      { pollInterval: 25, heartbeatInterval: 1_000 },
    );

    const handle = await app.poll();
    try {
      const trigger = randomUUID();
      await app.client().appendToStream(trigger, expected.noStream, [
        { type: "Triggered", data: { processId: randomUUID() } },
        { type: "Triggered", data: { processId: randomUUID() } },
      ]);

      // Both the projection and the PM observe both events.
      await waitFor(() => projSeen >= 2, 5_000, "projection to see 2 events");
      await waitFor(async () => {
        try {
          const rows = await app.client().readStream(targetStream, 1n, 10);
          return rows.length >= 2;
        } catch {
          return false;
        }
      }, 5_000, "PM-dispatched events to land");
    } finally {
      await handle.close();
    }
  });

  test("throws when no projections or PMs are registered", async () => {
    const app = new Instructed({ db: pool });
    await assert.rejects(() => app.poll(), /no projections or process managers/);
  });
});

// ---------------------------------------------------------------------------

describe("Instructed -- dispatch consistency wait", () => {
  test("waits for a named subscription to catch up before returning", async () => {
    const app = new Instructed({ db: pool });
    app.register(counter());

    const projName = `proj-${randomUUID().slice(0, 8)}`;
    let seen = 0;
    app.register(
      {
        type: projName,
        routeFn: (e: RecordedEvent) =>
          e.type === "Added" ? { partitionKey: "_default" } : "ignore",
        async handler() {
          seen++;
        },
      },
      { pollInterval: 25, heartbeatInterval: 1_000 },
    );
    const handle = await app.poll();
    try {
      await app.dispatch<CounterCommand>(
        "Counter",
        randomUUID(),
        { kind: "add", n: 5 },
        { consistency: [projName], consistencyTimeout: 5_000 },
      );
      // After dispatch returns, the projection must have advanced
      // past the appended event -- its handler ran.
      assert.ok(seen >= 1, `expected handler to have run, got ${seen}`);
    } finally {
      await handle.close();
    }
  });
});

// ---------------------------------------------------------------------------

describe("Instructed -- command router", () => {
  test("dispatch(command) routes via the registered router", async () => {
    const app = new Instructed({ db: pool });
    const Counter = counter();
    app.register(Counter);

    type AddN = Command<"AddN"> & { counterId: string; n: number };
    const router = commandRouter<AddN>({
      AddN: { aggregate: Counter, id: (c) => c.counterId },
    });
    app.register(router);

    const id = randomUUID();
    // Lean overload: no aggregateType, no streamUuid -- the router
    // resolves AddN to (Counter, id).
    await app.dispatch<AddN>({ type: "AddN", counterId: id, n: 7 });
    // Verify the event landed on the derived stream "Counter-<id>".
    const rows = await app.client().readStream(`Counter-${id}`, 1n, 10);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].type, "Added");
    assert.deepEqual(rows[0].data, { n: 7 });
  });

  test("dispatch(command) without a registered router throws", async () => {
    const app = new Instructed({ db: pool });
    app.register(counter());
    await assert.rejects(
      () =>
        app.dispatch<Command<"Whatever">>({ type: "Whatever" } as never),
      /no command router registered/,
    );
  });

  test("register(router) rejects a second router registration", () => {
    const app = new Instructed({ db: pool });
    const router: CommandRouter = () => ({
      aggregateType: "X",
      aggregateId: "y",
    });
    app.register(router);
    assert.throws(
      () => app.register(router),
      /already registered/,
    );
  });
});
