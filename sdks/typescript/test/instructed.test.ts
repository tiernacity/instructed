/**
 * Layer 5: `Instructed` facade tests -- facade-specific behaviour only.
 *
 * Underlying correctness is covered by the layered tests (client,
 * aggregate, routing-worker, processing-worker, projection-worker,
 * pm-worker, consistency). Cases here:
 *
 *   - registerAggregate + dispatch (by-name lookup, UnknownAggregateType)
 *   - registerProjection: partitionBy / routeFn mutual exclusivity
 *   - startWorker fans out routing+processing workers for a
 *     projection and a PM under one handle; close() stops them all
 *   - lazy dispatch-pool materialisation (no second pool until a PM
 *     is registered)
 *   - dispatch( ... , { consistency: [...] }) waits for the
 *     projection to catch up
 */

import { after, before, beforeEach, describe, test } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { closePool, getPool, truncateAll } from "./fixtures.ts";
import pg from "pg";
import {
  expected,
  Instructed,
  UnknownAggregateType,
} from "../src/index.ts";
import type {
  AggregateDefinition,
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
      return { event_type: "Added", data: { n: c.n } };
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
    try {
      const Counter = counter();
      app.registerAggregate(Counter);

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
    } finally {
      await app.close();
    }
  });

  test("does not own the user-supplied Pool; close() leaves it usable", async () => {
    const app = new Instructed({ db: pool });
    app.registerAggregate(counter());
    await app.dispatch<CounterCommand>("Counter", randomUUID(), { kind: "add", n: 1 });
    await app.close();
    const r = await pool.query("SELECT 1 AS x");
    assert.equal(r.rows[0].x, 1);
  });
});

// ---------------------------------------------------------------------------

describe("Instructed -- registerProjection validation", () => {
  test("registerProjection rejects mutually-exclusive partitionBy + routeFn", () => {
    const app = new Instructed({ db: pool });
    try {
      assert.throws(
        () =>
          app.registerProjection("p", {
            partitionBy: { kind: "sequential" },
            routeFn: () => ({ partitionKey: "k" }),
            async handler() {},
          }),
        /mutually exclusive/,
      );
    } finally {
      void app.close();
    }
  });
});

// ---------------------------------------------------------------------------

describe("Instructed -- startWorker fan-out", () => {
  test("runs a registered projection and PM under one handle; close stops both", async () => {
    const dispatchPool = new pg.Pool({
      host: process.env.PGHOST ?? "127.0.0.1",
      port: Number(process.env.PGPORT ?? 5432),
      user: process.env.PGUSER ?? "postgres",
      password: process.env.PGPASSWORD ?? "postgres",
      database: process.env.PGDATABASE ?? "instructed_test",
      max: 4,
    });
    const app = new Instructed({ db: pool, dispatchDb: dispatchPool });

    const Counter = counter();
    app.registerAggregate(Counter);

    // A projection on $all that counts events of type "Triggered".
    // SUB-A: legacy `selector` is recovered via a routeFn that
    // returns `"ignore"` for the would-be-skipped events.
    const projName = `proj-${randomUUID().slice(0, 8)}`;
    let projSeen = 0;
    app.registerProjection(
      projName,
      {
        routeFn: (e) =>
          e.event_type === "Triggered"
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
    app.registerProcessManager<{ done: boolean }>(
      pmName,
      {
        routeFn: (e) =>
          e.event_type === "Triggered"
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

    const handle = await app.startWorker();
    try {
      const trigger = randomUUID();
      await app.client().appendToStream(trigger, expected.noStream, [
        { event_type: "Triggered", data: { processId: randomUUID() } },
        { event_type: "Triggered", data: { processId: randomUUID() } },
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
      await app.close();
      await dispatchPool.end();
    }
  });

  test("throws when no projections or PMs are registered", async () => {
    const app = new Instructed({ db: pool });
    try {
      await assert.rejects(() => app.startWorker(), /no projections or process managers/);
    } finally {
      await app.close();
    }
  });
});

// ---------------------------------------------------------------------------

describe("Instructed -- lazy dispatch pool", () => {
  test("does not materialise dispatch pool until a PM is registered", async () => {
    const app = new Instructed({ db: pool });
    app.registerAggregate(counter());
    // Touch persist client through dispatch; the dispatch pool stays null.
    await app.dispatch<CounterCommand>("Counter", randomUUID(), { kind: "add", n: 1 });

    // No PM registered yet; `dispatchClient` would have to materialise
    // a sibling pool -- but since `db` is a Pool/Queryable with no
    // connection string, the only safe behaviour is to throw.
    let caught: unknown;
    try {
      app.dispatchClient();
    } catch (e) {
      caught = e;
    }
    assert.ok(caught instanceof Error, `expected an Error, got ${caught}`);
    assert.ok(
      (caught as Error).message.includes("must also be supplied"),
      `unexpected message: ${(caught as Error).message}`,
    );

    await app.close();
  });

  test("materialises dispatch pool on first registerProcessManager when given a connection string", async () => {
    const connString = `postgresql://${process.env.PGUSER ?? "postgres"}:${process.env.PGPASSWORD ?? "postgres"}@${process.env.PGHOST ?? "127.0.0.1"}:${Number(process.env.PGPORT ?? 5432)}/${process.env.PGDATABASE ?? "instructed_test"}`;
    const app = new Instructed({ db: connString });
    try {
      app.registerProcessManager<{}>(
        `pm-${randomUUID().slice(0, 8)}`,
        {
          routeFn: () => "ignore",
          initialState: () => ({}),
          apply: (s) => s,
          async handle() {
            return {};
          },
        },
      );
      // Dispatch client exists and is distinct from the persist client.
      assert.notEqual(app.dispatchClient(), app.client());
    } finally {
      await app.close();
    }
  });
});

// ---------------------------------------------------------------------------

describe("Instructed -- dispatch consistency wait", () => {
  test("waits for a named subscription to catch up before returning", async () => {
    const app = new Instructed({ db: pool });
    app.registerAggregate(counter());

    const projName = `proj-${randomUUID().slice(0, 8)}`;
    let seen = 0;
    app.registerProjection(
      projName,
      {
        routeFn: (e: RecordedEvent) =>
          e.event_type === "Added" ? { partitionKey: "_default" } : "ignore",
        async handler() {
          seen++;
        },
      },
      { pollInterval: 25, heartbeatInterval: 1_000 },
    );
    const handle = await app.startWorker();
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
      await app.close();
    }
  });
});
