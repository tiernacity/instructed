/**
 * Composed-concurrency correctness tests (TODO #3a).
 *
 * Each of the existing test files exercises one mechanism under
 * concurrency: `aggregate.test.ts` races two writers on one stream;
 * `subscription.test.ts` covers heartbeat-lease-loss for one worker;
 * the conformance harness races two `claim_subscription` calls. None
 * of them run the full system at once.
 *
 * The cases here wire several mechanisms together and assert
 * end-to-end invariants. They're deterministic-ish (N small, short
 * timeouts) and must run in seconds so they belong on the per-PR
 * path. The longer-running soak/load harness (TODO #3b) is a
 * separate piece of work.
 *
 * Scenarios:
 *
 *   1. Aggregate OCC × projector. N concurrent dispatchers force OCC
 *      retries on one aggregate; a projector on `$all` follows along.
 *      Final aggregate state (re-loaded from the events) must equal
 *      the projector's folded state. event_number must be globally
 *      gapless; stream_version on the aggregate stream must be 1..N.
 *
 *   2. Two projectors fighting for one subscription. Two workers
 *      claim the same (stream, name); short leases force a takeover
 *      mid-flight. `last_seen` must be monotone over the run; every
 *      event must be ack'd at least once; no two handlers may run
 *      the same event concurrently.
 *
 *   3. PM dispatching while projector observes. A PM listens on
 *      stream A and dispatches `add` commands to aggregate B; a
 *      concurrent appender adds to A; a projector on `$all`
 *      observes both. At quiescence: PM snapshot.source_version
 *      equals subscription.last_seen (PM-024); aggregate B's
 *      re-folded value equals the sum of triggers; the projector
 *      observed `count(A) + count(B)` events in order with no gaps.
 */

import { after, before, beforeEach, describe, test } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import pg from "pg";
import { closePool, getPool, truncateAll } from "./fixtures.ts";
import {
  Client,
  expected,
  runCommand,
  startProcessManager,
  startProjection,
} from "../src/index.ts";
import type {
  AggregateDefinition,
  DispatchedCommand,
  DomainEvent,
  ProcessManagerDefinition,
  ProjectionDefinition,
  RecordedEvent,
} from "../src/index.ts";

// ---------------------------------------------------------------------------
// Shared setup
// ---------------------------------------------------------------------------

let pool: pg.Pool;
let dispatchPool: pg.Pool;
let client: Client;
let dispatchClient: Client;

before(async () => {
  pool = await getPool();
  // PM needs a separate dispatch client (D-0011 / D-0012). Same DB,
  // different pool / connection.
  dispatchPool = new pg.Pool({
    host: process.env.PGHOST ?? "127.0.0.1",
    port: Number(process.env.PGPORT ?? 5432),
    user: process.env.PGUSER ?? "postgres",
    password: process.env.PGPASSWORD ?? "postgres",
    database: process.env.PGDATABASE ?? "instructed_test",
    max: 4,
  });
  client = new Client(pool);
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
  timeoutMs: number,
  label: string,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise((r) => setTimeout(r, 20));
  }
  throw new Error(`timeout waiting for ${label}`);
}

// A minimal counter aggregate, shared across scenarios.
interface CounterState {
  value: number;
}
type CounterCommand = { kind: "add"; n: number };
interface CounterEvent extends DomainEvent {
  type: "Added";
  data: { n: number };
}
function counter(): AggregateDefinition<
  CounterState,
  CounterCommand,
  CounterEvent
> {
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

/** Re-fold an aggregate from the stored events (no SDK cache). */
async function foldStream(streamUuid: string): Promise<CounterState> {
  let state: CounterState = { value: 0 };
  let from = 1n;
  while (true) {
    const rows = await client.readStream<{ n: number }>(streamUuid, from, 500);
    if (rows.length === 0) break;
    for (const r of rows) {
      if (r.event_type === "Added") {
        state = { value: state.value + r.data.n };
      }
    }
    from = rows[rows.length - 1].stream_version + 1n;
    if (rows.length < 500) break;
  }
  return state;
}

// ---------------------------------------------------------------------------
// Scenario 1: aggregate OCC × projector
// ---------------------------------------------------------------------------

describe("concurrent — aggregate OCC × projector", () => {
  // N concurrent commands on one aggregate force the SDK's OCC retry
  // loop. While that's happening, a projector follows `$all`. The
  // projector's folded state at quiescence MUST equal the aggregate's
  // re-loaded state. If anything in the OCC retry loop drops or
  // duplicates an event, this assert catches it.
  test("N concurrent commands: projector's folded state matches the aggregate", async () => {
    const N = 12;
    const stream = randomUUID();
    const Counter = counter();

    // Capture every event the projector sees, in delivery order.
    const projected: Array<{
      eventNumber: bigint;
      streamUuid: string;
      streamVersion: bigint;
      n: number;
    }> = [];

    const projection: ProjectionDefinition = {
      name: "p-counter",
      stream: "$all",
      startFrom: "origin",
      async handle(event: RecordedEvent) {
        if (event.event_type !== "Added") return;
        projected.push({
          eventNumber: event.event_number,
          streamUuid: event.stream_uuid,
          streamVersion: event.stream_version,
          n: (event.data as { n: number }).n,
        });
      },
    };

    const worker = startProjection(client, projection, {
      pollInterval: 25,
      leaseSeconds: 10,
    });

    try {
      // Fire N commands concurrently. Default retryBudget (5) is
      // enough; each loser reloads and retries.
      const values = Array.from({ length: N }, (_, i) => i + 1);
      // With N=12 contenders the default retryBudget (5) is too
      // tight — under pathological scheduling a writer can lose
      // more than 5 races in a row. Give every writer enough head
      // room to win once at least one slot is open. This is a test
      // tuning choice, not an SDK guarantee.
      const results = await Promise.all(
        values.map((n) =>
          runCommand(
            client,
            Counter,
            stream,
            { kind: "add", n } as CounterCommand,
            { retryBudget: N * 4 },
          ),
        ),
      );
      // Each command appended exactly one event.
      assert.equal(
        results.reduce((a, r) => a + r.length, 0),
        N,
        "every command must have appended exactly one event",
      );

      // Aggregate's stream must be 1..N with no gaps (proves the OCC
      // retry never appended twice and never skipped).
      const events = await client.readStream(stream, 1n, N * 2);
      assert.equal(events.length, N, `stream length: ${events.length} != ${N}`);
      for (let i = 0; i < N; i++) {
        assert.equal(
          events[i].stream_version,
          BigInt(i + 1),
          `gap at index ${i}: stream_version=${events[i].stream_version}`,
        );
      }

      // Re-fold the aggregate from disk (no cache).
      const expectedSum = values.reduce((a, n) => a + n, 0);
      const reloaded = await foldStream(stream);
      assert.equal(reloaded.value, expectedSum, "re-folded aggregate value");

      // Wait for the projector to catch up to the last appended event.
      const lastEn = events[events.length - 1].event_number;
      await waitFor(
        async () => {
          const pos = await client.readSubscriptionPosition(
            "$all",
            "p-counter",
          );
          return pos.lastSeen >= lastEn;
        },
        10_000,
        `projector to reach event_number ${lastEn}`,
      );

      // Filter to events on our test stream (other tests may share
      // the DB; truncateAll mitigates this, but be defensive).
      const ours = projected.filter((p) => p.streamUuid === stream);
      assert.equal(ours.length, N, "projector saw every aggregate event");

      // event_number strictly monotone across the projector's view.
      for (let i = 1; i < projected.length; i++) {
        assert.ok(
          projected[i].eventNumber > projected[i - 1].eventNumber,
          `event_number went backwards at index ${i}: ` +
            `${projected[i - 1].eventNumber} -> ${projected[i].eventNumber}`,
        );
      }

      // The projector's folded sum equals the aggregate's value.
      const projectedSum = ours.reduce((a, p) => a + p.n, 0);
      assert.equal(
        projectedSum,
        reloaded.value,
        "projector and aggregate disagree on final value",
      );
    } finally {
      await worker.close();
    }
  });
});

// ---------------------------------------------------------------------------
// Scenario 2: two projectors fighting for one subscription
// ---------------------------------------------------------------------------

describe("concurrent — two projectors, one subscription", () => {
  // Two workers race for the same (stream, name). At any moment only
  // one holds the lease; the other sees `already_claimed` (the SDK
  // surfaces this as the worker polling and waiting). Forcing a
  // takeover mid-flight must not lose events and must not let two
  // handlers run the same event concurrently.
  test("forced takeover: last_seen monotone, every event ack'd, no concurrent dual-delivery", async () => {
    const N = 20;
    const stream = randomUUID();

    // Seed the stream first so subscriptions can be claimed against it.
    await client.appendToStream(
      stream,
      expected.noStream,
      Array.from({ length: N }, (_, i) => ({
        event_type: "E",
        data: { i: i + 1 },
      })),
    );

    // Track events seen by each worker, plus an "in-flight" set to
    // catch concurrent dual-delivery of the same event.
    const seenA: bigint[] = [];
    const seenB: bigint[] = [];
    const inFlight = new Set<string>();
    const concurrencyViolations: string[] = [];

    function makeDef(
      name: string,
      seen: bigint[],
    ): ProjectionDefinition {
      return {
        name: "p-shared",
        stream,
        startFrom: "origin",
        async handle(event: RecordedEvent) {
          const key = event.event_id;
          if (inFlight.has(key)) {
            concurrencyViolations.push(`${name} re-entered event ${key}`);
          }
          inFlight.add(key);
          try {
            // Tiny synthetic delay so the window for dual-delivery is
            // observable. Without this, two workers would still each
            // process events serially and the in-flight check would
            // trivially never fire.
            await new Promise((r) => setTimeout(r, 5));
            seen.push(event.stream_version);
          } finally {
            inFlight.delete(key);
          }
        },
      };
    }

    // Sample last_seen periodically and assert monotonicity.
    const lastSeenSamples: bigint[] = [];
    const samplerStop = { stop: false };
    const sampler = (async () => {
      while (!samplerStop.stop) {
        try {
          const pos = await client.readSubscriptionPosition(stream, "p-shared");
          lastSeenSamples.push(pos.lastSeen);
        } catch {
          // Subscription not yet created on the first tick; ignore.
        }
        await new Promise((r) => setTimeout(r, 25));
      }
    })();

    const wA = startProjection(client, makeDef("A", seenA), {
      workerId: "worker-A",
      pollInterval: 25,
      leaseSeconds: 2,
      heartbeatInterval: 500,
    });
    const wB = startProjection(client, makeDef("B", seenB), {
      workerId: "worker-B",
      pollInterval: 25,
      leaseSeconds: 2,
      heartbeatInterval: 500,
    });

    try {
      // Let one worker make some progress.
      await waitFor(
        () => seenA.length + seenB.length >= 5,
        5_000,
        "first 5 events delivered to some worker",
      );

      // Force a takeover by stealing the lease via direct SQL. The
      // current holder's next heartbeat will get IS022, abort its
      // signal, and the other worker (or a new lease cycle on the
      // original) picks it up.
      await pool.query(
        `UPDATE instructed.subscriptions s
            SET claim_expires_at = now() - interval '1 second'
          WHERE s.stream_id = (
            SELECT stream_id FROM instructed.streams WHERE stream_uuid = $1
          ) AND s.subscription_name = 'p-shared'`,
        [stream],
      );

      // Wait for the cursor to reach N.
      await waitFor(
        async () => {
          const pos = await client.readSubscriptionPosition(stream, "p-shared");
          return pos.lastSeen >= BigInt(N);
        },
        10_000,
        `last_seen to reach ${N}`,
      );

      // No two handlers ran the same event concurrently.
      assert.deepEqual(
        concurrencyViolations,
        [],
        "expected no concurrent dual-delivery",
      );

      // At least one event was ack'd (trivially true) and every
      // event 1..N was delivered to *some* worker. Redelivery
      // (same stream_version appearing twice in the union of
      // seenA + seenB) is allowed — at-least-once.
      const union = new Set([...seenA, ...seenB].map((v) => v.toString()));
      for (let v = 1n; v <= BigInt(N); v++) {
        assert.ok(
          union.has(v.toString()),
          `event with stream_version=${v} was never delivered`,
        );
      }

      // last_seen monotonically non-decreasing across the run.
      for (let i = 1; i < lastSeenSamples.length; i++) {
        assert.ok(
          lastSeenSamples[i] >= lastSeenSamples[i - 1],
          `last_seen went backwards at sample ${i}: ` +
            `${lastSeenSamples[i - 1]} -> ${lastSeenSamples[i]}`,
        );
      }
    } finally {
      samplerStop.stop = true;
      await sampler;
      await wA.close();
      await wB.close();
    }
  });
});

// ---------------------------------------------------------------------------
// Scenario 3: PM dispatching while projector observes
// ---------------------------------------------------------------------------

describe("concurrent — PM × appender × projector", () => {
  // Setup:
  //   - Trigger stream A: a concurrent appender adds `Triggered{n}` events.
  //   - PM on $all: routes each Triggered → dispatches add{n} to stream B.
  //   - Aggregate B (Counter): receives `add` commands.
  //   - Projector on $all: observes every event (A and B).
  //
  // Invariants at quiescence:
  //   (a) PM snapshot.source_version equals event_number of the
  //       last Triggered event the PM processed (PM-020). After the
  //       PM also processes its own dispatched `Added` events (which
  //       its routes ignore), last_seen advances past source_version;
  //       PM-024 is the weaker relation source_version <= last_seen.
  //   (b) Aggregate B's re-folded value = sum of trigger n's.
  //   (c) Projector saw count(A) + count(B) events with strictly
  //       monotone event_number.
  test("PM-020/024 + cross-stream agreement under concurrent appends", async () => {
    const N = 8;
    const triggerStream = randomUUID();
    const targetStream = randomUUID();
    const pmName = `pm-${randomUUID().slice(0, 8)}`;
    const processId = randomUUID();
    const Counter = counter();

    // PM state: forwarded count.
    interface PmState {
      forwarded: number;
    }
    const pmDef: ProcessManagerDefinition<PmState> = {
      name: pmName,
      stream: "$all",
      routes: {
        Triggered: () => ({ kind: "continue", processId }),
      },
      initialState: () => ({ forwarded: 0 }),
      async handle(state, event) {
        const n = (event.data as { n: number }).n;
        const commands: DispatchedCommand[] = [
          {
            streamUuid: targetStream,
            aggregate: Counter,
            command: { kind: "add", n } as CounterCommand,
          },
        ];
        return {
          state: { forwarded: state.forwarded + 1 },
          commands,
        };
      },
    };

    // Projector observes everything on $all.
    const projected: bigint[] = [];
    const projDef: ProjectionDefinition = {
      name: "p-all",
      stream: "$all",
      startFrom: "origin",
      async handle(event: RecordedEvent) {
        projected.push(event.event_number);
      },
    };

    const pmWorker = startProcessManager(client, dispatchClient, pmDef, {
      pollInterval: 25,
      leaseSeconds: 10,
      heartbeatInterval: 1_000,
    });
    const projWorker = startProjection(client, projDef, {
      pollInterval: 25,
      leaseSeconds: 10,
    });

    try {
      // Appender: fire N Triggered events concurrently against the
      // trigger stream. Use 'any' so they don't fight over a version
      // (the PM doesn't care about ordering on the trigger stream).
      const values = Array.from({ length: N }, (_, i) => i + 1);
      const appendTasks = values.map((n) =>
        client.appendToStream(triggerStream, expected.any, [
          { event_type: "Triggered", data: { n } },
        ]),
      );
      await Promise.all(appendTasks);

      // Wait for: aggregate B to have received N events (one per
      // trigger). The PM is single-active on $all; it dispatches
      // commands serially per its own loop, so B's stream grows
      // monotonically as the PM works through the triggers.
      await waitFor(
        async () => {
          try {
            const rows = await client.readStream(targetStream, 1n, N * 2);
            return rows.length >= N;
          } catch (err) {
            // Stream not yet created — fine, keep waiting.
            if ((err as { code?: string }).code === "IS003") return false;
            throw err;
          }
        },
        15_000,
        `aggregate B to receive ${N} commands`,
      );

      // Wait for the projector to be caught up to the head of $all.
      // Head = N triggers + N dispatched events = 2N events.
      await waitFor(
        async () => {
          const head = await pool.query<{ event_number: string }>(
            `SELECT COALESCE(MAX(stream_version), 0)::text AS event_number
               FROM instructed.stream_events WHERE stream_id = 0`,
          );
          const max = BigInt(head.rows[0].event_number);
          const pos = await client.readSubscriptionPosition("$all", "p-all");
          return pos.lastSeen >= max && max === BigInt(2 * N);
        },
        15_000,
        `projector caught up to event_number ${2 * N}`,
      );

      // Wait for the PM to be fully caught up too (cursor at 2N — it
      // processes its own dispatched events on $all and treats them
      // as ignored routes, since `Added` is not in `routes`).
      await waitFor(
        async () => {
          const pos = await client.readSubscriptionPosition("$all", pmName);
          return pos.lastSeen >= BigInt(2 * N);
        },
        15_000,
        `PM caught up to event_number ${2 * N}`,
      );

      // ---- Invariant (b): aggregate B's value equals sum of triggers.
      const expectedSum = values.reduce((a, n) => a + n, 0);
      const reloaded = await foldStream(targetStream);
      assert.equal(
        reloaded.value,
        expectedSum,
        "aggregate B value disagrees with sum of triggers",
      );

      // ---- Invariant (c): projector saw 2N events with monotone EN.
      assert.equal(
        projected.length,
        2 * N,
        `projector saw ${projected.length} events, expected ${2 * N}`,
      );
      for (let i = 1; i < projected.length; i++) {
        assert.ok(
          projected[i] > projected[i - 1],
          `projector event_number not monotone at index ${i}: ` +
            `${projected[i - 1]} -> ${projected[i]}`,
        );
      }

      // ---- Invariant (a) / PM-020: snapshot.source_version equals
      //      the event_number of the last *triggering* event the PM
      //      acted on. This is NOT the same as subscription.last_seen
      //      once the PM has processed events its routes ignore (the
      //      cursor advances past them, the snapshot does not).
      //
      //      PM-024 says source_version doubles as a redelivery marker
      //      on restart — i.e. on re-claim the PM uses the snapshot's
      //      source_version, not the subscription's last_seen, to
      //      know which events have already been folded into state.
      //      So source_version <= last_seen is the correct relation,
      //      with equality only when the most recent processed event
      //      was routed (not ignored).
      const pmPos = await client.readSubscriptionPosition("$all", pmName);
      const snap = await client.readSnapshot<PmState>(`${pmName}-${processId}`);

      // The last Triggered event's event_number on $all.
      const lastTrigger = await pool.query<{ event_number: string }>(
        `SELECT MAX(se.stream_version)::text AS event_number
           FROM instructed.stream_events se
           JOIN instructed.events e USING (event_id)
          WHERE se.stream_id = 0 AND e.event_type = 'Triggered'`,
      );
      const lastTriggerEn = BigInt(lastTrigger.rows[0].event_number);

      assert.equal(
        snap.sourceVersion,
        lastTriggerEn,
        `PM-020 violated: snapshot.source_version=${snap.sourceVersion} ` +
          `!= event_number of last Triggered=${lastTriggerEn}`,
      );
      assert.ok(
        snap.sourceVersion <= pmPos.lastSeen,
        `PM-024 redelivery relation violated: source_version=${snap.sourceVersion} ` +
          `must be <= last_seen=${pmPos.lastSeen}`,
      );
      assert.equal(
        snap.data.forwarded,
        N,
        "PM forwarded count disagrees with trigger count",
      );
    } finally {
      await pmWorker.close();
      await projWorker.close();
    }
  });
});
