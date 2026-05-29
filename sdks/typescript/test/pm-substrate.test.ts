/**
 * PM L2/L3 split (step-5 follow-on, 2026-05-27) — substrate tests.
 *
 * The user-facing `startPmWorker` is exhaustively tested in
 * `pm-worker.test.ts`; this file pins down the L2 substrate
 * (`startPmSubstrate`) as a callable surface in its own right.
 *
 * Why this matters: a porter writing a Python / Go / Elixir SDK
 * reproduces the *substrate* (snapshot+ack lifecycle, rebuild,
 * lease management) and may ship a *different* command-dispatch
 * convenience on top. The substrate's contract — `handle` returns
 * `{ complete? }` only; no `commands` field; side effects happen
 * inside `handle` — is what these tests lock down.
 */

import { after, before, beforeEach, describe, test } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { closePool, getPool, truncateAll } from "./fixtures.ts";
import { Client, expected } from "../src/index.ts";
import { startRoutingWorker } from "../src/routing-worker.ts";
import {
  startPmSubstrate,
  type PmSubstrateDefinition,
} from "../src/pm-substrate.ts";
import type { RecordedEvent } from "../src/types/index.ts";
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

// ---- helpers ---------------------------------------------------------------

interface CounterState {
  value: number;
}

/** Event union; `E` in `PmSubstrateDefinition<S, E>`. The substrate's
 *  generic is the event union (each member compatible with `Event`),
 *  not the data shape alone — see `RecordedEvent<E>` in src/types.ts. */
type CounterEvent = { type: "Tick"; data: { by: number } };

async function appendN(
  events: Array<{ type: string; data: unknown }>,
): Promise<{ stream: string; ens: bigint[] }> {
  const stream = `pm-sub-${randomUUID().slice(0, 8)}`;
  const rows = await client.appendToStream(
    stream,
    expected.any,
    events.map((e) => ({ type: e.type, data: e.data })),
  );
  return { stream, ens: rows.map((r) => r.event_number) };
}

async function waitFor<T>(
  predicate: () => Promise<T | null | undefined>,
  timeoutMs = 5_000,
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const v = await predicate();
    if (v !== null && v !== undefined) return v;
    await new Promise((r) => setTimeout(r, 25));
  }
  throw new Error("waitFor timed out");
}

async function workItemState(
  name: string,
  partitionKey: string,
  eventNumber: bigint,
): Promise<{ state: string } | null> {
  const r = await pool.query<{ state: string }>(
    `SELECT state FROM instructed.subscription_work_items
      WHERE subscription_name = $1 AND partition_key = $2 AND event_number = $3`,
    [name, partitionKey, eventNumber.toString()],
  );
  if (r.rowCount === 0) return null;
  return r.rows[0];
}

function startRouter(name: string): RunningWorker {
  return startRoutingWorker<CounterEvent>(client, {
    name,
    routeFn: () => ({ partitionKey: "p1" }),
  });
}

// ---- tests -----------------------------------------------------------------

describe("startPmSubstrate — L2 contract", () => {
  test("happy path: handle returning {} writes snapshot and acks", async () => {
    const name = `sub-happy-${randomUUID().slice(0, 8)}`;
    await appendN([{ type: "Tick", data: { by: 1 } }]);

    const def: PmSubstrateDefinition<CounterState, CounterEvent> = {
      type: name,
      initialState: () => ({ value: 0 }),
      apply: (s, e) => ({ value: s.value + e.data.by }),
      // The substrate's handle signature: returns only { complete? }.
      // Crucially, no `commands` field is accepted -- the substrate
      // is unopinionated about side effects.
      handle: () => ({}),
    };

    const router = startRouter(name);
    const worker = startPmSubstrate(client, def);

    try {
      const en = await waitFor(async () => {
        const row = await pool.query<{ event_number: string }>(
          `SELECT event_number::text FROM instructed.subscription_work_items
            WHERE subscription_name = $1 AND state = 'done'
            ORDER BY event_number LIMIT 1`,
          [name],
        );
        return row.rowCount && row.rowCount > 0 ? row.rows[0] : null;
      });

      const ws = await workItemState(name, "p1", BigInt(en.event_number));
      assert.equal(ws?.state, "done");

      const snap = await client.readSnapshot<CounterState>(`${name}-p1`);
      assert.equal(snap.data.value, 1);
      assert.equal(snap.sourceType, name);
    } finally {
      await worker.stop();
      await router.stop();
    }
  });

  test("complete: true causes complete_pm_instance (snapshot deleted)", async () => {
    const name = `sub-complete-${randomUUID().slice(0, 8)}`;
    await appendN([{ type: "Tick", data: { by: 7 } }]);

    const def: PmSubstrateDefinition<CounterState, CounterEvent> = {
      type: name,
      initialState: () => ({ value: 0 }),
      apply: (s, e) => ({ value: s.value + e.data.by }),
      handle: () => ({ complete: true }),
    };

    const router = startRouter(name);
    const worker = startPmSubstrate(client, def);

    try {
      // Wait for the partition's work items to be cleared by
      // complete_pm_instance.
      await waitFor(async () => {
        const r = await pool.query<{ n: string }>(
          `SELECT count(*)::text AS n FROM instructed.subscription_work_items
            WHERE subscription_name = $1 AND partition_key = 'p1'`,
          [name],
        );
        return r.rows[0].n === "0" ? true : null;
      });

      // And the snapshot row is gone too.
      await assert.rejects(
        () => client.readSnapshot<CounterState>(`${name}-p1`),
        (err: Error) => err.name === "SnapshotNotFound",
      );
    } finally {
      await worker.stop();
      await router.stop();
    }
  });

  test("substrate is unopinionated about side effects (handle can do its own work)", async () => {
    // Demonstrates that the substrate doesn't care what `handle`
    // does between receiving the event and returning -- the only
    // contract is the return shape. A porter could legitimately
    // implement command dispatch, external API calls, or anything
    // else inside `handle` without the substrate knowing.
    const name = `sub-sideeffects-${randomUUID().slice(0, 8)}`;
    await appendN([{ type: "Tick", data: { by: 3 } }]);

    let sideEffectRan = false;

    const def: PmSubstrateDefinition<CounterState, CounterEvent> = {
      type: name,
      initialState: () => ({ value: 0 }),
      apply: (s, e) => ({ value: s.value + e.data.by }),
      handle: async () => {
        // Arbitrary user-defined side effect. The substrate sees
        // only the eventual return.
        await new Promise((r) => setTimeout(r, 5));
        sideEffectRan = true;
        return {};
      },
    };

    const router = startRouter(name);
    const worker = startPmSubstrate(client, def);

    try {
      await waitFor(async () => {
        const snap = await client.readSnapshot<CounterState>(`${name}-p1`)
          .catch(() => null);
        return snap;
      });
      assert.equal(sideEffectRan, true);
    } finally {
      await worker.stop();
      await router.stop();
    }
  });
});
