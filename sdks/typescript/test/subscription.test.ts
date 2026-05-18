/**
 * Layer 2: projection-worker tests.
 *
 * The required case for step 3 is heartbeat-lease-loss: a worker whose
 * lease is taken away mid-flight must abort its signal, fire onError,
 * and exit cleanly via `stopped`, all without advancing the cursor.
 * Surrounding cases cover the happy path, the selector contract (cursor
 * advances past skipped events per OQ-0003 resolution / §7), graceful
 * close, and handler-throws backoff (§11.5).
 */

import { after, before, beforeEach, describe, test } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { closePool, getPool, truncateAll } from "./fixtures.ts";
import {
  Client,
  expected,
  HandlerError,
  startProjection,
  SubscriptionLeaseLost,
} from "../src/index.ts";
import type {
  HandlerContext,
  ProjectionDefinition,
  RecordedEvent,
  RunningWorker,
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

/**
 * Wait for a predicate to become true, polling every 10ms.
 * Throws if `timeoutMs` elapses first.
 */
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

/** Append N events to a fresh stream. */
async function append(streamUuid: string, types: string[]): Promise<void> {
  await client.appendToStream(
    streamUuid,
    expected.any,
    types.map((t) => ({ event_type: t, data: {} })),
  );
}

// ---------------------------------------------------------------------------

describe("startProjection — happy path", () => {
  test("delivers events in order; advances the cursor", async () => {
    const stream = randomUUID();
    await append(stream, ["A", "B", "C"]);

    const seen: string[] = [];
    const def: ProjectionDefinition = {
      name: "p1",
      stream,
      startFrom: "origin",
      async handle(event) {
        seen.push(event.event_type);
      },
    };
    const w = startProjection(client, def, {
      pollInterval: 25,
      leaseSeconds: 30,
    });
    try {
      await waitFor(() => seen.length === 3, 3_000, "all 3 events delivered");
      // Cursor advanced past v3.
      await waitFor(async () => {
        const r = await client.readSubscriptionPosition(stream, "p1");
        return r.lastSeen === 3n;
      }, 3_000, "cursor at 3");
      assert.deepEqual(seen, ["A", "B", "C"]);
    } finally {
      await w.close();
    }
  });

  test("startFrom: 'current' skips pre-existing events", async () => {
    const stream = randomUUID();
    await append(stream, ["A", "B"]); // pre-existing, version 1, 2

    const seen: string[] = [];
    const def: ProjectionDefinition = {
      name: "p1",
      stream,
      startFrom: "current",
      async handle(event) {
        seen.push(event.event_type);
      },
    };
    const w = startProjection(client, def, { pollInterval: 25 });
    try {
      // give the worker a moment to claim + poll
      await new Promise((r) => setTimeout(r, 100));
      await append(stream, ["C"]);
      await waitFor(() => seen.length === 1, 3_000, "C delivered");
      assert.deepEqual(seen, ["C"]);
    } finally {
      await w.close();
    }
  });
});

describe("startProjection — selector (SDK-side)", () => {
  test("cursor advances past skipped events; handler only sees matches", async () => {
    const stream = randomUUID();
    await append(stream, ["Keep", "Skip", "Keep", "Skip", "Keep"]);

    const seen: string[] = [];
    const def: ProjectionDefinition = {
      name: "p1",
      stream,
      startFrom: "origin",
      selector: (e) => e.event_type === "Keep",
      async handle(event) {
        seen.push(event.event_type);
      },
    };
    const w = startProjection(client, def, { pollInterval: 25 });
    try {
      await waitFor(() => seen.length === 3, 3_000, "3 Keeps delivered");
      // Cursor advanced past all 5 events, not just the 3 matches
      // (INV-SUB-P-050 / §7).
      await waitFor(async () => {
        const r = await client.readSubscriptionPosition(stream, "p1");
        return r.lastSeen === 5n;
      }, 3_000, "cursor at 5");
      assert.deepEqual(seen, ["Keep", "Keep", "Keep"]);
    } finally {
      await w.close();
    }
  });
});

describe("startProjection — close()", () => {
  test("close() is idempotent; stopped resolves; lease is released", async () => {
    const stream = randomUUID();
    await append(stream, ["A"]);
    const def: ProjectionDefinition = {
      name: "p1",
      stream,
      startFrom: "origin",
      async handle() {
        /* no-op */
      },
    };
    const w = startProjection(client, def, { pollInterval: 25 });
    // Give it time to claim.
    await new Promise((r) => setTimeout(r, 100));

    const p1 = w.close();
    const p2 = w.close();
    assert.equal(p1, p2, "close() must return the same promise");
    await p1;
    await w.close(); // third call still resolves
    // Subscription row should be unclaimed (claimed_by NULL) after release.
    const { rows } = await pool.query<{ claimed_by: string | null }>(
      `SELECT s.claimed_by
         FROM instructed.subscriptions s
         JOIN instructed.streams st ON st.stream_id = s.stream_id
        WHERE st.stream_uuid = $1 AND s.subscription_name = 'p1'`,
      [stream],
    );
    assert.equal(rows[0].claimed_by, null);
  });
});

describe("startProjection — heartbeat-lease-loss (D-0018 / §11.9)", () => {
  test("external lease takeover trips the heartbeat → onError + stopped", async () => {
    const stream = randomUUID();
    await append(stream, ["A", "B", "C"]);

    const errors: Error[] = [];
    const handlerStarted = { count: 0 };
    let resolveHandlerStarted: () => void = () => {};
    const handlerStartedSignal = new Promise<void>((r) => {
      resolveHandlerStarted = r;
    });
    let observedSignalAbort = false;

    const def: ProjectionDefinition = {
      name: "p1",
      stream,
      startFrom: "origin",
      async handle(_event, ctx: HandlerContext) {
        handlerStarted.count += 1;
        if (handlerStarted.count === 1) resolveHandlerStarted();
        // Hang the first handler invocation until the SDK's signal fires.
        // Per §11.1 the abort fires immediately; we observe it here.
        await new Promise<void>((resolve) => {
          if (ctx.signal.aborted) {
            observedSignalAbort = true;
            resolve();
            return;
          }
          ctx.signal.addEventListener(
            "abort",
            () => {
              observedSignalAbort = true;
              resolve();
            },
            { once: true },
          );
        });
      },
    };

    const w = startProjection(client, def, {
      pollInterval: 25,
      leaseSeconds: 2,
      heartbeatInterval: 300,
      onError: (err) => errors.push(err),
    });

    try {
      // Wait for the worker to claim the lease and start the first handler.
      await handlerStartedSignal;

      // Forcibly transfer the lease to a different worker via direct SQL.
      // The next heartbeat fires extend_subscription_claim from "p1"'s
      // workerId and gets IS022.
      await pool.query(
        `UPDATE instructed.subscriptions s
            SET claimed_by = 'intruder',
                claim_expires_at = now() + interval '1 hour'
          WHERE s.stream_id = (SELECT stream_id FROM instructed.streams
                                 WHERE stream_uuid = $1)
            AND s.subscription_name = 'p1'`,
        [stream],
      );

      // The next heartbeat tick (~300ms) sees IS022, aborts the signal,
      // fires onError, and exits the loop. `stopped` resolves.
      await w.stopped;

      assert.ok(observedSignalAbort, "handler should have observed abort");
      assert.ok(
        errors.some((e) => e instanceof SubscriptionLeaseLost),
        `expected onError to include SubscriptionLeaseLost; got ${errors
          .map((e) => e.constructor.name)
          .join(",")}`,
      );

      // Cursor MUST NOT have advanced (the handler was aborted before
      // the SDK could call advance_subscription). The new holder
      // ("intruder") would redeliver from 0.
      const pos = await client.readSubscriptionPosition(stream, "p1");
      assert.equal(pos.lastSeen, 0n);
    } finally {
      await w.close();
    }
  });
});

describe("startProjection — handler-throws backoff (§11.5)", () => {
  test("retries the same event after backoff; onError fires; eventually advances", async () => {
    const stream = randomUUID();
    await append(stream, ["A"]);

    let attempts = 0;
    const errors: Error[] = [];

    const def: ProjectionDefinition = {
      name: "p1",
      stream,
      startFrom: "origin",
      async handle(_event) {
        attempts += 1;
        if (attempts < 2) throw new Error("boom");
        // second call succeeds
      },
    };
    const w = startProjection(client, def, {
      pollInterval: 25,
      leaseSeconds: 30,
      onError: (e) => errors.push(e),
    });
    try {
      await waitFor(
        async () => {
          const r = await client.readSubscriptionPosition(stream, "p1");
          return r.lastSeen === 1n;
        },
        5_000,
        "cursor advances to 1 after retry",
      );
      assert.ok(attempts >= 2);
      assert.ok(
        errors.some((e) => e instanceof HandlerError),
        "expected HandlerError in onError",
      );
      const he = errors.find((e) => e instanceof HandlerError) as HandlerError;
      assert.ok(he.cause instanceof Error);
      assert.equal((he.cause as Error).message, "boom");
    } finally {
      await w.close();
    }
  });
});

describe("startProjection — context shape (§11.1)", () => {
  test("HandlerContext carries workerId, position, and an AbortSignal — and nothing else of substance", async () => {
    const stream = randomUUID();
    await append(stream, ["A"]);

    let captured: HandlerContext | null = null;
    let capturedEvent: RecordedEvent | null = null;
    const def: ProjectionDefinition = {
      name: "p1",
      stream,
      startFrom: "origin",
      async handle(event, ctx) {
        captured = ctx;
        capturedEvent = event;
      },
    };
    const w = startProjection(client, def, {
      pollInterval: 25,
      workerId: "test-worker-1",
    });
    try {
      await waitFor(() => captured !== null, 3_000, "handler invoked");
      const ctx = captured!;
      const event = capturedEvent!;
      assert.equal(ctx.workerId, "test-worker-1");
      assert.equal(ctx.position.eventNumber, event.event_number);
      assert.equal(ctx.position.streamVersion, event.stream_version);
      assert.ok(ctx.signal instanceof AbortSignal);
      assert.equal(ctx.signal.aborted, false);
      // No leaked plumbing (D-0016 / NG-0015): the context has exactly
      // these three keys.
      assert.deepEqual(
        Object.keys(ctx).sort(),
        ["position", "signal", "workerId"],
      );
    } finally {
      await w.close();
    }
  });
});
