/**
 * Layer 0 round-trip tests. Exercises every procedure and every
 * SQLSTATE in the closed catalogue, against the live Postgres provided
 * by `docker compose up -d postgres`.
 */

import { after, before, beforeEach, describe, test } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { closePool, getPool, truncateAll } from "./fixtures.ts";
import {
  AppendOnlyViolation,
  Client,
  DuplicateEvent,
  expected,
  InstructedError,
  InvalidParameterValue,
  ReservedStreamUuid,
  SnapshotNotFound,
  StreamExists,
  StreamNotFound,
  SubscriptionLeaseLost,
  SubscriptionNotFound,
  WrongExpectedVersion,
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

function ev(
  type: string,
  data: Record<string, unknown> = {},
  extra: Record<string, unknown> = {},
) {
  return { type, data, ...extra };
}

describe("Client.appendToStream", () => {
  test("creates a new stream with expected.noStream and returns rows", async () => {
    const streamUuid = randomUUID();
    const rows = await client.appendToStream(streamUuid, expected.noStream, [
      ev("Created", { name: "alice" }),
      ev("Renamed", { name: "bob" }),
    ]);
    assert.equal(rows.length, 2);
    assert.equal(rows[0].stream_version, 1n);
    assert.equal(rows[1].stream_version, 2n);
    assert.equal(rows[0].event_number, 1n);
    assert.equal(rows[1].event_number, 2n);
    assert.ok(rows[0].created_at instanceof Date);
    assert.ok(rows[0].event_id);
  });

  test("expected.any creates or extends a stream", async () => {
    const s = randomUUID();
    await client.appendToStream(s, expected.any, [ev("A")]);
    const rows = await client.appendToStream(s, expected.any, [ev("B")]);
    assert.equal(rows[0].stream_version, 2n);
  });

  test("expected.streamExists raises StreamNotFound on absent stream (IS003)", async () => {
    await assert.rejects(
      () =>
        client.appendToStream(randomUUID(), expected.streamExists, [ev("X")]),
      (err) => err instanceof StreamNotFound && err.code === "IS003",
    );
  });

  test("expected.noStream raises StreamExists when stream exists (IS002)", async () => {
    const s = randomUUID();
    await client.appendToStream(s, expected.noStream, [ev("A")]);
    await assert.rejects(
      () => client.appendToStream(s, expected.noStream, [ev("B")]),
      (err) => err instanceof StreamExists && err.code === "IS002",
    );
  });

  test("expected.exact mismatch raises WrongExpectedVersion (IS001)", async () => {
    const s = randomUUID();
    await client.appendToStream(s, expected.noStream, [ev("A")]);
    await assert.rejects(
      () => client.appendToStream(s, expected.exact(99n), [ev("B")]),
      (err) => {
        if (!(err instanceof WrongExpectedVersion)) return false;
        return err.code === "IS001" && err.actualVersion === 1n &&
          err.expectedVersion === 99n;
      },
    );
  });

  test("duplicate event_id across appends raises DuplicateEvent (IS004)", async () => {
    const s = randomUUID();
    const id = randomUUID();
    await client.appendToStream(s, expected.noStream, [
      { event_id: id, type: "A", data: {} },
    ]);
    const s2 = randomUUID();
    await assert.rejects(
      () =>
        client.appendToStream(s2, expected.noStream, [
          { event_id: id, type: "B", data: {} },
        ]),
      (err) => err instanceof DuplicateEvent && err.code === "IS004",
    );
  });

  test("'$all' stream uuid raises ReservedStreamUuid (IS005)", async () => {
    await assert.rejects(
      () => client.appendToStream("$all", expected.any, [ev("X")]),
      (err) => err instanceof ReservedStreamUuid && err.code === "IS005",
    );
  });

  test("empty events array raises InvalidParameterValue (22023)", async () => {
    await assert.rejects(
      () => client.appendToStream(randomUUID(), expected.any, []),
      (err) =>
        err instanceof InvalidParameterValue && err.code === "22023",
    );
  });

  test("fills event_id when omitted (§11.2)", async () => {
    const s = randomUUID();
    const rows = await client.appendToStream(s, expected.noStream, [
      { type: "A", data: {} },
    ]);
    assert.ok(rows[0].event_id);
    assert.match(rows[0].event_id, /^[0-9a-f-]{36}$/);
  });

  test("persists causation_id / correlation_id when supplied verbatim", async () => {
    const s = randomUUID();
    const cid = randomUUID();
    const corr = randomUUID();
    await client.appendToStream(s, expected.noStream, [
      {
        type: "A",
        data: {},
        causation_id: cid,
        correlation_id: corr,
      },
    ]);
    const events = await client.readStream(s, 0n, 10);
    assert.equal(events[0].causation_id, cid);
    assert.equal(events[0].correlation_id, corr);
  });
});

describe("Client.readStream / readAll", () => {
  test("readStream returns events in order from given version", async () => {
    const s = randomUUID();
    await client.appendToStream(s, expected.noStream, [
      ev("A"),
      ev("B"),
      ev("C"),
    ]);
    const rows = await client.readStream(s, 2n, 10);
    assert.equal(rows.length, 2);
    assert.equal(rows[0].type, "B");
    assert.equal(rows[0].stream_version, 2n);
    assert.equal(rows[1].type, "C");
  });

  test("readStream on missing stream raises StreamNotFound (IS003)", async () => {
    await assert.rejects(
      () => client.readStream(randomUUID(), 0n, 10),
      (err) => err instanceof StreamNotFound && err.code === "IS003",
    );
  });

  test("readStream rejects '$all' as ReservedStreamUuid (IS005)", async () => {
    await assert.rejects(
      () => client.readStream("$all", 0n, 10),
      (err) => err instanceof ReservedStreamUuid && err.code === "IS005",
    );
  });

  test("readStream with bad qty raises InvalidParameterValue (22023)", async () => {
    const s = randomUUID();
    await client.appendToStream(s, expected.noStream, [ev("A")]);
    await assert.rejects(
      () => client.readStream(s, 0n, 0),
      (err) => err instanceof InvalidParameterValue,
    );
  });

  test("readAll returns events with original stream identity", async () => {
    const a = randomUUID();
    const b = randomUUID();
    await client.appendToStream(a, expected.noStream, [ev("A1")]);
    await client.appendToStream(b, expected.noStream, [ev("B1")]);
    await client.appendToStream(a, expected.any, [ev("A2")]);

    const rows = await client.readAll(0n, 10);
    assert.equal(rows.length, 3);
    assert.equal(rows[0].stream_uuid, a);
    assert.equal(rows[0].stream_version, 1n);
    assert.equal(rows[0].event_number, 1n);
    assert.equal(rows[1].stream_uuid, b);
    assert.equal(rows[1].event_number, 2n);
    assert.equal(rows[2].stream_uuid, a);
    assert.equal(rows[2].stream_version, 2n);
    assert.equal(rows[2].event_number, 3n);
  });

  test("readAll bad qty raises InvalidParameterValue (22023)", async () => {
    await assert.rejects(
      () => client.readAll(0n, -1),
      (err) => err instanceof InvalidParameterValue,
    );
  });
});

describe("Client.snapshots", () => {
  test("record + read round-trip", async () => {
    await client.recordSnapshot({
      sourceUuid: "src-1",
      sourceType: "Account",
      sourceVersion: 7n,
      data: { balance: 42 },
      metadata: { hint: "x" },
    });
    const snap = await client.readSnapshot<{ balance: number }>("src-1");
    assert.equal(snap.sourceUuid, "src-1");
    assert.equal(snap.sourceType, "Account");
    assert.equal(snap.sourceVersion, 7n);
    assert.deepEqual(snap.data, { balance: 42 });
    assert.deepEqual(snap.metadata, { hint: "x" });
  });

  test("record_snapshot is an upsert", async () => {
    await client.recordSnapshot({
      sourceUuid: "src-1",
      sourceType: "T",
      sourceVersion: 1n,
      data: { v: 1 },
    });
    await client.recordSnapshot({
      sourceUuid: "src-1",
      sourceType: "T",
      sourceVersion: 2n,
      data: { v: 2 },
    });
    const s = await client.readSnapshot<{ v: number }>("src-1");
    assert.equal(s.sourceVersion, 2n);
    assert.deepEqual(s.data, { v: 2 });
  });

  test("readSnapshot raises SnapshotNotFound (IS010)", async () => {
    await assert.rejects(
      () => client.readSnapshot("nope"),
      (err) => err instanceof SnapshotNotFound && err.code === "IS010",
    );
  });

  test("deleteSnapshot is idempotent (INV-SNAP-004)", async () => {
    await client.deleteSnapshot("never-existed");
    await client.recordSnapshot({
      sourceUuid: "x",
      sourceType: "T",
      sourceVersion: 1n,
      data: {},
    });
    await client.deleteSnapshot("x");
    await assert.rejects(
      () => client.readSnapshot("x"),
      (err) => err instanceof SnapshotNotFound,
    );
  });

  test("recordSnapshot rejects null source_uuid (22023)", async () => {
    await assert.rejects(
      () =>
        client.recordSnapshot({
          sourceUuid: "",
          sourceType: "T",
          sourceVersion: 0n,
          data: {},
        }),
      (err) => err instanceof InvalidParameterValue,
    );
  });
});

describe("Client.subscriptions", () => {
  test("claim creates a row and returns 'claimed'", async () => {
    const s = randomUUID();
    await client.appendToStream(s, expected.noStream, [ev("A")]);
    const r = await client.claimSubscription(s, "sub", "w1", 30, {
      startFrom: "origin",
    });
    assert.equal(r.result, "claimed");
    assert.equal(r.lastSeen, 0n);
    assert.equal(r.claimedBy, "w1");
    assert.ok(r.claimExpiresAt instanceof Date);
  });

  test("second worker sees 'already_claimed' (not an error)", async () => {
    const s = randomUUID();
    await client.appendToStream(s, expected.noStream, [ev("A")]);
    await client.claimSubscription(s, "sub", "w1", 30);
    const r2 = await client.claimSubscription(s, "sub", "w2", 30);
    assert.equal(r2.result, "already_claimed");
    assert.equal(r2.claimedBy, "w1");
  });

  test("startFrom: 'current' starts at head; ignored on re-claim", async () => {
    const s = randomUUID();
    await client.appendToStream(s, expected.noStream, [
      ev("A"),
      ev("B"),
      ev("C"),
    ]);
    const r1 = await client.claimSubscription(s, "sub", "w1", 30, {
      startFrom: "current",
    });
    assert.equal(r1.lastSeen, 3n);
    await client.releaseSubscription(s, "sub", "w1");
    // startFrom is ignored on subsequent claims (INV-SUB-P-021)
    const r2 = await client.claimSubscription(s, "sub", "w2", 30, {
      startFrom: "origin",
    });
    assert.equal(r2.lastSeen, 3n);
  });

  test("claim on missing stream raises StreamNotFound (IS003)", async () => {
    await assert.rejects(
      () => client.claimSubscription(randomUUID(), "sub", "w1", 30),
      (err) => err instanceof StreamNotFound && err.code === "IS003",
    );
  });

  test("claim on $all works (resolves to stream_id = 0)", async () => {
    const r = await client.claimSubscription("$all", "all-sub", "w1", 30);
    assert.equal(r.result, "claimed");
  });

  test("release clears holder; subsequent claim resumes from last_seen", async () => {
    const s = randomUUID();
    await client.appendToStream(s, expected.noStream, [ev("A"), ev("B")]);
    await client.claimSubscription(s, "sub", "w1", 30, { startFrom: "origin" });
    // Advance the cursor via the SUB-A routing primitive.
    await client.routeBatch(s, "sub", "w1", 1n, []);
    await client.releaseSubscription(s, "sub", "w1");
    const r2 = await client.claimSubscription(s, "sub", "w2", 30);
    assert.equal(r2.lastSeen, 1n);
  });

  test("release by non-holder raises lease-lost (IS022)", async () => {
    const s = randomUUID();
    await client.appendToStream(s, expected.noStream, [ev("A")]);
    await client.claimSubscription(s, "sub", "w1", 30);
    await assert.rejects(
      () => client.releaseSubscription(s, "sub", "intruder"),
      (err) => err instanceof SubscriptionLeaseLost,
    );
  });

  test("delete removes the row; missing raises IS020 (D-0009, not lenient)", async () => {
    const s = randomUUID();
    await client.appendToStream(s, expected.noStream, [ev("A")]);
    await client.claimSubscription(s, "sub", "w1", 30);
    await client.deleteSubscription(s, "sub");
    await assert.rejects(
      () => client.deleteSubscription(s, "sub"),
      (err) => err instanceof SubscriptionNotFound && err.code === "IS020",
    );
  });

});

describe("Append-only trigger (IS006)", () => {
  test("direct UPDATE on events raises AppendOnlyViolation", async () => {
    const s = randomUUID();
    await client.appendToStream(s, expected.noStream, [ev("A")]);
    await assert.rejects(
      () => pool.query(`UPDATE instructed.events SET event_type = 'B'`),
      (err: any) => {
        const mapped = (err && typeof err === "object" && (err as any).code)
          ? err
          : err;
        return (err as any).code === "IS006";
      },
    );
    // And via the SDK's mapper (Client doesn't expose raw SQL, but mapPgError
    // is exercised here by simulating a call): we re-check by catching the
    // raw pg error and mapping it. The Client surface itself never produces
    // IS006, but the class exists for ergonomic stack traces.
    try {
      await pool.query(`DELETE FROM instructed.events`);
      assert.fail("expected DELETE to raise");
    } catch (err: any) {
      assert.equal(err.code, "IS006");
      const { mapPgError } = await import("../src/errors/index.ts");
      const mapped = mapPgError(err);
      assert.ok(mapped instanceof AppendOnlyViolation);
      assert.ok(mapped instanceof InstructedError);
    }
  });
});

describe("Error class hierarchy", () => {
  test("every IS* class extends InstructedError", () => {
    const samples: InstructedError[] = [
      new WrongExpectedVersion("x"),
      new StreamExists("x"),
      new StreamNotFound("x"),
      new DuplicateEvent("x"),
      new ReservedStreamUuid("x"),
      new SnapshotNotFound("x"),
      new SubscriptionNotFound("x"),
      new SubscriptionLeaseLost("x"),
      new InvalidParameterValue("x"),
      new AppendOnlyViolation("x"),
    ];
    for (const s of samples) {
      assert.ok(s instanceof InstructedError, `${s.constructor.name}`);
    }
  });
});
