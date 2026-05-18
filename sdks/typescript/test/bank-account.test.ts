/**
 * Bank-account example — Phase 8 done-criterion integration test.
 *
 * Exercises the example code end-to-end against the docker-compose
 * Postgres: Account + Transfer aggregates, the Balances projection,
 * and the TransferProcessManager (compensation-by-refusal per D-0011).
 *
 * The test is the verifiable form of `examples/bank-account/main.ts`
 * — it imports the example modules directly and asserts on the
 * same end states.
 */

import { after, before, beforeEach, describe, test } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { closePool, getPool, truncateAll } from "./fixtures.ts";
import { Instructed, SnapshotNotFound } from "../src/index.ts";
import { Account } from "../examples/bank-account/account.ts";
import { Transfer } from "../examples/bank-account/transfer.ts";
import {
  balancesProjection,
  newBalancesView,
} from "../examples/bank-account/balances.ts";
import { transferProcessManager } from "../examples/bank-account/transfer-pm.ts";
import type pg from "pg";

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

const PG_URL = `postgresql://${process.env.PGUSER ?? "postgres"}:${process.env.PGPASSWORD ?? "postgres"}@${process.env.PGHOST ?? "127.0.0.1"}:${Number(process.env.PGPORT ?? 5432)}/${process.env.PGDATABASE ?? "instructed_test"}`;

async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs: number,
  label: string,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise((r) => setTimeout(r, 25));
  }
  throw new Error(`timeout waiting for ${label}`);
}

describe("bank-account example — end to end", () => {
  test("successful transfer + refused transfer", async () => {
    const app = new Instructed({ db: PG_URL });
    const view = newBalancesView();

    app.registerAggregate(Account);
    app.registerAggregate(Transfer);
    app.registerProjection(balancesProjection(view), {
      pollInterval: 25,
      heartbeatInterval: 1_000,
    });
    app.registerProcessManager(transferProcessManager(), {
      pollInterval: 25,
      heartbeatInterval: 1_000,
    });

    const worker = await app.startWorker();
    try {
      const alice = randomUUID();
      const bob = randomUUID();
      const aliceStream = `account-${alice}`;
      const bobStream = `account-${bob}`;

      await app.dispatch("Account", aliceStream, { kind: "Open", owner: "alice" });
      await app.dispatch("Account", bobStream, { kind: "Open", owner: "bob" });
      await app.dispatch(
        "Account",
        aliceStream,
        { kind: "Deposit", amount: 1_000 },
        { consistency: ["Balances"], consistencyTimeout: 10_000 },
      );
      assert.equal(view.balance.get(aliceStream), 1_000);
      assert.equal(view.balance.get(bobStream) ?? 0, 0);

      // ---- successful transfer ----
      const transferOk = randomUUID();
      await app.dispatch("Transfer", `transfer-${transferOk}`, {
        kind: "Request",
        from: alice,
        to: bob,
        amount: 300,
        transferId: transferOk,
      });
      await waitFor(
        () => view.balance.get(bobStream) === 300,
        10_000,
        "bob to be credited 300",
      );
      await waitFor(
        () => view.balance.get(aliceStream) === 700,
        10_000,
        "alice to be debited 300",
      );
      // The PM instance for transferOk reached `stop` (snapshot
      // deleted in the ack tx on the Deposited event).
      await waitFor(
        async () => {
          try {
            await app.client().readSnapshot(`TransferProcessManager-${transferOk}`);
            return false;
          } catch (err) {
            return err instanceof SnapshotNotFound;
          }
        },
        10_000,
        "successful-transfer PM to stop",
      );

      // ---- refused transfer ----
      const transferKo = randomUUID();
      await app.dispatch("Transfer", `transfer-${transferKo}`, {
        kind: "Request",
        from: bob,
        to: alice,
        amount: 5_000, // bob only has 300
        transferId: transferKo,
      });
      // Refusal stops the PM. No compensating command is needed
      // (D-0011): the Withdraw was refused atomically, the debit
      // never happened, so the only ledger entry is
      // WithdrawalRefused on bob's stream. Wait for that event to
      // appear directly — it's the visible side effect.
      await waitFor(
        async () => {
          const ev = await app.client().readStream(bobStream, 1n, 50);
          return ev.some((e) => e.event_type === "WithdrawalRefused");
        },
        10_000,
        "WithdrawalRefused to be appended to bob's stream",
      );
      // Then the PM instance for this transfer must reach `stop`
      // (snapshot deleted). It briefly exists between TransferRequested
      // and WithdrawalRefused, so check after the refusal lands.
      await waitFor(
        async () => {
          try {
            await app.client().readSnapshot(`TransferProcessManager-${transferKo}`);
            return false;
          } catch (err) {
            return err instanceof SnapshotNotFound;
          }
        },
        10_000,
        "refused-transfer PM to stop",
      );

      // Balances unchanged from after the successful transfer.
      assert.equal(view.balance.get(aliceStream), 700);
      assert.equal(view.balance.get(bobStream), 300);

      // Bob's stream now carries: AccountOpened, WithdrawalRefused.
      // Verify the refusal is recorded.
      const bobEvents = await app.client().readStream(bobStream, 1n, 50);
      const types = bobEvents.map((e) => e.event_type);
      assert.deepEqual(types, ["AccountOpened", "Deposited", "WithdrawalRefused"]);
    } finally {
      await worker.close();
      await app.close();
    }
  });
});
