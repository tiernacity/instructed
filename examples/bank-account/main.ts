/**
 * Bank-account example -- end-to-end driver.
 *
 * Sets up the Instructed facade, registers the two aggregates
 * (Account, Transfer), the Balances projection, and the
 * TransferProcessManager, then runs two scenarios:
 *
 *   1. Successful transfer: open alice (1000) and bob (0); transfer
 *      300 from alice -> bob; final balances 700 / 300.
 *   2. Refused transfer: try to transfer 5000 from bob -> alice; the
 *      Account aggregate emits WithdrawalRefused; the PM completes
 *      the partition; no Deposit ever lands on alice; balances stay
 *      at 700 / 300.
 *
 * Run against the docker-compose Postgres (instructed_test database):
 *
 *   docker compose up -d
 *   node --experimental-strip-types examples/bank-account/main.ts
 */

import { randomUUID } from "node:crypto";
import {
  Instructed,
  type RunningWorker,
} from "../../sdks/typescript/src/index.ts";
import { Account } from "./account.ts";
import { Transfer } from "./transfer.ts";
import {
  BALANCES_SUBSCRIPTION_NAME,
  balancesProjection,
  newBalancesView,
  type BalancesView,
} from "./balances.ts";
import {
  TRANSFER_PM_NAME,
  transferProcessManager,
} from "./transfer-pm.ts";

const PG_URL =
  process.env.INSTRUCTED_DATABASE_URL ??
  `postgresql://${process.env.PGUSER ?? "postgres"}:${process.env.PGPASSWORD ?? "postgres"}@${process.env.PGHOST ?? "127.0.0.1"}:${Number(process.env.PGPORT ?? 5432)}/${process.env.PGDATABASE ?? "instructed_test"}`;

async function main(): Promise<void> {
  const app = new Instructed({ db: PG_URL });
  const view: BalancesView = newBalancesView();

  app.registerAggregate(Account);
  app.registerAggregate(Transfer);
  // SUB-A registration: positional name + declarative input. The
  // worker options object (third arg) carries the routing/processing
  // knobs; the workers are spun up by `startWorker()` below.
  app.registerProjection(
    BALANCES_SUBSCRIPTION_NAME,
    balancesProjection(view),
    { pollInterval: 50, heartbeatInterval: 1_000 },
  );
  app.registerProcessManager(
    TRANSFER_PM_NAME,
    transferProcessManager(),
    { pollInterval: 50, heartbeatInterval: 1_000 },
  );

  const worker: RunningWorker = await app.startWorker();
  try {
    const alice = randomUUID();
    const bob = randomUUID();

    // 1. Open and deposit. We wait for the Balances projection via
    //    the consistency option on the funding dispatch.
    await app.dispatch("Account", `account-${alice}`, {
      kind: "Open",
      owner: "alice",
    });
    await app.dispatch("Account", `account-${bob}`, {
      kind: "Open",
      owner: "bob",
    });
    await app.dispatch(
      "Account",
      `account-${alice}`,
      { kind: "Deposit", amount: 1_000 },
      {
        consistency: [BALANCES_SUBSCRIPTION_NAME],
        consistencyTimeout: 10_000,
      },
    );
    log("opened + funded", view, alice, bob);

    // 2. Successful transfer.
    const transferOk = randomUUID();
    await app.dispatch("Transfer", `transfer-${transferOk}`, {
      kind: "Request",
      from: alice,
      to: bob,
      amount: 300,
      transferId: transferOk,
    });
    await waitForBalance(view, bob, 300);
    await waitForBalance(view, alice, 700);
    log("after successful transfer", view, alice, bob);

    // 3. Refused transfer (bob -> alice, 5000 -- bob only has 300).
    const transferKo = randomUUID();
    await app.dispatch("Transfer", `transfer-${transferKo}`, {
      kind: "Request",
      from: bob,
      to: alice,
      amount: 5_000,
      transferId: transferKo,
    });
    // Wait for the refusal to land on bob's stream; that's the only
    // observable side effect (no compensating command per D-0011).
    // Balances stay unchanged.
    await waitForRefusal(app, `account-${bob}`);
    log("after refused transfer", view, alice, bob);

    // Final sanity assertions.
    const aBal = view.balance.get(`account-${alice}`);
    const bBal = view.balance.get(`account-${bob}`);
    if (aBal !== 700 || bBal !== 300) {
      throw new Error(
        `unexpected final balances: alice=${aBal}, bob=${bBal} (expected 700 / 300)`,
      );
    }
    console.log("OK -- final balances:", { alice: aBal, bob: bBal });
  } finally {
    await worker.close();
    await app.close();
  }
}

function log(
  step: string,
  view: BalancesView,
  alice: string,
  bob: string,
): void {
  console.log(
    `[${step}] alice=${view.balance.get(`account-${alice}`) ?? "-"} bob=${view.balance.get(`account-${bob}`) ?? "-"}`,
  );
}

async function waitForBalance(
  view: BalancesView,
  accountUuid: string,
  expected: number,
  timeoutMs = 10_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (view.balance.get(`account-${accountUuid}`) === expected) return;
    await new Promise((r) => setTimeout(r, 25));
  }
  throw new Error(
    `timed out waiting for balance of ${accountUuid} = ${expected} (got ${view.balance.get(`account-${accountUuid}`)})`,
  );
}

async function waitForRefusal(
  app: Instructed,
  streamUuid: string,
  timeoutMs = 10_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const ev = await app.client().readStream(streamUuid, 1n, 50);
    if (ev.some((e) => e.event_type === "WithdrawalRefused")) return;
    await new Promise((r) => setTimeout(r, 25));
  }
  throw new Error(`timed out waiting for WithdrawalRefused on ${streamUuid}`);
}

main().catch((err) => {
  console.error("example failed:", err);
  process.exit(1);
});
