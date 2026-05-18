# Bank-account example

The Phase 8 done-criterion target for the `instructed` TypeScript SDK.
Two aggregates, one projection, one process manager, ~250 LOC of
example code in five files, exercising every layer of the SDK on a
canonical CQRS/ES scenario: account-to-account transfer with
compensation via refusal (D-0011).

## What's modelled

- **`account.ts` — `Account` aggregate.** Pure execute / apply,
  emits `AccountOpened`, `Deposited`, `Withdrawn`, and
  `WithdrawalRefused`. Refusal is a domain event, not a thrown
  exception — the process manager observes it via its routes.
- **`transfer.ts` — `Transfer` aggregate.** A trivial aggregate
  whose only command is `Request`, producing one
  `TransferRequested` event that kicks off the PM.
- **`balances.ts` — `Balances` projection (`$all`).** An idempotent
  in-memory fold over `AccountOpened` / `Deposited` / `Withdrawn`.
  The `lastEventByAccount` guard makes redelivery harmless.
- **`transfer-pm.ts` — `TransferProcessManager` (`$all`).**
  Routes by `transferId`:
  - `TransferRequested` → `{kind: 'start'}` → dispatch `Withdraw`
  - `Withdrawn` → `{kind: 'continue'}` → dispatch `Deposit`
  - `Deposited` → `{kind: 'stop'}` (success)
  - `WithdrawalRefused` → `{kind: 'stop'}` (no compensation needed)
- **`main.ts` — end-to-end driver.** Opens alice & bob, deposits
  1000 to alice, transfers 300 alice → bob (success), then tries to
  transfer 5000 bob → alice (refused). Verifies the final balances
  (700 / 300) and prints them.

## Running

The example points at the docker-compose Postgres provisioned at
the repo root (`docker compose up -d`). From this directory's
package root:

```sh
cd sdks/typescript
docker compose up -d              # one-off, in repo root
node --experimental-strip-types examples/bank-account/main.ts
```

Expected output:

```
[opened + funded] alice=1000 bob=0
[after successful transfer] alice=700 bob=300
[after refused transfer] alice=700 bob=300
OK — final balances: { alice: 700, bob: 300 }
```

The integration test `test/bank-account.test.ts` exercises the
same modules under `npm test`.

## What the example demonstrates

- The Layer 5 facade: `registerAggregate` /
  `registerProjection` / `registerProcessManager` declaratively,
  one `startWorker()` call to run them all under one handle,
  one `close()` to stop them all.
- By-name `dispatch` with a `consistency: ["Balances"]` clause that
  blocks until the projection has caught up with the deposit —
  no `:strong` shorthand (D-0010), the list is explicit.
- Process-manager routing: `start` / `continue` / `stop` /
  `ignore`, with `transferId` as the natural key.
- Compensation via refusal (D-0011): the PM never sends a
  compensating command because the `Withdraw` was atomically
  refused; the debit never happened, so there is nothing to undo.
- Cross-aggregate command dispatch from a PM (Account is dispatched
  to by the PM; Transfer drives the PM's start route) routed through
  the separate dispatch pool (D-0011 / D-0012 lock-set disjointness).
- Causation / correlation propagation (§11.8 / D-0017): the
  `Withdrawn` and `Deposited` events the PM produces carry
  `causation_id` = triggering event id, threaded automatically.
