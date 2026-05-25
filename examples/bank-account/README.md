# Bank-account example

Two aggregates, one projection, one process manager — the canonical
CQRS/ES scenario: account-to-account transfer with compensation via
refusal.

## What's modelled

- **`account.ts` — `Account` aggregate.** Pure execute / apply,
  emits `AccountOpened`, `Deposited`, `Withdrawn`, and
  `WithdrawalRefused`. Refusal is a domain event, not a thrown
  exception — the process manager observes it via its `routeFn`.
- **`transfer.ts` — `Transfer` aggregate.** A trivial aggregate
  whose only command is `Request`, producing one
  `TransferRequested` event that kicks off the PM.
- **`balances.ts` — `Balances` projection (over `$all`).** An
  idempotent in-memory fold over `AccountOpened` / `Deposited` /
  `Withdrawn`. A `lastEventByAccount` guard makes redelivery
  harmless. Routing-side filtering via a `routeFn` returning
  `"ignore"` for unrelated event types (the SUB-A replacement for
  the legacy `selector` parameter).
- **`transfer-pm.ts` — `TransferProcessManager` (over `$all`).**
  Routes by `transferId`:
  - `TransferRequested` → `{ partitionKey: transferId }`; `handle`
    dispatches `Withdraw`.
  - `Withdrawn` → same partition; `handle` dispatches `Deposit`.
  - `Deposited` → same partition; `handle` returns
    `{ complete: true }` (success terminates the PM partition).
  - `WithdrawalRefused` → same partition; `handle` returns
    `{ complete: true }` (no compensation needed).
- **`main.ts` — end-to-end driver.** Opens alice & bob, deposits
  1000 to alice, transfers 300 alice → bob (success), then tries
  to transfer 5000 bob → alice (refused). Verifies the final
  balances (700 / 300) and prints them.

## Running

```sh
docker compose up -d                            # one-off, in repo root
node --experimental-strip-types examples/bank-account/main.ts
```

Expected output:

```
[opened + funded] alice=1000 bob=0
[after successful transfer] alice=700 bob=300
[after refused transfer] alice=700 bob=300
OK -- final balances: { alice: 700, bob: 300 }
```

The integration test `sdks/typescript/test/bank-account.test.ts`
exercises an inlined copy of the same domain model under `npm test`.

## What the example demonstrates

- **The `Instructed` facade:** `registerAggregate` /
  `registerProjection` / `registerProcessManager` declaratively,
  one `startWorker()` to run them all under one handle, one
  `close()` to stop them all.
- **SUB-A routing + processing substrate:** every projection /
  PM spins up one routing worker (events → work items) and one
  processing worker (claim → handle → ack). The split is invisible
  at the registration surface but exposed at the `RunningWorker`
  layer.
- **By-name dispatch with a consistency wait:**
  `dispatch(..., { consistency: ["Balances"] })` blocks until the
  Balances subscription's catch-up predicate is true — the
  consistency list is explicit (no `:strong` shorthand).
- **PM-F routing:** `routeFn` returns either `{ partitionKey }`
  or `"ignore"` — no `start` / `continue` / `stop` directive
  enum. Lifecycle is a return-value concern in `handle`.
- **PM-C apply/handle split:** `apply` is the pure state fold
  (runs during rebuild too); `handle` is the side-effectful step
  (commands + `complete?`).
- **Compensation via refusal:** the PM never sends a compensating
  command because the `Withdraw` was atomically refused; the debit
  never happened, so there is nothing to undo. The successful and
  refused paths both end with `{ complete: true }`.
- **Cross-aggregate command dispatch from a PM:** the PM
  dispatches to `Account` via a separate connection from the one
  its subscription is using, keeping the dispatch lock-set
  disjoint from the persist-and-ack lock-set (D-0011 / D-0012).
- **Causation / correlation propagation:** the `Withdrawn` and
  `Deposited` events the PM produces carry `causation_id` =
  triggering event id, threaded automatically by the SDK.

## Upgrading from the pre-SUB-A shape

See [`docs/upgrade-notes/pm-f.md`](../../docs/upgrade-notes/pm-f.md)
for the breaking-change matrix: what the legacy `routes` /
`handle` / `selector` shapes turn into under SUB-A + PM-F + PRJ-A.
