# Bank-account example

A canonical CQRS / event-sourced bank: two aggregates, two
projections, one process manager. Each component runs in its own
foreground process so you can experiment with scaling, restarts,
and the periodic state-printing that mimics a real read-store.

## Layout

One file per type / function / module. Sub-directories per
aggregate keep the events and commands of each domain object
together and self-documenting; the file basename matches the
declared type name (`AccountDepositedTo` lives in
`account-deposited-to.ts`).

```
src/
  aggregates/
    account.ts                          Account aggregate
    transfer.ts                         Transfer aggregate
  commands/
    account/
      open-account.ts                   OpenAccount
      deposit-to-account.ts             DepositToAccount
      withdraw-from-account.ts          WithdrawFromAccount
      index.ts                          barrel + AccountCommand union
    transfer/
      request-transfer.ts               RequestTransfer
      mark-transfer-completed.ts        MarkTransferCompleted
      mark-transfer-failed.ts           MarkTransferFailed
      index.ts                          barrel + TransferCommand union
  events/
    account/
      account-opened.ts                 AccountOpened
      account-deposited-to.ts           AccountDepositedTo
      account-withdrawn-from.ts         AccountWithdrawnFrom
      account-withdrawal-refused.ts     AccountWithdrawalRefused
      index.ts                          barrel + AccountEvent union
    transfer/
      transfer-requested.ts             TransferRequested
      transfer-completed.ts             TransferCompleted
      transfer-failed.ts                TransferFailed
      index.ts                          barrel + TransferEvent union
  process-managers/
    transfer-pm.ts                      TransferProcessManager (the saga)
  projections/
    balances/
      projection.ts                     definition (routing + handler)
      queries.ts                        SQL helpers + readBalances
    transfers/
      projection.ts
      queries.ts
  command-router.ts                     maps Command -> (aggregate, id)
  common.ts                             DB URL, signal handling, requireArg
sql/
  read-store.sql                        bank_account.{balances,transfers}
scripts/
  start.ts                  docker compose up + apply both schemas
  projection-balances.ts    workers + periodic SELECT printout
  projection-transfers.ts   workers + periodic SELECT printout
  pm-transfer.ts            saga worker; commands + traced handle
  open-account.ts           short-lived: dispatch OpenAccount
  deposit.ts                short-lived: dispatch DepositToAccount
  transfer.ts               short-lived: dispatch RequestTransfer
docker-compose.yaml                     isolated PG on port 5433
```

### Naming conventions

- **Commands**: imperative verb + noun. `OpenAccount`,
  `DepositToAccount`, `WithdrawFromAccount`, `RequestTransfer`,
  `MarkTransferCompleted`, `MarkTransferFailed`.
- **Events**: aggregate-noun + past-tense verb. `AccountOpened`,
  `AccountDepositedTo`, `AccountWithdrawnFrom`,
  `AccountWithdrawalRefused`, `TransferRequested`,
  `TransferCompleted`, `TransferFailed`.
- **Type / value share a name**: each event / command file
  exports a `const X = "X" as const` (string discriminator) and
  a `type X` (the TypeScript shape). The same identifier stands
  in for the literal in value position and the union member in
  type position.
- **Stream names are storage detail.** Aggregates default to
  `<Type>-<id>` via the SDK's `prefixType(def.type)`; nothing
  in `src/` or `scripts/` constructs a stream name by hand.
  Applications identify aggregates by `(type, id)`.

**Storage layout.** The example uses two schemas in the same
database:

- `instructed.*` -- the event store (streams, events, snapshots,
  subscriptions, work items). Applied from `sql/instructed.sql`
  at the repo root.
- `bank_account.*` -- the application's read store. Two tables
  (`balances`, `transfers`), each carrying a `last_event_number`
  for idempotent UPSERTs under at-least-once redelivery. Applied
  from `sql/read-store.sql` here in the example.

The SDK never touches `bank_account.*` -- the projection handler
owns its read-store connection (D-0016). The PM's saga state
lives in `instructed.snapshots` via the PM-C primitive, so the
PM is multi-process-safe with no extra work.

## Running

Each command goes in its own terminal. The first one stays in the
foreground — Ctrl-C tears down the docker container, its volume,
and the bank_account read-store along with it.

Projections and the PM can each be run in multiple processes
for scale and HA -- per-stream partitioning on the Balances
projection and per-transfer partitioning on Transfers and the
PM mean the SDK's work-queue substrate divides events across
workers. The read tables converge regardless of which process
handles which partition.

```sh
# Terminal 1 -- bring up Postgres and apply the schema. Stay here.
cd examples/typescript/bank-account
npm install            # one-off
npm start

# Terminal 2 -- the Balances projection
npm run projection:balances

# Terminal 3 -- the Transfers projection
npm run projection:transfers

# Terminal 4 -- the TransferProcessManager
npm run pm:transfer

# Terminal 5 -- send commands
npm run open-account alice
npm run open-account bob
npm run deposit alice 1000
npm run transfer alice bob 300       # succeeds
npm run transfer bob alice 5000      # fails -- bob has 300
```

Watch the projection terminals: balances update after each
successful command; the transfer rows flip from `requested` to
`completed` (or `failed (insufficient funds)`) as the PM
dispatches its terminating mark-command.

## What the example demonstrates

- **The `Instructed` facade.** One chainable `register()` covers
  aggregates, command routers, projections and process managers;
  `poll()` returns a worker the application stops itself. The
  pool is supplied by the application and the application closes
  it — the facade does not own or close DB resources.
- **Multi-process scaling.** Each projection and the PM run in
  their own process. Adding a second `projection:balances`
  process while the first is running shows lease takeover and
  partition-level fan-out — there's no shared in-memory state
  to coordinate.
- **Real success/failure on the Transfer aggregate.**
  `TransferRequested` carries the request; the PM dispatches
  `MarkTransferCompleted` once the destination `AccountDepositedTo`
  lands, or `MarkTransferFailed { reason }` once an
  `AccountWithdrawalRefused` lands. Every transfer stream has a
  terminal outcome event.
- **Compensation via refusal (D-0011).** The PM never sends
  a compensating Account command — the `WithdrawFromAccount` was
  atomically refused, so the debit never happened. The PM still
  records the outcome on the Transfer aggregate, so the _transfer_
  has a failure event even though no Account event was reversed.
- **Idempotent mark-commands.** `MarkTransferCompleted` /
  `MarkTransferFailed` return `[]` (no events) if the transfer is
  already in the target stage, so PM redelivery is harmless.
- **Lean commands and a single command router.** PMs and CLI
  scripts emit bare `Command` objects (`{ type: "DepositToAccount",
  accountId, amount, ... }`); the application's command router
  (`src/command-router.ts`) is the one place that knows which
  aggregate owns which command, and how to extract the
  aggregate id from the command payload.
- **Two independent projections.** Balances and Transfers each
  get their own routing + processing worker pair; the SDK
  delivers each event to each subscription independently.

## Environment

- `INSTRUCTED_DATABASE_URL` overrides the default
  `postgresql://postgres:postgres@127.0.0.1:5433/bank_account`.
  If you set this, you're responsible for ensuring the database
  exists and the schema has been applied — `npm start` does both
  for the default URL.
- Node 22.18+ is required (default-on type stripping for `.ts`
  plus the `--conditions=development` exports lookup that lets
  the example import `instructed-sdk` straight from source
  without a build step).
