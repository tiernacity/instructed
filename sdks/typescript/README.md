# `instructed` TypeScript SDK

The reference SDK for `instructed`. Node 18+, `pg` 8.x as a peer
dependency. Drives the SQL contract in [`sql/instructed.sql`](../../sql/instructed.sql);
the SQL is the authoritative spec.

For the conceptual model see [`docs/concepts.md`](../../docs/concepts.md);
for the formal contract see [`docs/invariants.md`](../../docs/invariants.md);
for what the system promises see [`docs/guarantees.md`](../../docs/guarantees.md).

## Quickstart

```sh
docker compose up -d                                # in repo root
cd sdks/typescript
npm install
npm test                                            # runs against the live DB
```

End-to-end example:

```sh
cd examples/typescript/bank-account && docker compose up
```

## Imports and reserved names

A CQRS/ES library's core nouns — `Event`, `Command`, `Snapshot`,
`Client`, `Logger` — are exactly the nouns a domain modeller wants
to use. The SDK does **not** force you to give them up:

- **You define your own events and commands as plain data.** The
  SDK consumes them *structurally* (e.g. `RecordedEvent<E>`,
  `AggregateDefinition<S, C, E>`), so you never import the SDK's
  `Event`/`Command` to model a domain `Event` (a concert), a
  `Command`, etc. See `examples/typescript/bank-account`.
- **Recommended: namespace import**, so no SDK name lands in your
  module's flat namespace:

  ```ts
  import * as instructed from "instructed-sdk"

  const app = new instructed.Instructed({ db: pool })
  type MyDef = instructed.AggregateDefinition<S, C, E>
  ```

  Use named imports for the handful of symbols you reference a lot
  (`Instructed`, `commandRouter`, `onlyTypes`) if you prefer — just
  alias anything that clashes with your domain vocabulary.

**Reserved names.** The library namespaces everything it reserves
so application code can't collide with it:

- **Streams:** the global stream is `$all` (the `$` sigil marks it
  reserved); you choose every other stream name.
- **Storage:** the Postgres schema is `instructed`; errors are
  SQLSTATE class `IS`.
- **Metadata:** library-reserved metadata keys are prefixed
  `$instructed.` (e.g. `$instructed.snapshot_module_version`,
  SNAP-002). Any metadata key *without* that prefix is yours.

## Layer structure

Per [D-0027](../../docs/decisions.md#d-0027) the SDK ships as one
npm package (`instructed-sdk`) with two entry points:

  - **`instructed-sdk`** — the full surface; the conventional entry
    point. What application code imports.
  - **`instructed-sdk/core`** — L1 + L2 only; the porting-checklist
    inventory. For consumers writing their own L3 facade.

The three layers:

| Layer | Modules | Key exports | Purpose |
|---|---|---|---|
| **L1 — procedure bindings** | `client.ts`, `errors.ts` (SQLSTATE-bound classes), `types.ts` | `Client`, `InstructedError` + subclasses, wire shapes | One method per `instructed.*` stored procedure; SQLSTATE → typed-error translation. Every SDK port reproduces this surface verbatim. |
| **L2 — core behaviours** | `aggregate.ts`, `routing-worker.ts`, `processing-worker.ts`, `projection-worker.ts` (adapter), `pm-substrate.ts` | `runCommand`, `runCommandAndApply`, `startRoutingWorker`, `startProcessingWorker`, `startProjectionWorker`, `startPmSubstrate`, `ErrorPolicy`, `RetryBudgetExhausted` | Aggregate load-execute-append loop with OCC retry (D-0005); D-0025 per-batch routing worker; per-item lease + heartbeat processing worker; kind-specific projection / PM-substrate adapters. The PM substrate is the snapshot+ack lifecycle without command dispatch (that's L3). Every SDK port reproduces the *behaviours*; the shape can be language-idiomatic. |
| **L3 — conveniences** | `consistency.ts`, `instructed.ts`, `partition-by.ts`, `routing-helpers.ts`, `command-router.ts`, `aggregate-snapshots.ts`, `error-policies.ts`, `pm-worker.ts` | `Instructed`, `waitForProjection`, `PartitionBy`, `onlyTypes`, `commandRouter`, `runCommandWithSnapshots`, `exponentialBackoff`, `linearBackoff`, `retryUpTo`, `startPmWorker`, `ConsistencyTimeout`, `UnknownAggregateType` | Chainable `register()` for aggregates / projections / PMs / command routers; `dispatch(command)` via router or `dispatch(type, id, command)` explicit; `poll()` returns an application-owned worker; consistency-on-dispatch wait; `PartitionBy` sugar over the routing extension point; `onlyTypes` routing combinator; snapshot-policy orchestration over the L2 aggregate primitive; retry/error-policy standard library; by-value-`commands` PM wrapper over the L2 substrate. **May differ per language port.** |

L1 + L2 = the `instructed-sdk/core` sub-path. L1 + L2 + L3 = the
bare `instructed-sdk` entry. See `src/core.ts` and `src/index.ts`
for the authoritative export inventory.

### Extension points

The SDK offers three named extension points, each following the
**contract + standard library + escape hatch** pattern. Application
code either uses the shipped standard library or drops in its own
function obeying the contract.

| Extension point | Contract | Standard library | Source |
|---|---|---|---|
| Routing | `RoutingFn` → `RoutingDecision` | `PartitionBy` + `routingFnForPartitionBy` (modes: `sequential`, `per-event`, `per-key`) | `routing-worker.ts` + `partition-by.ts` |
| Aggregate snapshot policy | `SnapshotPolicy<S>.shouldSnapshot(state, version, eventsSinceLast)` | `everyN(n)` | `aggregate.ts` |
| Retry / error handling | `ErrorPolicy<PolicyState>(err, ctx, state) → { decision, state }` | `DEFAULT_ERROR_POLICY` (default) + `exponentialBackoff`, `linearBackoff`, `retryUpTo` (composable helpers) | `processing-worker.ts` + `error-policies.ts` |

The **contract** at each point is required-core: every SDK port
reproduces the shape, though language idiom may rename or restructure
(generics become interfaces, etc.). The **standard library** at each
point is idiomatic-not-required: a port may ship its own equivalents,
different equivalents, or none at all. See
[`sdks/porting-checklist.md`](../porting-checklist.md) for the
per-port reading list.

## Typical usage

```ts
import {
  Instructed,
  everyN,
  type AggregateDefinition,
  type ProjectionDefinition,
} from "instructed-sdk";

const Account: AggregateDefinition<AccountState, AccountCommand, AccountEvent> = {
  type: "Account",
  initialState: () => ({ opened: false, owner: null, balance: 0 }),
  execute(state, command) {
    /* return one event, an array of events, or throw */
  },
  apply(state, event) {
    /* return new state */
  },
  // Optional snapshot orchestration (L3); fires on every Nth command.
  snapshotPolicy: everyN(100),
  // Optional module-version tag (SNAP-002). Bump to invalidate
  // previously-written snapshots after a state-shape change.
  snapshotModuleVersion: "v1",
};

const balances: ProjectionDefinition = {
  type: "Balances",
  startFrom: "origin",
  handler: async (event, ctx) => {
    /* write to your read model; idempotent (handlers may redeliver) */
  },
};

import pg from "pg";
const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
const app = new Instructed({ db: pool })
  .register(Account)
  .register(balances);

const worker = await app.poll();

await app.dispatch("Account", "alice", { type: "OpenAccount", owner: "alice" });
await app.dispatch(
  "Account",
  "alice",
  { type: "DepositToAccount", amount: 1000 },
  { consistency: ["Balances"] },
);

await worker.close();   // application stops the worker
await pool.end();       // application closes the pool
```

The `Instructed` facade does **not** own the pool and does **not**
track the worker — both lifecycles are the application's. The
facade is a registration / dispatch surface, nothing more.

A worked end-to-end version, with a process manager modelling a
transfer, is in
[`examples/typescript/bank-account/`](../../examples/typescript/bank-account/).

## Errors

Every SQLSTATE is translated to a typed exception. L1 classes are
SQLSTATE-bound (every port reproduces them); L2 and L3 classes are
emitted by SDK runtime code at their respective layers.

```
Error
  └── InstructedError
        ├── AppendError                       (L1; base for IS001–IS005)
        │     ├── WrongExpectedVersion        (IS001)
        │     ├── StreamExists                (IS002)
        │     ├── StreamNotFound              (IS003)
        │     ├── DuplicateEvent              (IS004)
        │     └── ReservedStreamUuid          (IS005)
        ├── AppendOnlyViolation               (L1; IS006)
        ├── SnapshotNotFound                  (L1; IS010)
        ├── SubscriptionError                 (L1; base for IS020–IS022)
        │     ├── SubscriptionNotFound        (IS020)
        │     ├── SubscriptionAlreadyClaimed  (IS021)
        │     └── SubscriptionLeaseLost       (IS022)
        ├── WorkItemLeaseLost                 (L1; IS030)
        ├── InvalidParameterValue             (L1; 22023)
        ├── RetryBudgetExhausted              (L2; aggregate OCC retry loop)
        ├── ConsistencyTimeout                (L3; waitForProjection)
        ├── ConsistencyTargetError            (L3; waitForProjection)
        └── UnknownAggregateType              (L3; Instructed facade)
```

L1 classes ship via `instructed-sdk/core`; L2 adds
`RetryBudgetExhausted`; L3 adds the consistency / facade classes.
See `src/errors.ts` for the full mapping.

## What this SDK does not do

- **Cache aggregate state between commands.** Every dispatch reloads
  from the store. Configure `snapshotPolicy: everyN(N)` per aggregate
  to keep the load tail short.
- **Provide transactional atomicity between a handler and the cursor
  advance.** Handlers run outside any SDK transaction; the cursor
  advances in a separate short transaction after the handler returns.
  Handlers must be idempotent. See [D-0016](../../docs/decisions.md#d-0016).
- **Distribute one subscription across multiple processing workers
  with `concurrency_limit > 1`.** Single active routing worker per
  subscription (per-batch claim/release rotation under D-0025);
  multiple processing workers compete for work items via per-item
  claim. True multi-routing-worker subscriptions are deferred
  (ML-0013).

## Layout

```
sdks/typescript/
├── README.md                  -- this file
├── package.json
├── tsconfig{,.build,.cjs}.json
├── src/                       -- L1 + L2 + L3 (D-0027)
│   ├── client.ts              -- L1 procedure bindings
│   ├── errors.ts              -- L1 + L2 + L3 error classes (annotated)
│   ├── types.ts               -- L1 wire shapes
│   ├── aggregate.ts           -- L2 OCC loop
│   ├── aggregate-snapshots.ts -- L3 runCommandWithSnapshots (snapshot policy orchestration)
│   ├── routing-worker.ts      -- L2 D-0025 routing (extension point: routing)
│   ├── partition-by.ts        -- L3 PartitionBy sugar (routing std library)
│   ├── processing-worker.ts   -- L2 per-item lease + heartbeat (extension point: retry / error policy)
│   ├── error-policies.ts      -- L3 exponentialBackoff / linearBackoff / retryUpTo (retry std library)
│   ├── projection-worker.ts   -- L2 projection adapter over processing-worker
│   ├── pm-substrate.ts        -- L2 PM snapshot+ack lifecycle (substrate)
│   ├── pm-worker.ts           -- L3 by-value-`commands` wrapper over pm-substrate
│   ├── consistency.ts         -- L3 waitForProjection
│   ├── instructed.ts          -- L3 facade
│   ├── core.ts                -- public entry for `instructed-sdk/core`
│   ├── index.ts               -- public entry for `instructed-sdk`
│   └── internal/              -- private (worker-id, sleep, with-transaction)
└── test/                      -- node --test against docker-compose Postgres
```

Examples live under [`examples/typescript/`](../../examples/typescript/).
See [`examples/typescript/bank-account/`](../../examples/typescript/bank-account/)
for a multi-process end-to-end walkthrough.
