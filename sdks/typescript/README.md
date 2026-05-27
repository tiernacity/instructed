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
npm test                                            # 77 tests against the live DB
```

End-to-end example:

```sh
cd examples/typescript/bank-account && docker compose up
```

## Layer structure

Per [D-0027](../../docs/decisions.md#d-0027) the SDK ships as one
npm package (`instructed-sdk`) with two entry points:

  - **`instructed-sdk`** — the full surface; the conventional entry
    point. What application code imports.
  - **`instructed-sdk/core`** — L1 + L2 only; the porting-checklist
    inventory. For consumers writing their own L3 facade.

The three layers (per `SDK-REWORK-NOTES.md` and
`docs/todo/sdk-rework.md`):

| Layer | Modules | Key exports | Purpose |
|---|---|---|---|
| **L1 — procedure bindings** | `client.ts`, `errors.ts` (SQLSTATE-bound classes), `types.ts` | `Client`, `InstructedError` + subclasses, wire shapes | One method per `instructed.*` stored procedure; SQLSTATE → typed-error translation. Every SDK port reproduces this surface verbatim. |
| **L2 — core behaviours** | `aggregate.ts`, `routing-worker.ts`, `processing-worker.ts`, `projection-worker.ts` (adapter), `pm-worker.ts` | `runCommand`, `startRoutingWorker`, `startProcessingWorker`, `startProjectionWorker`, `startPmWorker`, `ErrorPolicy`, `RetryBudgetExhausted` | Aggregate load-execute-append loop with OCC retry (D-0005); D-0025 per-batch routing worker; per-item lease + heartbeat processing worker; kind-specific projection / PM adapters. Every SDK port reproduces the *behaviours*; the shape can be language-idiomatic. |
| **L3 — conveniences** | `consistency.ts`, `instructed.ts`, `PartitionBy` sugar in `projection-worker.ts` | `Instructed`, `waitForProjection`, `PartitionBy`, `ConsistencyTimeout`, `UnknownAggregateType` | By-name aggregate dispatch, projection / PM registration, single `startWorker()` / `close()`, consistency-on-dispatch wait, partition-by sugar. **May differ per language port.** |

L1 + L2 = the `instructed-sdk/core` sub-path. L1 + L2 + L3 = the
bare `instructed-sdk` entry. See `src/core.ts` and `src/index.ts`
for the authoritative export inventory; the annotated map lives in
[`docs/todo/sdk-rework.md`](../../docs/todo/sdk-rework.md).

## Typical usage

```ts
import { Pool } from "pg";
import { Instructed } from "./src/index.ts";   // or "instructed-sdk" once published

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const app = new Instructed({ pool });

app.registerAggregate({
  name: "Account",
  streamPrefix: "account-",
  initial: () => ({ version: 0, balance: 0 }),
  execute: (state, command) => { /* return events or throw */ },
  apply: (state, event) => { /* return new state */ },
});

app.registerProjection({
  name: "Balances",
  subscribe: { stream: "$all", startFrom: "origin" },
  handler: async (event, ctx) => { /* write to your read model */ },
});

const worker = await app.startWorker();

await app.dispatch(openAccount("alice"));
await app.dispatch(deposit("alice", 1000), { consistency: ["Balances"] });

await worker.close();
await pool.end();
```

A worked end-to-end version, with a process manager modelling a
transfer, is in [`examples/bank-account/`](../../examples/bank-account/).

## Errors

Every SQLSTATE is translated to a typed exception. Class hierarchy:

```
Error
  └── InstructedError
        ├── WrongExpectedVersionError      (IS001)
        ├── StreamExistsError              (IS002)
        ├── StreamNotFoundError            (IS003)
        ├── DuplicateEventError            (IS004)
        ├── ReservedStreamUuidError        (IS005)
        ├── AppendOnlyViolationError       (IS006)
        ├── SnapshotNotFoundError          (IS010)
        ├── SubscriptionNotFoundError      (IS020)
        ├── SubscriptionAlreadyClaimedError (reserved IS021)
        ├── SubscriptionLeaseLostError     (IS022)
        └── ConsistencyTimeoutError        (SDK-only)
```

See `src/errors.ts` for the full mapping.

## What this SDK does not do

- **Cache aggregate state between commands.** Every dispatch reloads
  from the store. Configure `snapshotPolicy: { every: N }` per
  aggregate to keep the load tail short.
- **Provide transactional atomicity between a handler and the cursor
  advance.** Handlers run outside any SDK transaction; the cursor
  advances in a separate short transaction after the handler returns.
  Handlers must be idempotent. See [D-0016](../../docs/decisions.md#d-0016).
- **Enforce snapshot module versioning.** The SQL contract has the
  metadata column but the v1 SDK does not enforce reject-on-mismatch.
  Applications that evolve aggregate schemas should handle this in
  their own `apply` / `initial`.
- **Distribute one subscription across multiple workers.** Single
  active worker per subscription. Throughput scales by splitting into
  multiple named subscriptions. Partitioned consumers are deferred
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
│   ├── routing-worker.ts      -- L2 D-0025 routing
│   ├── processing-worker.ts   -- L2 per-item lease + heartbeat
│   ├── projection-worker.ts   -- L2 adapter + L3 PartitionBy sugar
│   ├── pm-worker.ts           -- L2 snapshot+ack + L3 dispatch helper
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
