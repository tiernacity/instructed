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
node --experimental-strip-types examples/bank-account/main.ts   # in repo root
```

## Layer structure

The SDK splits into five layers. Each is independently usable; the
facade (Layer 5) is the conventional entry point.

| Layer | Module | Surface | Purpose |
|---|---|---|---|
| 0 | `src/client.ts` | `Client` | Thin wrappers over every stored procedure. `SQLSTATE → typed Error` translation. |
| 1 | `src/aggregate.ts` | `runCommand`, `AggregateDefinition` | Load-execute-append loop with OCC retry. |
| 2 | `src/subscription.ts` | `startProjection`, `ProjectionDefinition` | Persistent-subscription worker — claim, heartbeat, batch read, handler invocation, ack, release. |
| 3 | `src/process-manager.ts` | `startProcessManager`, `ProcessManagerDefinition` | Routing PM worker over the Layer 2 loop. Snapshot+ack in one short SDK-internal tx; dispatch on a separate connection. |
| 4 | `src/consistency.ts` | `waitForProjection` | Consistency-on-dispatch wait — polls subscription positions. |
| 5 | `src/instructed.ts` | `Instructed` facade | Registry of aggregates / projections / process managers; `dispatch`, `startWorker`. |

**Core vs conveniences.** Layers 0–2 are the **core** every SDK port
must provide (in some shape). Layers 3–5 are **conveniences** — they
could be replaced by language-idiomatic equivalents in another SDK
without breaking the SQL contract. See
[`docs/architecture.md`](../../docs/architecture.md) "SDK structure".

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
├── src/                       -- layers 0..5
│   ├── client.ts
│   ├── aggregate.ts
│   ├── subscription.ts
│   ├── process-manager.ts
│   ├── consistency.ts
│   ├── instructed.ts
│   ├── errors.ts
│   ├── types.ts
│   ├── index.ts               -- public re-exports
│   └── internal/              -- private (worker-id, retry, raw SQL)
└── test/                      -- node --test against docker-compose Postgres
```

Examples live at the repo root under [`examples/`](../../examples/).
