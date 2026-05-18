# SDK usage sketch (Phase 8 review companion)

This document is a placeholder/walkthrough that shows what writing
the bank-account example would *look like* against the SDK proposed
in [`sdk-design.md`](sdk-design.md). It exists to make the design's
abstraction level legible before any code lands under `sdks/typescript/`.

**Status: sketch.** Type signatures and call shapes are
indicative; nothing here compiles yet. Read it as
"if we built it as designed, this is what user code would read like".

**Revision note.** Iterative feedback produced seven refinements
that are now folded back into both this document and
`sdk-design.md`. The most consequential one (item 7) is a
reversal of D-0008:

7. **Handlers are opaque to the SDK (D-0016, supersedes
   D-0008).** The SDK does not give projection / PM handlers a
   database connection, a transaction, or an ORM handle. The
   handler receives the event, does whatever it does (Postgres,
   Elasticsearch, Redis, external API, in-memory cache, all of
   the above), and returns. If it returns successfully, the SDK
   advances the cursor in its own short transaction. If it
   throws, the cursor stays put and the event redelivers. The
   application owns idempotency — same contract Commanded
   provides (HND-031). The `ctx.tx` / `withTx` plumbing in
   earlier drafts of this document is gone; `Client.withTransaction`
   has been dropped from the SDK.

The first six refinements:

1. `execute` / `apply` (Commanded names), not `decide` / `apply`.
2. `initialState()` kept as a TS concession (Elixir's `defstruct`
   default mechanism has no clean TS analogue); rationale in
   `sdk-design.md` Layer 1.
3. Facade uses `register*` + a single `startWorker()`, matching
   absurd's `registerTask` + `startWorker` split. The application
   picks deployment topology by choosing which subset of
   `register*` calls each process makes; `startWorker()` runs
   everything registered in *this* process and returns one
   handle. The N-internal-cursors reality is hidden behind that
   one handle (acceptable because the topology choice was
   already made via register calls). The lower-layer building
   blocks (`startProjection`, `startProcessManager`) remain
   available for callers that want per-worker handles without
   the facade.
4. `apply(state, event)` takes the **domain payload `E`**, not
   `RecordedEvent<E>`. The SDK tracks aggregate version itself;
   `version: e.stream_version` never appears in user code.
5. `ProjectionDefinition` and `ProcessManagerDefinition` are
   exported, parallel to `AggregateDefinition`.
6. PM routing is a declarative per-event-type `routes` map, not
   a switch-on-`event_type` function. Absent keys = uninterested
   (mirrors Commanded's `interested?/1` callback semantics).

The examples below use the refined shapes throughout.

The sketch is in two parts:

1. **What absurd's quickstart looks like today**, as a baseline for
   the abstraction level a `pi`-ecosystem SDK consumer expects.
2. **The same bank-account demo, three ways:**
   - **2a** as the design currently stands (layered building blocks
     only).
   - **2b** with a proposed `Instructed` facade class layered on top
     (analogous to absurd's `Absurd` class).
   - **2c** side-by-side comparison.

The closing section poses the question the user surfaced
("does the proposed design offer the same level of abstraction as
absurd?") and lays out three answers to choose between.

---

## 1. Baseline: absurd's quickstart

From `absurd/sdks/typescript/examples/quickstart/`:

```ts
// worker.ts
import { Absurd } from "absurd-sdk";

const app = new Absurd({ queueName: "default" });

app.registerTask({ name: "provision-user", defaultMaxAttempts: 5 },
  async (params, ctx) => {
    const user = await ctx.step("create-user-record", async () => { /* ... */ });
    const delivery = await ctx.step("send-activation-email", async () => { /* ... */ });
    const activation = await ctx.awaitEvent(`user-activated:${user.user_id}`,
                                            { timeout: 3600 });
    return { user_id: user.user_id, status: "active", ...activation };
  });

await app.startWorker({ concurrency: 4 });
```

```ts
// client.ts
import { Absurd } from "absurd-sdk";
const app = new Absurd({ queueName: "default" });
const spawned = await app.spawn("provision-user", { user_id: "alice", email: "..." });
console.log(await app.awaitTaskResult(spawned.taskID, { timeout: 300 }));
await app.close();
```

Observations:

- **One class** (`Absurd`) does everything; connection is implicit
  (env var or default).
- The user writes **one async function per task**; the SDK supplies
  durability, retries, checkpointing, event-await.
- No mention of: pools, transactions, worker IDs, leases, batch
  size, polling intervals, error code translation. They all exist
  but live behind sensible defaults.

That is the abstraction bar. Now let us see what the proposed
`instructed` design produces.

---

## 2a. Bank-account demo as the design currently stands

The canonical CQRS demo: an `Account` aggregate (`open`, `deposit`,
`withdraw`), a `Balances` projection (one row per account), and a
`Transfer` process manager that moves money between two accounts
with compensation on the second leg failing.

### The aggregate

Uses Commanded's `execute` / `apply` names. State is **domain
only** — no version field, no SDK plumbing. `apply` receives
the domain payload (`AccountEvent`), not a `RecordedEvent<...>`.

```ts
// account.ts
import { AggregateDefinition, NewEvent } from "instructed-sdk";

type AccountState = { opened: boolean; balance: number };

type AccountEvent =
  | { type: "AccountOpened";     data: { owner: string } }
  | { type: "Deposited";         data: { amount: number } }
  | { type: "Withdrawn";         data: { amount: number } }
  | { type: "WithdrawalRefused"; data: { reason: string; amount: number } };

type AccountCommand =
  | { kind: "Open";     owner: string }
  | { kind: "Deposit";  amount: number }
  | { kind: "Withdraw"; amount: number };

export const Account: AggregateDefinition<AccountState, AccountCommand, AccountEvent> = {
  type: "Account",
  initialState: () => ({ opened: false, balance: 0 }),

  apply: (s, e) => {
    switch (e.type) {
      case "AccountOpened":     return { ...s, opened: true };
      case "Deposited":         return { ...s, balance: s.balance + e.data.amount };
      case "Withdrawn":         return { ...s, balance: s.balance - e.data.amount };
      case "WithdrawalRefused": return s;
    }
  },

  execute: (s, c): NewEvent<AccountEvent> | NewEvent<AccountEvent>[] => {
    switch (c.kind) {
      case "Open":
        if (s.opened) throw new Error("already open");
        return { event_id: crypto.randomUUID(), event_type: "AccountOpened",
                 data: { owner: c.owner } };
      case "Deposit":
        return { event_id: crypto.randomUUID(), event_type: "Deposited",
                 data: { amount: c.amount } };
      case "Withdraw":
        if (s.balance < c.amount)
          return { event_id: crypto.randomUUID(), event_type: "WithdrawalRefused",
                   data: { reason: "insufficient funds", amount: c.amount } };
        return { event_id: crypto.randomUUID(), event_type: "Withdrawn",
                 data: { amount: c.amount } };
    }
  },
};
```

### Issuing a command

```ts
import { Pool } from "pg";
import { Client, runCommand } from "instructed-sdk";
import { Account } from "./account";

const pool   = new Pool({ connectionString: process.env.DATABASE_URL });
const client = new Client(pool);

await runCommand(client, Account, `account-${accountId}`,
                 { kind: "Deposit", amount: 50 });
```

### The balances projection

The projection has its own **definition type** (`ProjectionDefinition`),
symmetric with `AggregateDefinition`. `startProjection` consumes
the definition and returns a worker handle the user owns.

The handler receives the event and a minimal context; it does
not receive a database connection. Idempotency is the
handler's concern — here we use a plain `pg` pool and write
idempotent UPSERTs. The same projection could equally write to
Elasticsearch, Redis, or anything else; the handler shape would
be identical.

```ts
// balances-projector.ts
import { ProjectionDefinition, startProjection } from "instructed-sdk";
import { client } from "./db";
import { appPool } from "./app-db";   // the application's OWN pool;
                                       // unrelated to the SDK's connection.

export const BalancesProjector: ProjectionDefinition = {
  name: "BalancesProjector",
  stream: "$all",
  handle: async (e, { position }) => {
    // Idempotent writes — each statement is safe to re-run on redelivery.
    // The `last_event` column lets us skip stale updates.
    switch (e.event_type) {
      case "AccountOpened":
        await appPool.query(
          `insert into balances(account_id, balance, last_event)
           values ($1, 0, $2)
           on conflict (account_id) do nothing`,
          [e.stream_uuid, e.event_number]);
        break;
      case "Deposited":
        await appPool.query(
          `update balances set balance = balance + $1, last_event = $2
           where account_id = $3 and last_event < $2`,
          [e.data.amount, e.event_number, e.stream_uuid]);
        break;
      case "Withdrawn":
        await appPool.query(
          `update balances set balance = balance - $1, last_event = $2
           where account_id = $3 and last_event < $2`,
          [e.data.amount, e.event_number, e.stream_uuid]);
        break;
    }
  },
};

const worker = startProjection(client, BalancesProjector);
process.on("SIGTERM", () => worker.close());
```

Notice:

- The handler signature is `(event, ctx) => Promise<void>`. No
  transaction, no ORM. The SDK has no opinion about how the
  projection persists state.
- Idempotency is on the application: the `last_event < $2` guard
  makes redelivery harmless (re-applying the same
  `Deposited(50)` for `event_number = 1234` no longer mutates
  the row because `last_event` already equals 1234).
- If the same projection wrote to Elasticsearch, the handler
  would `await es.index({ id: e.event_id, document: ... })`
  with `op_type: "create"` and treat the resulting
  `version_conflict` as a success. Same shape, different
  technology.

### The Transfer process manager

Routing is a **declarative `routes` map** keyed by event type;
an event whose type is absent is uninteresting. Each route
returns the `processId` (or a `{kind, processId}` shape if the
event should *start* or *stop* an instance, mirroring
Commanded's `interested?` `{:start, id} | {:continue, id} | {:stop, id}`).

A bare string returned from a route is sugar for `{kind:
'continue', processId}`. `TransferRequested` returns `{kind:
'start', processId}` so the SDK skips the snapshot load and
starts a fresh state.

Like projections, PMs have a `ProcessManagerDefinition` type,
symmetric with `AggregateDefinition` and `ProjectionDefinition`.

```ts
// transfer-pm.ts
import { ProcessManagerDefinition, startProcessManager } from "instructed-sdk";
import { Account } from "./account";
import { client, dispatchClient } from "./db";

type TransferState =
  | { stage: "starting" }
  | { stage: "debited"; from: string; to: string; amount: number }
  | { stage: "done" }
  | { stage: "refunded" };

// Convention: every transfer-related event carries metadata.transferId.
// The PM does not need to know which aggregate emitted the event —
// the route function reads the metadata field.
const byTransferId = (e: any) => e.metadata?.transferId ?? null;

export const TransferProcessManager:
  ProcessManagerDefinition<TransferState, any> = {
  name:   "TransferProcessManager",   // also used as snapshot source_type
  stream: "$all",

  routes: {
    TransferRequested:  (e) => ({ kind: "start",    processId: e.data.transferId }),
    Withdrawn:          (e) => ({ kind: "continue", processId: byTransferId(e) }),
    WithdrawalRefused:  (e) => ({ kind: "stop",     processId: byTransferId(e) }),
    Deposited:          (e) => ({ kind: "stop",     processId: byTransferId(e) }),
  },

  initialState: () => ({ stage: "starting" }),

  handle: async (state, event) => {
    switch (event.event_type) {
      case "TransferRequested":
        return {
          state,
          commands: [{
            streamUuid: `account-${event.data.from}`,
            aggregate:  Account,
            command:    { kind: "Withdraw", amount: event.data.amount },
          }],
        };
      case "Withdrawn":
        return {
          state: { stage: "debited", from: event.stream_uuid,
                   to: event.data.to, amount: event.data.amount },
          commands: [{
            streamUuid: `account-${event.data.to}`,
            aggregate:  Account,
            command:    { kind: "Deposit", amount: event.data.amount },
          }],
        };
      case "WithdrawalRefused":
        return { state: { stage: "refunded" } };
      case "Deposited":
        return { state: { stage: "done" } };
    }
  },
};

const worker = startProcessManager(client, dispatchClient, TransferProcessManager);
process.on("SIGTERM", () => worker.close());
```

Three things this shape gets right that the first sketch did not:

- The PM declares its event vocabulary **once**, in `routes`.
  Adding a new event the PM cares about means adding one map
  entry, not extending a switch in two places.
- The PM cannot accidentally process an event it forgot to
  route — `handle` is only reached for routed events.
- The `{kind: 'start' | 'continue' | 'stop'}` distinction is
  declarative at the routing layer, not implicit in `handle`.
  Stopped PMs have their snapshot deleted by the SDK; the
  next event for that `processId` starts a fresh instance.

### Strong consistency on the read-after-write

```ts
import { waitForProjection } from "instructed-sdk";

const appended = await runCommand(client, Account, streamUuid,
                                  { kind: "Deposit", amount: 50 });
await waitForProjection(client, appended,
                        [{ stream: "$all", name: "BalancesProjector" }],
                        { timeout: 2000 });
// safe to read from balances now
```

### Wiring

```ts
// db.ts
import { Pool } from "pg";
import { Client } from "instructed-sdk";

const pool         = new Pool({ connectionString: process.env.DATABASE_URL });
const dispatchPool = new Pool({ connectionString: process.env.DATABASE_URL });
// ^ second pool so the PM's dispatch session never shares a connection
//   with its persist-and-ack tx (D-0011/D-0012 lock-set disjointness).

export const client         = new Client(pool);
export const dispatchClient = new Client(dispatchPool);
```

---

## 2b. The same demo, with an `Instructed` facade

With the layer-5 facade from `sdk-design.md` §3.5, the user code
becomes the worker process below. Aggregates / projections / PMs
are all registered the same way; `startWorker()` runs everything
registered in this process and returns a single handle. Mirrors
absurd's `registerTask` + `startWorker` shape exactly.

The **application controls topology by choosing which `register*`
calls each process makes**: a worker process registers projections
and PMs and calls `startWorker()`; a command-issuing process
registers only the aggregates it needs to dispatch against and
never calls `startWorker()`; a mixed process registers both.

```ts
// worker.ts
import { Instructed } from "instructed-sdk";
import { Account } from "./account";
import { BalancesProjector } from "./balances-projector";
import { TransferProcessManager } from "./transfer-pm";

const app = new Instructed({ db: process.env.DATABASE_URL });

app.registerAggregate(Account);                  // needed for dispatch + by the PM
app.registerProjection(BalancesProjector);
app.registerProcessManager(TransferProcessManager);

const worker = await app.startWorker();
process.on("SIGTERM", async () => {
  await worker.close();
  await app.close();
});
```

```ts
// client.ts (a separate process; registers only the aggregate)
import { Instructed } from "instructed-sdk";
import { Account } from "./account";

const app = new Instructed({ db: process.env.DATABASE_URL });
app.registerAggregate(Account);

const appended = await app.dispatch("Account", `account-${id}`,
                                    { kind: "Deposit", amount: 50 },
                                    { consistency: ["BalancesProjector"] });
// consistency: [...] internally does the waitForProjection per D-0010.
await app.close();
```

Per-projection tuning lives on `registerProjection`:

```ts
app.registerProjection(BalancesProjector, {
  batchSize: 100,
  onError: (err) => log.error({ err }, "balances handler failed"),
});
```

What `Instructed` hides:

- The `Client` / `dispatchClient` split — the facade owns both
  pools and routes calls correctly (dispatch goes through the
  dispatch pool, persist-and-ack through the main pool). The
  user can still drop down via `app.client()` and
  `app.dispatchClient()` when needed.
- Worker IDs, lease seconds, batch sizes, poll intervals — all
  defaulted by the facade; overridable per `register*` call.
- The fact that `startWorker()` fans out into N internal
  subscription loops (one per registered projection / PM). The
  topology choice (which N) was already made via the user's
  register calls, so collapsing the fan-out to a single handle
  is not obfuscation — it's the absurd shape.
- The `consistency: [...]` option on `dispatch` is shorthand for
  the explicit `waitForProjection` call — which honours D-0010
  because the list is *still* explicit, just expressed at the
  dispatch call site.

What `Instructed` does **not** hide:

- The aggregate / projection / PM definitions themselves
  (`apply`, `execute`, `handle`) — those are application logic
  and cannot be abstracted away without sacrificing the
  CQRS/ES idiom.
- The handler signature itself — `(event, ctx) => Promise<void>`,
  per D-0016. The SDK does not (and the facade does not) give
  handlers a database connection; idempotency is the handler's
  concern, regardless of whether the handler is a projection
  into Postgres, Elasticsearch, Redis, or anything else.
- The distinction between aggregate streams, projections, and
  process managers — these are three different things in
  CQRS/ES, registered through three different `register*` calls.
- The topology choice. The user picks what runs where by
  choosing which subset of register calls each process makes;
  the facade never auto-discovers handlers.

---

## 2c. Side-by-side comparison

| Concern                          | absurd                          | instructed (2a, layered)                            | instructed (2b, facade)               |
|----------------------------------|---------------------------------|------------------------------------------------------|----------------------------------------|
| Connection wiring                | implicit (env var)              | explicit `new Pool` + `new Client`                   | implicit (env var); facade owns pools  |
| "Hello world" surface area       | `new Absurd()` + register + start | `new Pool` + `new Client` + `runCommand` / `startSubscriptionWorker` | `new Instructed()` + register + start  |
| Number of registered concepts    | task                            | aggregate, projection, PM (three primitives)         | aggregate, projection, PM              |
| Dispatch site                    | `app.spawn("task-name", params)` | `runCommand(client, Account, streamUuid, command)`   | `app.dispatch("Account", streamUuid, command)` |
| Cross-handler ordering boilerplate | none                          | `dispatchClient` pool, second `Client` instance      | none (facade routes it)                |
| Per-handler transaction          | not exposed                     | not exposed; handler owns idempotency (D-0016)       | not exposed; handler owns idempotency (D-0016) |
| Strong-consistency wait          | n/a in absurd's model           | `waitForProjection(client, appended, [{stream, name}])` | `dispatch(..., { consistency: [...] })` |
| Worker lifecycle                 | one `startWorker` (one loop, many tasks) | one `start*` per worker, one handle each | one `startWorker()`, one handle for all (fans out internally) |

The 2a column is more verbose than absurd's quickstart in three
mechanical ways: connection wiring, the dispatch-pool split,
and one `start*` call per worker. The facade in 2b folds all
three away without weakening any decision recorded in
`decisions.md`:

- Pools are facade-owned.
- The N-cursor fan-out happens inside `startWorker()`; the user
  chose the N when they made N `register*` calls.
- Deployment topology is expressed by which subset of register
  calls each process makes — the absurd pattern exactly.

What 2a buys for the extra verbosity: every concept is a separate
import, the SDK has no hidden state across calls, and an
application that wants to do something unusual (share a transaction
with non-`instructed` writes, run a worker out of a long-lived
script, use a different driver) does not have to fight the
facade.

---

## 3. The question this sketch frames

Three plausible answers to "what abstraction level should the SDK
ship?":

**Option A — layered building blocks only (the current design).**
Ship `Client`, `runCommand`, `startSubscriptionWorker`,
`startProcessManager`, `waitForProjection`. The bank-account
example reads like §2a. No facade.

- Pros: minimal API surface; every call site is explicit about
  which primitive it is using; no hidden coupling between
  registrations. Mirrors the SDK's "thin layer over Postgres"
  framing in the ROADMAP.
- Cons: noisier than absurd for the common case; the dispatch-pool
  requirement is a footgun if the user wires it wrong; a beginner
  cannot copy-paste a quickstart that is as short as absurd's.

**Option B — facade + layered, both shipped (recommended).**
Ship the layered API from §2a as the foundation and the
`Instructed` facade from §2b on top of it. The facade is a pure
composition of the lower layers; advanced users drop down when
they need to. This is the same shape absurd takes (the `Absurd`
class is the only documented entry point but it is built on
public lower-level functions like `claimTasks`, `executeTask`,
`bindToConnection`).

- Pros: matches absurd's abstraction bar; the bank-account example
  reads like §2b; the layered API stays available for advanced
  use; no decision in `decisions.md` needs to change.
- Cons: two surfaces to keep documented; the facade tempts users
  to register everything in one process when the SDK has no
  opinion about deployment topology.

**Option C — facade only, layered API unexported.**
Hide the layered building blocks; users only see `Instructed`.

- Pros: maximally absurd-shaped surface.
- Cons: forecloses on uses we know we will hit (sharing a
  transaction with non-`instructed` writes is a primary D-0008
  motivation; a user-owned `Client` is necessary for that). Not
  recommended.

The author of this sketch leans **Option B**. The design as
written (§2a) is correct and complete; adding the facade is purely
additive and does not require revisiting any prior decision. If
B is chosen, `docs/sdk-design.md` gets a new §3 layer 5
("`Instructed` facade") and the implementation sequencing in §10
adds a step between the existing steps 5 and 6 to ship the
facade before the bank-account example is written.

If A is chosen, the bank-account example will read like §2a and
we accept the verbosity gap with absurd.

If C is chosen, D-0008's "share a tx with application writes"
motivation needs a different mechanism, and that is a decision
worth recording explicitly.

**Status: B chosen, recorded as D-0015.** `sdk-design.md` now
includes the layer 5 facade and the API-naming refinements
(`execute` / `apply`, domain-only `apply` signature, declarative
PM `routes`, exported `ProjectionDefinition` /
`ProcessManagerDefinition` types). The implementation sequencing
builds the facade between layers 4 and the bank-account example.

---

## 4. What this document is *not*

- Not a spec. The layered API in §2a is what `sdk-design.md`
  already commits to; the facade in §2b is a proposal that needs
  user sign-off before it lands in `sdk-design.md`.
- Not compilable code. The imports, type names, and call shapes
  are illustrative.
- Not a substitute for the bank-account example under
  `sdks/typescript/examples/bank-account/`. That example is the
  Phase 8 done-criterion and will be built once the SDK exists.
