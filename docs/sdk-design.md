# Reference SDK design (Phase 8)

This document is the design pass for the first `instructed` SDK. No
SDK code lands until it stops surfacing new questions. The SQL file
is still the spec (`sql/instructed.sql` / `docs/sql-contract.md`);
this document only describes how a single language binding wraps
that contract.

The companion outputs of this pass are two new decisions
(D-0013 language choice, D-0014 transaction-boundary ergonomics)
and the resolution of OQ-0002 and OQ-0003. Both open questions
were explicitly deferred to Phase 8 in their original entries.

---

## 1. Language: TypeScript (Node 18+)

Absurd ships three SDKs (`sdks/{go,python,typescript}`). The
TypeScript binding is the most fleshed-out (1.6 kloc single file,
working worker loop, error code translation, examples) and is the
one a user is most likely to bridge from absurd to `instructed`
inside the same process. Matching that stack makes the
absurd-bridge pattern of D-0011 directly demonstrable in the
bank-account example.

Recorded as **D-0013**.

Concrete choices:

- **Runtime:** Node 18+ (matches absurd's `engines.node`).
- **Driver:** `pg` 8.x as a peer dependency. The SDK accepts a
  `pg.Pool`, `pg.Client`, or `pg.PoolClient` via the same
  `Queryable` type alias absurd uses
  (`Pick<pg.Client, "query"> | Pick<pg.PoolClient, "query">`).
- **Module shape:** dual ESM/CJS build, mirroring absurd's
  `tsconfig.build.json` + `tsconfig.cjs.json` setup.
- **Testing:** `node --test` against the live Postgres already
  installed in `instructed_test` (Phase 7's Docker compose
  service). Test fixtures truncate `instructed.*` tables between
  cases; no extra harness in v1.

Out of scope for Phase 8: Python and Go ports. Tracked under
ROADMAP "Beyond".

---

## 2. Package layout

```
sdks/typescript/
  package.json                -- name: instructed-sdk, peer dep pg ^8
  tsconfig.{build,cjs,json}
  src/
    index.ts                  -- re-exports the public surface
    client.ts                 -- Instructed class: connection wiring,
                                 low-level procedure wrappers
    errors.ts                 -- SQLSTATE -> Error subclass mapping
    types.ts                  -- RecordedEvent, ExpectedVersion,
                                 ClaimResult, snapshot/subscription types
    aggregate.ts              -- runCommand load-execute-append loop
                                 with OCC retry (D-0005)
    subscription.ts           -- claim/heartbeat/process/release worker
    process-manager.ts        -- PM worker (subscription + snapshot +
                                 dispatch loop), built on subscription.ts
    consistency.ts            -- waitForProjection helper (D-0010)
    internal/
      sql.ts                  -- raw SQL strings, parameter packing
      retry.ts                -- shared backoff / lease-aware sleep
      worker-id.ts            -- default = `${hostname}:${pid}:${nanoid}`
  test/
    *.test.ts                 -- one file per src/ module
    fixtures.ts               -- pool factory, truncate helper
  examples/
    bank-account/             -- the Phase 8 done-criterion target
                                 (built in a follow-up pass, not in
                                 this design)
```

`internal/` is unexported; everything reachable from `src/index.ts`
is part of the public contract.

---

## 3. Layered public surface

The SDK is deliberately layered. Each layer is usable on its own;
each higher layer is a documented composition of the layer below.
This is the answer the Phase 8 ROADMAP implies ("the SDK encodes
the call sequence and a worker loop; it never holds invariant-
bearing state").

Layer 0–4 are building blocks. **Layer 5 (`Instructed` facade)**
is the absurd-style top-level convenience added in response to
`sdk-usage-sketch.md` Option B; see §3.5 below.

### Layer 0 — `Client` (thin procedure wrappers)

One method per stored procedure. Argument shapes match the SQL
contract verbatim; results are typed rows. No retry, no
transactions opened by the SDK, no state. The only translation
is SQLSTATE → typed `Error` subclass.

```ts
class Client {
  constructor(con: Queryable | pg.Pool, opts?: ClientOptions);

  // events
  appendToStream(
    streamUuid: string,
    expected: ExpectedVersion,
    events: NewEvent[],
    options?: AppendOptions,
  ): Promise<AppendedEvent[]>;
  readStream(streamUuid: string, fromVersion: bigint, qty: number): Promise<RecordedEvent[]>;
  readAll(fromEventNumber: bigint, qty: number): Promise<RecordedEvent[]>;

  // snapshots
  recordSnapshot(snap: SnapshotInput): Promise<void>;
  readSnapshot(sourceUuid: string): Promise<Snapshot>;        // throws SnapshotNotFound
  deleteSnapshot(sourceUuid: string): Promise<void>;          // idempotent

  // subscriptions
  claimSubscription(...): Promise<ClaimResult>;               // 'claimed' | 'already_claimed'
  extendSubscriptionClaim(...): Promise<{ claimExpiresAt: Date }>;
  releaseSubscription(...): Promise<void>;
  readSubscriptionBatch(...): Promise<RecordedEvent[]>;       // requires open tx
  advanceSubscription(...): Promise<{ lastSeen: bigint }>;    // requires open tx
  readSubscriptionPosition(...): Promise<{ lastSeen: bigint }>;
  deleteSubscription(...): Promise<void>;

}
```

`ExpectedVersion` is a tagged union:

```ts
type ExpectedVersion =
  | { kind: 'any' }
  | { kind: 'noStream' }
  | { kind: 'streamExists' }
  | { kind: 'exact'; version: bigint };
```

Constants `expected.any`, `expected.noStream`, `expected.streamExists`,
`expected.exact(n)` provide the idiomatic call sites.

Transactions are SDK-internal. `runCommand` (layer 1) and the
worker loops (layers 2/3) open short transactions through the
client as needed; the public `Client` API exposes the
individual procedures only. There is no user-facing
`withTransaction` helper — see D-0016 for the reasoning
(handlers are opaque to the SDK; the SDK does not pass
connections to user code).

### Layer 1 — Aggregate runner

Naming and shape track Commanded's `execute/2` + `apply/2`
callbacks (`lib/commanded/aggregates/aggregate.ex`). The two
TypeScript-specific deviations are called out below.

```ts
interface AggregateDefinition<S, C, E> {
  type: string;                          // source_type for snapshots
  initialState(): S;                      // see note below
  execute(state: S, command: C): NewEvent<E> | NewEvent<E>[];  // pure
  apply(state: S, event: E): S;          // pure; SDK tracks version
  snapshotPolicy?: SnapshotPolicy<S>;    // optional, see §6
}
```

Notes:

- **`execute` / `apply`** match Commanded's callback names verbatim.
  `execute` may return a single event or an array; the SDK
  normalises. Returning `[]` (or `undefined`) means the command is
  a no-op — nothing is appended, no error, no version bump.
  Mirrors Commanded's `:ok | {:ok, []} | nil | []` no-op forms.
- **`apply` takes the domain event payload (`E`), not
  `RecordedEvent<E>`.** The SDK tracks aggregate version itself
  (it knows the stream_version of every event it folds); the
  user's domain `apply` only deals with domain state. This is the
  point §2a of `sdk-usage-sketch.md` flagged: `version:
  e.stream_version` does not belong in user code.
- **`initialState()`** is a TS concession. Commanded uses
  `defstruct` for the same effect (the aggregate module *is* its
  state, with field defaults); TS has no equivalent, so the user
  supplies a factory. The factory is called once per aggregate
  load; the SDK fills version tracking around it.

```ts
interface RunCommandOptions {
  retryBudget?: number;        // default 8
  expectedVersion?: ExpectedVersion;
                               // default 'exact' on loaded version
}

async function runCommand<S, C, E>(
  client: Client,
  def: AggregateDefinition<S, C, E>,
  streamUuid: string,
  command: C,
  opts?: RunCommandOptions,
): Promise<AppendedEvent[]>
```

`runCommand` realises the D-0004/D-0005 contract literally:

1. `readSnapshot(streamUuid)` (swallow `SnapshotNotFound`).
   A snapshot carries `(state, version)`; either may be absent
   (fresh stream).
2. `readStream(streamUuid, version + 1, pageSize)` paged until
   empty. For each event: `state = def.apply(state, event.data)`;
   `version = event.stream_version`. The SDK owns the version
   counter end-to-end — user code never reads or writes it.
3. `events = def.execute(state, command)` → normalise to array.
   Empty array short-circuits (no append, returns `[]`).
4. `appendToStream(streamUuid, expected.exact(version), events)`.
5. On `WrongExpectedVersion`: re-load from step 1, re-execute,
   retry up to `retryBudget`. Exhausting the budget raises
   `RetryBudgetExhausted` carrying the last error.
6. After commit: if `snapshotPolicy.shouldSnapshot(state, newVersion)`,
   `recordSnapshot(...)` in a follow-up call (NOT in the same
   transaction — snapshot writes are best-effort optimisations of
   the load path, and bundling them risks blocking unrelated
   writers on the snapshots row).

There is no aggregate cache (D-0004), no advisory lock (D-0005),
no concurrency control beyond OCC retry.

### Layer 2 — Projection worker

Named `Projection*` rather than `Subscription*` so the API surface
uses the CQRS term (the SQL contract still calls the underlying
primitive a subscription; the SDK uses "projection" for the
user-facing concept and reserves "subscription" for the raw
leased cursor).

Per D-0016, the handler is **opaque to the SDK**: it receives
the event and a minimal context, returns a promise, and the SDK
advances the cursor iff the promise resolves. The context
carries no connection, no ORM handle, no transaction. The
handler writes to whatever it writes to — Postgres,
Elasticsearch, Redis, an external API, an in-memory cache —
and is responsible for its own idempotency (NG-0015).

```ts
interface HandlerContext {
  workerId: string;
  position: { eventNumber: bigint; streamVersion: bigint };
  signal: AbortSignal;     // aborted on graceful shutdown / lease loss
}

type ProjectionHandler<E = any> =
  (event: RecordedEvent<E>, ctx: HandlerContext) => Promise<void>;

interface ProjectionDefinition<E = any> {
  name: string;                          // becomes subscription name
  stream?: string;                       // default '$all'
  handle: ProjectionHandler<E>;
  startFrom?: 'origin' | 'current' | bigint;   // first-claim only
  selector?: (e: RecordedEvent<E>) => boolean; // SDK-side; see §7
}

interface ProjectionWorkerOptions {
  workerId?: string;                  // default = generated
  batchSize?: number;                 // default 50
  leaseSeconds?: number;              // default 30
  heartbeatInterval?: number;         // default = leaseSeconds / 3
  pollInterval?: number;              // ms; default 250
  onError?: (err: Error) => void;
}

interface RunningWorker {
  close(): Promise<void>;             // graceful: stop loop, release lease
  stopped: Promise<void>;              // resolves when the loop exits
}

function startProjection<E>(
  client: Client,
  def: ProjectionDefinition<E>,
  opts?: ProjectionWorkerOptions,
): RunningWorker
```

The split between `ProjectionDefinition` (what the projection IS —
portable, declarative) and `ProjectionWorkerOptions` (how the
worker that runs it BEHAVES — deployment-shaped) is intentional
and mirrors the symmetry with `AggregateDefinition`.

The worker loop, per D-0016:

```
claim_subscription(...)
loop:
  events = read_subscription_batch(...)        -- short SDK tx, lock released on return
  if events.length === 0:
    sleep(pollInterval); continue
  for e of events:
    if (selector && !selector(e)) continue
    await handler(e, { workerId, position, signal })   -- NO SDK transaction
  advance_subscription(..., last.eventNumber)  -- short SDK tx, separate
  // heartbeat runs on a parallel timer; on SubscriptionLeaseLost
  // it aborts ctx.signal and sets a "stop" flag; the next loop
  // iteration exits.
on shutdown / lease lost:
  release_subscription(...) (best effort; ignore IS022)
```

The heartbeat lives in `setInterval`, fires
`extendSubscriptionClaim`, and on `IS022` (or `IS020`) aborts
`ctx.signal` (signalling any in-flight handler that it should
stop) and sets an internal `aborted = true`. The loop checks
`aborted` before the next batch read. **A worker MUST NOT call
`advance_subscription` after lease loss** (D-0006); a handler
that completes after lease loss has its ack silently dropped —
the new lease holder will redeliver the event.

Selector locus: **SDK-side filtering** per the resolution of
OQ-0003 below (§7). The handler is only called for matching
events; `advance_subscription` is called with the highest
*delivered-or-skipped* event_number, satisfying INV-SUB-P-050.

### Layer 3 — Process manager worker

A PM is a projection on `'$all'` (or a chosen stream) that loads
PM state from a snapshot, runs the user's `handle` (outside any
SDK transaction, per D-0016), dispatches the returned commands
through `runCommand` on a separate session (per D-0011/D-0012
lock-set disjointness), and then writes the new snapshot +
advances the cursor in one short SDK-internal transaction.
Only the SDK's own bookkeeping (snapshot + cursor) shares a
transaction; the user's `handle` does not.

Routing tracks Commanded's `interested?/1` callback shape but is
expressed as a declarative map keyed by event type rather than a
switch-on-`event_type` function. Any event whose type is absent
from `routes` is uninteresting and the cursor advances past it
without calling `handle`.

```ts
type RouteResult =
  | { kind: 'start'; processId: string }
  | { kind: 'continue'; processId: string }
  | { kind: 'stop'; processId: string }
  | { kind: 'ignore' };

// Per-event-type router: usually returns a processId derived from
// the event's data or metadata. Returning a bare string is sugar
// for { kind: 'continue', processId: <string> }.
type RouteFn<E> = (event: RecordedEvent<E>) => string | RouteResult | null;

interface ProcessManagerDefinition<S, E> {
  name: string;                          // becomes subscription name + snapshot prefix
  pmType: string;                        // source_type for snapshot
  stream?: string;                       // default '$all'
  routes: { [eventType: string]: RouteFn<E> };
  initialState(): S;
  handle(state: S, event: RecordedEvent<E>):
    Promise<{ state: S; commands?: DispatchedCommand[] }>;
}

interface DispatchedCommand {
  streamUuid: string;
  aggregate: AggregateDefinition<any, any, any>;
  command: any;
}

function startProcessManager<S, E>(
  client: Client,                 // pool used for persist-and-ack
  dispatchClient: Client,         // pool used for dispatch (separate session)
  def: ProcessManagerDefinition<S, E>,
  opts?: ProcessManagerWorkerOptions,
): RunningWorker
```

Per-event loop body, per D-0016:

```
routeFn = def.routes[event.event_type]
if (!routeFn) { advance; continue }            // not interested
routed = normaliseRouteResult(routeFn(event))
if (routed.kind === 'ignore') { advance; continue }

sourceUuid = `${def.name}-${routed.processId}`
state      = routed.kind === 'start'
             ? def.initialState()
             : (await client.readSnapshot(sourceUuid)) ?? def.initialState()
{ state, commands = [] } = await def.handle(state, event)
for (c of commands):
  // dispatch on the SEPARATE client/connection (D-0011/D-0012 lock-set
  // disjointness). Dispatch failures throw out of the handler call;
  // the SDK does NOT advance the cursor; the event redelivers.
  await runCommand(dispatchClient, c.aggregate, c.streamUuid, c.command)
// Persist-and-ack: snapshot + cursor advance in one short SDK tx.
// The handler itself ran outside any tx (D-0016); only the snapshot
// write and the advance share a transaction here.
BEGIN
  if (routed.kind === 'stop'):
    deleteSnapshot(sourceUuid)
  else:
    recordSnapshot({ sourceUuid, sourceType: def.pmType,
                     sourceVersion: event.eventNumber, data: state })
  advanceSubscription(..., event.eventNumber)
COMMIT
```

The PM's snapshot write IS co-transactional with the cursor
advance — the snapshot is SDK-owned state (PM-024 absorption
depends on it staying consistent with `last_seen`), not
application data, so D-0016's "handlers are opaque" reasoning
does not apply to it.

Note `dispatchClient` is a different `Client` (typically backed
by a different pool, or at minimum a separately checked-out
connection). This is internal SDK plumbing required by D-0011/
D-0012 lock-set disjointness; the PM author does not see it.

A PM handler that fails idempotency on redelivery is the PM
author's concern. Typical patterns: derive dispatched command
`event_id`s deterministically from `(pmName, processId,
eventNumber)` so re-dispatch hits `duplicate_event` (IS004)
harmlessly; or use the PM state to gate transitions
(`if state.stage !== 'starting' return { state, commands: [] }`).

### Layer 3.5 — `Instructed` facade (layer 5)

A single top-level class that bundles connection management and
aggregate lookup, layered purely on the building blocks above.
Matches the abstraction level of absurd's `Absurd` class.

```ts
interface InstructedOptions {
  db?: pg.Pool | pg.Client | string;     // env-var default like absurd
  dispatchDb?: pg.Pool | pg.Client | string;
                                          // separate pool for PM dispatch;
                                          // defaults to a sibling Pool with the
                                          // same connection string
  log?: Log;
  defaults?: {
    leaseSeconds?: number;
    batchSize?: number;
    pollInterval?: number;
    retryBudget?: number;
  };
}

interface RegistrationOptions {
  batchSize?: number;
  leaseSeconds?: number;
  heartbeatInterval?: number;
  pollInterval?: number;
  onError?: (err: Error) => void;
}

class Instructed {
  constructor(opts?: InstructedOptions | string);

  // ---- register: declares what this process can do --------------------
  // Application controls deployment topology by choosing which
  // register* calls to make in which process. Mirrors absurd's
  // registerTask / startWorker split.
  registerAggregate<S, C, E>(def: AggregateDefinition<S, C, E>): void;
  registerProjection<E>(def: ProjectionDefinition<E>,
                        opts?: RegistrationOptions): void;
  registerProcessManager<S, E>(def: ProcessManagerDefinition<S, E>,
                               opts?: RegistrationOptions): void;

  // ---- dispatch a command (uses the aggregate registry) --------------
  dispatch<C>(aggregateType: string, streamUuid: string, command: C,
              opts?: { consistency?: string[] | { stream: string; name: string }[];
                       consistencyTimeout?: number;
                       retryBudget?: number;
                       expectedVersion?: ExpectedVersion;
              }): Promise<AppendedEvent[]>;

  // ---- start the worker: runs every registered projection + PM -------
  // Returns a single handle; close() stops them all. Internally fans
  // out into N subscription loops (one per registered projection / PM),
  // each with its own leased cursor; that fan-out is an implementation
  // detail of the facade.
  startWorker(opts?: { workerId?: string }): Promise<RunningWorker>;

  // ---- escape hatches to lower layers ---------------------------------
  client(): Client;            // the main persist-and-ack client
  dispatchClient(): Client;    // the dispatch-pool client

  close(): Promise<void>;      // stops the worker (if started) and closes
                                // any owned pools.
}
```

Design notes:

- **`register*` declares; `startWorker()` runs.** The split is
  absurd's: the application picks topology by choosing which
  subset of register calls each process makes. A process that
  only dispatches commands registers aggregates and never calls
  `startWorker()`; a worker process registers projections / PMs
  and calls `startWorker()` once. Mixed processes register both.
- **One handle, many internal cursors.** `startWorker` fans out
  into one subscription loop per registered projection / PM. The
  handle's `close()` stops them all and waits for graceful
  release of every lease. This hides the N-cursors reality —
  acceptable because the topology choice (which N) was already
  made by the user's register calls.
- **Per-projection tuning lives on `register*`.** `RegistrationOptions`
  carries `batchSize`, `leaseSeconds`, etc. Facade-wide defaults
  go in `InstructedOptions.defaults`. `startWorker` itself only
  takes process-shaped options (worker id).
- **`registerAggregate` is necessary** because `dispatch` needs
  to look up the aggregate's `execute` / `apply` / `type` by
  name. The layered API (`runCommand(client, Account, ...)`)
  takes the definition by value and needs no registry.
- **`consistency` accepts a bare string list** (subscription
  names on `'$all'`) as sugar; the explicit form is the
  `{stream, name}` list. The list is *still* explicit per D-0010 —
  the facade does not add a `:strong` shorthand.
- **`close()` stops the worker and closes owned pools** in one
  call. Mirrors absurd's `app.close()`.

Full bank-account example using the facade is in
[`sdk-usage-sketch.md`](sdk-usage-sketch.md) §2b.

### Layer 4 — Consistency-on-dispatch wait

```ts
async function waitForProjection(
  client: Client,
  appended: AppendedEvent | AppendedEvent[],
  subscriptionNames: { stream: string; name: string }[],
  opts?: { pollInterval?: number; timeout?: number },
): Promise<void>
```

For each named subscription, polls `readSubscriptionPosition`
until `lastSeen >= target` (the highest event_number from
`appended` for `$all` subscriptions; the matching
`streamVersion` for per-stream subscriptions). Throws
`ConsistencyTimeout` on timeout. No `:strong` shorthand (D-0010).

---

## 4. Error-type hierarchy

One class per SQLSTATE in the catalogue, plus a small set of
SDK-level errors. Every class extends `InstructedError` so callers
can `instanceof` once at the boundary if they want to be lenient.

```
InstructedError
├── AppendError
│   ├── WrongExpectedVersion      IS001  { actualVersion?, expectedVersion? }
│   ├── StreamExists              IS002  { streamUuid }
│   ├── StreamNotFound            IS003  { streamUuid }   // also from readStream, claimSubscription
│   ├── DuplicateEvent            IS004  { eventId }
│   └── ReservedStreamUuid        IS005  { streamUuid }
├── SnapshotNotFound              IS010  { sourceUuid }
├── SubscriptionError
│   ├── SubscriptionNotFound      IS020  { streamUuid, subscriptionName, shard }
│   ├── SubscriptionAlreadyClaimed IS021 -- reserved; never thrown in v1
│   └── SubscriptionLeaseLost     IS022  { streamUuid, subscriptionName, shard, holder? }
├── InvalidParameterValue         22023  { hint }
├── AppendOnlyViolation           IS006  -- never thrown; surfaced if user
│                                            bypasses procedures
└── SDK-level (no SQLSTATE):
    ├── RetryBudgetExhausted      { lastError, attempts }
    ├── ConsistencyTimeout        { waitedMs, missing: string[] }
    └── HandlerError              { cause, event } -- wraps user-thrown
                                                       errors for onError
```

`StreamNotFound` is shared between `appendToStream(stream_exists)`,
`readStream`, and `claimSubscription` — same SQLSTATE, same class.
Callers distinguish by the calling context, not by type.

`AppendOnlyViolation` exists so a user who sees `IS006` gets a
meaningful name in the stack trace instead of a raw Postgres
`error` object; the hint string in the SQL trigger already says
"you bypassed the contract".

Translation lives in `errors.ts` as a single `mapPgError(err)`
function called from every procedure wrapper. The mapper inspects
`err.code` (the SQLSTATE) and constructs the right subclass,
copying `message`, `detail`, `hint`, and any procedure-supplied
context (we parse positional hints from the message in a small
set of cases; see the IS001 / IS004 entries above for which fields
get populated).

Standard Postgres errors (connection loss, serialization failure
on infrastructure issues) pass through unwrapped — they are
infrastructure failures, not contract failures, and the caller
should handle them as such.

---

## 5. OQ-0002 resolved (then superseded) — transaction-boundary ownership

**Original resolution (D-0014):** the SDK exposed both shapes —
a low-level `Client.withTransaction` for callers who wanted to
share a transaction with their own application writes, and a
high-level worker helper that internally opened the transaction
and passed it to the handler via `ctx.tx`.

**Current resolution (D-0016, supersedes D-0014's motivation):**
the SDK does not pass transactions to handlers at all.
`Client.withTransaction` is dropped. The handler is opaque to
the SDK; the SDK opens short transactions for its own
bookkeeping (`read_subscription_batch`, `advance_subscription`,
PM snapshot+advance) before and after the handler call, never
around it. Applications that need to make atomic writes
alongside an event handler do so with their own idempotency
mechanism — the SDK's transaction is not exposed to share.

OQ-0002 is removed from `docs/open-questions.md`. D-0014 is
retained in the decision log with a forward note pointing at
D-0016.

---

## 6. Snapshot policy

```ts
interface SnapshotPolicy<S> {
  shouldSnapshot(state: S, version: bigint, eventsSinceLast: number): boolean;
}
```

Default: never (caller opts in). Two prebuilt strategies in
`aggregate.ts`:

- `everyN(n)` — snapshot when `eventsSinceLast >= n`.
- `whenAppliedExceeds(loadCostThresholdMs)` — record the last
  apply duration on the aggregate state and snapshot once it
  crosses a threshold. (Pattern only; ships as a recipe in
  comments, not a public export, in v1.)

Snapshots are written in a separate `recordSnapshot` call after
the append commits. A failed snapshot write logs to `onError` and
is otherwise ignored — the load path will work without it, just
more slowly.

---

## 7. OQ-0003 resolved — selector locus

**Resolution:** v1 supports **only SDK-side selectors**
(option 1 from OQ-0003). The `selector?: (e: RecordedEvent) => boolean`
field on the subscription worker filters events after fetch; the
cursor advances to the last *fetched* event_number regardless of
match. This realises INV-SUB-P-050 exactly as written.

The SQL contract already reserves the door for server-side
selectors: `read_subscription_batch.p_options` is documented to
accept a future `selector` key without v1 callers breaking. If
the bank-account example or a later phase produces a workload
where sparse selectors (≤ a few percent match) make bandwidth a
real cost, we add server-side JSONB-predicate evaluation as an
additive option then. v1 keeps the contract surface minimal and
the selector vocabulary unrestricted (arbitrary application code).

Recorded as part of the design decisions; OQ-0003 is removed from
`docs/open-questions.md` and reborn as a `maybe-later.md` entry
("server-side selector evaluation") with the forward-compat note
that the `selector` option key is already reserved.

---

## 8. Worker-loop summary (single diagram)

```
                  +------------------------+
                  | startSubscriptionWorker|
                  +-----------+------------+
                              |
                       claim_subscription
                              |
              +---------------+----------------+
              |                                |
              v                                v
   +-------------------+          +------------------------+
   | heartbeat timer   |          | poll loop              |
   | every (lease/3) s |          |  BEGIN tx              |
   |                   |          |   read_batch (FOR UPD) |
   |  extend_claim ----+----->----+ if empty: COMMIT, sleep|
   |    on IS022/IS020:           |   for e in batch:      |
   |      set aborted = true      |     if selector(e):    |
   |                              |       handler(e, {tx}) |
   +-------------------+          |   advance_subscription |
                                  |  COMMIT                |
                                  +-----------+------------+
                                              |
                                       on aborted: stop
                                              |
                                   release_subscription (best-effort)
```

A process manager is the same diagram with the handler body
replaced by `read_snapshot → handle → dispatch on separate client
→ record_snapshot`, exactly as described in §3 layer 3.

---

## 9. What this design deliberately does not do

Cross-checked against `non-goals.md` and `decisions.md`:

- No aggregate cache, no per-aggregate process, no advisory locks
  (D-0004, D-0005).
- No `LISTEN`/`NOTIFY`; the worker sleeps for `pollInterval` when
  idle (D-0003).
- No `consistency: 'strong'` shorthand; explicit list only
  (D-0010).
- No transient subscriptions; `tail(stream, fn)` is a thin
  convenience over the persistent-subscription worker
  (D-0007). v1 ships without the convenience; callers compose
  it themselves if they want it.
- No partitioned consumers; `shard` defaults to 0 and is not
  exposed in the v1 public API (ML-0001).
- No saga DSL; PMs are the primitive (D-0011).

---

## 10. Sequencing for the implementation pass

Once this design is signed off:

1. `package.json`, build wiring, `Client` (layer 0) + `errors.ts`,
   round-trip tests against `instructed_test` for every procedure
   and every SQLSTATE. This is the equivalent of Phase 7's
   "validated against a live Postgres" pass for the SDK.
2. `aggregate.ts` (layer 1) + OCC-retry test (two concurrent
   writers).
3. `subscription.ts` (layer 2) + heartbeat-lease-loss test.
4. `process-manager.ts` (layer 3) + a smoke test using a
   single-event handler.
5. `consistency.ts` (layer 4) + timeout test.
6. `instructed.ts` (layer 5 facade) — thin composition over
   layers 0–4; adds `registerAggregate` + `dispatch` +
   `startProjection` + `startProcessManager` + pool management.
   Tests cover only the facade-specific behaviour (registry
   lookup, pool ownership, default propagation); the underlying
   correctness is already covered by 1–5.
7. `examples/bank-account/` — the Phase 8 done-criterion target.
   Drives a balance projection (subscription) and a
   `TransferProcessManager` (PM with compensation per D-0011).
8. Update `README.md` to point at the example and the SDK
   package.

Any question that surfaces while implementing one of these steps
goes into `docs/open-questions.md` immediately; any non-obvious
choice gets a new `D-` entry in `docs/decisions.md`. The design
above is the floor, not the ceiling — it will get amended in
review passes the way `mapping.md` and `sagas.md` did in their
respective phases.

---

## 11. Specification details settled in the closing pass

Seven items flagged at the end of the design review (see D-0015
implications). Each is settled here so an implementer can work
from the doc without coming back for clarifications.

### 11.1 Handler-context shape (opaque to the SDK; per D-0016)

The handler is opaque to the SDK. The context carries only what
the SDK can provide independent of the projection's target
technology:

```ts
interface HandlerContext {
  workerId: string;
  position: { eventNumber: bigint; streamVersion: bigint };
  signal: AbortSignal;     // aborted on graceful shutdown / lease loss
}
```

No `tx`, no `pgClient`, no ORM handle, no per-registration
adapter. The handler writes to whatever it writes to — Postgres,
Elasticsearch, Redis, an external API, an in-memory cache —
and is responsible for its own idempotency (NG-0015).

`signal` is provided so long-running handlers can cooperate with
graceful shutdown and lease loss. The handler is not *required*
to observe it; the SDK will drop the handler's eventual ack
silently if the lease was lost in the meantime (the new lease
holder will redeliver). Observing the signal is purely a
resource-cleanup courtesy.

### 11.2 `NewEvent.event_id` is optional; the SDK fills it

`NewEvent.event_id` is **optional** in the TypeScript type. When
omitted, the SDK fills it with `crypto.randomUUID()` immediately
before the `append_to_stream` call. When supplied, the SDK uses
the value verbatim.

This preserves D-0011's caller-supplied contract (the absurd-
bridge case passes a deterministic id derived from
`(task_id, step_name)`; the SDK does not interfere) while making
the common command path — where deduplication is not needed —
free of UUID boilerplate.

### 11.3 `apply` event shape

The SDK reads `RecordedEvent` from the store and projects it to
the domain shape before calling `apply`:

```ts
// What apply(state, event) actually receives:
type DomainEvent<E> = E & { type: string; data: unknown; metadata?: unknown };
```

Concretely, `apply` is invoked as `def.apply(state, { type:
row.event_type, data: row.data, metadata: row.metadata } as E)`.
The user's `E` union (e.g. `{ type: "Deposited"; data: { amount:
number } } | ...`) typically discriminates on `type` and the SDK's
projection lines up exactly.

`stream_uuid`, `stream_version`, `event_number`, `event_id`,
`causation_id`, `correlation_id`, `created_at` are **not**
included in the domain event passed to `apply`. They are SDK
bookkeeping; if a user genuinely needs causation/correlation
inside `apply` it goes through `metadata` (which is the
idiomatic place anyway).

Projection handlers and PM handlers, by contrast, receive the
full `RecordedEvent<E>` (they often need `stream_uuid` for
routing and `event_number` for cursoring decisions). The
asymmetry is deliberate: aggregates are pure domain folds; the
projection / PM layer is where event metadata becomes relevant.

### 11.4 PM `{kind: 'start'}` is lenient; no `start!` in v1

If a route returns `{kind: 'start', processId}` and a snapshot
already exists for that `processId`, the SDK **discards the
existing snapshot and starts from `initialState()`**. No error.
This matches Commanded's `:start` (lenient) semantics.

Commanded's `:start!` (strict — error if instance already
exists) is **deferred**. Adding it later means accepting a
`{kind: 'start!', processId}` route result and raising a typed
`ProcessAlreadyStarted` error. Tracked informally; no `ML-`
entry until a real workload demands it.

The `{kind: 'continue'}` route on a `processId` with no
snapshot is **also lenient**: state starts from `initialState()`
silently. Commanded's `:continue!` (strict) is deferred under
the same logic.

### 11.5 Handler-throws semantics

When a projection / PM handler throws (per D-0016, the handler
runs outside any SDK transaction, so there is nothing to roll
back):

1. The SDK **does not call `advance_subscription`**. The cursor
   stays put; the event will be redelivered on the next batch
   read.
2. `RegistrationOptions.onError(err, { event })` fires (the
   handler's thrown error wrapped in `HandlerError` with the
   offending event attached).
3. The worker loop applies an **exponential backoff between
   retries of the same event**: 250ms, 500ms, 1s, 2s, 4s, 8s,
   16s, capped at 30s. The backoff resets on the first
   successful handler call.
4. There is **no max-attempts limit in v1.** A poison event
   stalls the projection indefinitely — by design, because
   silently skipping it would corrupt downstream state.
   Operators see this as an unmoving cursor and an `onError`
   log line every 30 seconds. Poison-event quarantine is a
   Phase 9+ concern.

The heartbeat continues to fire during retry backoff, so the
lease is not lost.

**PM-specific note.** A PM `handle` may dispatch commands
before throwing. Those dispatches each open their own
transaction in the separate dispatch session and may have
already committed (and thus appended events into the store)
by the time the throw happens. The SDK does **not** unwind
them — it can't, the store is append-only (NG-0008). On
redelivery, the PM will see the same input event, may compute
the same set of commands, and may re-dispatch them. The
idempotency story:

- **Inside a single `handle` call, before the throw:**
  commands dispatched up to the throw point are durable;
  commands after the throw point never ran. The PM author
  controls the ordering (commands are dispatched in the order
  returned from `handle`); place irreversible / side-effecting
  commands last where possible.
- **Across redeliveries:** the recommended pattern is to
  derive each dispatched command's `event_id` deterministically
  from `(pmName, processId, sourceEventNumber, commandIndex)`,
  so a re-dispatch of the same command hits `IS004
  duplicate_event` and the SDK treats it as a no-op (see
  §11.2 on `event_id` defaulting — the SDK fills only when
  the caller omits, so deterministic ids are supplied
  verbatim). Alternative: have the PM's `handle` gate its
  command emission on `state` (`if (state.dispatchedWithdraw)
  return { state, commands: [] }`); the state is recovered
  from the last successful snapshot, so anything not yet
  reflected there will be re-evaluated. Both patterns are
  application-level; the SDK does not mandate either.

### 11.6 (removed) `withTransaction` nesting

Dropped: `Client.withTransaction` no longer exists (D-0016).
Nothing to nest. Section retained as a numbered placeholder so
the original §11.x references in review notes remain stable.

### 11.7 PM dispatch shape: by value, or by registry lookup

Commands returned from a PM `handle` may take either form:

```ts
type DispatchedCommand =
  | { aggregate: AggregateDefinition<any, any, any>;
      streamUuid: string; command: any }                    // by value
  | { aggregateType: string;
      streamUuid: string; command: any };                   // by name
```

**By value** works with the layered API (no facade, no
registry needed) and is refactor-safe (renaming the
aggregate's `type` string updates the lookup via the imported
definition). Recommended when the PM is co-located with the
aggregates it dispatches.

**By name** uses the `Instructed` facade's aggregate registry
to resolve `aggregateType` to a definition. Raises
`UnknownAggregateType` if the aggregate is not registered in
the process running the PM. Recommended when the PM and the
target aggregates live in different packages and importing
would create a circular dependency.

Both forms are first-class. The choice is stylistic, with the
one operational caveat (by-name requires the aggregate to be
registered in the *worker* process running the PM, not just in
the process that originally dispatched the triggering command).
