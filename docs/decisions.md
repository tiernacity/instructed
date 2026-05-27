# Decisions

Architectural decisions worth preserving — the ones that shape how
`instructed` behaves at the contract boundary. Implementation
choices subsumed by the working code are not recorded here; the
git log is the audit trail for those.

Format: short. Each entry records the decision, why it sits where
it does, and what it implies for the schema, the SDK, or both.

---

## D-0002 — One subscription = one routing cursor + a work queue

A subscription has exactly one *routing cursor* advanced by
exactly one active **routing worker**, and a *work queue* of
per-partition items consumed by **N processing workers in
parallel**. The routing worker turns events into work-item rows;
the processing workers claim items and run the handler. Per-
partition ordering is preserved by the claim predicate; across
partitions, processing is concurrent.

**Why:** A single cursor advanced atomically with the consumer's
work is the simplest model, but it serialises every consumer onto
one worker -- a partitioned consumer needs N. Pushing the
partitioning into the SDK ("name N sibling subscriptions
yourself") leaks shard management into application code. The
routing / processing split keeps the routing decision
single-active (which keeps the routing cursor monotone and the
catch-up predicate simple) while letting processing parallelise
over whatever partition shape the application declares.

**Implications:**

- The `subscriptions` table identity is
  `(stream_id, name, shard)` with `shard = 0` reserved at v1
  for a future operator-facing shard dimension. The
  per-subscription routing lease lives on this table.
- A second table `subscription_work_items` carries the queue;
  the per-item lease lives on its rows. Processing workers
  claim items with `FOR UPDATE SKIP LOCKED` plus a
  per-partition predicate; the routing worker never touches
  these rows in the hot path.
- Throughput of a single subscription is no longer bounded by
  one worker's processing rate; it is bounded by the routing
  worker's throughput (which is just "convert events to work
  items" -- pure CPU, no handler work) plus the processing
  parallelism the partition shape allows.

---

## D-0003 — Polling only; no `LISTEN`/`NOTIFY` in the contract

Workers discover new events by polling. The SQL contract does
not require `pg_notify` and no correctness property depends on
it.

**Why:** notifications are not durable. Treating them as the
primary delivery mechanism means a second code path that must
be kept consistent with the polling fallback — two paths, two
sets of bugs. A future wake-up optimisation is permitted only if
every notification is droppable without affecting correctness
(see ML-0002).

**Implications:**

- Strong-consistency-on-dispatch (D-0010) has a polling-interval
  latency floor.
- No background `LISTEN` connections are required of host
  applications.
- No notification-channel naming scheme to design.

---

## D-0004 — No in-memory aggregate cache; rehydrate on every command

Every command runs a fresh load (snapshot + events since)
followed by execute → append. There is no in-process registry
of active aggregates and no aggregate-lifespan concept.

**Why:** An in-memory cache reintroduces the
registry/coordination problems the design exists to avoid (who
owns the cache? what happens on node failure? how do two SDK
instances see consistent state?). Snapshots are the load-cost
mitigation — they don't need coherence across processes.

**Implications:**

- Aggregate load cost becomes the dominant per-command cost.
  Snapshot policy tuning matters.
- Without an in-memory cache, no per-aggregate coherence
  side-channel (subscription, broadcast, or other notification
  path) is required: every command's load reads the canonical
  sequence from the event log.
- An opt-in SDK-level cache could be added later as pure
  performance optimisation; it must not become a correctness
  boundary.

---

## D-0005 — Per-aggregate command serialisation via optimistic-lock retry

Two concurrent commands on the same aggregate race at append.
The unique constraint on `stream_events (stream_id, stream_version)`
makes at most one succeed for any given expected version; the
loser re-loads (cheaply, picking up the winner's events) and
re-evaluates, up to a configurable retry budget.

Advisory locks are **not** used to serialise commands.

**Why:** `append_to_stream` is the only place that holds a lock,
which keeps lock ordering trivial. Optimistic retry composes
with the snapshot-based load path without needing extra state
released on connection death. Advisory locks tied to a session
leak responsibility into connection-pool behaviour — the class
of bug the design exists to avoid.

**Implications:**

- Hot aggregates with many concurrent commands will retry. The
  retry budget is a real tuning knob.
- The load-execute-append loop must be cheap enough that retry
  is viable — the same property that makes D-0004 acceptable.

---

## D-0006 — Subscriptions are leased, not session-locked

The `subscriptions` table carries `claimed_by TEXT NULL` and
`claim_expires_at TIMESTAMPTZ NULL`. Workers claim via
`claim_subscription`, heartbeat via `extend_subscription_claim`,
and release cleanly via `release_subscription`. Lease takeover
on expiry is automatic in the next claim.

**Why:** Session-locked subscriptions (the alternative —
`pg_try_advisory_lock` held for the lifetime of the connection)
tie subscription ownership to connection ownership, which rarely
lines up with worker process ownership when a connection pool
sits in between. A leased model decouples claim lifetime from
connection lifetime and survives the pool cleanly.

**Implications:**

- The worker loop runs a heartbeat alongside its processing
  loop. If `extend_subscription_claim` raises
  `IS022 subscription_lease_lost` the worker MUST stop
  processing immediately; otherwise it risks double-delivery
  with the new holder.
- A crashed worker keeps the subscription unavailable until its
  lease expires. Lease TTL tuning is the operational knob.

---

## D-0009 — `delete_subscription` on a missing subscription is an error

Deleting a subscription that does not exist raises
`IS020 subscription_not_found` rather than succeeding silently.

**Why:** Silent success on a missing target hides operational
bugs (typo in subscription name; deleting the wrong tenant's
subscription). The error is cheap to surface and easy for
callers to swallow if they want idempotent delete. The
reverse — reading lenient behaviour out of a strict contract —
is impossible.

**Implications:**

- SDK helpers may offer an `ignoreMissing: true` option as a
  convenience for idempotent teardown.

---

## D-0010 — Strong-consistency-on-dispatch waits on an explicit subscription list

A dispatcher requesting strong consistency passes an explicit
list of subscription names:
`dispatch(..., { consistency: ["BalancesProjector", "OrderProjector"] })`.
After a successful append, the SDK polls each named
subscription's cursor until it has caught up, or raises
`ConsistencyTimeoutError` when the configured timeout elapses.

There is no "wait for everything" shorthand.

**Why:** A "wait for every handler that opted in" shorthand is
well-defined at the SDK layer — handlers register their
consistency mode when the worker starts, and an SDK-local
registry could collect them. But the store has no such metadata,
and any SDK-level shorthand would only cover subscriptions
managed through the same SDK instance — it would silently miss
subscriptions held by other processes. The explicit list is also
more honest: in practice the caller knows which projection(s)
they need to read their own writes against. A per-SDK-instance
auto-collection convenience remains compatible with v1 and may
be added later; the store primitive stays explicit.

**Implications:**

- The store exposes `read_subscription_position(stream, name)`
  returning `last_seen`.
- Latency is bounded below by the polling interval (per D-0003).
- The SDK may layer an auto-collection convenience over the
  explicit-list primitive when the dispatcher and handlers run
  in the same SDK instance, but the store contract takes only
  the explicit list.

---

## D-0011 — Process managers are the saga primitive; compensation is a command

A process manager subscribes to the event log, observes events,
optionally dispatches commands, optionally mutates its persisted
state. That's the whole saga primitive. Compensation is
"whatever commands a PM dispatches in response to failure
events". There is no separate compensation engine, no
forward/compensation step pairing, no compensation DSL.

When a workflow needs durable execution of an external side
effect (Stripe, email, third-party API), the PM dispatches a
command producing an `XRequested` event; a task in a system
like [absurd](https://github.com/earendil-works/absurd)
subscribes to that event, runs the side effect with its own
checkpoint/retry machinery, and appends a `XCompleted` or
`XFailed` event back into the event store. The PM consumes the
returning event like any other.

**Why:** The PM contract — durable snapshot-backed state,
ordered at-least-once subscription delivery, per-instance
persist-and-ack, and a dispatch helper that runs the full
load-execute-append cycle — already provides every primitive
needed to express compensating-command flows without inventing
new durability, ordering, or recovery machinery. A first-class
saga abstraction would duplicate this and add a parallel
schema/lock surface for no semantic gain.

**Implications:**

- No new tables. PM state stays in the snapshots table.
  Compensation commands flow through `append_to_stream`.
- Aggregate commands that can permanently fail in a way the
  saga needs to observe MUST emit failure events rather than
  silently returning errors. This is a modelling guideline,
  not a contract-level constraint.
- Cross-boundary tasks that emit events back into the store on
  retry MUST use a deterministic `event_id` derived from
  `(task_id, step_name)` so re-runs hit `IS004 duplicate_event`
  and don't double-append.
- A future SDK-level linear-saga helper (declarative
  `step / compensation` pairs that compile into a PM) is
  compatible with this decision — it would be SDK convenience,
  not a parallel primitive in the SQL contract.

---

## D-0012 — Global ordering via `$all`-as-stream with row-level lock

The `streams` table is seeded at install with a row
`(stream_id = 0, stream_uuid = '$all', stream_version = 0)`.
Every `append_to_stream` issues
`UPDATE streams SET stream_version = stream_version + N WHERE stream_id = 0 RETURNING stream_version - N`
to take the `$all` row lock, reserve a contiguous range of
global event numbers, and link each event into `$all` via
`stream_events`. The contiguous global numbering of
`event_number` falls out of the row lock being held for the rest
of the append transaction.

Lock acquisition order: per-stream row → `$all` row → `events`
→ `stream_events`.

**Why:** A `bigserial`/sequence on `events.event_number` is
faster but not gapless — sequences skip on rollback, sequence
cache, and restart. Downstream invariants
(INV-APPEND-003 gapless global, INV-SUB-P-030 strictly increasing
delivery, the CON-010 polling compare) assume gaplessness;
weakening them would be a much larger surface change than the
throughput ceiling justifies. `SERIALIZABLE` isolation +
`MAX(event_number) + 1` is correct but pays for gaplessness with
visible serialisation-failure retries that compose awkwardly
with the SDK's own OCC retry.

**Implications:**

- The `$all` row is the global serialisation point. Concurrent
  append throughput is bounded by it.
- `read_all` is the dedicated entry point for `$all`;
  `read_stream` rejects `'$all'` to make the asymmetry explicit
  at the SQL surface.
- `stream_events` carries both `(stream_id, stream_version)`
  and `(original_stream_id, original_stream_version)`. An
  `$all` read joins through `stream_id = 0` and projects the
  original-stream identity into the returned row.
- Replacing the `$all` row lock with a higher-throughput
  gap-preserving mechanism in a future version is an internal
  schema migration — the SDK-visible contract doesn't change.

---

## D-0016 — Handlers are opaque to the SDK; idempotency is the application's concern

The processing-worker loop is:

```
claim = claim_work_item(...)             -- short tx, FOR UPDATE SKIP LOCKED
event = read_all(claim.event_number, 1)  -- MVCC read
await handler(event, ctx)                -- NO SDK transaction
complete_work_item_projection(...)       -- short tx (or _pm / _pm_instance)
```

The handler receives the event and an opaque context (`workerId`,
`partitionKey`, `eventNumber`, `attempt`, `signal`); it does
**not** receive a Postgres connection, an ORM handle, or any
other SDK-owned resource. Handler returns successfully → SDK
runs the terminal-success step. Handler throws → the SDK's
error policy decides whether to retry against the same claim or
stop; in neither case does the terminal step fire.

**Why:** A previous design ran the handler inside the SDK's
transaction so projection writes and the work-item terminal
step committed together. That property only existed for
projections that happened to target the same Postgres database
the event store sat in. Real projections target Elasticsearch,
ClickHouse, Redis, BigQuery, HTTP APIs, in-memory maps — none
can share a Postgres transaction. The plumbing required to
support the Postgres-targeting case (per-handler ORM-agnostic
transaction wrappers, connections threaded through the handler
signature) was paid by every user and reaped only by
Postgres-targeted projections to the same DB.

**Implications:**

- Delivery is at-least-once. Handlers MUST be idempotent —
  typically via an idempotent UPSERT (Postgres), an `_id` keyed
  on `event_id` (Elasticsearch), `SETNX` (Redis), or whatever
  the target store offers.
- Process managers run an SDK-internal terminal-success
  transaction (snapshot UPSERT + work-item UPDATE-to-done in
  one short tx, or snapshot DELETE + all-work-items DELETE in
  one short tx for the terminal case) *after* the handler
  returns and any dispatched commands have committed. The PM
  dispatch path uses a separate connection from the
  terminal-step path to keep the lock sets disjoint.
- The SQL contract still supports a future opt-in
  co-transactional path for the narrow Postgres-projecting-into-
  same-database case (`complete_work_item_projection` and
  `complete_work_item_pm` are callable inside any well-formed
  transaction); v1 SDKs do not exercise it. See ML-0004.

---

## D-0018 — The PM routing surface is intentionally smaller than Commanded's

A process manager's routing callback returns
`{ partitionKey } | "ignore"` -- not a `start` / `continue` /
`stop` / `start!` / `continue!` / `false` directive set. The
lifecycle and strict-mode concerns those directives expressed
("this event terminates the instance"; "raise if state is
already/not yet initialised") move into the processing-layer
callbacks: `handle` returns `{ complete?: boolean }` for the
former; an application-level `apply` guard for the latter.

**Why:** the inherited directive set bundled four orthogonal
concerns at the routing layer (which instance, is this event
for this PM type, strict-first-event assertion, lifecycle
termination). Concerns 1 and 2 are intrinsic to routing.
Concerns 3 and 4 are application logic about instance state
dressed up as framework directives because an in-process
`GenServer` runtime had convenient access to both at the
routing layer.

`instructed` doesn't have processes. The routing layer reads
rows from `$all`, decides which work-item to write, and that's
it -- no per-instance state, no PM-level callbacks holding
references. The convenience that justified bundling disappears.
Keeping the directive set would have meant either re-introducing
an instance-state cache at the routing layer (which the design
exists to avoid), or quietly degrading the strict-mode
directives to advisory hints.

**Implications:**

- The PM routing callback is purely a function of the event:
  `(event, metadata) -> { partitionKey } | "ignore"`. No state
  parameter, no PM-level callbacks to consult.
- Strict-first-event assertions move into `apply` / `handle`,
  where state is naturally available: `if (state !==
  initialState()) throw new Error("already started")`.
- Instance termination is a return-value concern of `handle`:
  `{ complete: true }` triggers the framework to DELETE the
  snapshot and every remaining work-item for the partition in
  one transaction. Future events to a completed partition route
  as normal and run from `initialState()` again; permanent
  termination is an application-level pattern (encode it in
  `apply`'s state machine, or shape the routing function so it
  stops matching once the terminal upstream events have passed).
- A widening to `RoutingFn -> { partitionKey }[]` for
  one-event-many-partitions fan-out is backwards-compatible at
  the call site and is reserved for a later design pass; v1
  ships with the singular return.

---

## D-0017 — Causation and correlation propagation

The SDK fills `causation_id` and `correlation_id` automatically
with sensible defaults; callers may override either explicitly.

**Defaulting rules:**

- A call to `runCommand` mints a per-call `commandId` (default:
  fresh UUID; overridable). The SDK fills any unset
  `causation_id` on appended events with `commandId`. Effect:
  all events from one command share `causation_id = command_id`.
- A `correlationId` passed in `RunCommandOptions` is filled
  onto any unset `correlation_id` on appended events. If not
  supplied, the SDK does not invent one — chains start
  explicitly at the top.
- A process manager dispatching a command supplies
  `causationId = triggering_event.event_id` and
  `correlationId = triggering_event.correlation_id` to its
  internal `runCommand` call. The resulting events on the
  dispatched aggregate inherit both. Effect: a PM-dispatched
  command's events are causation-linked to the event that
  triggered the PM.

**Why:** the propagation has to be automatic for the
[`invariants.md`](invariants.md) AGG-020 / AGG-021 / PM-012
promises to hold without the application writing plumbing. The hybrid
SDK-fills-when-absent rule is symmetric with how `event_id` is
filled — one mental model: "the SDK fills caller-omitted ids
with sensible defaults; callers who care override."

**Implications:**

- `NewEvent` declares `event_id`, `metadata`, `causation_id`,
  and `correlation_id` all optional.
- Within a `runCommand` invocation, an explicit `causationId`
  in options wins over the per-call `commandId` for event
  defaulting. (This is what makes PM dispatch hand the
  triggering event id straight through.)
- The PM author writes no causation plumbing; the PM worker
  constructs the right options object internally.
- No SQL-contract change. `events.causation_id` and
  `events.correlation_id` are already nullable; the SDK is the
  only thing that decides when to fill them.

---

## D-0025 — Routing workers claim per batch, not per worker lifetime

The SDK's routing worker loop claims and releases the subscription
lease around each batch, not once at startup with a heartbeat. The
schema is unchanged; the loop shape and `claim_subscription`'s
internal locking are tightened to make work-stealing across
processes the natural default.

**Why:** the operational property is "any routing worker can pick
up routing work" — spinning up a second process should
immediately share routing load with the first, not idle waiting
for the first to die. A claim-once-and-heartbeat loop would give
one worker process *de facto* ownership of every subscription it
touched at startup; a second process would observe
`already_claimed` on every claim and never route until the first
failed. That is the failover model, not the work-stealing model.

Claim-per-batch eliminates the monopoly without introducing a
new axis of parallelism (no concurrent routing workers per
subscription; ML-0013 stays deferred). At any instant there is
still exactly one routing worker per subscription; the active
worker rotates per batch across whichever processes are running.

**The loop shape:**

```
loop while not closing:
  result = claim_subscription(stream, name, workerId, leaseSeconds)
  if result.result == 'already_claimed':
    sleep(pollInterval); continue
  events = readAll(result.lastSeen + 1, batchSize)
  if events.empty:
    release_subscription(...); sleep(pollInterval); continue
  decisions = [...routeFn per event...]
  try:
    route_batch(stream, name, workerId, lastEventNumber, decisions)
  catch IS022 subscription_lease_lost:
    continue            -- the batch is dropped; next claim decides
  release_subscription(stream, name, workerId)
```

The lease covers one batch. `extend_subscription_claim` is no
longer called by the SDK routing worker (the procedure stays in
the SQL contract for callers who want long-lease semantics above
the `Client` layer). `IS022` during `route_batch` is not fatal;
it means "your lease expired mid-batch, another worker may have
claimed; drop this batch and loop". The work-item PK +
`ON CONFLICT DO NOTHING` absorbs any duplicate INSERTs the dropped
batch would have caused.

**`claim_subscription` locking change.** With M processes polling
K subscriptions, every tick produces M − 1 losing claim attempts
per subscription. Under plain `SELECT ... FOR UPDATE`, each loser
briefly blocks on the winner's row lock. To eliminate that:
`claim_subscription` does an MVCC-snapshot existence + lease
check first (no lock); only when the row appears free / expired
does it take `FOR UPDATE SKIP LOCKED` and re-verify under the
lock. Losers either return `'already_claimed'` from the snapshot
read (live lease held by someone else) or fall through to a 0-row
`SKIP LOCKED` result (someone else is mid-write) — in both cases
the caller sees `'already_claimed'` without ever queueing on a
lock.

**Lock vs lease.** The schema distinguishes two mechanisms:

- **Postgres row lock** — held by `FOR UPDATE` inside one
  procedure call (claim / extend / route_batch / release).
  Microseconds. Released at commit.
- **Application-level lease** — the `claimed_by` /
  `claim_expires_at` columns. Consulted by `route_batch` etc.
  via column comparison, not via lock holding. Lives for the
  duration the SDK chooses (per-batch in this loop).

INV-SUB-P-010 ("at most one live routing worker holds the lease
at any moment") is enforced by the application-level lease, not
by Postgres row locks. The lock just keeps each procedure call's
read-modify-write atomic.

**RouteFn requirements (sharpened):** pure, deterministic, no
I/O, fast. Pure + deterministic because routing must be
replayable — `route_batch`'s PK absorbs duplicates only if the
second routing of the same event computes the same decision. No
I/O because routing latency is on the critical path between
append and projection visibility, scaled by event volume.
**Fast** specifically because routeFn duration is bounded above
by `leaseSeconds`: a batch that exceeds its lease has its
`route_batch` raise `IS022` and the work is redone by the next
worker. The right configuration is `leaseSeconds` comfortably
larger than the expected worst-case `batch_size × routeFn`
duration; if you can't bound that, shrink `batch_size`.

**Implications:**

- SDK option `heartbeatInterval` is removed from
  `RoutingWorkerOptions` (the SDK no longer heartbeats). The
  `extend_subscription_claim` SQL procedure remains.
- `claim_subscription` returning `'already_claimed'` is no
  longer a fatal startup error in the SDK; it is a back-off
  signal handled inside the loop.
- `route_batch` raising `IS022` is no longer fatal; the worker
  drops the partial batch and loops.
- `release_subscription` is called per batch in the steady
  state, not only on shutdown.
- ML-0013 (concurrent sharded routing) is **not** addressed by
  this decision and stays deferred. The work-stealing this
  decision enables is single-active-per-instant routing with
  per-batch rotation across processes — a different axis from
  ML-0013's concurrent live workers on disjoint shards.

---

## D-0026 — One `Client` per application; PM dispatch shares it

The SDK exposes one `Client` (and one underlying `pg.Pool` /
`Queryable`) for the entire application. PM workers dispatch
commands via `runCommand` against the same client used for every
other SDK call. There is no separate dispatch pool and no second
`Client` parameter on `startPmWorker`.

**Why.** A separate dispatch pool would, on the face of it,
isolate the PM's `appendToStream` from its own
`complete_work_item_pm` — but the isolation it claims to provide
is already provided by the SQL contract:

- Postgres locks are scoped to **transactions**, not to clients
  or pools. Two `Client` wrappers around the same pool are
  indistinguishable at the lock-acquisition layer.
- Every SDK procedure call is its own short transaction. A PM
  worker's calls — `claim_work_item`, the handler, `runCommand`
  → `appendToStream`, `complete_work_item_pm` — are sequential
  transactions on (potentially) different pooled connections.
  There is never a moment where one worker holds locks from two
  of these sets at once.
- Deadlock requires two transactions wanting two locks in
  opposite orders. The SQL contract documents lock-acquisition
  order per procedure, and the three sets — dispatch (`streams`,
  `events`, `stream_events`), routing (`subscriptions`,
  `subscription_work_items`), processing-terminal
  (`subscription_work_items`, `snapshots`) — are pairwise
  disjoint by construction. No cycle is possible whether the
  SDK uses one pool or two.

The two strategies for handling load and contention under one
pool are:

1. Spin up more processes. Per D-0025, additional processes
   share routing work batch-by-batch; per the work-item claim
   model with `FOR UPDATE SKIP LOCKED`, additional processes
   share processing work item-by-item. Adding processes evenly
   scales every workload type.
2. Run processes specialised by workload (an API-facing
   dispatcher tier, a projection-processing tier, a PM tier) and
   scale each independently. Per-process selection of which
   workers to start is a feature of `Instructed.startWorker`'s
   registration model.

Neither strategy requires multiple pools inside one process.
Connection-budget isolation between PM dispatch and the rest of
the SDK is a niche operational concern better solved by process
separation when it actually arises.

**Implications:**

- `Instructed` takes a single `db` option. There is no
  `dispatchDb`, no `dispatchClient()` accessor.
- `startPmWorker(client, def, opts)` takes one client.
- The lock-set disjointness statement in `sql-contract.md` is a
  property of the SQL contract; it does not require pool or
  client separation in the SDK.

---

## D-0027 — TS SDK ships as one package with a `core` sub-path; not split into two packages

The TypeScript SDK is published as a single npm package,
`instructed-sdk`, with two `exports` entry points:

  - `instructed-sdk` — the full surface: L1 (procedure bindings)
    + L2 (core behaviours) + L3 (idiomatic facade). The
    conventional entry point; what application code imports.
  - `instructed-sdk/core` — L1 + L2 only. The surface every
    SDK port must reproduce per the porting checklist. For
    consumers who want to build their own L3 facade.

**Why one package, not two.**

- The layer model that needs to be conveyed to porters is
  conveyed by the porting checklist doc and by the re-export
  grouping inside `src/index.ts`. The package-manager layer is
  not the right enforcement mechanism for a contract whose
  audience is "people writing the SDK in another language" —
  they read the checklist, not our `package.json`.
- The natural use-shape for application code is one import
  (`import { Instructed } from "instructed-sdk"`). Two packages
  would force every application to declare both for no
  application-visible benefit.
- Inside one package the layers move in lockstep, one changelog,
  one release. Two packages introduce version-skew costs
  (pinning, peer-dep matching) for no semantic gain.
- The L2/L3 split is "same node process, same DB, same audience"
  — closer to the `zod` / `drizzle-orm` / `kysely` shape (one
  package, multiple sub-paths) than the `@trpc/server` +
  `@trpc/client` shape (two packages because two ecosystems).

**Why the `core` sub-path is worth having.** A consumer who
wants to write their own L3 facade (custom routing surface,
alternative `dispatch` shape, different consistency mechanism)
can import from `instructed-sdk/core` and be guaranteed the
import doesn't pull `Instructed`, `waitForProjection`, or the
partition-by sugar. The sub-path is the package-level analogue
of the porting checklist: "this is what every SDK reproduces".
An L1-only sub-path is not offered — L1 without L2 (the
aggregate loop, the routing worker, the processing worker) is
not a viable build target.

**Reversibility.** Splitting into two packages later is
mechanical: the sub-path becomes the second package's name; the
re-export grouping stays.

**Implications.**

- `sdks/typescript/package.json` `exports` gains a second entry
  for `./core` pointing at `dist/core/index.{js,d.ts}` (and the
  CJS variant). Source lives at `sdks/typescript/src/core.ts`,
  re-exporting from the L1 and L2 modules.
- `sdks/typescript/src/index.ts` is reorganised to re-export in
  three labelled groups (L1, L2, L3) so the layering is legible
  even at the bare entry point. The bare entry re-exports
  everything `core` does, plus the L3 facade and helpers.
- Build configuration (`tsconfig.build.json` / `tsconfig.cjs.json`)
  emits `dist/core.{js,d.ts}` alongside `dist/index.{js,d.ts}`.
- No application-code breakage: every existing import from the
  bare `instructed-sdk` continues to resolve.
- The porting checklist (a future doc; TODO #2 deliverable)
  names `instructed-sdk/core` explicitly as the inventory of
  what a new-language SDK reproduces. Each language SDK chooses
  its own package shape; the TS SDK's choice does not propagate.
