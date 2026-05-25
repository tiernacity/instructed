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

**Why:** A "wait for everything" shorthand requires a registry
of which subscriptions belong to which application, which is
exactly the kind of cross-process coordinated state the design
exists to avoid. The explicit list is also more honest: in
practice the caller knows which projection(s) they want to read
their own writes against.

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
runs the terminal-success step. Handler throws → SUB-B error
policy decides whether to retry against the same claim or stop;
in neither case does the terminal step fire.

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
  one-event-many-partitions (`PM-A` fan-out) is
  backwards-compatible at the call site and is reserved for a
  later design pass; v1 ships with the singular return.
- See [`upgrade-notes/pm-f.md`](upgrade-notes/pm-f.md) for the
  pre-release migration matrix mapping each former directive to
  its new shape.

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
