# Maybe later

Capabilities not in scope for v1 but that we want to keep visible
so v1 design choices don't accidentally preclude them. Each entry
records what the thing is, why it's deferred, and what the v1
design must keep compatible.

This is distinct from [`non-goals.md`](non-goals.md), which is
about things we deliberately *won't* do.

---

## ML-0001 — Concurrent partitioned subscription consumers

**Status:** Superseded. The capability still matters, but it is no
longer a standalone item — it is one of the shapes the
subscription substrate redesign tracked in
[`todo/subscriptions.md`](todo/subscriptions.md) SUB-A is expected
to provide. Under the leading candidate (Design 3, decoupled
router + work queue), partitioned consumption falls out of varying
the partition key per subscription; there is no separate
"partitioned consumer" feature to add later.

The original framing follows for context.

Allow a single named subscription to be consumed by N workers in
parallel, with a partitioning function (typically over
`stream_id`) that preserves per-partition order while sacrificing
global order across partitions.

**Why deferred:** a single cursor advanced atomically with the
consumer's work is the simplest model that demonstrates the core
hypothesis, and is correct for any single-worker projection. The
two realisations both add real complexity (a side table of
per-shard offsets makes cursor advance non-atomic with projection
writes; N independent sibling subscriptions over disjoint shards
push shard management out of the contract and into the SDK).
Workloads that need this throughput today can split into multiple
named subscriptions manually.

**Forward-compat constraints on v1:**

- The `subscriptions` table identity is `(stream_id, name, shard)`
  with `shard = 0` reserved as the v1 default. A future shard
  dimension fits the existing schema without migration.
- `claim_subscription` / `advance_subscription` already accept a
  `p_options jsonb` argument; future shard-aware variants can
  grow keys without breaking v1 callers.
- The conformance harness has `test.skip` placeholders for
  `INV-SUB-P-040..042` so the partitioned-consumer cases can land
  without rewriting the unpartitioned tests.

---

## ML-0002 — `LISTEN`/`NOTIFY` wake-up optimisation

On commit, `append_to_stream` could emit `pg_notify` on a channel
keyed by subscription scope. SDK worker loops would `LISTEN` and
wake immediately rather than waiting for the next poll tick.

**Why deferred:** notifications are not durable. They must be a
strict optimisation on top of polling, never the primary
delivery mechanism. Adding the path now means two code paths to
keep consistent; deferring it leaves a simpler operational
surface.

**Forward-compat constraints on v1:**

- `append_to_stream` is free to add a `pg_notify` call at the end
  of its transaction without changing its return type or error
  contract.
- The polling-based worker loop must remain the primary
  correctness mechanism. Adding `LISTEN` must be a pure
  optimisation: if every notification is dropped, the system
  must still make progress.
- Subscription identity (name + scope) must be stable enough to
  map cleanly onto a notification channel name.

---

## ML-0003 — Server-side selector evaluation

Allow `read_subscription_batch` to accept a `selector` key in its
`p_options` argument (a JSONB-path expression or SQL predicate
over `data` / `metadata` / `event_type`) and filter events
server-side before returning. Reduces bandwidth for sparse
selectors at the cost of restricting the selector vocabulary to
the server's predicate language.

**Why deferred:** v1 ships SDK-side selectors only. The SDK reads
a batch, runs the application's predicate locally, calls the
handler only on matches, and advances the cursor to the last
*fetched* event_number. SDK-side is the simplest and most
expressive option — the predicate is arbitrary application code.

**Forward-compat constraints on v1:**

- `read_subscription_batch.p_options` is documented to accept a
  future `selector` key. Adding it MUST NOT change the v1
  default semantics (no key → all events returned).
- The SDK's arbitrary-predicate selector stays for use cases the
  server-evaluable vocabulary doesn't cover; the two are
  composable.
- Cursor-advance semantics don't change: the highest delivered-
  or-skipped event_number is what's acked, whether the skip
  happened server-side or client-side.

---

## ML-0004 — `bindToConnection` for caller-supplied transaction sharing

An SDK-level helper that lets a caller share their own open
Postgres transaction with `instructed` calls. Two distinct use
cases:

- **Dispatcher side:** `app.bindToConnection(myTx).dispatch(...)`
  runs the aggregate's load-execute-append cycle inside the
  caller's transaction, so command dispatch commits atomically
  with the caller's own application writes.
- **Projection side:** an opt-in `coTransactional: true` flag
  for projections that target the same Postgres database the
  event store lives in — the handler runs inside a transaction
  that also performs `advance_subscription`, re-enabling
  exactly-once consistency between projection write and cursor
  advance for that narrow case.

**Why deferred:**

- The layered API in v1 already lets advanced callers compose
  their own load-execute-append against `Client.appendToStream`
  inside a caller-owned transaction. ML-0004 is the ergonomic
  wrapper, not a new capability.
- The projection-side opt-in needs a per-registration plumbing
  story (which connection? owned by whom? what about lease loss
  mid-handler?) we shouldn't design speculatively.
- Purely additive on the facade and the layered helpers — no v1
  signature changes required.

**Forward-compat constraints on v1:**

- `Client` already accepts a `Queryable` (`pg.Pool`,
  `pg.Client`, or `pg.PoolClient`), so the underlying primitive
  is in place.
- The facade's registration options shape must be free to grow
  a `coTransactional?: boolean` flag without breaking v1
  callers.
- The PM dispatch path uses a separate connection per
  [D-0011](decisions.md#d-0011) / [D-0012](decisions.md#d-0012)
  lock-set disjointness; ML-0004 will not cover PMs in a first
  cut. Aggregates and projections only.

---

## ML-0006 — General "subscription has processed every relevant event ≤ N" wait predicate

The current consistency-wait primitive answers
"has subscription S processed event_number T?" by comparing
positions, and rejects cross-stream targets (a per-stream
subscription can only wait on appends to its own stream — see
`todo/consistency.md` CON-B).

A more general predicate would answer:

> Has subscription S processed every event with `event_number
> <= N` that S would ever have been notified about?

Useful when a caller knows global progress (an `event_number`)
and wants assurance that the projection has caught up "as much
as it ever will" relative to that global point, regardless of
which streams contributed.

**Why deferred:** the v1 reject-cross-stream behaviour matches
what callers intuitively mean. The general predicate is a real
feature, not a workaround, and deserves its own design pass —
likely an additional check inside the poll loop ("are there any
events on S's scope with `s.last_seen < event_number <= N`?")
that depends on the subscription substrate (SUB-A) being
decided first.

**Forward-compat constraints on v1:**

- Keep the cross-stream guard (`ConsistencyTargetError`) — it
  catches misuse of the simple predicate without preventing a
  future general predicate from being added under a different
  name or option flag.
- Likely interacts with SUB-A's work-queue model: the predicate
  becomes "is there any pending or claimed work item on S with
  `event_number <= N`?", which is naturally available there.

---

## ML-0007 — Aggregate Multi-step convenience

A helper for splitting one command into sequential steps with
intermediate state visible to later steps, similar to running
the aggregate's applier between event productions. Useful when
later steps in a command need to read state derived from
earlier events (e.g. "withdraw N, then if balance < 0 record
overdraft").

**Why deferred:** applications can fold the same logic by
computing all events in the handler and tracking running state
manually. The convenience is real but the pattern hasn't shown
up often enough to earn core-SDK status.

**Forward-compat constraints on v1:**

- Purely additive at the SDK layer. The aggregate execute
  signature returning `events[]` is unchanged; the helper just
  builds the array more ergonomically.

---

## ML-0008 — Aggregate state / version introspection helpers

Two helpers on the `Instructed` facade:

- `aggregateStateOf(definition, uuid) → Promise<state>` —
  returns the current folded state without dispatching a
  command.
- `aggregateVersionOf(definition, uuid) → Promise<bigint>` —
  returns the current version without folding state.

Useful for diagnostics, debugging, and read-only callers that
need state without taking the dispatch path.

**Why deferred:** trivially implementable on top of current
primitives (read snapshot, read events, fold). Adding the
helper is convenience, not capability. Worth doing when a
concrete use case asks for it.

**Forward-compat constraints on v1:**

- Purely additive on the facade.

---

## ML-0011 — Less-opinionated `PartitionBy` for projections

PRJ-A in `docs/todo/projections.md` ships a three-mode
`PartitionBy` surface (`sequential` / `per-event` /
`per-key`) as the v1 SDK shape. A future SDK revision may
collapse this to a less-opinionated surface, e.g.

```ts
type PartitionBy = (event) => string | null
// null means "don't route to this projection" (= ignore)
```

or a richer combinator surface that lets applications express
mixed strategies without enumerated kinds.

**Why deferred:** the three-mode shape is opinionated, but
opinionated in the direction of clarity (each mode names its
intent). The free-form shape is more flexible but loses the
intent signal that tooling (`instructedctl`, soak harness)
can use to label and inspect subscriptions. Ship the
opinionated default; revisit when a concrete application use
case needs something the three modes can't express.

**Forward-compat constraints on v1:**

- Widening from the three-mode union to a function shape is
  backwards-compatible at the SDK call site (every three-mode
  usage has a clean function-equivalent). The migration is a
  source-level rewrite, not a behaviour change.
- The routing layer's contract (`RouteFn` collects routing
  decisions per event) is unchanged regardless of how the SDK
  surfaces the shape to the application. This is an SDK
  convenience-layer concern, not a core concern.

---

## ML-0010 — Configurable post-success retention for projection work-items

Under the SUB-A proposed design (see `docs/todo/subscriptions.md`)
and PRJ-E in `docs/todo/projections.md`, projection work-items
are DELETEd in the same transaction as the handler's successful
read-model write. No `done` row is persisted.

Applications that want a brief ops-visibility window — e.g.
"keep `done` rows for K minutes so `instructedctl` can show
recent processed events per projection" — would need a
`keepDoneFor: Duration` option on `registerProjection`. The
processing worker would UPDATE-to-`done` instead of DELETE
when the option is set, and a background retention task would
clean rows older than the configured window.

**Why deferred:** no correctness implication. Audit / visibility
needs today are met by application-level audit logs and by the
in-flight work surface (`pending`/`claimed`/`failed` rows are
always visible to `instructedctl`). Ship the simpler
immediate-delete default first; add the option if real ops
feedback asks for it.

**Forward-compat constraints on v1:**

- The catch-up predicate (`waitForProjection`) already filters
  on `state IN ('pending','claimed','failed')`, so adding
  retained `done` rows later doesn't change the predicate.
- The work-item schema already includes a `state` column; no
  migration would be needed when the option ships.

---

## ML-0009 — Force-snapshot administrative operation

An operator-facing "take a snapshot of aggregate X now"
operation, regardless of the configured snapshot policy.
Useful post-deploy (warm up snapshot caches) and pre-archive
(checkpoint before bulk operations).

**Why deferred:** belongs in `instructedctl` (TODO #7) when
that lands, not in the SDK surface. Apps that need it today
can read state + write a snapshot via existing SDK
primitives.

**Forward-compat constraints on v1:**

- No SDK signature change required; the SDK already exposes
  `record_snapshot`. `instructedctl` consumes the same
  primitive.

---

## ML-0012 — Routing-worker `close()` strategy: flush vs drop

The SUB-A routing worker (slice 4 of the SUB-A
implementation) drops the partial batch when `close()` or a
lease-loss abort fires mid-batch: no `route_batch` call, no
cursor advance, no work-item INSERTs. The relaunched worker
re-reads from `lastSeen` and the work-items PK (`ON CONFLICT
DO NOTHING`) absorbs any duplicates.

The alternative — "flush on close": run `route_batch` with
the decisions accumulated so far, advance the cursor part-way
through the batch — is also defensible. It would shorten the
re-route window on graceful shutdown by exactly the number of
events already processed at close time.

Both behaviours are observationally equivalent for
correctness (the catch-up predicate doesn't care which one
you pick; the PK constraint makes either crash-safe). The
choice is a UX call about operator expectations:

- *Drop* (current default): "close is an abort". Simpler
  mental model. Slightly more re-route work on relaunch.
  Matches the worker's own crash-recovery semantics, so the
  graceful and ungraceful paths look identical from outside.
- *Flush*: "close is a checkpoint". Shorter re-route window.
  Introduces a class of partial-batch boundaries that only
  exist on graceful close.

**Why deferred:** no concrete user pain yet. Revisit if soak
runs or a real operator complains about re-route overhead on
restart, or if a per-worker option `closeBehaviour: 'drop' |
'flush'` becomes a clear win.

**Forward-compat constraints on v1:**

- No schema or procedure change required; both behaviours sit
  entirely inside the worker loop.
- A future per-worker option (`closeBehaviour`, or a callback
  the worker invokes at close time deciding flush vs drop)
  is additive; current callers see no behaviour change.
