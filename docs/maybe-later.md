# Maybe later

Capabilities not in scope for v1 but that we want to keep visible
so v1 design choices don't accidentally preclude them. Each entry
records what the thing is, why it's deferred, and what the v1
design must keep compatible.

This is distinct from [`non-goals.md`](non-goals.md), which is
about things we deliberately *won't* do.

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

## ML-0003 — Server-side routing-decision evaluation

Allow `route_batch`'s caller — or a future `route_batch_eval`
variant — to express the per-event routing decision as a
JSONB-path / SQL predicate evaluated server-side, instead of an
arbitrary callback invoked per event in the routing worker.
Reduces routing-worker CPU for sparse routes at the cost of
restricting the routing vocabulary to the server's predicate
language.

**Why deferred:** v1 ships SDK-side routing only. The routing
worker reads a batch from `read_all`, runs the application's
`routeFn` per event, and writes the resulting decisions via
`route_batch`. SDK-side is the simplest and most expressive
option — the predicate is arbitrary application code.

**Forward-compat constraints on v1:**

- A future server-side variant would be a new procedure
  (`route_batch_eval`?) or a new key on `route_batch`'s
  `p_options`. Either way, adding it MUST NOT change v1's
  `route_batch` semantics (decisions still computed by the
  caller).
- The SDK's arbitrary-`routeFn` mode stays for use cases the
  server-evaluable vocabulary doesn't cover; the two are
  composable.
- The atomic `route_batch` cursor advance + work-item INSERTs
  contract is unchanged.

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
  that also performs `complete_work_item_projection`,
  re-enabling exactly-once consistency between projection write
  and work-item DELETE for that narrow case.

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

The v1 SDK ships a three-mode `PartitionBy` surface
(`sequential` / `per-event` / `per-key`) for projection
registration. A future SDK revision may collapse this to a
less-opinionated surface, e.g.

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

Under SUB-A, projection work-items are DELETEd as the terminal
step of a successful handler run. No `done` row is persisted for
projections (the PM path differs: PMs UPDATE the row to `done`
plus UPSERT a snapshot in one tx, because the `done` rows back
PM-state rebuild).

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

---

## ML-0013 — Multi-routing-worker subscriptions (`concurrency_limit > 1`)

The SUB-A routing worker is single-active per subscription:
`claim_subscription` returns a non-error `'already_claimed'` row to
any would-be second claimer, and `concurrency_limit` is effectively
fixed at 1 in v1 (D-0002). Within-subscription parallelism is
provided entirely on the processing side via the work queue
(INV-SUB-W-010/011 — N processing workers, partitioned by
`partition_key`).

A future variant would let multiple routing workers share a
subscription, each routing a disjoint shard of the source stream
(or of `$all`). This is a different axis of parallelism to the
work queue's: it scales the routing-decision throughput, not the
handler throughput. Plausible motivations: a `$all` subscription
whose routing decision is expensive (server-side JSONB-path
selectors per ML-0003), or a workload where the routing worker is
CPU-bound on `interested?` evaluation for a high-fanout PM.

**Why deferred:** no observed need. The processing-side
parallelism SUB-A ships handles every workload we've modelled.
Adding a second axis of distribution before there's a concrete
pain point would complicate the lease model (per-shard leases on
the subscriptions row), the cursor model (one `last_seen` per
shard, or a merge function), and the conformance surface (three
new INV-SUB-P-04x cases come into scope, sketched below, in a
form shaped by whichever shard mechanism lands).

Note: the per-batch claim/release work-stealing model the v1
routing worker uses (see [D-0025](decisions.md#d-0025)) is
**not** ML-0013. That model keeps one active routing worker per
subscription at any instant; the active worker just rotates per
batch across processes. ML-0013 is specifically about
*concurrent* routing workers, each owning a disjoint shard,
holding live leases simultaneously.

**Forward-compat constraints on v1:**

- The `shard` column reserved on `subscriptions` (v1 default 0;
  see invariants.md "Identity" paragraph for Part E) is the
  forward-compat hook. Identity is `(stream_uuid, name, shard)`;
  v1 collapses to one shard per `(stream_uuid, name)`.
- `IS021 subscription_already_claimed` is reserved but not raised
  in v1 (sql-contract.md). A multi-routing-worker variant would
  raise it once the per-shard claim count exceeded
  `concurrency_limit`.
- A partition-selector API (per ML-0003) becomes a prerequisite
  for stickiness across routing workers, not just within a
  single routing worker's work queue.

**Conformance shapes to add when implemented.** These were
previously stubbed as skipped tests in
`tests/conformance/test/subscription-partitioned.test.ts`; that
file has been removed (it referenced functionality we haven't
built). The shapes are recorded here so they survive the file's
deletion:

- **INV-SUB-P-040 — multi-subscriber distribution under
  `concurrency_limit > 1`.** Every event MUST be delivered to
  exactly one of the live subscribers; the total number of live
  subscribers is capped at `concurrency_limit`.
  *Setup (under the future API):* claim subscription with
  `concurrency_limit: 3` from three workers; a 4th claim returns
  `IS021 subscription_already_claimed`; append K events; each
  worker reads its batch and advances.
  *Assertions:* union of received event-ids has size K; pairwise
  intersection is empty; the 4th claim raises IS021.

- **INV-SUB-P-041 — `partition_by` stickiness, intra-partition
  order, and rebalance.** With a `partition_by` selector, every
  event for which `partition_by(event)` returns the same value
  MUST be delivered to the same subscriber (modulo subscriber
  failure + rebalance); intra-partition order MUST equal
  `event_number` order.
  *Setup:* three workers with `concurrency_limit = 3` and a
  selector extracting `partition_key` from each event; append
  events tagged `A, A, B, A, B` in that global order.
  *Assertions:* the worker that received the first A-event
  received every A-event (same for B); each worker's events
  appear in `event_number`-ascending order. Rebalance variant:
  the A-worker releases or its lease expires; a fresh worker
  claims and resumes from the A-partition cursor; the still-live
  B-worker continues to see only B-events.

- **INV-SUB-P-042 — no `partition_by`:
  exactly-once-among-live-subscribers, no stickiness.** Without
  a selector, the ONLY guarantee is "every event is delivered
  to exactly one of the live subscribers". This is
  INV-SUB-P-040's weaker companion — it documents what is *not*
  promised. An implementation that routes every event to
  worker-1 would still satisfy INV-SUB-P-042 (it would only
  fail INV-SUB-P-040's stronger "distribute" wording).
  *Setup:* three workers with `concurrency_limit = 3` and no
  selector; append K events.
  *Assertions:* union has size K; pairwise intersection is
  empty; NO assertion on stickiness, order across workers, or
  distribution fairness.

When ML-0013 lands, these shapes get re-written against the
chosen claim / shard / selector API and added as real tests
(probably in a fresh `subscription-sharded.test.ts` or similar).
