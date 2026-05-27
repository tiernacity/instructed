# Porting checklist

What to build when porting the `instructed` SDK to a new language.
The TypeScript SDK in `sdks/typescript/` is the reference; this
doc is the reading list that turns it into a porter's spec.

Status: **incomplete**. Step 5 of the SDK rework
(`docs/todo/sdk-rework.md` §7) writes this doc one slice at a
time. As of 2026-05-27 the routing extension point (slice 1) and
aggregate snapshot policy (slice 2) are documented; the retry /
error policy (slice 3) section will land with that slice. The
rest of the doc is structural and stable.

Reference SQL contract: `docs/sql-contract.md` plus `sql/instructed.sql`.
Reference SDK: `sdks/typescript/src/`. Reference invariants:
`docs/invariants.md`.

---

## 1. The three-layer model

Per `docs/todo/sdk-rework.md` §1 and §2:

  - **L1 — procedure bindings.** One method per `instructed.*`
    stored procedure; SQLSTATE → typed-error translation. **Every
    port reproduces this surface verbatim** (the SQL contract is
    the source of truth). The TS SDK's `client.ts`, `errors.ts`,
    and the wire half of `types.ts` are the reference.
  - **L2 — core behaviours.** The aggregate load-execute-append
    loop with OCC retry (D-0005); the per-batch claim/release
    routing worker (D-0025); the per-item lease + heartbeat
    processing worker; kind-specific projection / PM adapters.
    **Every port reproduces the behaviours; the shape may differ
    per language** (TS uses functions returning a `RunningWorker`;
    Go might use a `Worker` struct; Elixir a `GenServer`; Python
    an `async with` context manager).
  - **L3 — conveniences.** By-name dispatch, registration, the
    consistency-on-dispatch wait, partitioning sugar. **May
    differ per language port**, or be omitted entirely if not
    idiomatic.

The TypeScript SDK exposes L1+L2 as the `instructed-sdk/core`
sub-path entry (`src/core.ts`) and L1+L2+L3 as the bare entry
(`src/index.ts`). A porter reading `core.ts` sees exactly the
required surface.

---

## 2. Required L1 surface

Mirror every `instructed.*` SQL procedure as a method on a
`Client`-shaped object. SQLSTATE codes are listed in
`docs/invariants.md`; each maps to a typed error class in your
language's idiom. See `sdks/typescript/src/client.ts` and
`errors.ts`.

This section will be filled in with the per-procedure inventory
during a later pass; the TS `Client` is currently the
authoritative list.

---

## 3. Required L2 behaviours

See `sdks/typescript/src/` for the reference implementations:

  - `aggregate.ts` — `runCommand` (load/execute/append with OCC
    retry; D-0005, AGG-001..010).
  - `routing-worker.ts` — D-0025 per-batch routing worker.
  - `processing-worker.ts` — per-item lease + heartbeat
    processing loop.
  - `projection-worker.ts` — projection adapter
    (`complete_work_item_projection`).
  - `pm-worker.ts` — PM adapter (snapshot+ack tx; rebuild on
    miss/mismatch).

Each is annotated in `docs/todo/sdk-rework.md` §2 with the
behaviours a port MUST reproduce.

---

## 4. Extension points

The SDK offers three named extension points, each a place where
application code plugs in either a shipped strategy or its own
function. Each follows the **contract + standard library + escape
hatch** pattern (see `docs/todo/sdk-rework.md` §7.1):

  - **Contract.** The function signature the SDK calls. **Required
    core.** Every port reproduces the shape, though language idiom
    may rename or restructure (TS generics become Go interfaces,
    Elixir behaviours, Python protocols).
  - **Standard library.** A handful of shipped fixed strategies
    for the common cases, built on top of the contract.
    **Idiomatic, not required.** Ship the same set, a different
    set, or none.
  - **Escape hatch.** Application code drops in its own function
    obeying the contract. Always available.

### 4.1 Routing

**Contract.** Given an event from the source stream, decide
where it goes. Two outcomes: route to a named partition, or
ignore (skip; nothing is written to the work queue).

In TypeScript:

```ts
type RoutingDecision = { partitionKey: string } | "ignore";

type RoutingFn<E = unknown> = (
  event: RecordedEvent<E>,
) => RoutingDecision | Promise<RoutingDecision>;
```

In any language: a function from event to `Routed(partitionKey)`
or `Ignored`. The partition key is a string; the SDK uses it as
the `partition_key` column in `subscription_work_items` and to
group work items into work-stealing units for the processing
worker.

**Contract obligations on the user function:**

  - Pure (no I/O). The routing worker calls this once per event,
    inside a per-batch lease window; I/O makes the lease budget
    unpredictable.
  - Deterministic. The routing cursor is monotone (D-0025); a
    crashed mid-batch worker re-routes the same events, and the
    work-item PK absorbs duplicate INSERTs only if `routeFn`
    produces the same decision both times.
  - Bounded duration. `batchSize × routeFn` MUST complete inside
    `leaseSeconds`; otherwise `route_batch` raises `IS022` and
    the work is redone.
  - Failures are not silent. A thrown / errored `routeFn` stalls
    the worker and surfaces via the worker's error callback;
    SUB-A invariant "no silent skip."

**Routing is required core** (L2). A port that drops it can't
implement work-stealing across processes under D-0025. The TS
reference is `routing-worker.ts:RoutingFn`.

**Standard library (TS).** `PartitionBy` in
`sdks/typescript/src/partition-by.ts` ships three modes:

  - `sequential` — single partition (key = `"_default"`); fully
    serial processing.
  - `per-event` — partition key = event number; fully parallel.
  - `per-key` — user-supplied key extractor; the general case.

None of these can produce `"ignore"`; routing-time filtering uses
the escape hatch (raw `RoutingFn`).

**Standard library obligations on a port.** **Idiomatic, not
required.** Ship `sequential` / `per-event` / `per-key`
equivalents if they fit the language, in whatever shape is
natural; ship more if your users need them; ship none and
document the raw `RoutingFn` shape only. The audit in
`partition-by.ts`'s module comment records why the TS port
stopped at three: `per-event-type`, `hash-modulo-N`, and
routing-time filtering all collapse cleanly to `per-key` or the
escape hatch.

### 4.2 Aggregate snapshot policy

**Contract.** Given the post-append state of an aggregate,
decide whether to write a snapshot.

In TypeScript:

```ts
interface SnapshotPolicy<S> {
  shouldSnapshot(
    state: S,
    version: bigint,
    eventsSinceLast: number,
  ): boolean;
}
```

In any language: a function (or interface method, or protocol)
from `(state, version, eventsSinceLast)` to boolean.
`eventsSinceLast` counts events folded into the current state
since the last persisted snapshot (or since `initialState()` for
a never-snapshotted stream).

**Required core vs. idiomatic.** The contract itself is
**idiomatic, not required**: a port may shape the snapshot-policy
declaration differently (a `snapshot_every: 100` field directly
on the aggregate definition, a `Decider` protocol, a closure
field, etc.). What's required-core is one rung lower: every port
MUST expose `record_snapshot` as an L1 primitive separate from
`append_to_stream`, and the orchestration MUST treat snapshot
writes as best-effort (failure logs and continues; the command
still succeeds). See D-0019.

**Orchestration is L3.** The TypeScript L2 primitive
(`runCommand` in `aggregate.ts`) does load + execute + append +
OCC retry only; it does **not** inspect `def.snapshotPolicy`.
The L3 wrapper `runCommandWithSnapshots` (in
`aggregate-snapshots.ts`) wraps a sibling primitive
(`runCommandAndApply`, which additionally folds the appended
events through `apply` to produce the post-append state) and,
on success, invokes the policy and best-effort writes the
snapshot via a separate `record_snapshot` call.

  - **Events are correctness.** Append failure must surface to
    the caller; the OCC retry loop and the `IS001 →
    WrongExpectedVersion` translation exist for this.
  - **Snapshots are performance.** Failure logs and continues;
    the next load falls back to the previous snapshot (or full
    re-fold from origin) without breaking correctness.

Bundling the two into one atomic SQL call would couple their
failure modes (forcing a snapshot-write failure to either fail
the command or be silently swallowed). Keeping them as two L1
calls preserves the distinction.

**Standard library (TS).** `everyN(n)` in `aggregate.ts` — the
only shipped policy. Audit conclusion (step-5 slice 2,
2026-05-27): further helpers (time-elapsed,
state-size-threshold) will be added when a concrete use case
demands them; not shipping speculatively.

**Standard library obligations on a port.** **Idiomatic, not
required.** Ship `everyN` (or its idiomatic equivalent — a
field-on-the-definition, a constructor argument, whatever fits)
if your users need it; ship more if they ask; or document the
raw contract and require users to bring their own.

**Cross-language note.** A port's L3 orchestrator may take a
different shape from TS's `runCommandWithSnapshots` (a method
on a `Repository` object, an `Aggregate` actor's tick, etc.) as
long as: (a) the L1 append and L1 snapshot calls remain
separate; (b) snapshot failure is best-effort; (c) the policy
sees the post-append state, version, and the
events-since-last-snapshot counter.

### 4.3 Retry / error policy

*To be documented with slice 3 of the SDK rework (see
`docs/todo/sdk-rework.md` §7.4).*

Sketch: `ErrorPolicy<PolicyState>(err, ctx, state) ->
{ decision, state }`. The SDK threads `state` per work item;
first call gets `undefined`. Standard library:
`exponentialBackoff`, `linearBackoff`, `retryUpTo`, and a
composition helper.

---

## 5. May differ per language

L3 of the TS SDK (the `Instructed` facade,
`registerAggregate` / `registerProjection` /
`registerProcessManager`, `waitForProjection`, the `PartitionBy`
sugar) is idiomatic to JS/TS and async-by-default. Each port
decides what idiom fits:

  - Python: context managers, `async with`, possibly `asyncio`
    queues for worker coordination.
  - Go: structs and methods; `context.Context` for cancellation;
    goroutines for the worker loops.
  - Elixir: `GenServer`s for the worker loops; supervisor trees;
    `Application` callbacks for lifecycle.

The required surface stays the same (L1 verbatim, L2
behaviours, the three extension-point contracts). Everything
above that is open.
