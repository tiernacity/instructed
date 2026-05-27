# TypeScript SDK rework — working file

Tracks TODO #2 (SDK restructuring). Sets up the porting checklist
that TODO #6 (additional language SDKs) consumes. Reads as a
research artefact first, work-in-progress second; the canonical
framing is `SDK-REWORK-NOTES.md` in the repo root, which this file
refines.

Status (2026-05-27):
  - step 1 (annotated export map): landed in §2 below.
  - step 2 (cheap layer-1 cleanups): TODO #15 landed. TODO #14
    parked (needs a SQL-contract decision; left in TODO.md).
  - step 3 (package shape decision): landed as
    [D-0027](../decisions.md#d-0027) -- single `instructed-sdk`
    package with an `instructed-sdk/core` sub-path for L1+L2.
  - step 4 (re-export grouping + sub-path wiring): open.
  - step 5 (pluggable extension points): open.

---

## 1. Layer model — reconciliation

The handoff notes (`SDK-REWORK-NOTES.md` §2) describe **three** layers
addressed at the porter:

  - **L1** — procedure bindings (the "SQL driver"); every port
    reproduces verbatim.
  - **L2** — core behaviours (the "correct-by-construction
    runtime"); every port reproduces the behaviour, shape may differ
    per language.
  - **L3** — conveniences (the "idiomatic facade"); may differ
    freely per language.

The current TS SDK README describes **five** layers (0..5). Those
five map cleanly onto the three:

| Notes layer | README layer(s) | Files |
|---|---|---|
| L1 procedure bindings | L0 (`Client`) | `client.ts`, `errors.ts`, the wire half of `types.ts` |
| L2 core behaviours | L1 + L2 + L3 (snapshot semantics half) | `aggregate.ts`, `routing-worker.ts`, `processing-worker.ts`, plus the snapshot+ack tx in `pm-worker.ts` |
| L3 conveniences | L3 (dispatch half) + L4 + L5 | `projection-worker.ts` partition sugar, the dispatch half of `pm-worker.ts`, `consistency.ts`, `instructed.ts` |

This document uses the **three-layer** numbering when classifying
exports below, because that's what the porting checklist needs. The
README's five-layer split is a TS-internal organising aid (each
"layer" corresponds to one source file) and can survive within the
TS SDK as a more granular view, but the porter-facing document
should speak in threes.

**Asymmetries surfaced by the reconciliation.** A handful of files
straddle the L2/L3 boundary. Each is called out under its
classification below; the rework needs to either split the file or
mark the asymmetry deliberately in docs. Untangling these is part of
step 5 (pluggable extension points), not step 1.

---

## 2. Annotated export map

Every public symbol re-exported through `sdks/typescript/src/index.ts`,
classified by layer. Source file in parens; notes call out anything
straddling a layer boundary or deserving porter attention.

### L1 — procedure bindings (every port reproduces verbatim)

**`client.ts`** — the `Client` class. One method per SQL procedure.
SQLSTATE → typed-error translation. No SDK-opened transactions, no
retry, no cached state. Direct mirror of `sql/instructed.sql`.

  - `Client`, `ClientOptions`
  - Methods (each a thin wrapper):
    - events: `appendToStream`, `readStream`, `readAll`
    - snapshots: `recordSnapshot`, `readSnapshot`, `deleteSnapshot`
    - subscriptions: `claimSubscription`, `extendSubscriptionClaim`,
      `releaseSubscription`, `readSubscriptionBatch`,
      `advanceSubscription`, `readSubscriptionPosition`,
      `deleteSubscription`
    - SUB-A work queue: `routeBatch`, `claimWorkItem`,
      `extendWorkItemClaim`, `completeWorkItemProjection`,
      `completeWorkItemPm`, `completePmInstance`, `failWorkItem`,
      `isSubscriptionCaughtUp`, `listPmRebuildEvents`

  Two of these still hold legacy / pre-SUB-A shapes that the rework
  should review:
    - `readSubscriptionBatch` and `advanceSubscription` survive from
      the pre-SUB-A single-cursor subscription path. The SUB-A
      worker substrate doesn't call them; they're only used by
      external callers writing their own custom long-lease loops
      against the `Client`. **Decision needed:** keep as part of the
      porter contract or move out of the L1 surface. Leaning: keep,
      since the SQL contract exposes both procedures and the L1 surface
      is defined as "the SQL contract"; document them as "useful for
      bespoke loops, not used by the supplied workers."
    - `claimSubscription`'s return type is the **TODO #15** bug:
      narrower than the SQL contract permits. Fix is the warm-up for
      this rework.

**`errors.ts`** — SQLSTATE → class hierarchy. Part of L1 because the
class set is what `Client` translates into.

  - `InstructedError` (base)
  - Append errors: `AppendError`, `WrongExpectedVersion`,
    `StreamExists`, `StreamNotFound`, `DuplicateEvent`,
    `ReservedStreamUuid`, `AppendOnlyViolation`
  - Snapshots: `SnapshotNotFound`
  - Subscriptions: `SubscriptionError`, `SubscriptionNotFound`,
    `SubscriptionAlreadyClaimed`, `SubscriptionLeaseLost`
  - Work items: `WorkItemLeaseLost`
  - Misc procedure errors: `InvalidParameterValue`
  - **Not L1** but living here:
    `RetryBudgetExhausted` (L2; emitted by the aggregate retry
    loop), `ConsistencyTimeout` and `ConsistencyTargetError` (L3;
    emitted by `waitForProjection`), `UnknownAggregateType` (L3;
    emitted by the `Instructed` facade), `HandlerError` (TBC; check
    callers). These are sensibly co-located but a porter reading
    `errors.ts` cannot tell which classes they MUST reproduce
    (SQLSTATE-bound) vs. which are layer-specific. The rework
    should either split the file or annotate each class with the
    layer that emits it. Leaning: annotate; splitting would scatter
    related classes.
  - `mapPgError`, `MapPgErrorContext`: L1 internal, currently
    exported. Probably should not be public; revisit during
    re-export grouping (step 3).

**`types.ts`** — wire-shape contracts for `Client` arguments and
results. L1 verbatim:

  - `Queryable` (pg.Pool / pg.Client structural shape)
  - `JsonValue`
  - `ExpectedVersion`, `expected` helper
  - `NewEvent`, `AppendedEvent`, `RecordedEvent`
  - `AppendOptions`
  - `SnapshotInput`, `Snapshot`
  - `ClaimResult` (currently overstates its types; see TODO #15)
  - `StartFrom`, `ClaimSubscriptionOptions`, `SubscriptionShardOption`
  - `RouteDecision`, `RouteBatchResult`
  - `ClaimedWorkItem`, `CompletePmInstanceResult`

### L2 — core behaviours (every port reproduces the behaviour)

**`aggregate.ts`** — load-execute-append loop with OCC retry.
Implements AGG-001..010 / D-0005. The reference shape for "what
every SDK does with a command."

  - `runCommand` — the load/execute/append/retry loop
  - `DEFAULT_RETRY_BUDGET` — observable default (5 attempts)
  - `everyN` — snapshot policy helper (L2; the policy hook is L2,
    sensible defaults that ship with the SDK are L2-internal)
  - `AggregateDefinition<S, C, E>`, `RunCommandOptions`,
    `SnapshotPolicy<S>`, `DomainEvent`

  Porter notes:
    - the load path swallows `StreamNotFound` at version 0 (treats a
      missing stream as empty); every port reproduces this.
    - causation defaulting to `commandId`, correlation defaulting
      to the `correlationId` option; explicit fields on `NewEvent`
      win verbatim. AGG-020/021.
    - snapshot write is best-effort; failures `console.warn` and
      don't fail the command. The "best-effort" semantics are L2;
      the "use `console.warn`" choice is TS-specific and should not
      be in the porting checklist. Logger surface is open.
    - explicit `expectedVersion` disables OCC retry (D-0019).

**`routing-worker.ts`** — D-0025 per-batch claim/release routing
worker. The routing half of SUB-A.

  - `startRoutingWorker` — the loop
  - `DEFAULT_ROUTING_BATCH_SIZE`, `DEFAULT_ROUTING_LEASE_SECONDS`,
    `DEFAULT_ROUTING_POLL_INTERVAL_MS`
  - `RoutingDecision` (`{ partitionKey } | "ignore"`)
  - `RoutingFn`, `RoutingDefinition`, `RoutingWorkerOptions`

  Porter notes:
    - per-batch claim/release (D-0025) is the SDK's contract with
      the store; not a TS-only choice. Drop a long-lived-lease
      routing worker and the multi-process work-stealing story
      breaks.
    - `routeFn` is pure user code; thrown errors stall the worker
      (no silent skip). This is an SDK invariant.
    - `IS022` mid-batch is non-fatal under D-0025 (the work-item
      PK absorbs duplicates from the takeover worker). Porters
      MUST implement the same recovery.

**`processing-worker.ts`** — kind-agnostic processing loop with
per-item lease + heartbeat. Hosts the SUB-B `ErrorPolicy` surface.

  - `startProcessingWorker` — the loop
  - `DEFAULT_PROCESSING_LEASE_SECONDS`,
    `DEFAULT_PROCESSING_POLL_INTERVAL_MS`, `DEFAULT_ERROR_POLICY`
  - `ProcessingHandler`, `ProcessingCompleter`,
    `ProcessingHandlerContext`, `ProcessingWorkerDefinition`,
    `ProcessingWorkerOptions`
  - `ErrorPolicy`, `ErrorPolicyDecision`, `ErrorPolicyContext`

  Porter notes:
    - per-item heartbeat at `leaseSeconds/3` (with floor 1s) is the
      SDK's contract with the store; the heartbeat is what keeps
      the work-item lease alive through long handlers.
    - `wasTakeover` surfaced via `onError` is informational; not
      load-bearing.
    - the default `ErrorPolicy` (exponential backoff, retry
      forever, cap 30s) is **observable behaviour** today (SUB-B
      "What lands" item 2). A port must either reproduce this
      default or call out the divergence prominently.
    - the `complete` callback is kind-specific and supplied by the
      L2 adapter (projection / PM); the loop itself is kind-agnostic.

**`pm-worker.ts` snapshot+ack semantics** — the "snapshot is part
of the ack" tx (PM-C / PM-F) is L2. Specifically:
    - the `loadState` flow (snapshot read → module-version compare →
      rebuild via `listPmRebuildEvents` on miss/mismatch);
    - the `apply` fold of the claimed event before `handle`;
    - the `complete_work_item_pm` (UPDATE work item + UPSERT
      snapshot in one tx) vs. `complete_pm_instance` (terminal
      DELETE) split based on `handle`'s return.

  The `runCommand`-based **dispatch helper** in this file
  (`for (const c of commands) await runCommand(...)`) is L3
  (convenience). It composes existing L2 primitives but is not
  itself an SDK invariant — a port that exposed a different shape
  for "PM handler returns commands" (e.g. yield-style, or "PM
  hands the SDK a list of dispatches and the SDK fans them out
  in parallel") would still be conformant. See §3 below.

  Symbols all live in `pm-worker.ts` today:
    - `startPmWorker` — composition: L2 state-load + L2 snapshot/ack
      + L3 dispatch helper
    - `PM_SNAPSHOT_MODULE_VERSION_KEY` — L2 (metadata key the SQL
      contract relies on the SDK choosing consistently)
    - `PmDefinition`, `PmHandleResult`, `PmHandlerContext`,
      `PmWorkerOptions`, `DispatchedCommand`

  Module-version handling here is the live wire that TODO #5
  (SNAP-002) touches. Today the SDK rebuilds-on-mismatch silently;
  TODO #5's question is whether this becomes a pluggable hook.

### L3 — conveniences (may differ per language)

**`projection-worker.ts`** — adapter that wraps
`startProcessingWorker` with `complete_work_item_projection`. The
adapter itself is borderline L2/L3 (every port needs *some* way to
ack a projection); the `PartitionBy` sugar is unambiguously L3.

  - `startProjectionWorker` — adapter
  - `routingFnForPartitionBy` — L3 sugar over `RoutingFn`
  - `SEQUENTIAL_PARTITION_KEY` — L3 (implementation detail of the
    sugar)
  - `PartitionBy` — L3
  - `ProjectionHandler`, `ProjectionHandlerContext`,
    `ProjectionDefinition`, `ProjectionWorkerOptions`

**`consistency.ts`** — `waitForProjection` polling helper.

  - `waitForProjection`
  - `DEFAULT_WAIT_POLL_INTERVAL_MS`, `DEFAULT_WAIT_TIMEOUT_MS`
  - `SubscriptionRef`, `WaitForProjectionOptions`

  L3 because (a) the wait shape is a convenience over the L1
  `isSubscriptionCaughtUp` predicate, and (b) per
  `SDK-REWORK-NOTES.md` §3 ML-0002 may rework the polling
  mechanism into LISTEN/NOTIFY later. Cross-stream guard (CON-B)
  is L3-internal validation.

**`instructed.ts`** — the `Instructed` facade. By-name aggregate
dispatch, projection/PM registration, single `startWorker()`,
single `close()`, `consistency:` option on `dispatch`.

  - `Instructed`, `InstructedOptions`, `InstructedDefaults`
  - `RegistrationOptions`, `RegisterProjectionInput`,
    `RegisterProcessManagerInput`, `DispatchOptions`

  All L3. A Python port would ship something quite different
  (context managers, async iterators, ...).

**Shared (no layer)** — `internal/running-worker.ts` exports
`RunningWorker { stopped, close() }`. Re-exported from `index.ts`
as the worker-handle shape. L2 internal interface, leaks through
L3.

---

## 3. Asymmetries and decisions to record

Surfaced by the classification pass; each is open for the rework:

1. **`pm-worker.ts` mixes L2 (snapshot+ack tx, module-version
   rebuild) with L3 (in-loop `runCommand` dispatch).** Splitting
   could either be physical (two files) or notional (one file,
   docstring split). The shape that lets a porter say "I must
   reproduce the snapshot+ack; my language's PM facade is mine"
   is what we need. Likely: keep the file, add a section header
   between the two, mark the dispatch helper as a TS-specific
   convenience in the porting checklist.

2. **`errors.ts` co-locates SQLSTATE-bound classes (L1, porter MUST
   reproduce) with layer-specific ones (`RetryBudgetExhausted` L2,
   `ConsistencyTimeout` L3, `UnknownAggregateType` L3, `HandlerError`
   TBC).** Annotate per class with the layer that emits it; do not
   split the file (the closed set is more useful contiguous).

3. **`readSubscriptionBatch` / `advanceSubscription` survive on the
   `Client` from the pre-SUB-A single-cursor era.** They are still
   in the SQL contract and a porter MUST expose them at L1, but the
   supplied SDK workers don't use them. Document as "bespoke loop
   primitives, not used by `startRoutingWorker`."

4. **`mapPgError` and `MapPgErrorContext` are publicly re-exported
   today.** Probably should not be. Step-3 (re-export grouping)
   decision; either drop from `index.ts` or move under an
   `instructed-sdk/internal` sub-path.

5. **The `Instructed` facade's `consistency:` option lives on
   `DispatchOptions` and lights up `waitForProjection` after
   append.** Both pieces are L3. The mechanism (server-side
   `is_subscription_caught_up` predicate) is L1. ML-0002 may
   eventually rework the polling into LISTEN/NOTIFY; the L3 shape
   should stay stable across that change.

6. **`RunningWorker` is a shared worker-handle shape that crosses
   L2 (each worker function returns one) and L3 (`Instructed`
   composes them).** Probably stays in `internal/` but exported;
   the porting checklist names the *contract* (`stopped`, `close`),
   not the file location.

---

## 4. Pluggable extension points — starting list

Direct lift from `SDK-REWORK-NOTES.md` §3, indexed for the work
ahead. Each is a candidate for step 5 (land one or two first).

From the opening message:

  1. **Retry / backoff strategy** — currently hard-coded
     exponential-backoff-retry-forever inside `processing-worker.ts`
     as `DEFAULT_ERROR_POLICY`. `errorPolicy` hook already exists;
     the question is what default-replacements ship in the box
     (`retryUpTo`, `quarantineAfter`, ...). Interlocks with TODO #7
     (`instructedctl` operator surface for `failed` rows).
  2. **Routing to partition keys** — `routeFn` + `PartitionBy` sugar.
     Review whether the current contract is the right one.
  3. **Snapshot policies** — `SnapshotPolicy<S>` for aggregates
     (`everyN` only). Module-version handling for PMs (TODO #5) is
     the open promotion question.
  4. **Error policies and handling** — see (1); same hook, the
     conversation distinguishes "the retry curve" from "the
     terminal handling."
  5. **Middleware** — not present today. Likely separate chains
     for command-dispatch and event-handler paths. Use cases:
     tracing, metrics, structured logging, multi-tenant routing,
     request-id propagation.

From the bank-account example work:

  6. **PM-handler-dispatch error visibility** — errors from
     SDK-dispatched commands surface via `onError`, not the PM's
     `try/catch`. Decide: document asymmetry, or unify.
  7. **Read-store connection lifecycle** — every projection script
     manages its own pool. Candidate: `registerReadStore` that
     ties pool lifecycle to the worker.
  8. **Per-event handler verbosity** — boilerplate around
     "switch on event_type → idempotent UPSERT guarded by
     `last_event_number`." Optional typed projection helper.
  9. **Partition-claim observability** — no "I just claimed
     partition X" hook today. Belongs in middleware /
     observability.

Already in TODO that touches this layer:

  - **TODO #5** — SNAP-002 snapshot module versioning. Currently
    SDK rebuilds-on-mismatch silently; should be pluggable with a
    sensible default.
  - **TODO #14** — `claim_work_item` IS020 first-startup race.
    Layer 1 / layer 2 boundary; decision parked (preferred fix:
    SQL contract change).
  - **TODO #15** — `claim_subscription` nullable diagnostic
    fields. Pure L1; landing now as the rework warm-up.
  - **`docs/maybe-later.md` ML-0006..0012** — scan during the
    rework to see which want pulling forward.

---

## 5. Open questions for the next decision round

  - **Physical package shape.** ~~Single `instructed-sdk` package
    with documented sub-paths, or two packages
    (`@instructed/core` + `@instructed/runtime`)?~~ **Decided:**
    [D-0027](../decisions.md#d-0027) -- single package, with an
    `instructed-sdk/core` sub-path exposing L1+L2 (the porting
    checklist surface). The bare `instructed-sdk` entry adds the
    L3 facade and helpers on top.
  - **Re-export grouping in `src/index.ts`.** The re-exports
    should make the L1/L2/L3 split legible. Probably:
    ```ts
    // L1 — procedure bindings
    export { Client, ... } from "./client.ts";
    export * from "./errors.ts";  // (after L2/L3 annotation)
    export * from "./types.ts";

    // L2 — core behaviours
    export { runCommand, ... } from "./aggregate.ts";
    export { startRoutingWorker, ... } from "./routing-worker.ts";
    export { startProcessingWorker, ... } from "./processing-worker.ts";
    export { startProjectionWorker, ... } from "./projection-worker.ts";
    export { startPmWorker, ... } from "./pm-worker.ts";

    // L3 — conveniences
    export { waitForProjection, ... } from "./consistency.ts";
    export { Instructed, ... } from "./instructed.ts";
    ```
    Should land alongside or after step 2's package-shape decision
    so the comments above re-exports can name the package layout
    they imply.
  - **Porting checklist as a separate doc.** `SDK-REWORK-NOTES.md`
    §2 suggests `docs/porting-checklist.md`. Probably right; the
    audience (a porter writing a Python/Go/Elixir SDK) is distinct
    enough from the existing docs to warrant its own page.

---

## 6. Stale documentation to fix in the same pass

  - `sdks/typescript/README.md` "Layer structure" table references
    `subscription.ts` and `process-manager.ts` — both removed
    during SUB-A. Update to the actual current file set (or to the
    L1/L2/L3 view after the rework).
  - The same README's example invocation
    (`node --experimental-strip-types examples/bank-account/main.ts`)
    is out of date: examples moved to
    `examples/typescript/bank-account/` in commit `5d39ae3`.
  - SDK design doc — mentioned in `aggregate.ts` ("see
    `docs/sdk-design.md` §3 layer 1"); confirm whether it survives
    the rework or whether this file replaces it.
