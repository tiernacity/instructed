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
  - step 4 (re-export grouping + sub-path wiring): landed.
    `src/core.ts` is the L1+L2 entry; `src/index.ts` re-exports it
    plus L3. Asymmetries 2 (errors annotation), 4 (`mapPgError`
    hidden), and the `PartitionBy`-sugar-as-L3 split all
    addressed; see `§3 resolution status` below.
  - step 5 (pluggable extension points): **complete**
    (2026-05-27). All three slices landed. PM L2/L3 split
    (asymmetry #1) and `quarantineAfter` (TODO #7 co-design)
    parked with reasons; SNAP-002 / snapshot module versioning
    stays under TODO #5, not here. TODO #2 (SDK restructuring)
    is discharged; the porting checklist (`docs/porting-checklist.md`)
    is in place; TODO #6 (additional language SDKs) can now
    start against a stable `core.ts`.

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

Surfaced by the classification pass.

### Resolution status:

  - **#1 `pm-worker.ts` L2/L3 mix**: **deferred to step 5.** The
    clean physical split is to extract an L2 PM substrate whose
    `handle` callback returns no commands (snapshot+ack only),
    then make the current `startPmWorker` a thin L3 wrapper that
    interprets a returned `commands` list via `runCommand`. That
    refactor changes the user-facing `PmDefinition.handle`
    signature and intersects directly with the pluggable
    extension-points discussion (specifically the
    "PM-handler-dispatch error visibility" item from §4). Doing
    it now would prejudge that conversation; step 5 owns it.
  - **#2 `errors.ts` layer annotation**: landed (step 4).
  - **#3 `readSubscriptionBatch` / `advanceSubscription` pre-SUB-A
    survivors**: landed. Both stay on `Client` and stay in
    `instructed-sdk/core` (a port MUST expose every SQL
    procedure). Each now carries a docstring noting it is not
    called by the supplied workers under D-0025; new code should
    prefer the routing-worker substrate; the procedures survive
    for callers writing bespoke long-lease loops above the
    `Client` layer.
  - **#4 `mapPgError` / `MapPgErrorContext` public re-exports**:
    landed (step 4).
  - ~~**#5 `Instructed.dispatch`'s `consistency:` option**~~:
    **retired from the asymmetries list.** On review this isn't an
    asymmetry -- both the option (L3) and the mechanism (L1
    `is_subscription_caught_up`) sit in their right layers. The
    note that ML-0002 may rework the mechanism without changing
    the shape is forward-looking, not a present-day problem.
  - **#6 `RunningWorker` shared handle**: landed (step 4).

New asymmetry surfaced during step 4 and now resolved:

  - **PartitionBy sugar location**: landed. Moved to
    `src/partition-by.ts` so the file boundary matches the layer
    boundary. `projection-worker.ts` is now pure L2 (the
    adapter); `partition-by.ts` is pure L3 (the sugar);
    `instructed.ts` and `index.ts` updated to import from the
    new location; one test file updated (the rest import via the
    public `instructed-sdk` entry, which is unaffected).

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
  - **Re-export grouping in `src/index.ts`.** Landed in step 4.
    Source of truth: `src/core.ts` re-exports L1+L2 with section
    headers; `src/index.ts` re-exports `./core.ts` then adds L3
    behind a second labelled section. The CJS and ESM `dist/`
    outputs both carry a `core.{js,d.ts}` alongside `index.*`
    so the `instructed-sdk/core` sub-path resolves at runtime.
  - ~~**Porting checklist as a separate doc.**~~ Landed during
    step-5 slice 1 (2026-05-27) as
    [`docs/porting-checklist.md`](../porting-checklist.md).
    Initial scaffold covers the three-layer model, the L1/L2
    section skeletons (to be filled in across later passes),
    and the routing extension point in full. Slices 2 and 3
    will append the snapshot-policy and error-policy sections.

---

## 6. Stale documentation to fix in the same pass

  - ~~`sdks/typescript/README.md` "Layer structure" table references
    `subscription.ts` and `process-manager.ts`~~ -- fixed in step 4;
    table now describes the actual L1/L2/L3 file set and names the
    `instructed-sdk/core` sub-path.
  - ~~The same README's example invocation
    (`node --experimental-strip-types examples/bank-account/main.ts`)
    is out of date~~ -- fixed in step 4; README now points at
    `examples/typescript/bank-account/`.
  - ~~SDK design doc — mentioned in `aggregate.ts` ("see
    `docs/sdk-design.md` §3 layer 1"); confirm whether it survives
    the rework or whether this file replaces it. **Open.** Grep
    confirms the file does not exist; the comment is a dangling
    reference.~~ **Done (2026-05-27)** during step-5 slice 2:
    `aggregate.ts`, `internal/with-transaction.ts`, and
    `types.ts` all had dangling references cleaned up
    (replaced with cross-references to `docs/todo/sdk-rework.md`
    where relevant, or simply deleted).

---

## 7. Step 5 plan — first slices

Step 5 ("pluggable extension points") is not about adding new
hooks. The SDK already implements the relevant hooks; step 5's
job is to **recognise the pattern they share, make each
contract precise, fill out the standard library where it's
thin, and resolve the one genuinely-new technical question**
(stateful retry policies).

### 7.1 The pattern

At each plug point the SDK offers three things:

  - **A contract** — the function signature the SDK calls.
  - **A standard library** — a handful of shipped fixed
    strategies for the common cases, built on top of the
    contract.
  - **An escape hatch** — drop in your own function obeying the
    contract.

Three points in the current SDK already follow this pattern:

  | Plug point                  | Contract                                                              | Shipped library                                              | Source                                  |
  | --------------------------- | --------------------------------------------------------------------- | ------------------------------------------------------------ | --------------------------------------- |
  | Aggregate snapshot policy   | `SnapshotPolicy<S>.shouldSnapshot(state, version, eventsSinceLast)`   | `everyN(n)`                                                  | `aggregate.ts`                          |
  | Routing to partition keys   | `RoutingFn` → `RoutingDecision`                                       | `routingFnForPartitionBy(PartitionBy)` + `SEQUENTIAL_PARTITION_KEY` | `routing-worker.ts` + `partition-by.ts` |
  | Retry / error handling      | `ErrorPolicy(err, ctx) → ErrorPolicyDecision`                         | `DEFAULT_ERROR_POLICY` (exponential backoff)                 | `processing-worker.ts`                  |

The step-5 work is to discharge each as a deliberate, documented
extension point, fix the asymmetries surfaced while doing so,
and name the contracts on the porting checklist that TODO #6
consumes.

### 7.2 Slice 1 — Routing (lightest)

**Why first.** The contract is in good shape and the standard
library covers the common cases (per-stream, single-partition
via `SEQUENTIAL_PARTITION_KEY`, user-supplied). The work is
mostly recognition and documentation, with one or two
standard-library additions if they fall out cheaply. Cheapest
way to validate the framing in §7.1 before tackling the
harder slices.

**Shape.**

  - Document `RoutingFn` + `RoutingDecision` explicitly as
    "the routing extension point" in the SDK README and
    porting checklist. Today the contract is implicit in the
    TS type; promote it to a named concept.
  - Audit `PartitionBy` and `routingFnForPartitionBy`: confirm
    the shipped strategies (`per-stream`, the implicit
    single-partition case) cover what users actually reach
    for. Per-event-type / hash-modulo-N are candidates if a
    case appears; otherwise hold.
  - Porting checklist: routing is a **required** core concept
    (the SDK's contract with the store under D-0025 is
    per-batch claim/release; you can't drop it). The TS-side
    `PartitionBy` sugar is **idiomatic, not required**;
    porters ship their own equivalents.

**Touches.** `routing-worker.ts` and `partition-by.ts` (doc
comments only, expected); README "Layer structure" table;
new section in the porting checklist.

**Out of scope.** Routing-fn shape changes. The contract has
survived SUB-A and the bank-account example unchanged; no
revision proposed.

### 7.3 Slice 2 — Aggregate snapshot policy (layer recut + SQL question)

**Why second.** The `SnapshotPolicy<S>` hook already exists and
works. The step-5 work here is **a layer recut** that came out
of the design conversation: today `runCommand` (L2) both
invokes the policy *and* writes the snapshot as a separate
best-effort SQL call after the append. The cleaner cut per the
three-layer model is:

  - **Core (L1+L2)** is unopinionated about snapshot policy.
    It accepts events to persist and, optionally, a snapshot
    to persist alongside them.
  - **SDK convenience layer (L3, or an L3-shaped wrapper
    around an L2 primitive)** owns the policy invocation:
    after the user's handler returns events, call
    `shouldSnapshot`; if true, compute the snapshot via
    `apply` over the just-emitted events; hand the
    (events, optional-snapshot) bundle to core.

This recut makes "shape may differ per language" fall in the
right place (Python / Go / Elixir can each present snapshot
policy idiomatically) and shrinks the porting-checklist
surface (porters only have to expose the core append-with-
optional-snapshot primitive).

**How does core accept the bundle? Decided: two separate L1
calls, snapshot stays best-effort.** `appendToStream` and
`recordSnapshot` remain distinct procedures. The L3 wrapper
calls them back-to-back: append first; if the policy fired and
the append succeeded, call `recordSnapshot` as a follow-up;
failures there are `console.warn`-and-continue per D-0019.

**Rationale.** Events and snapshots are different kinds of
thing and warrant different error responses:

  - **Events are a correctness concern.** Event sourcing
    *requires* that emitted events persist; failure here
    must fail the command and surface to the caller. The
    OCC retry loop and the `IS001 → WrongExpectedVersion`
    translation exist to make this explicit.
  - **Snapshots are a performance concern.** They exist so
    that loading an aggregate doesn't re-fold from origin
    every time. A missing or stale snapshot makes the next
    load slower; it does **not** break correctness. Failure
    here is a warning, not an error.

Atomic persistence (options B/C in earlier drafts — SDK-opened
tx, or extending `append_to_stream` to take an optional
snapshot payload) would *couple* the two failure modes,
forcing a snapshot-write failure to either fail the command
(wrong: snapshots aren't correctness-critical) or be silently
swallowed inside one tx (wrong: hides observable signal).
Keeping them as separate L1 calls preserves the distinction
and keeps the SDK out of the business of opening
transactions. D-0019 (snapshot writes are best-effort) stands.

"Core receives events + optional snapshot together" therefore
lands as a *conceptual* unification at L3 — the L3 wrapper
orchestrates both — not a physical one at the SQL contract.
No SQL change. The porting checklist names the two-call
orchestration as part of the L3 surface, with the
"shape may differ per language" caveat: a porter could
legitimately fold the two calls into one L3 method (TS), two
separate methods, or a builder pattern, provided the
best-effort semantics on the snapshot side are preserved.

**Standard library audit.** `everyN` is the only shipped
policy today. Candidates to add if cheap: `everyNSeconds`
(time-elapsed), `everyNEvents` (alias for clarity),
`whenStateSize(predicate)`. Don't ship speculatively;
add what the next worked example actually needs.

**Touches.** `aggregate.ts` (extract policy invocation to L3
wrapper, leave `runCommand` as load-execute-append); possibly
a new `src/aggregate-with-snapshots.ts` or similar to host the
wrapper; `core.ts` and `index.ts` re-exports; SQL contract
(option C only); tests; README. The dangling
`docs/sdk-design.md` reference in `aggregate.ts` goes away in
the same pass.

**Out of scope.** Snapshot module versioning (SNAP-002 /
TODO #5) is a *correctness* concern, orthogonal to this
*performance* concern. Stays under TODO #5; this slice does
not pre-empt it.

### 7.4 Slice 3 — Retry / error policy (the stateful one)

**Why last.** This is the slice with genuinely-new technical
work: how does a retry policy carry state across attempts on
the same work item?

**The contract shape.** Generic-typed, state-as-fold:

```ts
type ErrorPolicy<PolicyState = undefined> = (
  err: unknown,
  ctx: ErrorPolicyContext,
  state: PolicyState | undefined,
) => { decision: ErrorPolicyDecision; state: PolicyState };
```

The SDK threads a per-work-item slot. First call gets
`undefined`; subsequent calls get whatever the previous call
returned. `PolicyState` is existential to the SDK (it doesn't
inspect the value, just hands it back next time).

Today's `DEFAULT_ERROR_POLICY` is a pure function of
`(err, attempt)`; it migrates as `PolicyState = undefined`
and nothing changes for default users. The new shape unlocks
policies that *need* state — token-bucket budgets, "back off
harder if the last three errors had the same SQLSTATE",
etc.

**The sub-decision: scope of state.**

  - **Per-work-item (lifecycle = one work-item attempt loop,
    reset on success).** Matches today's `attempt` reset.
    Supports: backoff curves, retry-up-to-N,
    last-error-driven decisions within an item.
  - **Per-worker-process (long-lived).** Adds: token buckets,
    circuit breakers, adaptive policies. Cross-language: the
    Go / Elixir port would express this as an enclosing
    struct / GenServer rather than a state slot; the
    contract still works but the lifecycle name needs
    pinning down.
  - **Persistent across worker restarts.** Almost certainly
    out of scope; name it parked.

  **Lean.** Per-work-item by default. Per-worker as a follow-on
  if a concrete user appears; the contract above is
  forward-compatible (a worker-scoped policy would close over
  its long-lived state in a closure and ignore the slot).

**Standard library.** Ship: `exponentialBackoff({ baseMs,
capMs, jitter? })`, `linearBackoff({ stepMs, capMs })`,
`retryUpTo(n, inner)`, and a composition helper
(`retryUpToWithBackoff(n, inner)`). Default stays
exponential-retry-forever (today's behaviour).

**Cross-language note for the porting checklist.** The TS
generic `PolicyState` disappears in Python / Go / Elixir;
they'd use closures, interfaces, or process state respectively.
The contract a porter MUST reproduce is the *shape*: "policy
returns a `(decision, opaque-state)` pair; SDK threads the
state forward through the attempt loop for one work item."

**Touches.** `processing-worker.ts` (the `ErrorPolicy` type and
the attempt loop); `core.ts` re-exports the type and the
shipped library; tests for each shipped strategy.
`quarantineAfter` stays parked for TODO #7 co-design.

### 7.5 Parked deliberately

  - **`quarantineAfter` error-policy helper.** Co-design with
    TODO #7's `instructedctl` `failed`-row surface. The
    contract from §7.4 is forward-compatible; the helper
    can ship later without re-doing the type.
  - **PM L2/L3 split (asymmetry #1) + candidate #6
    (PM-handler-dispatch error visibility).** Originally
    queued as a step-5 slice ("second slice" in the prior
    draft). Re-parked: it's not really an *extension point*
    in the §7.1 sense — it's a layer-recut similar to slice
    2, but without a corresponding contract that needs
    naming. Land it as a follow-on after step 5 closes
    (still before TODO #6 hits porting, ideally), or in the
    same pass as TODO #7 if that comes first.
  - **Middleware (candidate #5).** Doesn't fit the
    contract+library pattern (middleware is a *chain*, not a
    single function). Separate post-step-5 conversation when
    a concrete user forces a shape.
  - **Read-store connection lifecycle (candidate #7),
    per-event handler verbosity (candidate #8),
    partition-claim observability (candidate #9).** Pure
    ergonomics / DX; no TODO interlock; defer.
  - **SNAP-002 / snapshot module versioning.** Stays under
    TODO #5 (correctness, not an extension point).
    `aggregate.ts:loadAggregate` reads `snap.data` and trusts
    the shape; a developer who changes their state shape
    between deploys gets handed stale-shape state. PMs are
    half-protected via `PmDefinition.snapshotModuleVersion` +
    the `PM_SNAPSHOT_MODULE_VERSION_KEY` metadata key;
    aggregates have nothing. Real gap; not this conversation.

### 7.6 Exit criteria for step 5

Step 5 closes when:

  - ~~Slice 1 has landed: routing's contract is named in the
    README and porting checklist; the shipped library is
    audited and either confirmed or extended; routing is
    flagged "required core concept" while `PartitionBy` is
    flagged "idiomatic, not required."~~ **Done (2026-05-27).**
    `routing-worker.ts` and `partition-by.ts` doc comments
    name the extension point and record the standard-library
    audit (three modes confirmed, no additions);
    `sdks/typescript/README.md` gains an "Extension points"
    table and fixes the stale `PartitionBy`-in-
    `projection-worker.ts` reference;
    `docs/porting-checklist.md` lands with the routing section
    complete.
  - ~~Slice 2 has landed: policy invocation is moved out of
    `runCommand` into an L3 wrapper; `appendToStream` and
    `recordSnapshot` remain two L1 calls with snapshot
    best-effort (D-0019 preserved, cross-referenced); the
    standard library is audited; the dangling
    `sdk-design.md` reference in `aggregate.ts` is gone.~~
    **Done (2026-05-27).** L2 `aggregate.ts` now exposes
    `runCommand` (load+execute+append+OCC; no snapshot work)
    and `runCommandAndApply` (same plus post-append state
    fold); L3 `aggregate-snapshots.ts` is the new file hosting
    `runCommandWithSnapshots`. `Instructed.dispatch` and the
    PM-worker command dispatch both switched to the L3
    wrapper, so facade-level snapshot behaviour is unchanged.
    A new test pins down that L2 `runCommand` does NOT invoke
    `snapshotPolicy`. `everyN` audit recorded inline (no
    additional helpers shipped); dangling `sdk-design.md`
    reference removed; D-0019 cross-referenced from both
    `aggregate.ts` and `aggregate-snapshots.ts`. Porting
    checklist §4.2 filled in. All 132 tests pass.
  - ~~Slice 3 has landed: `ErrorPolicy` carries a `PolicyState`
    generic; the SDK threads the slot per work item;
    `exponentialBackoff`, `linearBackoff`, `retryUpTo`, and a
    composition helper ship; default behaviour unchanged.~~
    **Done (2026-05-27).** `ErrorPolicy<PolicyState = undefined>`
    with return shape `{ decision, state }` lands in
    `processing-worker.ts`. The SDK threads `policyState`
    per work item (`undefined` initial; reset on success). New
    L3 file `error-policies.ts` ships `exponentialBackoff`,
    `linearBackoff`, and `retryUpTo`; composition is plain
    function wrapping (no extra helper). `DEFAULT_ERROR_POLICY`
    semantics preserved verbatim (still exponential-100-30000,
    retry forever). `ProcessingWorkerDefinition`,
    `ProjectionDefinition`, `PmDefinition`, and
    `startProcessingWorker` / `startProjectionWorker` /
    `startPmWorker` all gained an optional `PolicyState` type
    parameter (defaults to `undefined`; backward-compatible for
    every existing caller). Facade-level registration types use
    `ErrorPolicy<any>` for state-slot type erasure with a note;
    direct `startProcessingWorker` callers can opt into strong
    typing. New `test/error-policies.test.ts` covers the
    helpers (10 unit tests) plus an integration test pinning
    down the per-work-item state lifecycle. All 142 tests pass.
  - ~~The three contracts (snapshot policy, routing, error
    policy) are named on the porting checklist as the
    extension-point family, with "shape may differ per
    language; contract stays" called out.~~ **Done
    (2026-05-27).** `docs/porting-checklist.md` §4 names the
    three contracts as an explicit family and calls out the
    required-core / idiomatic-not-required split throughout.
  - ~~Parked items in §7.5 have been re-confirmed as parked.~~
    **Re-confirmed (2026-05-27).** No new TODO has surfaced
    pulling them forward; PM L2/L3 split + dispatch error
    visibility (asymmetry #1) and `quarantineAfter` are the
    two principal parked items, both with documented reasons.

**Step 5 exit (2026-05-27).** TODO #2 (SDK restructuring) is
discharged. `docs/porting-checklist.md` is in place against a
stable `core.ts` / `index.ts` split. TODO #6 (additional
language SDKs) can now begin against a documented contract
surface.
