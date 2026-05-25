# SUB-A implementation plan

This is the ordered implementation plan for landing SUB-A and
its required siblings. It exists so a fresh session does not
have to re-derive the slice order from the design files. Delete
this file when the work lands.

## Status snapshot (for a fresh session picking this up)

**Done:** Slices 1–11.

Commit log (newest first):
- `<this commit>` SUB-A slice 11: soak harness re-baseline on the
  SUB-A worker substrate.
- `492cfb2` SUB-A slice 10: bank-account example migration to
  the SUB-A registration shapes + PM-F upgrade note.
- `d22b663` SUB-A slice 9: Instructed facade onto the SUB-A
  worker modules.
- (prior) SUB-A slice 8: `waitForProjection`
  reimplementation against the catch-up predicate.
- `83b16c6` TODO #13: record streams_stream_uuid_key race
  (pre-existing, unrelated; noted while running slice-7 tests).
- `04bcf5a` SUB-A slice 7: process-manager processing worker.
- `a9463f9` SUB-A slice 6: projection processing worker.
- `13b15c6` SUB-A: unify projection and PM routing surface
  (option (c) course-correction).
- `1e4e2f1` SUB-A: drop PRJ-C from scope (D-0016 fix-up).
- `dc0fd31` SUB-A impl plan: status snapshot +
  carried-forward decisions.
- `984da26` SUB-A slice 5: processing worker (common claim
  mechanics).
- `8554a9a` ML-0012 documentation.
- `76a7b79` SUB-A slice 4: routing worker.
- `23d0bd0` SUB-A slice 3: TS SDK core wrappers.
- `1b08e1c` SUB-A slice 2: core work-queue procedures.
- `fbd7b9c` SUB-A slice 1: subscription_work_items schema.

**Next:** Slice 12 (documentation patches and working-file close-out).

Slice 9 is the breaking-API-change slice for the SDK surface:
it removes the legacy `registerProjection(name, handler)` and
`registerProcessManager(name, { handle, ... })` shapes and
rewires the facade onto the slice 4–7 worker modules
(`startRoutingWorker` + `startProjectionWorker` for the
projection case; `startRoutingWorker` + `startPmWorker` for
the PM case). New surface per PRJ-A option (c) and PM-F:

  - `registerProjection(name, { partitionBy?, routeFn?, handler })`
    — `partitionBy` and `routeFn` mutually exclusive; default
    `partitionBy: { kind: 'sequential' }`. Legacy `selector` is
    expressed as a `routeFn` returning `"ignore"`.
  - `registerProcessManager(name, { routeFn, apply, handle,
    initialState, snapshotModuleVersion? })`. The old
    single-`handle` signature is removed (not deprecated;
    breaking).
  - `Instructed.dispatch` is unchanged.
  - The new modules should be re-exported from
    `sdks/typescript/src/index.ts` here (slices 6–8 deferred
    this).
  - Existing tests under `sdks/typescript/test/instructed.test.ts`
    and `bank-account.test.ts` will need updating; slice 10
    handles the bank-account *example* migration, but slice 9's
    facade tests need to compile.

**Process the original session followed and the new session
should keep:**

- Read this file first, then the SUB-A proposed-design subsection
  of `subscriptions.md` (line 283 onwards), then
  `process-manager.md` PM-C/PM-F, then `projections.md` PRJ-A/B/E
  (PRJ-C dropped; back-reference only).
- Land each slice as one coherent commit with its tests in the
  same commit. No "tests later" passes.
- Before each slice, list any design ambiguities and any
  knob choices, and ask the user to confirm. Do not extend the
  design unilaterally.
- After each slice, surface any decisions that went beyond what
  the design files state explicitly (errors codes, semantics
  defaults, etc.) in the commit message and in the chat summary.
- Re-baseline the test DB whenever the schema changes
  (`docker exec instructed-postgres-1 psql -U postgres -d postgres
  -c "DROP DATABASE IF EXISTS instructed_test"`); both the
  conformance fixture (`tests/conformance/test/fixtures.ts`) and
  the SDK fixture (`sdks/typescript/test/fixtures.ts`) re-install
  the schema on first connection. No schema change in slices 6
  or 8; slice 7 added `list_pm_rebuild_events`. Slice 9 should
  not need any schema change.

## Carried-forward decisions (don't re-litigate)

These were resolved during slices 1–5 and should be honoured
by the remaining slices unless a new design question explicitly
reopens them.

- **No `sql/migrations/` files.** v1 hasn't shipped a release
  and there are no consumers; schema changes land directly in
  `sql/instructed.sql`. The migrations README's `<from>-<to>.sql`
  convention will reactivate at first release tag.
- **`subscription_work_items` PK is the composite
  `(stream_id, subscription_name, shard, partition_key,
  event_number)`**, expanded from the design's illustrative
  `subscription_id`. The `subscriptions` table has no surrogate
  id at v1; expanding the FK was simpler than retrofitting one
  in this work. If a surrogate id is added later the PK shrinks
  with no semantic change. Documented inline on the table.
- **`ON DELETE CASCADE`** from `subscription_work_items` to
  `subscriptions`. Cleaner test ergonomics; matches operator
  expectations.
- **Per-state CHECK constraints** on `subscription_work_items`:
  `state ∈ {pending,claimed,failed,done}`,
  `claimed ⇔ (claimed_by AND lease_expires_at)`,
  `failed ⇔ failed_at`, `error_text` only on failed rows.
  Mechanism-level; the procedures maintain them; the CHECKs
  catch any future direct-SQL bug.
- **New SQLSTATE `IS030 work_item_lease_lost`.** Covers both
  "row gone" and "claimed_by mismatch" on terminal calls;
  either way "stop". Mapped to the `WorkItemLeaseLost` typed
  error in the SDK (with `partitionKey` / `eventNumber`
  context fields).
- **`is_subscription_caught_up` shipped as a SQL function**
  (not just a documented query). Slice 8's `waitForProjection`
  reimplementation calls it directly.
- **`route_batch`'s cursor advance is monotone**
  (`greatest(last_seen, p_new_cursor)`), not a straight assign.
  Defensive against crash-replay and re-entry; equivalent
  under normal single-active routing.
- **`route_batch` requires the caller to hold the subscription
  lease** (IS022 on mismatch). Enforced at the SQL boundary,
  not just in the SDK.
- **`claim_work_item` does NOT take a subscription lease.**
  Open to any processing worker. The subscription-level lease
  exists only for the routing worker.
- **`complete_pm_instance` takes no `worker_id` and no
  triggering `event_number`.** Followed the slice 2 signature
  literally. Idempotent on second call (returns zero counts).
  A takeover worker also reaching `complete: true` re-runs
  it as a no-op.
- **`extend_work_item_claim` was carried forward into slice 5**
  from slice 2. The slice 2 brief didn't list it; slice 5's
  "lease renewal heartbeat during long handler execution"
  needed it. Mirrors `extend_subscription_claim` in shape and
  raises IS030 on lease loss. Conformance covered.
- **Routing worker `close()` mid-batch DROPS the partial
  batch**, not flushes. Same observable behaviour as a crash;
  re-launched worker re-reads from `lastSeen` and the
  work-items PK absorbs duplicates. Recorded as **ML-0012**
  in `docs/maybe-later.md` so the alternative remains a
  documented option.
- **`routeBatch` wire format: `event_number` is a JSON number**
  (not string). Safe up to 2^53; documented as an inline
  known-gap in `Client.routeBatch`. Re-evaluate before v1 if
  a realistic deployment approaches that bound.
- **Processing worker `SubscriptionNotFound` on claim is a
  retry, not a fatal.** The processing worker may be started
  before the routing worker has created the subscription row;
  it sleeps `pollInterval` and tries again. All other
  `claim_work_item` errors surface via `onError` and retry.
- **SUB-B `'stop'` decision does NOT call `fail_work_item`.**
  Per SUB-B: "stop terminates the worker. Other workers may
  still pick up the work item". The row stays `claimed`,
  lease expires, redelivery handles it. `failed` is reserved
  for the future convenience-wrapper layer (`quarantineAfter`)
  and operator action only.
- **`complete()` IS030 in the processing worker -> markAborted.**
  Lease was taken over between handle and complete; redelivery
  via lease expiry handles the item. Same for any non-IS030
  `complete` error: not safe to re-run `handle` without
  `complete` since the handler may not be idempotent.
- **New modules are NOT yet exported from
  `sdks/typescript/src/index.ts`.** The layer-5 facade in
  slice 9 will wire them. Tests import directly via the
  module path. Slice 9 is the right place to decide the
  public surface; until then keep this internal.
- **Slice 7 added a new SQL function `list_pm_rebuild_events`**
  in `sql/instructed.sql`. Cold-path read used by PM-state
  rebuild when the snapshot is missing (IS010) or carries a
  `snapshot_module_version` (in metadata) that no longer
  matches the SDK's compiled-in version. Conformance covers
  the procedure. Test-DB rebase was required at the slice 7
  boundary; subsequent slices need a rebase only if they add
  further schema changes.
- **PM snapshot uuid format = `${def.name}-${partitionKey}`.**
  Parallels the legacy `${def.name}-${processId}` shape; the
  PM-F partition_key replaces the legacy processId.
- **SDK-reserved snapshot metadata key =
  `snapshot_module_version`** (matches the SQL-contract
  reservation in `record_snapshot`'s doc-comment, line ~1049).
  Set on write when `def.snapshotModuleVersion` is supplied;
  compared on read to decide between snapshot-load and
  rebuild. Absent on both sides = match.
- **`startPmWorker(client, dispatchClient, def, opts)`**
  mirrors legacy `startProcessManager`'s two-Client shape for
  D-0011 lock-set disjointness. Throws at construction if the
  two are the same instance. (Two different `Client`
  wrappers around the same pool *is* allowed; D-0011 is about
  per-call session isolation, not pool identity.)
- **PM worker shares per-item state between the slice-5
  `handle` and `complete` callbacks via a
  `Map<"${pk}:${en}", { sourceUuid, stagedState, complete }>`.**
  The map entry is written when `handle` succeeds and read by
  `complete`. SUB-B `retry-in` overwrites the entry on re-run;
  on `stop` or lease loss, `complete` never fires and a stale
  entry just orphans (worker is exiting). The same pattern
  was rejected for projections (no need with PRJ-C dropped)
  but is required for PMs because terminal-vs-non-terminal
  completion + the staged state must flow across the two
  callbacks.
- **PM-E known gap surfaced in `pm-worker.ts` module header.**
  Re-dispatch on SUB-B `retry-in` or lease-takeover
  redelivery may produce duplicate events at the aggregate;
  no IS004 protection without deterministic event IDs.
  Closing this gap is PM-E and is explicitly out of scope for
  SUB-A.
- **Pre-existing test flake in `sdks/typescript/test/concurrent.test.ts`**
  ("N concurrent commands: projector's folded state matches
  the aggregate") trips on a `streams_stream_uuid_key`
  duplicate-insert race ~1 in 5 runs. Predates slice 7;
  verified by running the slice-6 baseline. Recorded as
  **TODO #13** in `TODO.md` with a fix sketch. Out of scope
  here.
- **`waitForProjection` target is always `event_number`**
  (slice 8) for both `$all` and per-stream subscriptions —
  the SUB-A catch-up predicate compares in event_number
  space throughout, and the SUB-A routing worker (slice 4)
  advances `subscriptions.last_seen` using `event.event_number`
  for both source kinds. The legacy single-cursor model's
  per-stream-stream_version target is gone. Public API
  shape unchanged (callers still pass `AppendedEvent` rows
  unchanged); only the internal target field changed. Note
  the schema comment on `subscriptions.last_seen` (lines
  236–240 of `sql/instructed.sql`) still describes the
  legacy stream_version semantics for per-stream subs —
  stale under SUB-A; slice 12 doc-patches pass should
  reconcile.

## Knob defaults (locked in, slices 1–7)

| Knob | Value | Source |
|---|---|---|
| Lease duration (routing) | `30s` | design illustration |
| Lease duration (processing) | `30s` | mirrors routing |
| Routing batch size | `100` | confirmed |
| Routing poll interval | `200ms` | confirmed |
| Processing poll interval | `200ms` | confirmed |
| Lease heartbeat | `lease*1000/3`, min `1s` | confirmed |
| Worker ID format | `${hostname}:${pid}:${uuidv4-8}` | confirmed; reused from existing `defaultWorkerId` |
| Default error policy | exponential, base `100ms`, factor `2`, cap `30s`, retry forever | per SUB-B "What lands" #2 |
| Projection partition key (sequential) | `"_default"` | slice 6 |
| Projection partition key (per-event) | `String(event.event_number)` | slice 6 |
| PM snapshot uuid format | `${def.name}-${partitionKey}` | slice 7 |
| PM snapshot module-version metadata key | `snapshot_module_version` | slice 7; matches the SQL contract reservation |

Later slices that introduce new knobs (batch sizes for the
projection / PM branches, etc.) should propose a default and
ask, per the original process.

## What's in the tree now (for orientation)

SQL:
- `sql/instructed.sql` — canonical spec. Contains the
  `subscription_work_items` table, the nine SUB-A procedures
  (`route_batch`, `claim_work_item`, `extend_work_item_claim`,
  `complete_work_item_projection`, `complete_work_item_pm`,
  `complete_pm_instance`, `fail_work_item`,
  `is_subscription_caught_up`, `list_pm_rebuild_events`), and
  the IS030 SQLSTATE.

SDK (`sdks/typescript/src/`):
- `client.ts` — layer-0 wrappers extended with the nine
  SUB-A methods (including `listPmRebuildEvents` from slice 7).
- `errors.ts` — `WorkItemLeaseLost` (IS030) added;
  `MapPgErrorContext` extended with `partitionKey` /
  `eventNumber`.
- `types.ts` — `RouteDecision`, `RouteBatchResult`,
  `ClaimedWorkItem`, `CompletePmInstanceResult`.
- `routing-worker.ts` — SUB-A routing worker (slice 4).
  Exposes `startRoutingWorker`, `RoutingDecision`,
  `RoutingFn`, `RoutingDefinition`, `RoutingWorkerOptions`,
  and the `DEFAULT_ROUTING_*` constants.
- `processing-worker.ts` — SUB-A processing worker (slice 5).
  Kind-agnostic. Exposes `startProcessingWorker`,
  `ProcessingHandler`, `ProcessingCompleter`,
  `ProcessingHandlerContext`, `ErrorPolicy`,
  `ErrorPolicyDecision`, `DEFAULT_ERROR_POLICY`, and the
  `DEFAULT_PROCESSING_*` constants. Slices 6 and 7 plug in
  here via the `complete` callback.
- `projection-worker.ts` — SUB-A projection adapter (slice 6).
  Exposes `startProjectionWorker`, `PartitionBy`,
  `routingFnForPartitionBy`, `ProjectionHandler`,
  `ProjectionHandlerContext`, `ProjectionDefinition`,
  `SEQUENTIAL_PARTITION_KEY`. `complete` callback is the
  one-line `client.completeWorkItemProjection(...)`. Per
  D-0016 the handler ctx is opaque — no tx, no Queryable.
- `pm-worker.ts` — SUB-A PM adapter (slice 7). Exposes
  `startPmWorker(client, dispatchClient, def, opts)`,
  `PmDefinition`, `PmHandleResult`, `PmHandlerContext`,
  `DispatchedCommand`, `PM_SNAPSHOT_MODULE_VERSION_KEY`.
  Implements PM-C apply/handle split + PM-F lifecycle
  (`complete: true`). Per-item state shared between
  slice-5 `handle` and `complete` via a per-worker Map.
  Known gap: PM-E (deterministic event IDs) is out of
  scope; documented in the module header.
- `consistency.ts` — `waitForProjection` reimplemented
  against `is_subscription_caught_up` (slice 8). Public API
  shape unchanged; per-stream-stream_version target is gone
  (everything is in event_number space now).
- `subscription.ts`, `process-manager.ts` — the **old**
  projection / PM workers. Untouched in slices 1–8. Slice 9
  will replace the facade and may remove these (PM-F is a
  breaking change at the SDK surface per the slice-9 brief).
- `index.ts` — NOT yet extended with the new modules. Slice
  9 is the right place.

Tests:
- `tests/conformance/test/subscription-work-items-schema.test.ts`
  — slice 1.
- `tests/conformance/test/subscription-work-items-procedures.test.ts`
  — slices 2 + 5 + 7 (extend_work_item_claim under "SUB-A
  slice 5"; list_pm_rebuild_events under "SUB-A slice 7").
- `tests/conformance/test/smoke.test.ts` — procedure-presence
  catalogue extended (includes `list_pm_rebuild_events`).
- `sdks/typescript/test/client-work-queue.test.ts` — slice 3.
- `sdks/typescript/test/routing-worker.test.ts` — slice 4.
- `sdks/typescript/test/processing-worker.test.ts` — slice 5.
- `sdks/typescript/test/projection-worker.test.ts` — slice 6.
- `sdks/typescript/test/pm-worker.test.ts` — slice 7.
- `sdks/typescript/test/consistency.test.ts` — slice 8
  (existing legacy-worker tests + a new
  "SUB-A work-item conjunct" describe block).

Fixtures (`fixtures.ts` in both `tests/conformance/test/` and
`sdks/typescript/test/`) have `subscription_work_items` in
their TRUNCATE list.

## Test commands

```sh
# Re-baseline test DB after schema changes:
docker exec instructed-postgres-1 psql -U postgres -d postgres \
  -c "DROP DATABASE IF EXISTS instructed_test"

# SDK:
cd sdks/typescript && npm run type-check && npm test

# Conformance:
cd tests/conformance && npm test
```

As of the end of slice 8: SDK 149 pass / 0 fail reliably;
conformance 163 pass / 0 fail / 3 pre-existing skipped.

Progression: 127 (slice 5) → 138 (+11 slice 6) → 146 (+8
slice 7) → 149 (+3 slice 8). Conformance: 157 (slice 5) →
163 (+6 slice 7's list_pm_rebuild_events).

Known pre-existing test flake (NOT a SUB-A regression —
recorded as **TODO #13** in `TODO.md`): an intermittent
`streams_stream_uuid_key` duplicate-insert race in
`sdks/typescript/test/concurrent.test.ts` ("N concurrent
commands: projector's folded state matches the aggregate").
Verified against origin/main commit `a982b4d` (pre-SUB-A
baseline): fails ~3/10 runs there with the identical error.
Urgent follow-up but unrelated to SUB-A; do not let it block
slice 9+.

Known pre-existing type-check failures in
`examples/bank-account/` (CommonJS / `verbatimModuleSyntax`).
These land for migration in slice 10; ignore them in earlier
slices.

## Goal

Land the routing-vs-processing substrate (SUB-A) end to end:
new schema, new core procedures, new SDK worker loops, updated
PM and projection registration shapes, migrated bank-account
example, soak re-baseline. After this work the SDK exposes the
work-queue model as the only subscription mechanism; the
single-cursor model is gone.

## Reading order

1. This file.
2. `docs/todo/subscriptions.md` — SUB-A is the substrate. Skip
   the historical "Designs 1 / 2 / 3 comparison" sections; read
   the "Proposed design (Design 3 with the PM-F routing shape)"
   subsection and below.
3. `docs/todo/process-manager.md` — PM-C (apply/handle split)
   and PM-F (simplified routing shape + lifecycle).
4. `docs/todo/projections.md` — PRJ-A (registration surface),
   PRJ-B (no apply/handle split), PRJ-E (immediate-delete on
   success). PRJ-C as originally written is dropped; read its
   current text as a back-reference to D-0016 in
   `docs/decisions.md`.
5. Skim `docs/maybe-later.md` ML-0010 and ML-0011 just to know
   what's *not* in scope for this work.

## In-scope items (the must-land set)

- SUB-A core mechanism (schema, routing worker, processing
  worker, claim semantics, catch-up predicate, lease takeover).
- SUB-B core primitives only (`retry-in`, `stop` return values
  from the error-policy hook). Convenience wrappers
  (`retryUpTo`, etc.) ship later.
- SUB-C routing-side batching (baked into the routing hot
  path).
- PM-B as wiring of SUB-A for the PM case.
- PM-C: `apply` (pure state fold) + `handle` (commands +
  `complete?`) split.
- PM-F: `RouteFn → { partitionKey } | "ignore"`; lifecycle
  via `complete: true`.
- PRJ-A: three-mode `PartitionBy` registration surface
  (sugar) **plus** a raw `routeFn: RouteFn` escape hatch for
  the projection case that also wants to ignore some events
  at routing time. Unified-routing shape with PMs; see the
  slice-9 brief for the facade wiring.
- PRJ-B: projections keep the single-callback shape.
- PRJ-E: immediate-DELETE of projection work-items on success.
  Handler is opaque to the SDK; the DELETE runs as its own
  short SDK-owned tx *after* the handler returns. No
  framework-supplied tx is threaded through the handler
  signature (D-0016 in `docs/decisions.md`).
- `waitForProjection` reimplementation against the new
  catch-up predicate.
- Bank-account example migration.
- Soak harness re-baseline.

## Out of scope (do not pull in)

- PM-A (SDK-level fan-out). Post-release; nothing to do.
- PM-E (multi-command deterministic event IDs). Orthogonal;
  separate slice of work, not this one.
- PRJ-D (rebuild as operator action). Ships with
  `instructedctl` (TODO #7).
- `instructedctl` itself (TODO #7). Including
  `skip_work_item_with_audit` operator command.
- CON-B (`waitForProjection` cross-stream guard). Independent;
  may ship before or after this work.
- ML-0010 (configurable projection retention).
- ML-0011 (less-opinionated `PartitionBy`).
- `INV-SUB-*` triage as a separate artifact — invariants get
  updated incrementally as each slice touches them, plus a
  consolidation pass in the final slice. Conformance harness
  rewrite (TODO #11) is its own follow-on.

If a design question arises that isn't covered by the design
files, **ask before deciding**. Do not extend the design
unilaterally.

## Slices

Each slice is a coherent commit (or a small ordered cluster of
commits). Land in order. Land tests with the slice they cover —
no "tests later" pass.

### Slice 1 — Schema migration

**Scope:**
- New SQL file under `sql/migrations/` creating
  `subscription_work_items` and its partial index.
- No application code changes. No procedure changes. No SDK
  changes.

**Acceptance:**
- Migration applies cleanly to a fresh database.
- Existing conformance suite still passes (nothing touches the
  new table yet).
- The docker-compose test database picks up the migration on
  rebuild.

**Design refs:** SUB-A "Schema additions" subsection.

### Slice 2 — Core SQL procedures

**Scope:** Add SQL procedures wrapping the hot paths.

- `route_batch(subscription_id, decisions, new_cursor)` —
  multi-row INSERT into `subscription_work_items` + UPDATE on
  `subscriptions.last_seen`, one tx. `ON CONFLICT DO NOTHING`
  for crash-safety of partial batches.
- `claim_work_item(subscription_id, worker_id, lease_seconds)`
  — the `NOT EXISTS` per-partition predicate +
  `FOR UPDATE SKIP LOCKED`. Includes the lease-takeover branch
  (`state = 'claimed' AND lease_expires_at < now()`).
- `complete_work_item_projection(subscription_id,
  partition_key, event_number)` — DELETE the row.
- `complete_work_item_pm(subscription_id, partition_key,
  event_number, snapshot_payload, snapshot_version,
  snapshot_module_version)` — UPDATE to `'done'` + UPSERT
  snapshot, one tx.
- `complete_pm_instance(subscription_id, partition_key,
  snapshot_uuid)` — DELETE snapshot + DELETE all work-items
  for partition, one tx.
- `fail_work_item(subscription_id, partition_key,
  event_number, error_text)` — UPDATE to `'failed'`.
- Catch-up predicate as a function or a documented query
  (either is fine; pick one).

**Acceptance:**
- Each procedure has SQL-level tests (insert via procedure;
  read via SELECT; assert state transitions).
- The claim query under concurrent callers respects
  per-partition ordering and `SKIP LOCKED` distribution.
- The fail path produces a `failed` row that subsequent claims
  do not pick up for the same partition.
- The `complete_pm_instance` path is atomic (snapshot delete
  and work-item delete commit or rollback together).

**Design refs:** SUB-A "Routing worker", "Processing worker",
"Work-item lifecycle by subscription kind".

### Slice 3 — TS SDK core wrappers

**Scope:** Thin typed wrappers around the slice 2 procedures.

- New files under `sdks/typescript/src/core/` (or wherever the
  existing aggregate core wrappers live — match style).
- Each wrapper translates SQLSTATE to typed errors, same as
  the existing aggregate-path core.

**Acceptance:**
- Unit tests via the test database for each wrapper.
- Error translation tested for at least one SQLSTATE per
  procedure.

**Design refs:** TODO #2 (SDK core vs. conveniences split) for
the style. The new procedures are part of the porting checklist.

### Slice 4 — Routing worker

**Scope:**
- New module: routing worker loop.
- Reads a batch from `$all` past `subscriptions.last_seen`,
  invokes the user-supplied `RouteFn` per event, collects
  routed decisions, calls `route_batch`.
- Single-active-worker semantics on the subscription row
  (acquire lease before reading; renew during work).
- Polling interval + lease duration configurable; pick
  reasonable defaults (ask if unsure — see "Open
  implementation knobs" below).
- No processing worker yet; routed work items just accumulate.

**Acceptance:**
- Tests for: batch atomicity (cursor and inserts commit
  together); routing-decision determinism (same input batch
  produces same rows); `"ignore"` decisions produce no rows;
  crash mid-batch leaves cursor un-advanced; re-run after crash
  re-routes the same events (idempotent via PK conflict).
- Race-safety test: append event N, observe that
  `last_seen >= N` is never visible without the corresponding
  work-item rows being visible too.

**Design refs:** SUB-A "Routing worker — hot path", SUB-C
"Resolved: ships with SUB-A".

### Slice 5 — Processing worker (common claim mechanics)

**Scope:**
- New module: processing worker poll loop.
- Calls `claim_work_item`; on claim, reads the event payload;
  invokes the kind-specific handler (stubbed for now); calls
  the kind-specific completion procedure.
- Lease renewal heartbeat during long handler execution.
- Failure path: handler throws → SUB-B error policy hook →
  `retry-in` (re-poll after delay, leave item `pending` or
  re-claim) or `stop` (worker exits, item returns via lease
  expiry).
- Default error policy: today's "exponential backoff capped at
  30s, retry forever" (per SUB-B "What lands" item 2).

**Acceptance:**
- Tests for: per-partition ordering under concurrent
  claimants; parallel-across-partitions throughput;
  lease-takeover after worker death; `failed` row blocks its
  partition only; default error policy back-off.

**Design refs:** SUB-A "Processing worker — claim and
complete", SUB-A "Lease takeover", SUB-B "What's settled".

### Slice 6 — Processing worker, projection branch

**Scope:**
- New module `sdks/typescript/src/projection-worker.ts` that
  wraps `startProcessingWorker` (slice 5) with a projection
  adapter. Per D-0016 the handler is opaque to the SDK: no
  tx threaded through the handler signature, no `ctx.tx`,
  no `Queryable` in the handler context. The adapter's
  `complete` callback is the one-line
  `client.completeWorkItemProjection(...)`, which runs as its
  own short SDK-owned tx after the handler returns.
- Three `PartitionBy` modes (sugar over a routing-layer
  `RoutingFn`; the helper lives in this module and is reused
  by the slice-9 facade):
  - `sequential` → routing produces partition `'_default'`.
  - `per-event` → routing produces partition equal to
    `String(event.event_number)`.
  - `per-key` → routing calls the user-supplied `key(event)`
    function; partition key is its string return.
  None of the three modes emit `"ignore"`. A projection that
  wants routing-side filtering uses the raw `routeFn` escape
  hatch wired at slice 9.

**Acceptance:**
- Tests for: each `PartitionBy` mode behaves as specified
  (sequential = serial; per-event = max parallelism;
  per-key = parallel across keys, serial within);
  immediate-delete (no `done` row ever exists for a
  projection); handler throw leaves the work item `claimed`
  with the lease still held under the error-policy retry
  loop (no spurious commit-without-handler); the DELETE
  is a single procedure call, not wrapped in any
  framework-supplied user-facing tx.

**Design refs:** PRJ-A, PRJ-E, D-0016.

### Slice 7 — Processing worker, PM branch

**Scope:**
- Plug a PM handler into the slice 5 worker.
- State load:
  - If snapshot present and `snapshot_module_version` matches:
    state = snapshot. (Existing snapshot path.)
  - Else: rebuild — read all `done` work-items for the
    partition where `event_number < claimed_event_number`,
    fetch each event by primary key, fold via `apply` from
    `initialState()`.
- Run `apply(state, claimed_event)` → `staged_state`.
- Run `handle(staged_state, claimed_event)` → `{ commands?,
  complete? }`.
- Dispatch each command via `runCommand` (existing aggregate
  path). Commands run on separate sessions, per D-0011.
- After successful dispatch:
  - If `complete: true` → `complete_pm_instance` (DELETE
    snapshot + all work-items in one tx).
  - Else → `complete_work_item_pm` (UPDATE to `done` + UPSERT
    snapshot in one tx; snapshot payload = `staged_state`,
    `source_version = claimed_event.event_number`).
- Note: per PM-E (out of scope here) the dispatch path is
  *not* yet idempotent against redelivery. Document this as a
  known gap for this slice; PM-E closes it later.

**Acceptance:**
- Tests for: snapshot load happy path; rebuild via `apply` on
  missing snapshot; rebuild via `apply` on
  `snapshot_module_version` mismatch (state matches
  freshly-built); `complete: true` deletes both snapshot and
  every work-item for partition in one tx; non-terminal path
  advances both snapshot and work-item state; multi-command
  emissions dispatch in declaration order; failure during
  dispatch leaves the work-item `claimed` (lease will expire,
  redelivery happens).

**Design refs:** PM-C entire section, PM-F "Decided: delete on
complete", PM-F "Interaction with PM-C", SUB-A "Work-item
lifecycle by subscription kind".

### Slice 8 — `waitForProjection` reimplementation

**Scope:**
- New implementation against the SUB-A catch-up predicate
  (both conjuncts: routing cursor ≥ T AND no in-flight
  work-items ≤ T).
- Polling with timeout; configurable timeout per call.
- Replace the existing single-cursor wait.

**Acceptance:**
- Tests for: caught-up returns immediately; not-caught-up
  blocks then returns; timeout raises a typed error; race
  safety at the start (append + immediate wait does not
  spuriously return caught-up).

**Out of scope here:** the cross-stream guard (CON-B). The
guard ships separately.

**Design refs:** SUB-A "Catch-up predicate (for
`waitForProjection`)" in the proposed-design section.

### Slice 9 — `Instructed` facade updates

**Scope:**
- `registerProjection(name, { partitionBy?, routeFn?, handler })`
  — three-mode `PartitionBy` sugar, default `sequential`;
  *or* a raw `routeFn: RoutingFn` escape hatch for projections
  that need routing-side filtering or a partition shape the
  three sugar modes can't express. `partitionBy` and `routeFn`
  are mutually exclusive; supplying both is a registration-time
  error. The legacy `selector` parameter has no direct
  replacement; its observable behaviour is recovered by a
  `routeFn` that returns `"ignore"` for the would-be-skipped
  events. Surface this in the slice-10 upgrade note.
- `registerProcessManager(name, { routeFn, apply, handle,
  initialState, snapshotModuleVersion?, snapshotPolicy? })` —
  new shape; the old single-`handle` signature is removed
  (not deprecated — this is a breaking change at the SDK
  surface).
- The facade wires the registration to the slice 4–7 workers.
- `Instructed.dispatch` is unchanged except for any consistency
  list options that referenced the old subscription model.

**Acceptance:**
- Integration tests: end-to-end (append → route → claim →
  handle → ack) for one projection and one PM.
- The bank-account example tests still compile (they're
  rewritten in slice 10; here we just ensure the facade types
  don't break compilation).

**Design refs:** PRJ-A "Proposal", PM-F "Proposal" + "What
lands" item 1.

### Slice 10 — Bank-account example migration

**Scope:**
- Port the existing PM in `examples/bank-account/` to the new
  shape: `RouteFn` + `apply` + `handle`.
- Update any tests under `examples/` that referenced the old
  PM signature.
- Write a one-page upgrade note (per PM-F "What lands" item 2)
  somewhere obvious — possibly `docs/upgrade-notes/pm-f.md`
  or appended to `docs/concepts.md` — covering the four
  Commanded-directive collapses.

**Acceptance:**
- Bank-account example tests pass.
- Upgrade note shows the before/after for at least one PM.

**Design refs:** PM-F "How each Commanded directive collapses"
table.

### Slice 11 — Soak harness re-baseline

**Scope:**
- Run `tests/soak/` against the new substrate.
- Update invariant checks for the new work-item-shaped
  realities (PM-024 and PM-FORWARD-TOTAL stay; INV-SUB-P-* may
  reshape).
- Document any new gaps in `tests/soak/README.md`.

**Acceptance:**
- Soak run on the new substrate completes; all invariant
  checks pass.
- New gaps (if any) are recorded in `Known gaps`.

**Design refs:** TODO #3b done-list entry for the existing
harness.

### Slice 12 — Documentation patches and working-file close-out

**Scope:**
- `docs/invariants.md`: update SNAP-002 honest-gap wording —
  the PM case is now no worse than the aggregate case (per
  PM-C "Recommendation").
- `docs/invariants.md`: full `INV-SUB-*` triage pass.
  Invariants that no longer describe the user-facing contract
  get marked `[mechanism-only]` or removed.
- `docs/decisions.md`: new entry "we don't inherit Commanded's
  directive set because we don't inherit Commanded's runtime"
  (per PM-F "What lands" item 6).
- `docs/maybe-later.md`: remove ML-0001 (dissolved into SUB-A).
- `TODO.md`: close out item #12 references; mark SUB-A done.
- Delete the four working files in `docs/todo/`:
  `subscriptions.md`, `process-manager.md`, `projections.md`,
  `sub-a-implementation.md` (this file). Anything that needs
  to survive moves into `docs/architecture.md` or
  `docs/concepts.md`.

**Acceptance:**
- A reader of `docs/` only (no `docs/todo/`) gets a coherent
  picture of the new substrate.

## Cross-cutting concerns

These are not their own slices but apply to every slice:

- **Same-tx atomicity is load-bearing** in the framework's
  own state, in three places:
  - Routing: cursor advance + work-item INSERTs (slice 4).
    Race-safety of `waitForProjection` depends on this.
  - PM non-terminal: work-item UPDATE-to-done + snapshot
    UPSERT (slice 7).
  - PM terminal: snapshot DELETE + all-work-items DELETE
    (slice 7).
  Projections explicitly do *not* extend this list. Per
  D-0016, the projection handler is opaque to the SDK and may
  target any store (Postgres, Elasticsearch, Redis, an HTTP
  API). `complete_work_item_projection` runs as its own short
  SDK-owned tx *after* the handler returns; idempotency of
  the handler against at-least-once redelivery is the
  application's responsibility.
- **`failed` rows are sacred.** No code path auto-skips or
  auto-deletes them. Only operator action (out of scope here;
  ships with `instructedctl`) can transition them.
- **No silent skip for any PM event.** Even on failure, the
  PM partition stalls; it does not advance past the failure.

## Open implementation knobs (ask if unsure)

Design files leave these unspecified. Pick a reasonable
default and call it out in the slice's commit message, or ask:

- Lease duration default (SUB-A illustratively uses `30s`).
- Routing batch size default (SUB-C says "to be tuned").
- Routing worker polling interval (when no `LISTEN/NOTIFY`).
- Processing worker polling interval (same).
- Lease renewal heartbeat interval (some fraction of lease
  duration).
- Worker ID format (UUID v4? `hostname:pid:random`?).
- Default error-policy backoff curve (SUB-B says "today's
  behaviour — exponential backoff capped at 30s, retry
  forever").

## "Done" definition

The work lands when:

- All 12 slices have landed.
- The TS SDK test suite (existing + new) is green.
- The conformance suite is green (with the `INV-SUB-*`
  triage applied).
- The soak harness re-baseline run completes clean.
- The bank-account example runs end-to-end against the new
  substrate.
- The four `docs/todo/` files for this work (this file plus
  the three design files) are deleted; their decided content
  has migrated into `docs/architecture.md` and `docs/concepts.md`
  as appropriate.

After this, the next pieces of work to tackle are: PM-E
(deterministic event IDs for PM-dispatched commands), CON-B
(cross-stream `waitForProjection` guard) if not already
landed, `instructedctl` (TODO #7), and the conformance
overhaul (TODO #11).
