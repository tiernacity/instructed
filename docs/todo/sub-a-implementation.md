# SUB-A implementation plan

This is the ordered implementation plan for landing SUB-A and
its required siblings. It exists so a fresh session does not
have to re-derive the slice order from the design files. Delete
this file when the work lands.

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
   PRJ-B (no apply/handle split), PRJ-C (read-model
   transactionality), PRJ-E (immediate-delete on success).
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
- PRJ-A: three-mode `PartitionBy` registration surface.
- PRJ-B: projections keep the single-callback shape.
- PRJ-C: same-tx read-model + framework write.
- PRJ-E: immediate-DELETE of projection work-items on success.
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
- Plug a projection handler into the slice 5 worker.
- On handler success: `complete_work_item_projection` in the
  same transaction as the handler's read-model write
  (`ctx` exposes the tx — PRJ-C).
- Three `PartitionBy` modes:
  - `sequential` → routing produces partition `'_default'`.
  - `per-event` → routing produces partition equal to
    `event_number` (or another unique value).
  - `per-key` → routing calls the user-supplied `key(event)`
    function.

**Acceptance:**
- Tests for: each `PartitionBy` mode behaves as specified
  (sequential = serial; per-event = max parallelism;
  per-key = parallel across keys, serial within); same-tx
  atomicity (read-model write rolled back ⇒ work-item not
  deleted); immediate-delete (no `done` row ever exists for a
  projection).

**Design refs:** PRJ-A, PRJ-C, PRJ-E.

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
- `registerProjection(name, { partitionBy?, handler })` —
  three-mode `PartitionBy`, default `sequential`.
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

- **Same-tx atomicity is load-bearing.** Three places:
  - Routing: cursor advance + work-item INSERTs (slice 4).
    Race-safety of `waitForProjection` depends on this.
  - PM non-terminal: work-item UPDATE-to-done + snapshot
    UPSERT (slice 7).
  - PM terminal: snapshot DELETE + all-work-items DELETE
    (slice 7).
  - Projection: handler's read-model write + work-item DELETE
    (slice 6).
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
