# Process manager — outstanding work

**Origin:** 2026-05-23 re-review of the invariant catalogue
(closed as [TODO #1](../../TODO.md), Done items section) plus
follow-up conversation the same day.

PM is the primitive that needs the most attention. This file collects the
open items in one place rather than scattering them across `maybe-later.md`
and the review document. Some are pre-release blockers; some are post-release
work; one or two are still open design questions.

Status legend:

- **Decided** — direction agreed; awaits implementation.
- **Designed, awaiting confirmation** — analysis below offers a recommendation;
  the user needs to confirm before it's promoted to **Decided**.
- **Open** — multiple paths still live; needs discussion.

Release-relevance legend:

- **Pre-release** — should land before we call any SDK GA.
- **Post-release** — can ship later as additive work.

---

## PM-A — Fan-out routing (one event → many PM instances)

**Status:** Designed, awaiting confirmation.
**Release-relevance:** Post-release.

### Problem

Today a `RouteFn` returns at most one `processId`. An event that
logically affects N instances of the same PM type (e.g. `BatchApproved`
affecting every Order in the batch) cannot be expressed directly.

### Two paths

1. **SDK-level fan-out.** `RouteFn` may return `RouteResult[]`; the
   SDK parallelises (or serialises) dispatch across the named
   instances and reconciles acks before advancing the cursor.

2. **Modelling-level fan-out via event/PM composition.** A first PM
   sees `BatchApproved` and emits per-instance routing events
   (`OrderApprovalRequested` × N); a second PM consumes those and
   continues each `Order`. The fan-out happens in domain terms.

### Recommendation

**Path 2.** Reasons:

- Path 1 forces the SDK to choose a fan-out concurrency policy
  (sequential? parallel? bounded? what about ordering across the
  fanned-out events?). Every choice we bake in becomes an assumption
  applications can't override.
- Path 2 uses primitives the application already has and keeps the
  semantics in domain code where the modelling decisions belong.
- The cost is `N` extra rows in the event log per fan-out event,
  which is cheap relative to the clarity gain.
- Trade: performance/optimisation → expressivity/flexibility. The
  user explicitly chose this trade.

### What lands

- Worked-example walkthrough in `docs/concepts.md` (or a dedicated
  patterns doc) showing the fan-out shape.
- No SDK code change.
- A `maybe-later.md` entry **rejecting** SDK-level fan-out and
  pointing at the modelling pattern, so the question doesn't get
  re-opened.

---

## PM-B — Per-instance progress tracking

**Status:** Moved to [`subscriptions.md`](./subscriptions.md) SUB-A.

What started as a PM-specific question — "how does a PM type with
many active instances achieve per-instance progress so a stuck
instance doesn't stall the type and so throughput scales with
workers?" — turned out, on the §2.5 walkthrough, to be a question
about the shape of every subscription in the system. The full
three-design discussion (per-instance subscriptions / partitioned
cursors / decoupled router + work queue) and the generalisation to
projections lives in `subscriptions.md` SUB-A, where it belongs.

**PM-specific consequences that survive the move:**

- The hard constraint "the SDK MUST NOT silently skip an event for
  any instance" applies to whichever SUB-A design wins. Per-event
  per-instance skip via `instructedctl` with explicit state-loss
  acknowledgement remains the only escape.
- The PM `RouteFn` is the natural carrier of the partition key
  under SUB-A Design 3 — it returns `{ kind, processId }` and the
  `processId` *is* the partition key for the work-queue.
- `:start` / `:continue` / `:stop` directives all produce work
  items (one row each) under Design 3; `:ignore` produces no row.
  PM-A's modelling-level fan-out, if applied, generates one
  upstream event per intended target, which the routing layer
  enqueues as N separate work items, one per partition.

When SUB-A is decided, the PM-specific work to wire up is small:
adopt the chosen mechanism, map `RouteFn` results into work-item
inserts, and adjust PM-C's `apply`-driven state rebuild to read
from the work-queue history rather than the (now non-existent) PM
cursor.



---

## PM-C — `handle` / `apply` split

**Status:** Designed, awaiting confirmation.
**Release-relevance:** Pre-release.

### Problem

Today the PM signature is

```ts
handle(state, event) → { state, commands? }
```

which merges command-production with state-mutation. Consequence:
PM state cannot be rebuilt from events without re-dispatching
commands. If a snapshot is missing or rejected (SNAP-002 version
mismatch), the PM resumes with `initialState()` and loses
accumulated state — quietly. Aggregates don't have this problem
because their applier is pure-state.

### Proposal

Split the SDK callbacks:

```ts
// Pure: produce commands. May read state. May NOT mutate state.
handle(state, event) → DispatchedCommand[]

// Pure: fold one event into state. Never fails.
apply(state, event) → state
```

Worker flow becomes:

1. Load state — from snapshot if present and version matches;
   otherwise, replay routed events through `apply` from
   `start_from` to current and rebuild.
2. For each unseen routed event:
   - `commands = handle(state, event)`
   - dispatch each command
   - `state = apply(state, event)`
   - persist snapshot + advance subscription cursor in one tx.

### What this unlocks

- **PM state survives snapshot-version-mismatch.** Fall back to
  event replay via `apply`. State rebuilds cleanly; no
  re-dispatching, because only `apply` runs during the rebuild.
- Sharper mental model. `apply` is to PMs what the event applier is
  to aggregates.
- Cleaner test ergonomics — `apply` and `handle` test independently.

### Cost

- Two callbacks instead of one — slightly more ceremony per PM.
- Event replay during state rebuild is O(N) in number of routed
  events for that instance since origin. For long-running
  instances this matters; mitigation is the existing snapshot
  policy (the rebuild only runs when the snapshot is missing or
  version-mismatched, which should be rare).
- API change to existing PMs (the bank-account example PM and
  whatever lives in `examples/`).

### Recommendation

Do the split. Pre-release because it changes the PM contract
shape, and it removes a sharp edge from SNAP-002. Update SNAP-002
honest gap wording to reflect that the PM case is now no worse
than the aggregate case.

### Implementation notes

- `apply` runs against the *raw* event (post-upcast, if upcasting
  lands). It does *not* receive `metadata` parameters beyond what
  the event carries, mirroring the aggregate applier.
- On a route directive of `start`, `apply` runs from
  `initialState()` against the triggering event.
- On `stop`, `apply` runs against the triggering event before the
  snapshot is deleted — preserves the invariant that `source_version`
  always matches "state has folded events up to event_number =
  source_version" right up to the moment of deletion.
- Existing PM definitions need migration. Provide a one-page
  upgrade note; the bank-account example is the reference.

---

## PM-D — Per-instance stalling on poison events

**Status:** Merged into PM-B — see above.

Per-instance stalling is the same problem as per-instance throughput
scaling, expressed from a different angle: both reduce to "each
instance has its own progress". Whichever design wins for PM-B
(per-instance subscriptions, or partitioned cursors with
`partition_by: process_uuid`), per-instance stalling falls out
automatically.

Hard constraint carried into PM-B: **the SDK MUST NOT silently skip
an event for any instance.** A stuck instance stalls in isolation;
the operator chooses what to do via `instructedctl`, with explicit
per-event per-instance acknowledgement of any state loss.

---

## PM-E — Multi-command per routed event: idempotency

**Status:** Designed, awaiting confirmation.
**Release-relevance:** Pre-release.

### What the user decided

- Parallel commands are simply **allowed**. No opt-in, no `parallel`
  marker, no signature gymnastics. `handle` returns 0..N commands
  and the SDK dispatches them in order.
- Idempotency on redelivery is required (the SDK can't ship a
  primitive that produces duplicate commands on a normal crash).
- The user asked whether PM-side dedup (the PM checks its state
  before dispatching) could be the idempotency mechanism instead
  of deterministic event IDs at the aggregate boundary.

### Why PM-side dedup alone cannot be the idempotency mechanism

The dispatch path and the persist-and-ack path run on **different
sessions** (D-0011 lock-set disjointness). The worker flow is:

```
1. handle → commands
2. dispatch each command          ← session A, commits as it goes
3. apply event → new state
4. snapshot + advance subscription ← session B, one short tx
```

Crash between (2) and (4): dispatched commands are durable; the
snapshot is not. On redelivery, the PM reads the *old* snapshot,
runs `handle` again, and its state does not record that the
previous dispatch happened (because the snapshot recording that
fact never committed). Re-dispatch produces a duplicate at the
aggregate.

This cannot be fixed PM-side. A two-phase commit ("intent"
snapshot before dispatch, "completed" snapshot after) has the same
crash window between intent-write and dispatch — on recovery we
still don't know whether the dispatch reached the DB, and
re-dispatching is still required, and still produces duplicates
without some other idempotency token.

The durable layer (the aggregate's append boundary) is the only
place idempotency can robustly live. Hence deterministic event
IDs.

### Deterministic event IDs + `IS004`

- The PM dispatch path passes a seed into `runCommand`:
  `(triggering_event.event_id, command_index)`.
- `runCommand` derives `event_id` deterministically from
  `(seed, event_index_within_command)` for any event the aggregate
  produces whose `event_id` is not explicitly set.
- On redelivery, re-dispatch re-evaluates the aggregate handler
  against current state. The handler may produce the same events,
  different events, or no events. In any case, the SDK presents the
  append with the same deterministic event IDs.
- The append is atomic. `IS004 duplicate_event` on the first event
  aborts the whole append, leaving the stream as the first attempt
  recorded it. The SDK treats `IS004` on the PM-dispatch path as a
  successful no-op.
- If the first attempt never committed, the redelivery's append
  succeeds fresh — also correct.

Append atomicity is what makes this robust against the aggregate
producing different events on retry (different state under it).
First event collides → whole append aborts → stream reflects only
the attempt that actually committed.

### Where PM-side dedup fits

PM-side dedup remains useful as a **pure optimisation**:

- If the PM tracks dispatched commands in its state, the
  optimisation skips the aggregate handler invocation entirely for
  the redelivery case (where deterministic IDs would silently
  absorb the redundant work, the PM-side check skips the work).
- For expensive aggregate handlers this is real cost saving.
- For trivial aggregates it adds bookkeeping for no benefit.
- The PM author writes whatever they want; the SDK doesn't fight
  them. Deterministic IDs are the durable safety net underneath.

The SDK does **not** model PM-side dedup as a first-class feature.
It's idiomatic application code, not a framework concern.

### What lands

1. **`handle` returns 0..N commands.** No opt-in keyword. The
   existing TypeScript shape
   `{ state, commands?: DispatchedCommand[] }` already supports
   this; tighten the docs to say multi-command emissions are
   normal and the commands run in declaration order.

2. **SDK-minted deterministic event IDs for PM-dispatched
   commands.** Always-on. The PM worker's `runCommand` invocation
   threads a seed through; the aggregate path uses it to fill any
   omitted `event_id`. Applications that explicitly set
   `event_id` in the aggregate handler opt out and take
   responsibility for their own idempotency.

3. **SDK treats `IS004` on the PM dispatch path as silent
   success.** The PM dispatch path needs to distinguish this case
   from the application-code-path case (where `IS004` is a real
   error to surface to the caller). Concretely: `runCommand` gains
   an internal flag or context marker that the PM worker sets,
   and the SDK swallows `IS004` only when that flag is set.

4. **New invariant.** Next free `PM-###` documents the contract:

   > **PM-###** — The SDK MUST mint deterministic event IDs for
   > events produced by PM-dispatched commands when the aggregate
   > handler omits `event_id`. The seed MUST be deterministic in
   > `(triggering_event.event_id, command_index_in_handle_output,
   > event_index_in_aggregate_output)`. On redelivery of a routed
   > event, re-dispatch MUST be idempotent under at-least-once
   > delivery; `IS004 duplicate_event` returned from an aggregate
   > append in the PM dispatch path MUST be treated as silent
   > success.

5. **Honest gaps acquires one entry:** applications that override
   `event_id` in aggregate handlers opt out of SDK-provided
   idempotency for PM-triggered commands and must provide their
   own.

6. **Worked example in docs.** The transfer saga done both ways:
   sequenced (one command per step, wait for resulting event) as
   the typical shape; parallel commands (audit + notify) as the
   independent-commands shape. Both work; the modelling choice is
   the application's.

---

## Cross-cutting: ordering across PM-A, PM-B, PM-C, PM-E

(PM-D is merged into PM-B.)

Suggested pre-release sequencing:

1. **PM-A**, **PM-C**, **PM-E** proceed in parallel — none
   depends on PM-B's design choice. Each lands when ready.
2. **PM-B** is parked for a dedicated design pass. Once chosen,
   it likely subsumes some PM-C and PM-E plumbing (e.g. PM-E's
   `IS004`-silent-absorption flag may want to live in whichever
   layer Design 3's instance worker runs in).

---

## Open questions consolidated

The implementation session needs decisions on these before starting:

1. **PM-B**: parked. Next pass needs to pick between three designs
   (per-instance subscriptions / partitioned cursors / decoupled
   router + work queue), with SQL hot-path sketches and operator
   surface sketches for each.
2. **PM-B operator escape**: confirm "explicit per-event
   per-instance skip, with state-loss acknowledgement; no
   automatic-skip option at all" is the intended shape regardless
   of which design wins. (Currently captured as a hard
   constraint.)
3. **PM-A**: confirm SDK-level fan-out is rejected and the
   `maybe-later` entry records the rejection.
4. **PM-C**: confirm migration of existing PMs (the bank-account
   example) to the split shape as part of the change.
