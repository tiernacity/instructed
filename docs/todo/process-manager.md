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
  under SUB-A Design 3 — see PM-F for the simplified shape
  `RouteFn → { partitionKey } | "ignore"`. The `partitionKey`
  (for PMs, the instance uuid) keys the work queue.
- Under Design 3: a returned `partitionKey` produces one work
  item; `"ignore"` produces none. There is no `:start` /
  `:continue` / `:stop` directive set; instance lifecycle is the
  processing layer's concern via `handle`'s `complete?` return
  (PM-F). PM-A's modelling-level fan-out, if applied, generates
  one upstream event per intended target, which the routing layer
  enqueues as N separate work items, one per partition.

When SUB-A is decided, the PM-specific work to wire up is small:
adopt the chosen mechanism, map `RouteFn` results into work-item
inserts, and adjust PM-C's `apply`-driven state rebuild to read
from the work-queue history rather than the (now non-existent) PM
cursor.



---

## PM-C — `handle` / `apply` split

**Status:** Decided. Required for SUB-A: the per-PM work-item
retention contract ("keep work-items until `complete: true` so
state is reconstructible on any snapshot-version-mismatch")
is only meaningful if `apply` exists as a pure state-fold
separate from `handle`'s command dispatch. PM-C and PM-F land
together; SUB-A's PM path depends on both.
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
- **Snapshotting becomes mechanically symmetric with
  aggregates** (same primitive, same load-with-snapshot path,
  same load-without-snapshot path, same losslessness property).
  SNAP-002's honest-gap wording can drop the PM-specific
  caveat: snapshot-version-mismatch triggers full event replay,
  identically for aggregates and PMs.

### Residual asymmetries with aggregate snapshotting

Two asymmetries remain after PM-C because they reflect what a
PM intrinsically *is*, not how it's wired:

**1. Replay source.** An aggregate replays its own stream from
`stream_events` — rows we never remove. A PM (under SUB-A
Design 3) replays the events it was *routed*, identified via
the `subscription_work_items` table — rows whose lifecycle is
tied to the PM instance, not to age.

The per-kind retention contract (SUB-A "Work-item lifecycle by
subscription kind") is:

- **PMs**: `done` work-items for a partition are retained
  until that PM signals `complete: true`. On `complete: true`,
  the framework DELETEs the snapshot and every work-item for
  the partition (including the triggering one) in one
  transaction. Until then, those rows form the durable record
  needed to rebuild state losslessly via `apply` on any
  snapshot-version-mismatch.
- **Aggregates**: no analogue; the event log is never cleaned.

The contract is now symmetric in spirit: state is always
reconstructible until the application explicitly signals it's
done with the instance. The asymmetry is in *what* gets
replayed (own stream vs. routed events identified via
work-items), not in whether replay is possible.

Consequence: a long-running PM accumulates one work-item row
per routed event for the life of the instance. Storage cost is
real but bounded by routing rate. Levers for applications that
find it expensive: (i) more aggressive `complete: true` on
terminal sub-phases, (ii) sharded partition_keys so individual
instances stay bounded, (iii) accept the cost. The alternative
(silent unrecoverability on `apply` evolution) is worse.

Snapshot cadence and work-item retention are independent: a
snapshot does not release work-items; the only releaser is
`complete: true`. Cadence is back to being purely a
performance knob.

**2. Terminality.** An aggregate's full reality is derivable
from its events alone via `apply`. A PM under PM-F has a
side-channel signal — `handle` returning `{ complete: true }` —
that the runtime acts on (deletes the snapshot and the
partition's work-items) but that is not automatically reflected
in `state`.

**Idiom (recommended).** Encode terminality in state too:
`apply` sets e.g. `state.terminated = true` on the same event
that causes `handle` to return `complete: true`. Replay then
re-arrives at the terminal status without runtime help. This
is the aggregate-shaped discipline ("if it matters for state,
put it in `apply`") and should be documented as the canonical
pattern in the upgrade note.

**Mechanism (fallback).** If applications find the idiom
awkward, the snapshot row can carry a `terminal_at_event_number`
field restored alongside `state` on load. Costs a column;
eliminates the discipline requirement. Defer until a real
example trips on the idiom.

### Cost

- Two callbacks instead of one — slightly more ceremony per PM.
- Event replay during state rebuild is O(N) in number of routed
  events for that instance since origin. For long-running
  instances this matters; mitigation is the existing snapshot
  policy (the rebuild only runs when the snapshot is missing or
  version-mismatched, which should be rare).
- API change to existing PMs (the bank-account example PM and
  whatever lives in `examples/`).
- Storage of work-items for long-running PMs (see "Residual
  asymmetries" below). One small row per routed event, kept
  for the life of the instance. Bounded by routing rate; the
  cost of the lossless-rebuild contract.

### Recommendation

Do the split. Pre-release because it changes the PM contract
shape, and it removes a sharp edge from SNAP-002. Update SNAP-002
honest gap wording to reflect that the PM case is now no worse
than the aggregate case.

### Implementation notes

- `apply` runs against the *raw* event (post-upcast, if upcasting
  lands). It does *not* receive `metadata` parameters beyond what
  the event carries, mirroring the aggregate applier.
- On first routed event for a partition (no snapshot present),
  state starts at `initialState()` and `apply` folds the
  triggering event onto it before `handle` runs.
- On `handle` returning `{ complete: true }`, the snapshot is
  DELETEd and every work-item for the partition is DELETEd in
  one tx (PM-F). `apply` need not run on the triggering event
  in this terminal case because no post-completion state is
  retained — unless the application relies on `apply`-encoded
  terminality being visible to `handle` itself (in which case
  `apply` runs first, `handle` sees the terminal state, returns
  `{ complete: true }`, and the framework discards).
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

## PM-F — Simplified PM surface: routing key only; lifecycle in handler

**Status:** Decided.
**Release-relevance:** Pre-release.

### Problem

The current PM directive set inherited from Commanded —
`{:start, :start!, :continue, :continue!, :stop, false}` returned
by `interested?` — bundles four orthogonal concerns at the
routing layer:

1. *Which instance is this event for?* (`partition_key`)
2. *Is this event for this PM type at all?* (`ignore`)
3. *Strict assertion: is this the first event for this instance?*
   (`:start!` / `:continue!`)
4. *Lifecycle: this event terminates the instance.* (`:stop`)

Concerns (1) and (2) are intrinsic to routing — they decide
whether and where to do work. Concerns (3) and (4) are
application logic about instance state, dressed up as framework
directives because Commanded's in-process `GenServer` runtime had
convenient access to both at the routing layer. We don't have
processes; we have workers reading rows. The convenience
disappears.

### Proposal

Reduce the routing surface to the minimum that's actually
routing-shaped:

```ts
type PartitionKey = string
RouteFn(event, metadata) → { partitionKey: PartitionKey } | "ignore"
```

Lift the rest into the processing-layer callbacks (mirroring
PM-C):

```ts
apply(state, event) → state                              // pure fold
handle(state, event) → {
  commands?: DispatchedCommand[],
  complete?: boolean,                                    // delete snapshot + work-items
}
```

This is the aggregate shape: the framework decides which row to
load; the application decides what to do with it.

### How each Commanded directive collapses

| Commanded                | Where it lives now                                              |
| ------------------------ | --------------------------------------------------------------- |
| `{:start, uuid}`         | `RouteFn → { partitionKey: uuid }`; processor loads/initialises |
| `{:continue, uuid}`      | same — no distinction at routing                                |
| `{:start!, uuid}`        | `handle` raises if `state !== initialState()`                   |
| `{:continue!, uuid}`     | `handle` raises if `state === initialState()`                   |
| `{:stop, uuid}`          | `handle` returns `{ complete: true }`                           |
| `false`                  | `RouteFn → "ignore"`                                            |

Routing becomes purely "is this event for this PM type, and if
so, which instance?" — by construction stateless w.r.t. instance
state, which matches the constraint that emerged from §2.5 and
matches Commanded's own `interested?/2` signature (which has no
state parameter).

### Interaction with PM-A

A trivial widening — `RouteFn → PartitionKey[] | "ignore"` —
would address PM-A (fan-out to N instances per event) at the
routing layer. PM-A is currently decided in favour of
modelling-level fan-out (one PM emits per-instance events that a
second PM consumes), explicitly trading performance for
expressivity. We keep the singular return for now to honour that
decision. Singular → list is backwards-compatible; if PM-A is
ever re-opened, the API can stretch without breaking existing
PMs.

### Interaction with PM-B (SUB-A)

PM-B's "consequences that survive the move" subsection above is
updated for this shape: `RouteFn` returns `{ partitionKey } |
"ignore"` with no directive enum; under SUB-A Design 3, a
returned `partitionKey` produces one work item, `"ignore"`
produces none, and the `complete: true` flag from `handle`
causes the snapshot row AND every work-item for the partition
(including the triggering one) to be DELETEd in one
transaction (see "Decided: delete on complete" below).

Future events to a `complete`-d partition route as normal and
run from `initialState()`. Applications that need "permanently
terminated" semantics encode it in their own state (in `apply`,
so it survives replay) or shape `RouteFn` to stop matching
once the upstream events that mark terminal have passed.

### Interaction with PM-C

PM-C splits today's `handle` into `apply` (pure state fold) and
`handle` (commands only). PM-F widens PM-C's `handle` return
from `DispatchedCommand[]` to `{ commands?, complete? }`. The
two changes share the callback surface and land together.

### Cost

- Applications that relied on framework-level strict checks
  (`:start!` / `:continue!`) write three lines of guard code in
  `handle`. The bank-account example doesn't use them; an audit
  of remaining examples is part of landing this.
- Framework-level `:stop` semantics move to processing time. Was
  already unavoidable under any SUB-A design (routing can't see
  state to decide "this event terminates").

### What lands

1. Updated PM callback types in the TS SDK (`RouteFn`, `apply`,
   `handle` per the shapes above).
2. Audit of existing PMs in `examples/` (only the bank-account
   PM today) and a migration of each to the new shape, plus a
   one-page upgrade note for external consumers.
3. PM-B's consequences subsection in this file revised to drop
   directive references (done in this edit).
4. PM-C's `handle` signature updated to include `complete?` (do
   not land PM-C without PM-F or vice versa — they share the
   callback surface).
5. On `complete: true`, the processing worker's terminal-tx
   DELETEs the snapshot AND every work-item for the partition
   (including the triggering one). One transaction.
   Non-terminal success path is UPDATE the triggering item to
   `done` and UPSERT the snapshot.
6. The reasoning above captured in `docs/decisions.md` (one
   paragraph: "we don't inherit Commanded's directive set
   because we don't inherit Commanded's runtime").

### Decided: delete on complete

On `handle` returning `{ complete: true }`, the processing
worker DELETEs the snapshot row AND DELETEs every work-item
for the partition (including the triggering one), all in one
transaction. No tombstone, no terminal-state flag in the
framework layer; no `done` row left behind.

Rationale:

- Same as aggregates: whether an aggregate/PM can be "reopened"
  is an application-level concern. The framework doesn't model
  it.
- In practice, completed processes are discarded; the event
  log is the system of record. If an application wants a
  completed PM to linger (audit, late-arriving events), that's
  application logic.
- The `complete: true` signal is therefore best understood as
  "I no longer need this state; clean up the storage row."
  It's a row-lifecycle hint, not a domain-lifecycle hint.
- Soft-delete, re-open, and "reject events for terminal
  partitions" are application concerns. The natural place is
  `apply` encoding a terminal flag (per the
  `apply`-encodes-terminality idiom in PM-C); a terminal
  partition's future events run `handle` against state with
  `terminated: true` and the application chooses what to do
  (no-op, raise, audit, re-open).

Consequence for the per-kind retention contract (see SUB-A
"Work-item lifecycle by subscription kind"): `complete: true`
is the **only** mechanism that releases a PM partition's
work-items. There is no age-based retention for PM work-items.
The contract: while you have an active PM instance we keep
enough to reconstruct it from origin via `apply`; when you say
you're done, we discard everything.

---

## Cross-cutting: ordering across PM-A, PM-B, PM-C, PM-E, PM-F

(PM-D is merged into PM-B, which is itself resolved into SUB-A.)

Suggested pre-release sequencing:

1. **PM-C** + **PM-F** land together — they share the callback
   surface (`apply` split + `handle` return widened with
   `complete?`) and are both prerequisites for the PM path of
   SUB-A.
2. **PM-B** is the application of SUB-A's proposed design to
   the PM case. Implementation comes with SUB-A; the contract
   is already captured in SUB-A's "PM-B constraints" table.
3. **PM-E** lands with or after PM-C/PM-F; the
   `IS004`-silent-absorption flag lives in the PM processing
   worker (the layer that calls `runCommand` on dispatched
   commands).
4. **PM-A** is post-release; no dependency on the others.

---

## Open questions consolidated

Status after the SUB-A / PM-F / PM-C / lifecycle-contract
close-out:

1. ~~**PM-B**: parked.~~ **Resolved**: SUB-A Design 3
   (decoupled router + work queue) chosen; PM-B's per-instance
   progress falls out of `partition_by = process_uuid`. See
   SUB-A's "Proposed design" + "PM-B constraints — how
   they're satisfied" tables.
2. ~~**PM-B operator escape**: confirm "explicit per-event
   per-instance skip ...".~~ **Confirmed** as part of the
   PM-B constraints table in SUB-A; realised as the
   `skip_work_item_with_audit` procedure.
3. **PM-A**: confirm SDK-level fan-out is rejected and the
   `maybe-later` entry records the rejection. (Still open;
   post-release.)
4. ~~**PM-C**: confirm migration of existing PMs.~~ **Decided**
   as part of PM-C status; bank-account migration is a
   land-item.
5. ~~**PM-F**: confirm the simplification.~~ **Decided**: PM-F
   adopted; Commanded directive set not inherited; on
   `complete: true`, snapshot AND every work-item for the
   partition DELETEd in one tx.

The one remaining open question for the PM file is PM-A's
fan-out rejection, which is post-release.
