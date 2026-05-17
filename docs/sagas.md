# Sagas / Process Managers — Phase 6

The purpose of this document is to settle the one open mapping
entry left after Phase 4: **PM-030 (compensation)**. D-0001 already
committed `instructed` to treating compensation as a first-class
concern; this phase decides the mechanism.

The work is done in passes:

| Pass | Scope | Status |
|------|-------|--------|
| 1 | Survey absurd's task/step model; lay out the three candidate shapes concretely | done |
| 2 | Take a position; record the decision | done |
| 3 | Implications for schema and SDK loop; Phase 7 inputs | **this commit** |

Pass 1 deliberately does not pick a winner. It surveys what each
candidate actually looks like, so Pass 2's choice is grounded.

---

## Background

CQRS/ES sagas are multi-aggregate, multi-step workflows that often
need to undo partially-completed work when a later step fails. The
canonical example: book a hotel; book a flight; if the flight
booking fails permanently, cancel the hotel.

Commanded's position on this (PM-030 in `guarantees.md`):
*compensation is not a first-class concept*. The process manager
just dispatches whatever commands it dispatches; if those commands
happen to be `CancelHotelBooking`, the saga compensates, but the
framework has no notion of a "compensating step" paired with a
"forward step". The PM is purely reactive.

D-0001 in `decisions.md` recorded our intent to do better than that
— while explicitly ruling out the option of "punt to absurd, the
application's problem entirely". The three remaining candidates,
from the ROADMAP entry for this phase:

1. **Punt to absurd.** Ruled out by D-0001.
2. **First-class saga abstraction inside `instructed`** — explicit
   forward-step / compensation-step pairing, with the framework
   tracking which steps have completed and walking compensations
   in reverse on failure.
3. **Process managers stay pure** (events in → commands out +
   state); side-effecting workflows are delegated to absurd by
   spawning tasks, and the bridge back is an event the PM
   consumes.

Pass 1 surveys absurd to ground (2) vs (3), then describes each
candidate in concrete terms.

---

## What absurd actually provides

(Surveyed from `absurd/docs/concepts.md`, `absurd/sql/absurd.sql`
function list, `absurd/AGENTS.md`.)

Absurd is a Postgres-native durable-execution / workflow system.
Its primitives are:

- **Task** — top-level unit of work. Named handler, JSON
  parameters, queued. Spawn-time idempotency keys deduplicate.
- **Step** — a named checkpoint *inside* a task handler. The
  first successful execution of a step persists its return value
  in a `c_*` table; on any subsequent run of the task the step
  is replayed by returning the cached value, not by re-executing
  the function body.
- **Run** — one execution attempt of a task. Retries create a new
  run that shares the same checkpoints.
- **Lease** — workers claim tasks with `claim_task`; the claim is
  extended on every checkpoint write. Lease loss means another
  worker takes over (so brief overlapping execution is possible).
- **Events** — `await_event(name)` suspends the task until
  `emit_event(name, payload)` is called elsewhere. Payloads are
  immutable: the first emit wins. Optional timeout on await.
- **Sleep** — suspend until a duration or absolute time, then
  resume.
- **Retries** — at task level, with `fixed` / `exponential` /
  `none` strategies, configurable `maxAttempts` and `maxDuration`.

The crucial property for this phase: **checkpoints are
forward-only**. A successfully-completed step's result is cached;
the step is *not re-executed*. There is no
"un-complete-this-checkpoint" call, no `compensate` primitive, no
"walk checkpoints in reverse" capability. Compensation, expressed
in absurd, is just "more application code in the task handler that
runs after the failing step".

In other words: absurd's contribution to a durable workflow is
forward progress with persistent checkpoints. Backward progress is
the application's problem to express.

This is the precise gap D-0001 refused to leave to the
application.

---

## The three candidates, concretely

### Candidate 1 — Punt to absurd

Ruled out by D-0001. Recorded here only for symmetry: it would mean
`instructed` says "PMs are reactive; if your domain needs
compensation, build a saga out of absurd tasks". The forward/
compensate pairing would live entirely in application-written
absurd task handlers, with no support from `instructed`. The
problem (per D-0001): we lose the most distinctive idiom of
CQRS/ES sagas, and we silently delegate to a system whose
checkpointing model is the wrong shape for this job (forward-only).

### Candidate 2 — First-class saga abstraction inside `instructed`

Shape: `instructed` grows a new primitive alongside aggregates,
handlers, and PMs. A saga is a named, ordered list of *step pairs*:

```pseudo
saga BookTrip(params) {
  step bookHotel
    forward     = dispatch BookHotel(params.hotelId)
    compensate  = dispatch CancelHotelBooking(params.hotelId)

  step bookFlight
    forward     = dispatch BookFlight(params.flightId)
    compensate  = dispatch CancelFlightBooking(params.flightId)

  step chargeCard
    forward     = call stripe.charge(params.cardToken)
    compensate  = call stripe.refund(...)
}
```

The framework owns a `sagas` (and `saga_steps`) table family
tracking which steps have completed for each saga instance. The
engine executes steps in order; on a permanent failure at step N,
it walks steps N-1..1 invoking their `compensate` handlers in
reverse, then marks the saga as `compensated` (or `failed` if a
compensation itself fails).

Required schema additions (sketch — full design only if this
candidate wins):

- `saga_instances (id, saga_name, params jsonb, status, current_step, …)`.
- `saga_step_log (saga_id, step_name, direction, status, result, claimed_by, …)`.
- A `claim_saga_step` / `complete_saga_step` / `fail_saga_step`
  procedure family analogous to absurd's `claim_task` /
  `complete_run`.
- Likely a lease story for saga workers, in the same shape as
  D-0006 for subscriptions.

SDK surface: a saga DSL (or builder API) listing the step pairs;
a worker loop that pulls saga instances needing forward or
backward work and runs their handlers.

What this buys: the forward/compensate pairing is explicit and
inspectable; the framework can guarantee compensations run in
reverse order; tooling can show "where is this saga right now".

What this costs:

- A second workflow abstraction next to PMs. PMs already give us
  *event-driven, state-carrying, command-dispatching* workflows;
  sagas give us *linear, step-list, paired-compensation*
  workflows. Two ways to do similar things — application authors
  must choose, and choose right.
- Linear shape doesn't fit reactive workflows. "Book hotel and
  flight in parallel; on either failure, cancel the other; on
  both success, charge card" is natural as a PM (events fan in
  and out), awkward as a step list.
- The "step that calls Stripe" inside a saga DSL is really an
  absurd-task in disguise — durable execution of an external
  call with retries and a checkpoint. Implementing this inside
  `instructed` would either reinvent absurd's task/step layer or
  delegate to it (which then makes the saga DSL a thin wrapper
  over absurd plus a compensation table).
- New schema tables, new procedures, new lock-ordering rules in
  Phase 7. Visible weight.

### Candidate 3 — PMs pure; side effects delegated to absurd

Shape: PMs in `instructed` remain exactly what Phase-4 Pass-3
mapped — events in → commands out + state. *Compensation is
expressed in PM handler code* as command dispatches in response
to failure events. The PM's `handle/3` clauses simultaneously
encode forward and backward transitions:

```pseudo
process_manager BookTrip {
  handle OrderPlaced(orderId, hotelId, flightId, cardToken) =
    state := { phase: :booking_hotel, ... }
    dispatch BookHotel(hotelId, sagaId: orderId)

  handle HotelBooked(orderId, _) when state.phase == :booking_hotel =
    state := { ...state, phase: :booking_flight, hotelBooked: true }
    dispatch BookFlight(flightId, sagaId: orderId)

  handle FlightBookingFailed(orderId, _) when state.hotelBooked =
    state := { ...state, phase: :compensating }
    dispatch CancelHotelBooking(hotelId, sagaId: orderId, reason: :flight_failed)

  handle HotelBookingCancelled(orderId, _) when state.phase == :compensating =
    state := { ...state, phase: :compensated }
    -- PM stops itself: returns {:stop, processUuid} on the next interested? call
}
```

Side-effecting steps (Stripe charge, email send) are not
expressed inside the PM. The PM dispatches a `RequestPayment`
command which writes a `PaymentRequested` event; an absurd task
subscribes (logically: an event handler) to that event, runs the
durable Stripe call with checkpoints, and on completion emits a
`PaymentProcessed` or `PaymentFailed` event back into the event
store via `append_to_stream`. The PM consumes that returning
event like any other.

This is, in CQRS/ES terms, the existing model: aggregates own
their state machines; PMs orchestrate across aggregates by
reacting to events and dispatching commands. The compensating
command is just another command.

What this buys:

- No new abstraction. The forward/compensate pairing is implicit
  in the PM's handler clauses (one clause emits the forward
  command; another clause, listening for the failure event, emits
  the compensating command). This is the *Commanded* model — but
  with D-0008's co-transactional persist-and-ack making the PM's
  own recovery semantics tighter than Commanded's reference
  adapter manages.
- Reactive workflows fit naturally (events fan in and out; PMs
  are state machines, not step lists).
- The boundary with absurd is principled: the event store is the
  integration surface. Absurd tasks subscribe to events that
  request side effects and emit events that report results. PMs
  never call absurd directly; they don't need to know absurd
  exists.
- No new schema tables. PM state is already snapshots (PM-020).
  Compensating commands go through the same `append_to_stream`
  as forward commands. Subscriptions deliver events both ways.

What this costs (and how it differs from Commanded's
"compensation isn't first-class"):

- The forward/compensate *pairing* is not surfaced anywhere as a
  data structure. A reader of the PM module has to recognise
  which `handle/3` clauses are "the compensation path". This is
  the cost Commanded pays too.
- Tooling cannot say "saga is in step 3 of 5"; it can only show
  the PM's state field, which is whatever the application chose
  to put there (e.g. a `phase` enum).
- The honesty about "compensation is just a command" places the
  weight on the application's modelling discipline: failure
  events must be designed to exist (`FlightBookingFailed` is a
  real event in the system, not an exception) and the PM must
  handle them.

### Why candidate 3 still satisfies D-0001

D-0001 ruled out "punt to absurd; compensation is the application's
problem entirely". Candidate 3 is not that. Candidate 3 says:

- Compensation **is** first-class — because it is *just a command*,
  and `instructed` provides every primitive needed to dispatch
  commands durably, in order, with at-least-once delivery, with
  state recovery on crash, and (per D-0008) with persist-and-ack
  atomicity that Commanded doesn't get out of its reference
  adapter.
- The application does not invent its own durability, ordering,
  or recovery machinery to make compensation work. It writes the
  PM clauses. The framework runs them.
- The integration with absurd, when side effects are needed, is
  a known pattern with a clear protocol (request event → task
  → result event), not "do whatever".

The contrast with D-0001's punt: the punt would have said "for
sagas, use absurd; instructed doesn't help". Candidate 3 says
"sagas live in instructed PMs, with absurd as the side-effect
executor when needed". The PMs *are* the saga support.

---

---

## Pass 2 — Position

**Decision: candidate 3.** Process managers in `instructed` stay
pure (events in → commands out + state). Compensation is a
first-class concern in the sense that D-0001 demands, because the
PM contract — durable state, ordered at-least-once event
delivery, transactional persist-and-ack (D-0008), and a `dispatch`
helper that runs the full load-execute-append cycle — gives the
application every primitive it needs to express
compensating-command flows without inventing its own durability,
ordering, or recovery. Side-effecting workflows are delegated to
absurd tasks, with the event store as the integration surface in
both directions.

Recorded as **D-0011** in `decisions.md`.

### Why not candidate 2

Candidate 2 — a first-class saga DSL inside `instructed` with
explicit step-pair tracking — was tempting. It would have produced
a nicer story for tooling (
"saga is at step 3 of 5; here is
which steps have committed and which compensations have run").
It was rejected for four reasons, in descending weight:

1. **It duplicates the PM model.** A PM is already an
   event-driven, state-carrying, command-dispatching workflow.
   A saga DSL adds a second, structurally similar abstraction.
   Application authors would have to choose between PM and saga
   for every workflow, and most would choose wrong at least
   once. Two abstractions with overlapping semantic ranges is
   the kind of bloat we set out to avoid.
2. **Linear step lists don't fit reactive workflows.** Real
   CQRS/ES sagas fan in and out: "book hotel and flight in
   parallel; charge card when both confirm; cancel the other if
   either fails" is natural as a state machine, awkward as a
   forward/compensate step list. A DSL would either pretend
   linearity or grow combinators (parallel, alternative, retry,
   compensate-conditionally) until it became a state-machine
   notation with extra ceremony.
3. **The side-effect step inside the DSL is absurd in disguise.**
   A saga step that calls Stripe is a durable external call with
   retries and a checkpoint. That is precisely what absurd
   provides. Either we reinvent it inside `instructed` (NIH) or
   we delegate to absurd (in which case the saga DSL is a thin
   wrapper plus a compensation table — most of the weight is in
   the existing absurd integration, not in the new instructed
   abstraction).
4. **Schema and lock-ordering weight.** Candidate 2 needs new
   tables (`saga_instances`, `saga_step_log`), new procedures
   (`claim_saga_step`, `complete_saga_step`, `fail_saga_step`),
   a saga-worker lease story, and a place in the Phase 7 lock
   ordering. Candidate 3 needs none of these — PM state is
   already snapshots, PM consumption is already a subscription,
   PM dispatch is already `append_to_stream`.

### What the position commits us to

- **PMs are the saga primitive.** There is no separate `saga`
  abstraction. The PM contract (PM-001..024, 031 in
  `mapping.md`) is the saga contract.
- **Compensation is a command.** Compensating actions are
  modelled as commands the PM dispatches in response to failure
  events. There is no `compensate` keyword, no reverse-walk
  engine, no paired-step data structure. The forward/compensate
  pairing lives in the PM module's `handle/3` clauses (one
  clause emits the forward command on the success-side event;
  another emits the compensating command on the failure-side
  event).
- **Failure events must exist as first-class domain events.**
  This is the modelling discipline candidate 3 imposes. The PM
  cannot compensate in response to an exception that wasn't
  raised; it can only react to events that were appended. Every
  aggregate command that can fail "permanently" in a way the
  saga needs to know about must produce a failure event
  (`FlightBookingFailed`, `PaymentDeclined`, ...) rather than
  silently returning `{:error, ...}` to its caller. The PM
  subscribes to those events the same way it subscribes to
  success events.
- **Side-effecting steps are absurd tasks bridged through
  events.** When a PM needs work that isn't expressible as a
  command-on-an-aggregate (charge a card, send an email, call
  an external API), the pattern is: dispatch a command that
  produces a `XRequested` event; an absurd task subscribes
  (logically, as an instructed event-handler-shaped consumer)
  to that event, runs the durable side effect with absurd's
  checkpoint/retry machinery, and on completion or permanent
  failure calls `append_to_stream` to emit a `XCompleted` or
  `XFailed` event back into the event store. The PM consumes
  the result like any other event. Neither system reaches
  across to the other directly.
- **Cross-boundary idempotency is the absurd task's job.** When
  an absurd task emits a returning event after retries, it MUST
  ensure the event is appended at most once. The integration
  pattern relies on caller-supplied `event_id` (already part of
  the contract per INV-APPEND-001) and the `:duplicate_event`
  error (INV-APPEND-030): the task derives `event_id`
  deterministically from its `task_id` and the step name, so a
  re-run that re-emits gets a clean duplicate-event signal
  instead of inserting a second copy. This is captured as a
  forward-pointing constraint in the Phase 7 inputs (Pass 3).

### Open question deliberately *not* surfaced

A tempting question: "do we need a registry / table of
`pm_instance ↔ spawned_absurd_task` so we can answer 'is this
saga waiting on a task right now?' for tooling?" The answer for
v1 is no — the PM's snapshot data is where that linkage lives,
and the application chooses its own shape for it. Surfacing it as
a first-class table would be the start of building candidate 2 by
accident. Recorded here so a later phase can deliberately revisit
rather than drift into it.

---

## Pass 3 — Schema and SDK implications

### Schema

No new tables. The full saga primitive set in `instructed` is the
union of tables Phase 4 has already reserved:

| Need | Table from Phase 4 | Used as |
|------|--------------------|---------|
| PM instance state | `snapshots` (PM-020..024) | One row per PM instance, keyed `"<pm_name>-<process_uuid>"`. The serialised `data` carries whatever state the application chose to model (`phase` enum, intermediate booking IDs, references to in-flight absurd tasks, ...). |
| Saga's view of the world | `events` (Part B/C of `invariants.md`) | The PM's subscription reads forward events *and* failure events from here. "Failure event" is not a structural concept; it is just an event whose semantics the application designed for. |
| Saga's command emissions | `events` again | Compensating and forward commands both dispatch through `append_to_stream` exactly the same way. |
| Saga's event consumption | `subscriptions` (INV-SUB-P-*) | One persistent subscription per PM type, leased per D-0006. Cursor advances co-transactionally with the PM's state snapshot per D-0008. |
| Linkage to absurd tasks | (none) | Carried inside the PM's snapshot `data` if the application wants the linkage durable. No first-class instructed table records `pm_instance ↔ absurd_task` (see Pass 2's "deliberately not surfaced" note). |

The Pass-1 candidate-2 sketch — `saga_instances`, `saga_step_log`,
`claim_saga_step` family — is discarded wholesale. None of those
artifacts will appear in the Phase 7 schema.

### SDK loop

The PM worker loop is exactly what Pass-3 of `mapping.md` already
specified under PM-011:

1. Read an event from the PM's subscription via
   `read_subscription_batch`.
2. Look up the instance: call `interested?` on the event; if it
   produces `{:start, uuid}` or `{:continue, uuid}`, load the
   instance state via `read_snapshot("<pm_name>-<uuid>")`.
3. Call `handle(state, event, metadata)`; collect the returned
   command list. **Compensating commands are returned here in
   the same way as forward commands** — the only difference is
   *which* clause produced them (the clause matching a failure
   event vs. one matching a success event).
4. Dispatch each command through the dispatch helper. Each
   dispatch runs its own load-execute-append cycle on its target
   aggregate, in its own transaction.
5. Apply the triggering event to the instance state
   (`apply(state, event)`).
6. In a single transaction: `record_snapshot` for the new state,
   then `advance_subscription` past the triggering event, then
   commit (D-0008).

Compensation does not introduce a new branch in this loop, a new
step, or a reordering. It is just — step 3 produces a command
list that includes `CancelHotelBooking` instead of (or alongside)
`BookFlight`. Step 4 dispatches it the same way. Step 6 records
the state transition (`phase: :compensating`) in the snapshot the
same way.

The absurd-bridge case adds no new step either. The PM dispatches
a command in step 4 that records a `PaymentRequested` event into
the store. A separate process — an absurd task whose handler
subscribes to the relevant event stream — reads that event,
performs the durable external call with absurd's checkpoint /
retry machinery, and at the end calls `append_to_stream` with a
`PaymentProcessed` or `PaymentFailed` event. The PM's
subscription, on a later iteration of its own loop, picks up that
returning event and reacts to it in step 3.

From the PM worker loop's perspective, there is no "the workflow
is waiting on an external system" state — it is event-driven
throughout. The only thing that distinguishes "waiting for an
absurd task" from "waiting for an aggregate command's success
event" is which event eventually arrives. This is the property
that makes candidate 3 cheap: the same loop handles both.

### Error and retry semantics

Unchanged from PM-013 / PM-031 in `mapping.md`. A failed command
dispatch from step 4 surfaces to the application's `error/3`
callback with the usual vocabulary (`{:retry, ...}`,
`{:skip, ...}`, `{:continue, commands, context}`,
`{:skip, :discard_pending | :continue_pending}`, `{:stop, ...}`).
The application chooses whether a dispatch failure (e.g. the
target aggregate rejecting the compensating command) translates
into another compensating attempt, a different compensating
command, or giving up. None of this is saga-specific; it is the
PM error-handling contract.

### What "first-class" means under this decision

The substance of D-0001 is preserved by the following concrete
properties, none of which require new mechanism beyond what Phase
4 already specified:

1. **Compensation runs reliably under crashes.** The PM's
   snapshot is co-transactional with its cursor advance (D-0008),
   so a worker that crashes between dispatching the compensating
   command and recording the new state will re-deliver the
   triggering failure event on restart, re-dispatch the
   compensating command (idempotent at the aggregate by
   optimistic-locking + INV-APPEND-030), and converge.
2. **Compensation is ordered.** Because the PM's subscription
   delivers in strictly increasing order (INV-SUB-P-030) and the
   state snapshot is the durable record of "where we are", the
   PM cannot dispatch a compensating command in response to a
   failure event whose precursor event it hasn't already
   processed.
3. **Compensation is causation-tracked.** The compensating
   command inherits `causation_id = failure_event.event_id`
   automatically (PM-012). The original command's `causation_id`
   was the triggering event's id, and so on up the chain. A
   compensation flow is traceable end-to-end through the
   `causation_id` graph in the events table.
4. **Side-effecting compensation is durable.** Cancelling a
   Stripe charge is itself a side effect; under candidate 3 it
   is itself an absurd task spawned by a `RefundRequested` event
   dispatched by the PM in its compensation path. The same
   bridge pattern that handles `PaymentRequested` handles
   `RefundRequested`. There is one pattern, used twice.

This is the bar D-0001 set: the application is not left to
build durability, ordering, or recovery for compensation flows.
It is left to write the PM clauses.

---

## Phase 7 inputs

Forward-pointing constraints that the SQL contract must honour to
make D-0011 work. Most are restatements or tightenings of items
already in Phase 4's mapping; they are gathered here so Phase 7
does not have to rederive them.

1. **No saga-specific tables.** The Phase 7 schema set is
   `streams`, `events`, `snapshots`, `subscriptions` (and
   whatever join / sequence artifacts OQ-0001 introduces). PMs
   and sagas add nothing.

2. **`event_id` is caller-supplied, not server-generated.**
   INV-APPEND-001 already says "every event MUST be assigned a
   unique `event_id`" but does not pin *who* assigns it.
   D-0011's absurd-bridge pattern relies on the *caller*
   supplying the id, so that an absurd task replaying a step can
   re-derive the same id and get a clean `:duplicate_event`
   instead of a second insert. Phase 7 MUST keep `event_id` as a
   caller input on `append_to_stream`. The SDK is responsible
   for generating a fresh UUID for normal commands; the
   absurd-bridge task is responsible for deriving it
   deterministically from `(task_id, step_name)`.

3. **`:duplicate_event` is a public error, not reference-only.**
   INV-APPEND-030 flagged this as `[reference-only, optional]`
   in the abstract Commanded contract. Under D-0011 it becomes a
   load-bearing error code for the cross-system idempotency
   pattern. Phase 7's `append_to_stream` MUST surface it (a
   distinct SQLSTATE or sentinel return value) rather than
   folding it into `wrong_expected_version`. The SDK is
   responsible for mapping the SQLSTATE to the language-native
   error type, and an absurd-task helper is responsible for
   treating it as success (the prior emit already succeeded).

4. **`append_to_stream` MUST be callable from any session,
   including a session that is *not* the dispatching SDK.** An
   absurd task is a different process, often a different SDK
   binary, possibly a different programming language. The stored
   procedure must not assume the caller has set any session
   GUCs, opened any prior transaction, or claimed any lease.
   This is already implicit in Phase 4 but worth pinning
   explicitly because the cross-boundary use case is the first
   one that breaks if it slips.

5. **`read_subscription_batch` must surface enough information
   for failure-event recognition to be a domain-modelling
   concern, not a framework one.** Concretely: the `event_type`
   string and the `data` JSONB are delivered to the SDK as-is
   (per INV-META-010/011), and the SDK delivers them to the PM
   handler as-is. The framework does not need to know which
   event types are "failure" events; the PM's pattern-match on
   `event_type` is the only classification step.

6. **Lock ordering: PM persist-and-ack transaction holds
   `snapshots` and `subscriptions` row locks; the PM's
   compensating command dispatch (step 4 above) runs in a
   *separate* transaction that holds `streams`, `events`, and
   (per OQ-0001) the global-ordering serialisation point.**
   These are disjoint lock sets, in agreement with D-0008's
   constraint that `advance_subscription` must not take a lock
   the SDK could plausibly hold from earlier statements. Phase 7
   must keep them disjoint when laying out lock-acquisition
   orders in the stored-procedure docstrings.

7. **No new `consistency: [...]` semantics needed for sagas.**
   D-0010 already covers the case where a dispatcher wants to
   wait for a specific subscription to catch up. A PM that wants
   to dispatch a follow-up command only after a specific
   projection has seen its prior emission uses the same explicit
   list mechanism. No saga-specific waiting primitive is
   required.

8. **Optional future helper (informational, not a requirement):**
   should Phase 8's worked example produce a workflow whose
   shape is genuinely linear-with-pairable-compensation, an
   SDK-level helper that compiles a step-pair declaration into a
   PM module is compatible with this contract — it would emit
   the same `handle/3` clauses by hand-written rule, generate
   the same `record_snapshot` / `advance_subscription` calls,
   and use the same `append_to_stream` for dispatch. Phase 7
   need not reserve any schema for this; the helper lives
   entirely in the SDK if it is built at all.

---

## Phase 6 status

The roadmap's Phase-6 done-criterion ("a choice is made and
recorded in `decisions.md`, and the implications for the schema
and SDK are written into `mapping.md`") is met:

- Choice recorded: D-0011 in `decisions.md`.
- Schema implications: none beyond Phase 4; recorded in PM-030's
  updated mapping entry and in this document's Pass 3.
- SDK implications: PM-011's existing handle → dispatch → apply →
  persist → ack ordering is unchanged; compensating commands are
  dispatched in step 3 alongside any forward commands. Recorded
  in PM-030's updated mapping entry and in this document's
  Pass 3.
- Phase 7 inputs catalogued above so the SQL contract phase does
  not have to rederive them.

No new entries in `open-questions.md`. The PM-instance ↔ absurd-task
linkage question raised in Pass 2 is *deliberately* not surfaced as
an OQ; promoting it now would be the first step toward accidentally
building candidate 2.

---

## Appendix — Developer-facing framing

The rest of this document is design-rationale voice ("why is the
system shaped this way"). This appendix is the user-guide voice
("what do I do on Monday morning"), preserved here so the eventual
user docs — likely written alongside the Phase 8 SDK or after
Phase 9 — don't have to rediscover it from scratch. The framing
crystallises something about candidate 3 that the design voice
doesn't quite carry: the absence of a saga DSL feels like a loss
until you see it reframed as "the PM contract is already enough".

### The opening

> You don't add sagas. You add a process manager.

### The three things you write

A PM module is three callbacks:

1. **`interested?(event)`** — pattern-match on event types to say
   "this event starts a new instance / continues this instance /
   stops this instance / ignore". Identity is the business key
   that ties the workflow together (`orderId`, `bookingId`).
2. **`handle(state, event, metadata)`** — for each event the
   instance receives, return a list of commands to dispatch.
   Forward-progress events return forward commands; failure
   events return compensating commands. Both are just
   `dispatch X(...)` calls.
3. **`apply(state, event)`** — fold the event into the instance's
   state (typically a `phase` enum plus the relevant intermediate
   IDs).

That's the whole shape. No saga DSL, no step pairing, no
`compensate` keyword.

### What you have to design

Two modelling disciplines, neither framework-enforced:

- **Failure events must exist.** If `BookFlight` can permanently
  fail in a way the saga needs to react to, the `Flight`
  aggregate must emit `FlightBookingFailed` — not just return an
  error to its dispatcher. The PM can only react to events that
  exist in the store.
- **The state machine shape lives in your snapshot data.** Since
  the framework doesn't track "saga is at step 3 of 5", you
  encode that yourself — typically a `phase` enum.

### Side effects (Stripe, email, third-party APIs)

You don't make those calls from the PM. The PM dispatches a
command that emits a `PaymentRequested` event. You write an
**absurd task** subscribed to that event; it does the durable
external call with checkpoints and retries, and at the end it
`append_to_stream`s a `PaymentProcessed` or `PaymentFailed` event
back. The PM picks that up on a later iteration like any other
event.

One rule for the absurd task: derive the returning event's
`event_id` deterministically from `(task_id, step_name)` so a
retried emission gets `:duplicate_event` (treated as success)
rather than double-appending.

### What you get for free

- Durable state across worker crashes (snapshots co-transactional
  with cursor advance, per D-0008).
- Ordered, at-least-once event delivery.
- `causation_id` chains the entire flow end-to-end — forward and
  compensation — through the events table. Traceability without a
  saga-log table.
- The compensation path is the same code shape as the forward
  path; one set of mechanics, used twice.

### What you don't get

- A tooling view that says "saga is at step 3 of 5". Tooling sees
  whatever your snapshot `data` field contains.
- Any framework opinion about which of your `handle/3` clauses
  are "compensating". A reader of the PM module recognises that
  from the pattern-matches on failure event types.

### Mental shift from other systems

- **From Commanded:** same model, with stronger durability under
  D-0008. No surprise.
- **From step-and-compensate DSLs (Temporal sagas, Camunda,
  MassTransit):** you express the same workflow as a reactive
  state machine instead of a step list. Linear workflows feel
  slightly more verbose; reactive / fan-in-fan-out workflows
  feel substantially more natural.
