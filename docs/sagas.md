# Sagas / Process Managers — Phase 6

The purpose of this document is to settle the one open mapping
entry left after Phase 4: **PM-030 (compensation)**. D-0001 already
committed `instructed` to treating compensation as a first-class
concern; this phase decides the mechanism.

The work is done in passes:

| Pass | Scope | Status |
|------|-------|--------|
| 1 | Survey absurd's task/step model; lay out the three candidate shapes concretely | done |
| 2 | Take a position; record the decision | **this commit** |
| 3 | Implications for schema and SDK loop; Phase 7 inputs | pending |

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

### What Pass 3 must cover

- Schema implications: confirm "no new tables" against the Pass-1
  candidate-2 sketch and against PM-020..024 in `mapping.md`.
- SDK loop: where compensation fits relative to PM-011's
  handle → dispatch → apply → persist → ack ordering. (Hypothesis:
  nowhere new — the compensating command is dispatched in step 3
  of PM-011 like any other.)
- Phase 7 inputs: forward-pointing constraints on the SQL contract,
  especially around caller-supplied `event_id` and the
  `:duplicate_event` error being part of the public contract (not
  reference-only).
