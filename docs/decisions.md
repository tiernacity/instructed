# Decisions

A running log of design decisions made during the `instructed` exploration.
Newest entries at the top. Each entry records *what* was decided, *why*,
and *what it precludes* — so that future revisits can re-evaluate the
context, not just the conclusion.

Open questions that have not yet been decided live in
[`open-questions.md`](open-questions.md) (created when the first one is
recorded). Capabilities deliberately deferred to a later version are
tracked in [`maybe-later.md`](maybe-later.md).

---

## D-0020 — Process-manager worker: `{kind:'start'}` discards in-place; load-time `StreamNotFound` is empty stream; same-`Client` is a construction error

**Date:** 2026-05-18

**Context:** implementing Layer 3 (`startProcessManager`, Phase 8
step 4) required pinning three details the design doc left to the
implementer.

**Decided:**

1. **`{kind: 'start'}` discards an existing snapshot in-memory; the
   ack tx overwrites it.** Per §11.4 the start route is lenient: if a
   snapshot already exists for the `processId`, the SDK ignores it,
   calls `def.initialState()`, runs `handle`, dispatches commands, and
   writes a fresh snapshot (same `source_uuid`) in the ack
   transaction. `record_snapshot` is a full-row upsert (INV-SNAP-002),
   so a separate delete-then-record dance is unnecessary. No
   `start!` strict variant in v1.

2. **`runCommand` treats `StreamNotFound` (IS003) during load as an
   empty stream when `version == 0`.** The aggregate runner is the
   entry point for first-ever commands against a new aggregate;
   read_stream on a non-existent stream raises IS003, and the runner
   now catches it as the equivalent of an empty event page. Only the
   `version == 0` case is swallowed — a non-zero version reaching
   IS003 implies the stream was deleted out from under us, which v1
   does not allow but which we still surface as the underlying error
   rather than silently treat as empty. This is a step-2 gap that
   step 4's PM smoke test exposed (PM dispatches the first command
   to a fresh target stream).

3. **Construction-time guard for same-`Client` persist/dispatch.**
   Per §3 layer 3 and D-0011/D-0012 the persist-and-ack session and
   the dispatch session must be different so their lock sets are
   disjoint. `startProcessManager` throws `InstructedError` at
   construction (cheap, deterministic, before any procedure call) if
   `client === dispatchClient`. The check is by identity, not
   structural — sharing the *underlying pool* across two `Client`
   instances is fine (the two pool checkouts produce distinct
   sessions); sharing the `Client` wrapper is not. The Layer 5
   facade will be responsible for materialising a second pool
   lazily so PM authors never face this concern.

**Consequences:**

- The PM worker's `processEvent` loop is a single state machine:
  route → load state → handle → dispatch → ack-tx. Each transition
  failure (snapshot load, handle throw, any dispatch, ack-tx) triggers
  the shared §11.5 backoff and re-enters at `load state`, so a
  redelivery after a partially-dispatched batch re-runs `handle`
  with the same input event. PM authors' idempotency contract per
  §11.5 PM-specific note still applies.
- The dispatch loop short-circuits on the first failure: subsequent
  commands in the same `commands` array are NOT attempted for that
  attempt. They will be retried on the next pass through `handle`.
  This is the conservative reading of §11.5 ("commands dispatched up
  to the throw point are durable; commands after the throw point
  never ran") applied symmetrically to dispatch failures.
- The ack tx is the only place the SDK wraps two procedures in a
  single transaction. `withTransaction` (internal) checks out a
  dedicated session from the underlying pool, BEGINs, runs the
  callback against a session-bound `Client`, then COMMITs.

**Sources:** `sdk-design.md` §3 layer 3, §11.4, §11.5, §11.8, §11.9;
`sql/instructed.sql` :: `record_snapshot` (INV-SNAP-002),
`advance_subscription` (D-0016 / PM-023); D-0011, D-0012, D-0016,
D-0017.

---

## D-0019 — Aggregate runner: retry semantics, explicit-expected-version, snapshot-write failure, load page size

**Date:** 2026-05-18

**Context:** implementing Layer 1 (`runCommand`, Phase 8 step 2)
required pinning four details the design doc left under-specified.
The SDK ships with these defaults so a user can call `runCommand`
without extra knobs; revisit if benchmarks or real workloads
show them wrong.

**Decided:**

1. **`retryBudget` is the number of retries after the first attempt.**
   `retryBudget: 0` therefore means *one* attempt total; the first
   `IS001` raises `RetryBudgetExhausted` carrying the underlying
   `WrongExpectedVersion`. The default of 5 (mapping.md AGG-010)
   permits up to 6 attempts total. This is the natural reading of
   "retry up to retryBudget" in `sdk-design.md` §3 layer 1 and
   matches Commanded's `retry_attempts` field shape (their default
   is 0; ours is 5).

2. **Explicit `RunCommandOptions.expectedVersion` disables OCC retry.**
   When the caller supplies an explicit expected version, a
   mismatch surfaces as the underlying `WrongExpectedVersion`
   rather than triggering the retry loop. The default-expected-
   version case (where the SDK constructs `expected.exact(V)` from
   the loaded version) is the only path that retries. Rationale:
   an explicit expected version is a caller-asserted invariant; if
   it is wrong on attempt N it is wrong on attempt N+1, because
   the SDK would still assert the same value. Retrying would
   either be a no-op (still wrong) or silently change the
   semantics of the assertion (re-derive against the new head).
   Both are surprising; surfacing the error is honest.

3. **Snapshot-write failures are non-fatal and currently log via
   `console.warn`.** A failed `recordSnapshot` after a successful
   append never fails the command — the load path works without
   a snapshot, just more slowly. v1 does not expose a logger or
   `onError` on `RunCommandOptions`; output goes to `console.warn`
   prefixed with `[instructed]`. A logger surface can be added
   later without breaking callers; tracked informally and revisited
   when the facade (layer 5) lands a `log` option.

4. **Aggregate-load page size is an internal constant of 500.**
   `runCommand` pages `readStream` in 500-event chunks during
   load. Not exposed on `RunCommandOptions`: tuning it requires
   workload data we do not have yet, and a knob here would
   freeze the loader's internal contract. If a real workload
   makes this matter, add an option then.

**Consequences:**

- `RetryBudgetExhausted.attempts` is always >= 1 (the first try
  counts as attempt 1). With the default budget, it is between 1
  and 6.
- Callers that need at-most-one-attempt OCC semantics still set
  `retryBudget: 0` explicitly; callers that want to assert a
  precise head version ("this command is only valid against
  version N") use `expectedVersion: expected.exact(Nn)` and
  accept that they get the raw `WrongExpectedVersion`.
- Snapshot-write hardening (retries, dead-letter, async
  enqueue) is explicitly *not* in v1; the design's claim that
  snapshots are "best-effort optimisations of the load path"
  (sdk-design.md §3 layer 1) is realised literally.

**Sources:** `sdk-design.md` §3 layer 1, §6, §11.8; `mapping.md`
AGG-010; D-0005 (no advisory lock, OCC only); D-0017 (causation/
correlation defaulting).

---

## D-0018 — Worker lifecycle, `AbortSignal`, and ack-tx-on-shutdown

**Date:** 2026-05-17

**Context:** the Phase 8 SDK design review surfaced that
`RunningWorker` (the handle returned from `startProjection` /
`startProcessManager` / `Instructed.startWorker`) declared
`close()` and `stopped` without pinning their semantics:
idempotency of `close()`, whether `close()` waits for `stopped`,
what `stopped` resolves to on lease loss vs poison-event loop,
when `AbortSignal` aborts relative to the in-flight handler, and
whether the SDK commits an ack transaction whose handler
returned before `close()` fired. None of these were blocking on a
new design choice; they needed pinning so the implementer does
not have to invent them.

**Decision:** pin the following worker-lifecycle semantics. They
apply uniformly to `startProjection`, `startProcessManager`, and
the internal workers spawned by `Instructed.startWorker`.

1. **`close()` is idempotent.** Calling it on a worker that is
   already stopping returns the same `Promise<void>` as the
   first call. Calling it after `stopped` has resolved is a
   no-op that resolves immediately.
2. **`close()` resolves after `stopped` resolves *and* after a
   best-effort `release_subscription` call.** Failures of the
   release call (lease already lost, network blip on shutdown)
   are swallowed; the worker is gone regardless.
3. **`stopped` always resolves; it never rejects.** Fatal
   conditions (lease loss, repeated handler throws with
   exhausted backoff, fatal SQL errors during the SDK's own
   bookkeeping) fire `onError` and then `stopped` resolves
   normally. Callers that want to detect abnormal exit observe
   `onError`; the `stopped` promise is purely a "this worker
   has finished its loop" signal, mirroring absurd's worker
   shape.
4. **`AbortSignal` aborts immediately** on `close()` *or* on
   lease loss (`IS022` / `IS020` from the heartbeat or the ack
   tx). The signal is not delayed until the in-flight handler
   returns — a handler observing the signal can begin cleanup
   while the SDK continues to await its promise.
5. **The SDK still awaits the in-flight handler's promise**
   even after the signal aborts. If the handler resolves, the
   SDK skips the `advance_subscription` call (the `aborted`
   flag is set; advancing under a lost lease would raise IS022
   anyway). If the handler rejects, the SDK fires `onError`
   with the wrapped `HandlerError` and exits the loop without
   advancing.
6. **In-flight ack transactions are allowed to commit.** If
   `close()` is called after the handler has returned but
   before the SDK has committed `advance_subscription`, the ack
   tx commits normally; the SDK only refuses to *start a new
   batch read* after `close()`. Dropping an in-flight ack would
   simply force a redelivery the handler must absorb
   idempotently anyway; committing it is strictly cheaper.
7. **Heartbeat failures other than `IS022` / `IS020` retry
   once** with a 250ms delay; a second failure is treated as
   effective lease loss (sets `aborted`, fires `onError`, exits
   the loop). Transient network failures during heartbeat
   should not bring down an otherwise-healthy worker on the
   first hiccup; persistent failures should not silently keep
   the worker running while another holder takes over.

**Rationale:** every item above is the cheapest correct
semantics for the stated scenario. The shape mirrors absurd's
worker lifecycle (where `close()` is idempotent, `stopped`
always resolves, and fatal conditions surface through hooks).
None of this constrains the SQL contract.

**Implications:**

- `docs/sdk-design.md` §3 layer 2 picks up the lease-loss-race
  and heartbeat-failure sentences; §11 gains §11.9 documenting
  the lifecycle semantics above.
- The implementer can ship the worker loop without coming back
  for clarifications on the close/abort/ack interplay. The
  bank-account example does not need to exercise any of these
  paths explicitly; the conformance harness (Phase 9) will.
- No SQL-contract change. `advance_subscription` and
  `release_subscription` already raise on lease loss; this
  decision only specifies how the SDK responds to those errors.

---

## D-0017 — Causation / correlation propagation (SDK-default with explicit overrides)

**Date:** 2026-05-17

**Context:** `mapping.md` AGG-020/021 and PM-012 commit the SDK
to propagating `causation_id` and `correlation_id` through
command dispatch (every event in a single command shares
`causation_id = command_uuid`; PM-dispatched commands inherit
`causation_id = triggering_event.event_id` and `correlation_id
= triggering_event.correlation_id`). The first Phase 8 design
pass had no place to thread either value: `RunCommandOptions`
had `retryBudget` / `expectedVersion` only, `DispatchedCommand`
had `streamUuid` / `aggregate` / `command` only, and the
bank-account sketch built `NewEvent`s with neither field set.
Without a decision, the bank-account example would silently
establish a different contract than `mapping.md` promises and
the Phase 9 Commanded conformance harness would fail the first
time it exercised AGG-020 / PM-012.

**Decision:** the SDK fills `causation_id` and `correlation_id`
automatically with sensible defaults and allows the caller to
override either. This is the **hybrid** shape (the same
defaulting pattern §11.2 already chose for `event_id`):

1. **`NewEvent<E>` declares both fields optional.** The user's
   `execute` may set them; if it does, the SDK uses the
   supplied values verbatim. If it does not, the SDK fills them
   per the rules below.
2. **`runCommand` mints a per-call `commandId`** (default:
   `crypto.randomUUID()`; overridable via
   `RunCommandOptions.commandId`). For every event the SDK
   appends whose `causation_id` is unset, the SDK fills it with
   `commandId`. This realises AGG-020.
3. **`runCommand` accepts an optional `correlationId`** in its
   options; if supplied, the SDK fills any unset
   `correlation_id` on the appended events with it. If not
   supplied, the SDK does not invent one (callers that want a
   correlation chain start it explicitly at the top of the
   chain). This realises AGG-021.
4. **PM dispatch threads causation/correlation from the
   triggering event.** When the PM worker calls `runCommand`
   for a `DispatchedCommand`, it passes:
   - `commandId = crypto.randomUUID()` (a fresh command id;
     each PM-dispatched command is its own command).
   - `causationId = triggering_event.event_id` (overriding the
     default; this is the per-event causation, not the
     per-command one — see clause 5 below).
   - `correlationId = triggering_event.correlation_id` (if
     present).
   These flow into `runCommand`'s defaulting so the dispatched
   command's appended events get the right values without the
   PM author writing any plumbing. This realises PM-012.
5. **One subtle precedence:** within a `runCommand` invocation,
   `causationId` (if supplied in options) wins over the
   per-call `commandId` for event defaulting. This is so PM
   dispatch can hand the triggering event's id straight through
   as the causation, which is the Commanded behaviour. The
   `commandId` option remains available for callers that want
   the AGG-020 "all events share command id" shape on a
   top-level dispatch; the PM path overrides it explicitly.
6. **`DispatchedCommand` does not grow new fields.** The PM
   worker constructs the right options object internally; the
   PM author writes only `{ streamUuid, aggregate, command }`
   (or the by-name variant per §11.7).

**Rationale:**

- **Symmetric with `event_id` (§11.2).** The same
  SDK-fills-when-absent rule applies to all three
  caller-supplied id fields, so there is one mental model: "the
  SDK fills caller-omitted ids with sensible defaults; callers
  who care override."
- **Application code stays free of plumbing.** The bank-account
  example's `execute` does not need to know about
  `causation_id` or `correlation_id`; the PM author does not
  need to know about them either. The mapping.md promises hold
  without the caller doing anything.
- **The Commanded conformance harness (Phase 9) maps directly.**
  AGG-020/021 and PM-012 become observable defaults; the test
  cases are "dispatch a command, read back the appended events,
  assert all share a causation_id" / "PM consumes event X,
  asserts dispatched command's appended events have
  causation_id = X.event_id".
- **The absurd-bridge pattern (D-0011) is unaffected.** Absurd
  tasks that re-append into the event store with deterministic
  `event_id`s also typically supply their own `causation_id`
  (the triggering event's id); the SDK's defaulting only fires
  when the field is absent, so explicit supply works
  identically.

**Implications:**

- **`docs/sdk-design.md` §3 layer 0** declares `NewEvent<E>` with
  optional `event_id`, `metadata`, `causation_id`,
  `correlation_id`.
- **`docs/sdk-design.md` §3 layer 1** adds `commandId?: string`
  and `correlationId?: string` to `RunCommandOptions`, and
  documents the defaulting rules above.
- **`docs/sdk-design.md` §3 layer 3** documents that the PM
  worker constructs `RunCommandOptions` internally with
  `causationId = triggering_event.event_id` and
  `correlationId = triggering_event.correlation_id`; the PM
  author writes no causation plumbing.
- **`docs/sdk-design.md` §11** gains §11.8 cross-referencing
  this decision.
- **`docs/sdk-usage-sketch.md`** — the bank-account `execute`
  example no longer needs to be updated to set causation_id
  manually; the existing event-construction shape is correct
  *because* the SDK fills the field. A one-line comment in the
  sketch (or in the design doc) noting "causation_id is filled
  by the SDK from the per-call commandId; see D-0017" is
  enough.
- **`docs/mapping.md` AGG-020 / AGG-021 / PM-012** verdicts
  remain *equivalent*; this decision is the realisation note.
- No SQL-contract change. `events.causation_id` and
  `events.correlation_id` are already nullable; the SDK is the
  only thing that decides when to fill them.

---

## D-0016 — Handlers are opaque to the SDK; cursor advance is independent; idempotency is the application's concern (supersedes D-0008)

**Date:** 2026-05-17

**Context:** D-0008 committed the SDK to a co-transactional
persist-and-ack pattern: the worker opens a transaction, fetches
a batch, runs the handler (which writes to its projection tables
in the same transaction), then calls `advance_subscription`
before commit. Projection writes and cursor advance commit
together. This bought exactly-once-at-the-transaction-level for
projections, removing the idempotency burden on a class of
applications.

The Phase 8 SDK design review surfaced the implicit assumption
in D-0008: **the projection target is Postgres**. Real
projections target Elasticsearch, ClickHouse, Redis, BigQuery,
an external HTTP API, an in-memory cache, a different database,
or any combination. None of these can share a Postgres
transaction. The plumbing required to keep D-0008's property
for the Postgres case — a registration-time `withTx` mapper
building an ORM-agnostic wrapper around the SDK's pg
connection, exposed as `ctx.tx` to handlers — was real surface
area (`sdk-design.md` §11.1 in its first form) and provided
zero value to non-Postgres projections.

**Decision:** reverse D-0008. The handler is **opaque to the
SDK**. The worker loop is:

```
loop:
  events = read_subscription_batch(...)        -- short tx, lock released
  for e in events:
    await handler(event, ctx)                  -- no SDK transaction
  advance_subscription(..., last_position)     -- short tx, separate from the handler
```

Handler returns successfully → SDK advances the cursor. Handler
throws → SDK does not advance; event redelivers next iteration.
There is no SDK-owned transaction wrapping the handler call.
The handler receives the event and an opaque context
(`workerId`, `position`); it does not receive a Postgres
connection, an ORM handle, or any other SDK-owned resource.

Idempotency becomes the application's responsibility, matching
Commanded's contract (HND-031) exactly. Applications that
project into Postgres typically use an idempotent UPSERT or a
`processed_events` side table; applications that project
elsewhere use whatever their target's idempotency story is.

**Rationale:**

- **Projection targets are application-domain.** D-0008's
  property only existed for Postgres-targeted projections. The
  cost (forcing every handler to receive a database handle,
  forcing every user to either use raw `pg` or supply a
  per-registration ORM mapper) is paid by all users; the
  benefit was reaped only by Postgres-targeted projections that
  happened to write to the same database the event store sits
  in. Inverted bargain.
- **The handler signature collapses to its simplest possible
  shape:** `(event, ctx) => Promise<void>`. No `tx`, no
  `pgClient`, no `withTx`, no ORM-agnostic mapper, no
  per-registration adapter. Matches absurd's task-handler shape
  (`(params, ctx) => Promise<R>`) and Commanded's event-handler
  shape almost exactly.
- **The strong-consistency-on-dispatch story is unchanged.**
  `waitForProjection` still polls `read_subscription_position`;
  "caught up" still means "handler returned and SDK acked".
  Small extra latency window between handler-return and
  ack-commit (one round-trip), inconsequential.
- **Lock-set disjointness (D-0011/D-0012) still holds.** PM
  persist-and-ack tx now writes only snapshots and subscriptions
  (no user tables); dispatch still uses a separate connection.
  Internal SDK plumbing; user does not see it.
- **Commanded contract parity** simplifies the Phase 9
  conformance harness mapping.

**Implications:**

- **D-0008 is superseded** but its entry remains in this log
  for the audit trail, with a forward pointer to D-0016 added
  in-place.
- **D-0014's motivation weakens but the decision survives in
  spirit.** The original D-0014 reasoning included "share a tx
  with the application's own writes" — that motivation is gone.
  `Client.withTransaction` is dropped entirely (no remaining
  use case in v1; `runCommand` opens its own tx, worker loops
  open their own short txs). The D-0014 entry is updated with
  a note.
- **`docs/sdk-design.md` §11.1** (ORM-agnostic Tx surface)
  collapses to one sentence: handler is opaque to the SDK.
- **`docs/sdk-design.md` §11.6** (`withTransaction` nesting)
  is removed; `Client.withTransaction` no longer exists.
- **`docs/mapping.md` HND-031** changes verdict from "tighter
  on the recommended pattern" to "equivalent to Commanded";
  related entries (HND-012, HND-023, PM-024, etc.) lose their
  D-0008 cross-references and are simplified accordingly.
- **`docs/sql-contract.md`** subscription-worker recommended
  call pattern flips from "BEGIN; read_batch; handler;
  advance; COMMIT" to two independent short transactions
  bracketing the handler. The SQL contract itself does not
  change — `advance_subscription` is still callable inside any
  well-formed transaction; the SDK simply chooses not to use
  that capability.
- **`docs/non-goals.md` gains NG-0015**: "co-transactional
  persist-and-ack is not a v1 guarantee; handlers are responsible
  for their own idempotency".
- A future SDK feature for the narrow Postgres-projecting-into-
  same-database case (an opt-in `coTransactional: true` flag
  that re-enables the D-0008 path) is not precluded but is not
  in v1. Not tracked as `ML-` unless a real workload demands it.

---

## D-0015 — Reference SDK public API shape

**Date:** 2026-05-17

**Context:** the design pass for the TypeScript SDK
(`docs/sdk-design.md`) and the consumer-facing walkthrough
(`docs/sdk-usage-sketch.md`) went through three review rounds.
The first draft proposed a layered building-block API only
(`Client`, `runCommand`, `startSubscriptionWorker`,
`startProcessManager`, `waitForProjection`). Review surfaced
that the absurd `Absurd` class sets the abstraction bar a `pi`-
ecosystem consumer expects, and that several names and signatures
in the first draft diverged from Commanded in ways that would
leak SDK plumbing into application code. This decision
consolidates the shape that emerged.

**Decision:** v1 SDK ships **layered building blocks + a
top-level `Instructed` facade** (Option B from
`sdk-usage-sketch.md` §3). The facade is a pure composition of
the lower layers; nothing in the SQL contract changes; no prior
decision is revisited. The public surface is fixed as follows:

1. **Aggregate callbacks use Commanded's names: `execute` and
   `apply`.** `execute(state, command)` may return a single
   event, an array, `undefined`, or `[]` (the no-op forms
   mirror Commanded's `:ok | {:ok, []} | nil | []`).

2. **`apply` takes the domain event payload `E`, not
   `RecordedEvent<E>`.** The SDK tracks aggregate version itself
   by counting events folded from the stream; user code never
   reads or writes `stream_version`. Aggregate state is
   domain-only.

3. **`initialState()` is required on `AggregateDefinition`.** A
   TypeScript concession — Elixir's `defstruct` default mechanism
   has no clean TS analogue. The factory is called once per
   aggregate load; the SDK fills version tracking around it.

4. **Three exported definition types, symmetric:**
   `AggregateDefinition<S, C, E>`,
   `ProjectionDefinition<E>`,
   `ProcessManagerDefinition<S, E>`. Each is the portable,
   declarative description of what the thing IS; per-process
   tuning (batch size, lease seconds, error handler) lives in a
   separate `RegistrationOptions` / `*WorkerOptions` shape so
   the definition can be defined once and run in different
   processes with different operational knobs.

5. **PM routing is a declarative `routes` map keyed by event
   type**, not a switch-on-`event_type` function. Each entry is
   `(event) => string | {kind, processId} | null`. Absent keys
   mean "not interested". Mirrors Commanded's `interested?/1`
   callback semantics in a TS-shaped, declarative form.

6. **Facade has `register*` + `startWorker()`, mirroring
   absurd's `registerTask` + `startWorker`.** Three register
   methods (`registerAggregate`, `registerProjection`,
   `registerProcessManager`); one start method. Application
   controls deployment topology by choosing which subset of
   `register*` calls each process makes. `startWorker()` fans
   out into one internal subscription loop per registered
   projection / PM and returns a single handle; the N-cursors
   fan-out is hidden because the user already chose the N via
   register calls.

7. **Layered API is the foundation, not a private layer.**
   `Client`, `runCommand`, `startProjection`,
   `startProcessManager`, `waitForProjection` remain exported.
   The facade is built on them; advanced users (tests, unusual
   deployment topologies, applications composing `instructed`
   with non-`instructed` Postgres writes) drop down when
   needed.

**Rationale:** the facade closes the abstraction gap with
absurd (`docs/sdk-usage-sketch.md` §2c table), while the
layered API preserves every property the prior decisions buy.
Commanded-aligned names reduce the friction for users coming
from that ecosystem (the conformance harness in Phase 9 will
also be cleaner to map). Domain-only `apply` and declarative
`routes` keep SDK plumbing out of application code — the
repeated principle in `ROADMAP.md`'s guiding principles ("SDKs
are thin; they encode the call sequence; they never hold
invariant-bearing state") cuts both ways: the SDK should not
leak its bookkeeping into application code either.

**Implications:**

- `docs/sdk-design.md` is authoritative for type signatures and
  the worker-loop body; `docs/sdk-usage-sketch.md` is the
  consumer-facing walkthrough that demonstrates the resulting
  ergonomics in the bank-account demo.
- Implementation sequencing in `docs/sdk-design.md` §10:
  layer-0 client → layer-1 aggregate runner → layer-2 projection
  worker → layer-3 PM worker → layer-4 consistency wait →
  layer-5 facade → bank-account example.
- The `Instructed` facade's `registerAggregate(def)` keys the
  registry by `def.type` (the same string used for snapshot
  `source_type`); `dispatch(aggregateType, ...)` looks up by
  that key. No separate name is needed.
- The `consistency: ["BalancesProjector"]` sugar on
  `app.dispatch` resolves bare strings to `{stream: "$all",
  name}` pairs; explicit `{stream, name}` form is the escape
  hatch for non-`$all` projections. The list remains explicit
  per D-0010; no `:strong` shorthand is reintroduced.
- Future API additions (e.g. server-side selectors per
  ML-0003, partitioned consumers per ML-0001) are additive on
  the registration options and worker-options shapes; the
  facade signatures above do not need to change.
- A handful of small specification details — `tx` parameter
  exact type, `NewEvent.event_id` defaulting, `start` vs
  `start!` PM routing strictness, handler-throws semantics,
  `withTransaction` nesting policy, PM-internal dispatch
  shape, exact apply-event stripping rule — are deferred to a
  final design closing pass (or to the implementation pass
  with per-question D- entries as they land). Listed for the
  next pass; not blocking on this entry.

---

## D-0014 — Subscription handlers see the SDK-opened transaction; high-level workers wrap it; low-level callers may open their own

**Note (added 2026-05-17, post-D-0016):** D-0016 supersedes
D-0008, removing the co-transactional persist-and-ack pattern
entirely. The motivation for this decision — "share a tx with
the application's own writes" — is gone, and `Client.withTransaction`
has been dropped from the SDK. The entry is kept in the log for
the audit trail; it no longer reflects the current design.

**Date:** 2026-05-17

**Context:** OQ-0002 asked where `BEGIN`/`COMMIT` live for the
co-transactional persist-and-ack pattern D-0008 mandates. Three
shapes were on the table: (1) SDK exposes individual procedures,
caller opens the transaction; (2) SDK exposes a `process_batch`
helper that opens the transaction internally; (3) two-phase
commit with a `pending_ack` column.

**Decision:** v1 ships **both (1) and (2)** as layered APIs:

- `Client.withTransaction(fn)` is the only place in the SDK that
  issues `BEGIN`/`COMMIT`. Low-level callers use it directly when
  they want to share a transaction between subscription handlers
  and their own application writes.
- `startSubscriptionWorker` and `startProcessManager` call
  `withTransaction` internally and pass the bound `Client` to the
  user handler via `HandlerContext.tx`. The handler may use it
  for its own writes (which then commit atomically with
  `advance_subscription`) or ignore it entirely.
- The handler **MUST NOT** issue `BEGIN`/`COMMIT`/`ROLLBACK`
  through `ctx.tx`. Documented in the SDK reference; not enforced
  at runtime in v1 (would require tagging `Client` instances with
  their transaction state — acceptable cost, but deferred).

Candidate (3) is rejected: it defeats the simplicity D-0008 was
buying and reintroduces the at-least-once-pair semantics we
specifically tightened.

**Rationale:** the two shapes serve disjoint audiences. Beginners
and the bank-account example get (2) — they never see a
transaction handle. Applications composing `instructed` with
other Postgres writes get (1) — they own the boundary. The SQL
contract supports both with no change; `advance_subscription` is
already safe inside any well-formed transaction (D-0008 Phase 7
constraint).

**Implications:**

- One sanctioned `BEGIN`/`COMMIT` path means one place to reason
  about lock acquisition for the persist-and-ack tx, matching the
  D-0011/D-0012 lock-set disjointness story.
- The PM dispatch helper requires a *second* `Client` bound to a
  separate connection so dispatch happens in its own transaction
  per the lock-set disjointness story. The SDK takes this as a
  constructor parameter; documentation-enforced.
- Future runtime check ("is this client in a transaction?") is a
  pure SDK-internal addition; the public API does not change.

OQ-0002 is now removed from `open-questions.md`.

---

## D-0013 — Reference SDK is TypeScript on `pg` 8.x

**Date:** 2026-05-17

**Context:** Phase 8 needed a language for the first SDK. The
ROADMAP narrowed it to "TypeScript or Python, to match absurd's
existing stack". Absurd ships three SDKs (`sdks/{go,python,typescript}`);
the TypeScript one is the most complete (≈1.6 kloc, working
worker loop, error-code translation, examples).

**Decision:** the first `instructed` SDK is TypeScript on Node
18+, using `pg` 8.x as a peer dependency, with the same
`Queryable = pg.Client | pg.PoolClient | pg.Pool` shape absurd
uses.

**Rationale:** matching absurd's most-developed SDK lets the
D-0011 absurd-bridge pattern be demonstrated end-to-end in the
same process, with the same driver and the same pool conventions.
It also imports a worked operational baseline for the worker
loop (heartbeat / lease / graceful shutdown) we can adapt rather
than invent.

**Implications:**

- Dual ESM/CJS build mirroring absurd's `tsconfig.build.json` +
  `tsconfig.cjs.json` layout.
- `node --test` against the Phase 7 Docker compose service
  (`instructed_test`); no extra harness in v1.
- Python and Go ports are out of scope for Phase 8 and are
  carried under ROADMAP "Beyond" without further commitment.
- The conformance harness (Phase 9) is intended to be language-
  agnostic in its assertions; the TypeScript SDK's tests should
  not bake in invariants only checkable from JS.

---

## D-0012 — Global ordering via `$all`-as-stream with row-level lock

**Date:** 2026-05-17

**Context:** OQ-0001 had three candidates for the serialisation
point that gives `event_number` its globally-unique,
monotonically-increasing, **gapless** property under concurrent
writers (INV-APPEND-003):

1. **`$all`-as-stream with row-level lock** — what Commanded's
   reference adapter does. A real `streams` row for `$all`
   (`stream_id = 0`); every append issues an
   `UPDATE streams SET stream_version = stream_version + N
    WHERE stream_id = 0 RETURNING ...` that takes the `$all`
   row's row lock for the rest of the transaction, assigns
   contiguous global numbers, and links each event into `$all`
   via `stream_events`. Strong, simple, preserves the invariant
   as written. Serialises every append in the store at the `$all`
   row.
2. **`bigserial`/sequence on `events.event_number`** — cheap and
   highly concurrent, but sequences can skip values (rolled-back
   transactions, sequence cache, restart gaps). Would force
   INV-APPEND-003 to be weakened from "gapless" to "monotone,
   possibly gapped", which in turn would force every downstream
   reader (subscriptions, `$all` reads, the PM cursor, the
   strong-consistency wait helper in D-0010) to be audited for
   gap-tolerance.
3. **`SERIALIZABLE` isolation + `MAX(event_number) + 1`** —
   correct and gapless, but throughput is bounded by the
   serialisation-failure / retry rate. Retries are observable to
   the SDK and compose awkwardly with the SDK's own optimistic-
   lock retry on `wrong_expected_version` (D-0005): a single
   command can now fail for two unrelated reasons that require
   the same recovery action, and operators reading logs would
   see SERIALIZABLE retries as a distinct, opaque error class.

**Decision:** **candidate 1 — `$all`-as-stream with row-level
lock**, as in the reference adapter. The v1 schema includes a
`streams` row with `stream_id = 0` and `stream_uuid = '$all'`,
seeded at install time. `append_to_stream` updates that row inside
its transaction (`UPDATE streams SET stream_version = stream_version
+ N WHERE stream_id = 0 RETURNING stream_version - N AS
initial_event_number`), then links every event into `$all` via
`stream_events` with contiguous numbers starting at `initial_event_number
+ 1`. Lock acquisition order is documented in the
`append_to_stream` docstring: per-stream `streams` row first,
then `$all` `streams` row, then `events`, then `stream_events`.

**Rationale:**

- **INV-APPEND-003 as written.** Candidate 1 is the only option
  that preserves the invariant literally. Candidates 2 and 3
  either weaken the contract or pay for keeping it with
  observable retries. The mapping document, the conformance
  harness target, and several downstream invariants
  (INV-SUB-P-030 "strictly increasing delivery order by
  event_number", CON-010's polling comparison) all assume
  gaplessness; rewriting them for tolerance to sequence gaps is
  a larger surface change than the throughput ceiling justifies
  for v1.
- **Mechanism parity with the reference adapter** is a feature.
  The reference adapter has run in production at non-trivial
  scale under exactly this lock pattern. We inherit the
  empirical confidence that the contention point is not the
  bottleneck most workloads hit first — the per-stream lock and
  the application's own command latency tend to dominate.
- **Lock-ordering simplicity.** `append_to_stream` is already
  the only mechanism that takes a lock (per D-0005, no advisory
  locks; per D-0008, `advance_subscription` does not take a lock
  the SDK could hold). Adding `$all` to the lock set is a single
  additional row, acquired in a fixed order; the disjointness
  property D-0011's Phase 7 input #6 demands is preserved.
- **Forward compatibility.** Should v2 need higher append
  throughput, the `$all` row remains a *mechanism* of
  INV-APPEND-003, not part of the SDK-visible contract.
  Replacing it with a sharded counter, a Redis-backed allocator,
  or anything else that preserves gaplessness is an internal
  schema migration. Replacing it with a non-gapless mechanism
  would be a contract change, which is precisely the change
  we are choosing not to make now.

**Implications:**

- **Schema.** The `streams` table is seeded with one row at
  install time: `(stream_id = 0, stream_uuid = '$all',
  stream_version = 0)`. The `CHECK (stream_uuid <> '$all')`
  constraint from the mapping (INV-STREAM-003) is restated as
  `CHECK (stream_uuid <> '$all' OR stream_id = 0)` so the seed
  row passes while user-supplied collisions still fail.
- **`$all` is a real stream**, queryable via
  `read_all(from_event_number, qty)` (per the ROADMAP
  procedure list). `read_stream` rejects `stream_uuid = '$all'`
  to force callers through the dedicated entry point;
  alternatively, the procedure could route. The chosen shape is
  the dedicated entry point — it makes the asymmetry between
  per-stream and global reads explicit at the SQL surface.
- **`stream_events` is a real table**, not a view. It carries
  `(event_id, stream_id, stream_version, original_stream_id,
  original_stream_version)` exactly like the reference adapter.
  This realises INV-READ-005..008 directly: an `$all` read joins
  through `stream_events.stream_id = 0`, and the projection
  carries the *original* stream identity via
  `original_stream_id` / `original_stream_version`.
- **Concurrent append throughput is bounded by the `$all` row
  lock**, not by per-stream optimistic-lock contention. This is
  the same throughput ceiling Commanded operators have, and is
  recorded in `non-goals.md` only by implication (we make no
  promise of higher global write throughput than Commanded does).
  Higher-throughput mechanisms remain a candidate for
  `maybe-later.md` once Phase 8/9 has produced realistic
  benchmarks. Not currently tracked as ML- — adding it now
  would be premature.
- **Lock-acquisition order is fixed for the lifetime of v1.**
  Documented in the `append_to_stream` docstring as: per-stream
  row (or insert) → `$all` row → events → stream_events. Other
  procedures (`record_snapshot`, `read_subscription_batch`,
  `advance_subscription`, etc.) acquire row locks in disjoint
  sets, per D-0011's Phase 7 input #6.
- **Conformance harness (Phase 9)** can use the reference
  adapter's `$all` semantics directly without translation. The
  bandwidth gap between candidate 1 and candidate 2 will only
  show up in benchmarks, not in correctness tests; we record
  here that benchmarking is a Phase-9-or-later concern.

The entry for OQ-0001 is now removed from `open-questions.md`; its
text is preserved in the **Context** section above per the file's
opening convention.

---

## D-0011 — Compensation is a command; PMs are the saga primitive; side effects bridge to absurd via events

**Date:** 2026-05-17

**Context:** D-0001 committed `instructed` to treating compensation
as a first-class concern and explicitly ruled out "punt to
absurd". PM-030 in `mapping.md` was the artifact left for Phase 6
to resolve. The three remaining candidates: (1) punt — already
ruled out; (2) a first-class saga abstraction inside `instructed`
with forward/compensation step pairing and its own table family;
(3) PMs stay pure (events in → commands out + state), with
side-effecting workflows delegated to absurd tasks and bridged
through the event store.

**Decision:** **candidate 3**. Process managers are the saga
primitive. Compensation is modelled as commands the PM dispatches
in response to failure events. There is no separate `saga`
abstraction, no paired-step DSL, no compensation engine walking
backward through committed steps. When a workflow needs durable
execution of an external side effect (Stripe, email, third-party
API), the PM dispatches a command that produces a `XRequested`
event; an absurd task subscribes to that event, runs the side
effect with its own checkpoint/retry machinery, and appends a
`XCompleted` or `XFailed` event back into the event store on
completion. The PM consumes that returning event like any other.

**Rationale:** the PM contract — durable snapshot-backed state,
ordered at-least-once subscription delivery, co-transactional
persist-and-ack (D-0008), and a dispatch helper that runs the
full load-execute-append cycle — already provides every primitive
needed to express compensating-command flows without inventing
new durability, ordering, or recovery machinery. This satisfies
D-0001's bar ("first-class") because the application is not left
to build saga support itself; it is left to *use* the existing PM
support, knowing that compensation flows are not a special case
but the same shape as forward flows. Candidate 2 was rejected on
four grounds, documented in `sagas.md`: it duplicates the PM
model; linear step lists don't fit reactive workflows that fan in
and out; its "side-effect step" is absurd in disguise and would
either be NIH'd or thin-wrap absurd anyway; it carries real
schema and lock-ordering weight in Phase 7.

**Implications:**

- **No new tables.** PM state remains in the snapshots table per
  PM-020..024. Compensation commands flow through
  `append_to_stream`. The PM's subscription is the same
  persistent-subscription primitive Pass 2 of `mapping.md`
  already specified. Phase 7 inherits zero new schema obligations
  from saga support.
- **PM-011 ordering is unchanged.** The compensating command is
  dispatched in step 3 (`dispatch all returned commands`) of the
  handle → dispatch → apply → persist → ack sequence, indistinguishable
  from a forward command.
- **Failure events become a modelling obligation.** Aggregate
  commands that can permanently fail in a way the saga needs to
  observe MUST produce failure events rather than silently
  returning errors to the dispatcher. This is a documentation /
  SDK-guidance concern, not a contract-level one. The conformance
  harness in Phase 9 does not test for it.
- **Cross-boundary event idempotency.** An absurd task that emits
  an event back into the store after a retry MUST do so with a
  deterministic `event_id` derived from `(task_id, step_name)`,
  so that a re-run hits INV-APPEND-030's `:duplicate_event` path
  and does not double-append. This requires caller-supplied
  `event_id` to remain part of the public contract (it already
  is, per INV-APPEND-001) and the `:duplicate_event` error to be
  surfaced (not reference-only).
- **PM-030 in `mapping.md`** receives a verdict: realised by
  PM-001..024 + 031 — no additional mechanism. The mapping entry
  is updated to record this and to cross-reference D-0011.
- **Tooling cost is acknowledged.** A linear-saga DSL would let
  tooling display "saga is at step 3 of 5". With candidate 3,
  tooling can only show the PM's state field (whatever the
  application put there). Accepted.
- **A future linear-saga helper is not precluded.** Should a
  worked example in Phase 8 produce a linear-and-pairable
  workflow often enough to want sugar, an SDK-level helper that
  generates the right PM clauses from a step-pair declaration is
  compatible with this decision. The helper would compile into
  PMs; it would not become a parallel primitive in the SQL
  contract. Not currently tracked in `maybe-later.md`; revisit
  if Phase 8 demands it.

---

## D-0010 — Strong-consistency-on-dispatch waits on an explicit subscription list

**Date:** 2026-05-17

**Context:** Commanded's strong-consistency-on-dispatch (CON-001..013)
lets a caller pass `consistency: :strong` to `Application.dispatch/2`
and have the call block until every strongly-consistent handler in
the application has acked at least up to the appended events. The
mechanism is a `Commanded.Subscriptions` GenServer holding an ETS
table of `(handler_name, position)`, fed by pubsub from each
handler on every successful ack. `consistency: :strong` (no list)
works because the registry knows which handlers to wait for.

D-0003 already commits us to polling, not pubsub. The remaining
question is: how does the dispatcher know *which* subscription
cursors to poll?

Three shapes:

1. A persistent `consistency_groups` table populated by handler
   registrations on startup; dispatch reads from it. Reintroduces a
   piece of coordinated state across SDK processes.
2. SDK-process-local registry: the SDK process running handlers
   tracks them in-memory. Works only when the dispatcher and the
   handlers share an SDK process. Fragile across deployment
   topologies.
3. **Explicit list at dispatch time.** The caller passes
   `consistency: ["AccountBalanceProjector",
   "OrderPositionsProjector"]`. The SDK polls the named
   subscriptions' cursors. No registry.

**Decision:** v1 supports only the explicit-list form. The SDK
MAY offer an in-process convenience that auto-collects names from
handlers registered in the same SDK instance, but the contract is
the explicit list. `consistency: :strong` (no list) is not
supported.

**Rationale:** the explicit list is the only shape that needs no
cross-process coordination and no extra schema. It is also the
most honest: "which handlers do you care about catching up?" is a
real question the caller usually has an answer to. Implicit `:strong`
in Commanded papers over the fact that not every strongly-
consistent handler is interesting to every caller — commonly the
caller only cares about one or two specific projections.

**Implications:**

- The store exposes `read_subscription_position(stream, name) ::
  bigint` returning `last_seen`. The dispatch helper polls this
  for each named subscription until each is `>= the appended
  event's position`, or until `consistency_timeout` elapses
  (`{:error, :consistency_timeout}`).
- Latency is bounded below by the polling interval (per D-0003).
- The SDK has freedom to layer an auto-collection convenience over
  the explicit-list primitive; the SQL contract stays minimal.
- Conformance harness: the `consistency: :strong` shorthand test
  case is skipped or rewritten to use the list form; documented in
  `non-goals.md`.

---

## D-0009 — `delete_subscription` on a missing subscription is an error

**Date:** 2026-05-17

**Context:** Commanded's abstract adapter contract specifies that
`delete_subscription` on a non-existent subscription returns
`{:error, :subscription_not_found}` (INV-SUB-P-062). The reference
adapter actually returns `:ok` silently — a documented divergence
in `invariants.md` where the reference adapter is *more lenient*
than its own contract.

**Decision:** `instructed` matches the abstract contract: deleting
a subscription that does not exist returns the
`subscription_not_found` error. We do not adopt the reference
adapter's silent success.

**Rationale:** silent success on a missing target hides operational
bugs (typo in subscription name; deleting the wrong tenant's
subscription). The error is cheap to surface and easy for callers
to swallow if they want idempotent delete. The reverse — reading
lenient behaviour out of a strict contract — is impossible.

**Implications:**

- Conformance harness (Phase 9) will test for the error.
- SDK helpers may offer an `ignore_missing: true` option as a
  convenience for idempotent teardown, but that lives in the SDK,
  not the SQL contract.

---

## D-0008 — Cursor advance is co-transactional with handler writes [SUPERSEDED BY D-0016]

**Superseded 2026-05-17 by D-0016.** D-0016 reverses this
decision: the projection target is application-domain (often
not Postgres at all), so the co-transactional property could
not be provided uniformly and the API plumbing required to
provide it for the Postgres case was paid by every user.
Handlers are now opaque to the SDK; cursor advance happens in
its own short transaction after the handler returns. See D-0016
for full context. The entry below is preserved for the audit
trail.

## D-0008 — Cursor advance is co-transactional with handler writes (original)

**Date:** 2026-05-17

**Context:** Commanded's reference adapter advances the persistent
subscription cursor as a separate SQL statement *after* the handler
returns. The atomicity of the handler's projection write and the
cursor advance is explicitly *not* provided (HND-031). Applications
that need a projection to be exactly-once-consistent with the cursor
use strong-consistency-on-dispatch (Part E of `guarantees.md`) or
build idempotency keys into their projections.

**Decision:** `instructed`'s `advance_subscription` stored procedure
is callable from within an SDK-opened transaction so that the
projection write and the cursor advance commit together (or roll
back together). The SDK's handler loop:

```
BEGIN;
  -- handler does its projection writes
  CALL advance_subscription(name, last_event_number);
COMMIT;
```

This is **tighter** than Commanded: with this pattern, a
successfully-committed handler invocation has both written its
projection and advanced its cursor; a crash mid-handler rolls back
both; redelivery is at-least-once at the *transaction* level rather
than at the (write, advance) pair level. Idempotency on the
projection side is now optional rather than mandatory.

**Rationale:** Postgres already has the right primitive (the
transaction). The reference adapter doesn't use it because
Commanded handlers run in a separate process from the event store
and can't share a transaction across the BEAM boundary. The SDK
does not have that constraint — it owns the connection.

**Implications:**

- Cursor advance is **not** required to be co-transactional; an SDK
  may also do projection-then-advance in separate transactions and
  rely on application-level idempotency. The contract supports both
  patterns; the recommended pattern is co-transactional.
- `advance_subscription` MUST be safe to call from any transaction
  that holds no conflicting locks. In particular it must not take a
  lock that the SDK could plausibly hold from earlier statements.
  This becomes a lock-ordering constraint for Phase 7.
- Selectors (INV-SUB-P-050) that skip events still advance the
  cursor in this transaction; the SDK passes the highest
  *delivered-or-skipped* event_number.

---

## D-0007 — Drop transient subscriptions from v1

**Date:** 2026-05-17

**Context:** Commanded's adapter exposes `subscribe(meta, stream)`
for transient, fire-and-forget pub/sub (INV-SUB-T-001..005). The
store pushes `{:events, events}` messages to the subscriber
process; no cursor, no ack, lost on process exit. Commanded uses
this internally for the aggregate's self-subscription (AGG-025) and
for the `Subscriptions` registry's strong-consistency notifications
(CON-002..003).

In `instructed`, both internal uses are gone: D-0004 drops the
aggregate cache (so no AGG-025), and CON-* (Pass 3) will be
realised by polling persistent cursors per D-0003.

Applications also call `subscribe/2` directly when they want a
live event tail. Realising that in Postgres without push requires
either (a) ad-hoc polling — which is what persistent subscriptions
already are — or (b) `LISTEN`/`NOTIFY`, which D-0003 puts out of
v1.

**Decision:** v1 has no transient-subscription primitive in the
SQL contract. Live tails are expressed as a persistent
subscription with `start_from: :current` plus a teardown call when
the consumer is done. The SDK MAY offer an ergonomic
`tail(stream, fn)` helper that wraps this pattern.

**Rationale:** transient subscriptions are a BEAM ergonomic, not a
CQRS/ES semantic. Removing them simplifies the contract surface
and keeps every event-delivery path going through the same
cursor-and-claim primitives, which means there is one place to
reason about ordering, ack, and redelivery.

**Implications:**

- INV-SUB-T-001..005 are dropped from the realised contract.
- The conformance harness (Phase 9) will skip the transient-
  subscription test cases and the divergence is explicit in
  `non-goals.md` (Phase 5).
- The transient "live tail" use case is satisfied by persistent
  subscriptions; the SDK helper hides the create/teardown pair.

---

## D-0006 — Subscriptions are leased, not session-locked

**Date:** 2026-05-17

**Context:** Commanded's reference adapter implements
single-active-subscriber (INV-SUB-P-010..012) with
`pg_try_advisory_lock` held for the lifetime of the database
session. When the session closes (worker exits, network drop), the
lock is released automatically and another subscriber can attach.
This ties subscription ownership to connection ownership, which
rarely lines up with worker process ownership when a connection
pool sits in between.

Absurd's task scheduler solves the analogous problem with a
row-level lease: `claimed_by TEXT, claim_expires_at TIMESTAMPTZ`
on each task, with `claim_task` (allocate), `extend_claim`
(heartbeat), and timeout reclamation built into the next claim.

**Decision:** `instructed`'s `subscriptions` table carries
`claimed_by TEXT NULL` and `claim_expires_at TIMESTAMPTZ NULL`.
The SDK calls:

- `claim_subscription(name, worker_id, lease_secs)` — atomically
  acquires the subscription if unclaimed *or* if the existing
  claim has expired. Returns `:ok` with the cursor, or
  `:already_claimed` with the current holder for diagnostics.
- `extend_subscription_claim(name, worker_id, lease_secs)` —
  heartbeat. Fails if `claimed_by <> worker_id`, which is the
  signal that the worker has lost the subscription and must stop.
- `release_subscription(name, worker_id)` — clean release on
  graceful shutdown.

We do **not** use `pg_advisory_lock` for subscription claim.

**Rationale:** leasing decouples claim lifetime from connection
lifetime, matches absurd's pull-based shape, and survives the
connection-pool middlebox cleanly (a returned connection does not
release the lease). It does introduce one new operational concern
— lease TTL tuning — but that knob is also the natural place to
express "how long do we tolerate a silent worker before another
takes over". The absurd codebase has working production
intuitions for this we can borrow.

**Implications:**

- INV-SUB-P-010..012 are realised by lease semantics rather than
  by session locks.
- The SDK worker loop runs a heartbeat alongside its processing
  loop. If `extend_subscription_claim` fails the worker MUST stop
  processing immediately; otherwise it risks double-delivery with
  the new holder. (This becomes a real correctness boundary,
  documented in the SDK.)
- A crashed worker keeps the subscription unavailable until its
  lease expires. Default lease TTL needs to balance fast failover
  vs. tolerating GC pauses; tuning is deferred to Phase 7/8.
- The `concurrency_limit` knob (INV-SUB-P-010) is fixed at 1 in v1
  per D-0002; the lease realisation generalises naturally to N by
  adding a `shard` column (per ML-0001's forward-compat note).

---

## D-0005 — Per-aggregate command serialisation via optimistic-lock retry, not advisory locks

**Date:** 2026-05-17

**Context:** Commanded serialises concurrent commands targeting the
same aggregate via the GenServer mailbox (AGG-011, classed as a BEAM
mechanism). `instructed` has no per-aggregate process to inherit this
from. Two realisations are available in Postgres:

1. **Optimistic locking + retry.** Each command does its own
   load-execute-append. Concurrent commands on the same aggregate race
   at append; INV-APPEND-013 guarantees that at most one succeeds for
   a given expected_version, and AGG-010 mandates that the loser
   re-loads (cheaply, picking up the winner's events) and re-evaluates.
2. **Per-aggregate advisory lock** for the duration of
   load-execute-append. Two commands on the same aggregate then queue
   on the lock; neither ever fails its append.

**Decision:** v1 uses optimistic-lock retry (option 1). Advisory locks
are not used to serialise commands.

**Rationale:** option 1 keeps `append_to_stream` the only place that
holds a lock, which keeps lock ordering trivial. Optimistic retry
composes with the snapshot-based hydration path without needing extra
state to be released on connection death. Advisory locks tied to a
session leak responsibility into connection-pool behaviour, which is
the class of bug we are trying to avoid.

**Implications:**

- Hot aggregates with many concurrent commands will retry. The retry
  budget (`AGG-010`, configurable) becomes a real tuning knob.
- The SDK's load-execute-append loop must be cheap enough that retry
  is a viable strategy. This is the same property that makes D-0004
  acceptable.
- An advisory-lock variant can be added later as an opt-in
  optimisation for write-hot aggregates without changing the SQL
  contract; tracked informally for now (no ML- entry yet — revisit
  in Phase 8 if benchmarking demands it).

---

## D-0004 — No in-memory aggregate cache; rehydrate on every command

**Date:** 2026-05-17

**Context:** Commanded keeps each active aggregate as a long-lived
GenServer holding its current state (AGG-004). Subsequent commands
skip the load step. This is classified BEAM-mechanism in
`guarantees.md` — it is an optimisation, not a semantic guarantee.
The same mechanism props up AGG-011 (mailbox serialisation),
AGG-025 (self-subscription to catch external writes), and AGG-030
(lifespan).

**Decision:** `instructed` does not cache aggregate state between
commands. Every command runs a fresh load (snapshot + events since)
→ execute → append. There is no in-process registry of running
aggregates and no `aggregate_lifespan` concept.

**Rationale:** the core hypothesis is that a thin SDK over Postgres
is enough. An in-memory cache reintroduces the registry/coordination
problems we set out to avoid (who owns the cache? what happens on
node failure? how do two SDK instances see consistent state?).
Snapshotting (Part D of `invariants.md`) is the load-cost mitigation;
it is more honest than caching because it does not require coherence
across processes.

**Implications:**

- AGG-004, AGG-011, AGG-025, AGG-030 disappear as mechanisms. The
  *semantics* they backed are either intrinsic (AGG-011 → see D-0005)
  or not needed (AGG-025 is unnecessary without a cache; AGG-030
  vanishes with the cache).
- Aggregate load cost becomes the dominant per-command cost. The
  snapshot policy (SNAP-001..002) and its tuning matter more than
  they do in Commanded.
- An SDK-level cache can be added later as a pure performance
  optimisation if a single SDK process owns a hot aggregate, but it
  must not become a correctness boundary. Not tracked as ML- yet;
  revisit if Phase 8 benchmarks justify it.

---

## D-0003 — Polling only; no `LISTEN`/`NOTIFY` in the contract

**Date:** 2026-05-17

**Context:** Some operations — notably waiting for a projection to catch
up after a command dispatch, in order to provide pseudo-strong-consistency
to a caller — could be served either by polling the subscription cursor
or by listening on a Postgres notification channel emitted at append time.

**Decision:** the SQL contract does *not* require or use `pg_notify`.
SDKs poll. `LISTEN`/`NOTIFY` may be added later as a transparent
latency optimisation, but is not part of the v1 contract and no
correctness property may depend on it. Tracked as ML-0002 in
[`maybe-later.md`](maybe-later.md).

**Implications:**

- Strong-consistency-on-dispatch incurs a polling latency floor (driven
  by the SDK's poll interval, not by Postgres).
- No background `LISTEN` connections are required of host applications.
- Simpler operational surface; no notification-channel naming scheme to
  design.

---

## D-0002 — One subscription = one cursor = one active worker (v1)

**Date:** 2026-05-17

**Context:** Commanded supports concurrent consumption of a single
subscription via a `partition_by` function (events are hashed to N
workers; order is preserved within a partition). Realising this in
Postgres needs either (a) a side table of per-shard offsets atomic with
projection writes, or (b) N independently-named subscriptions over
disjoint shards. Both add real complexity and neither is needed to
demonstrate the core hypothesis.

**Decision:** in v1, a subscription has exactly one cursor advanced
atomically with the consumer's work, and at most one worker holds the
lease at any time. Concurrent partitioned consumption is deferred and
tracked as ML-0001 in [`maybe-later.md`](maybe-later.md).

**Implications:**

- Throughput of a single projection is bounded by one worker's
  serial processing rate. Applications that need more throughput must,
  for now, split into multiple named subscriptions.
- Cursor advance can be made trivially atomic with projection writes
  (same transaction), which is the property that matters most.
- The "concurrent partitioned subscription" design space stays open for
  a future phase; nothing in v1 should preclude it.

---

## D-0001 — Saga rollback / compensation is a first-class concern, mechanism TBD

**Date:** 2026-05-17

**Context:** Absurd's task/step model checkpoints forward progress only;
there is no native "undo step N-1 because step N failed permanently"
primitive. CQRS/ES sagas frequently require compensating actions
(canonical example: book-hotel succeeds, book-flight fails, cancel-hotel
must run). Pushing this entirely onto application code loses what is
arguably the most distinctive idiom of sagas.

**Decision:** `instructed` will treat compensation as a first-class
concern. The mechanism is not yet decided — see Phase 6 in
[`ROADMAP.md`](ROADMAP.md) — but the option of "punt everything to
absurd" is ruled out by this decision.

**Implications:**

- Saga / process manager design cannot be finalised until Phase 6.
- `instructed` will likely own its own table family for saga state
  rather than fully delegating to absurd, even if absurd is used for
  fire-and-forget side-effect tasks.
- The SQL contract from Phase 7 must reserve room for saga tables.
