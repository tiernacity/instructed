# Roadmap

This is a living document. It describes the phases of the `instructed`
exploration, what each phase produces, and what must be true before the
phase is considered done. Phases are largely sequential but artifacts from
earlier phases will be revisited and refined as later phases expose gaps.

Each phase produces one or more documents under `docs/`. Decisions made
along the way are recorded in [`decisions.md`](decisions.md) as short
ADR-style entries. Open questions surfaced but not yet resolved are
tracked in [`open-questions.md`](open-questions.md) (created when the
first question lands). Capabilities deliberately deferred — things we
likely *will* want eventually but are out of scope for v1 — are
recorded in [`maybe-later.md`](maybe-later.md), each with the
forward-compatibility constraints they impose on v1 design.

## Guiding principles (recap)

- Postgres is the core. Invariants live in SQL — schema constraints,
  stored procedures, careful lock ordering.
- Pull-based, no coordinator. Workers claim work via leases; there is no
  push channel and no global registry.
- SDKs are thin. They encode the call sequence and a worker loop; they
  never hold invariant-bearing state.
- Honest about trade-offs. Where we lose a guarantee Commanded provides,
  we say so explicitly rather than reinvent it.

---

## Phase 1 — Framing — **done**

**Goal:** establish what we are doing, in what order, and capture the
decisions already made.

**Artifacts:**

- `README.md` — one-paragraph orientation.
- `docs/ROADMAP.md` — this document.
- `docs/decisions.md` — seeded with decisions made during initial
  exploration.

**Done when:** the above are in the repository.

---

## Phase 2 — Invariants extracted from Commanded's adapter contract — **done**

Produced `docs/invariants.md`. Surfaced one open question (OQ-0001 on
concurrent `:any_version` append ordering).

**Goal:** produce a precise, mechanical specification of what a CQRS/ES
event store must guarantee, derived from Commanded's adapter interface
and its conformance tests — not from prose docs.

**Inputs:**

- `commanded/lib/commanded/event_store/adapter.ex` (the 10-callback
  contract).
- `commanded/lib/commanded/event_store/{event_data,recorded_event,snapshot_data,subscription}.ex`
  (the data shapes that cross the contract boundary).
- The Commanded test suite for adapter conformance (this is the *real*
  spec — more precise than the docs).

**Approach:**

For each callback, document:

- Inputs and outputs.
- Atomicity guarantees (single statement? transactional set?).
- Ordering guarantees (per-stream, global, both?).
- Error cases as a closed set with their semantics (`wrong_expected_version`,
  `stream_not_found`, `subscription_already_exists`, …).
- Idempotency expectations on the caller side.
- What the caller is allowed to assume after a successful return, and
  after each kind of error.

The output is a catalogue of *constraints*, not yet a design. We are
deliberately not deciding how Postgres will realise them in this phase.

**Artifact:** `docs/invariants.md`.

**Done when:** every adapter callback and every field on `RecordedEvent`
has been accounted for, and the catalogue has been re-read end-to-end
without producing new questions.

---

## Phase 3 — Guarantees of the layers above the event store — **done**

Produced `docs/guarantees.md`. Classified each guarantee as intrinsic
(I), BEAM-mechanism (B), or convenience (C) — the basis for Phase 4
mapping decisions. The most important findings:

- Per-aggregate command serialisation is a BEAM mechanism (AGG-011);
  `instructed` gets it from optimistic-lock retry or an advisory lock,
  not from a registered process.
- Process manager state is persisted **as a snapshot** in the
  event store (PM-020..024), keyed by
  `"<pm_name>-<process_uuid>"`. The snapshot's `source_version`
  doubles as the per-instance `last_seen_event`. We can reuse the
  same trick.
- Strong-consistency-on-dispatch (CON-001..013) is semantically
  intrinsic but mechanism-BEAM (in-VM pubsub + ETS). Our polling
  variant is semantically equivalent but higher-latency.
- Compensation in Commanded is *not* first-class (PM-030) — it is
  whatever commands a PM dispatches. D-0001 already commits us to
  doing better.

**Goal:** the adapter contract is necessary but not sufficient. Commanded
adds guarantees on top of it — per-aggregate command serialization, retry
on version conflict, snapshot policy, subscription delivery semantics,
strong-consistency-on-dispatch, process manager state persistence. We need
the same kind of mechanical catalogue for these.

**Inputs:**

- `commanded/lib/commanded/aggregates/*` (aggregate process, state
  builder, execution context, lifespan).
- `commanded/lib/commanded/event/handler.ex` and supervisor.
- `commanded/lib/commanded/process_managers/*`.
- `commanded/guides/{Aggregates,Commands,Events,Process Managers,Read Model Projections}.md`
  as cross-reference, not as primary source.

**Approach:** for each guarantee, write down what it is, what mechanism
in Commanded provides it (almost always: a GenServer holding state, plus
OTP registration), and what failure modes it covers.

**Artifact:** `docs/guarantees.md`.

**Done when:** every Commanded-level guarantee has an entry and the
mechanisms have been classified as (a) intrinsic to CQRS/ES, (b)
optimisation specific to BEAM/OTP, or (c) ergonomic convenience.

---

## Phase 4 — Postgres-native realisation of each invariant — **done**

Produced `docs/mapping.md` in three review passes (append/read/aggregate;
snapshots/subscriptions/handlers; PMs/consistency/dispatch). Every
Commanded invariant and guarantee has a verdict with the single
deliberate exception of **PM-030 (compensation)**, which is parked
for Phase 6 per D-0001. Ten new design decisions (D-0004..D-0010
plus three from earlier phases) and three open questions
(OQ-0001..OQ-0003) were recorded along the way. The principal
tightenings over Commanded are co-transactional cursor advance
(D-0008) and schema-enforced reservations; the principal
loosenings are no in-memory aggregate cache, no transient
subscriptions, polling-floor on strong-consistency, and no
`consistency: :strong` shorthand.

**Goal:** for every entry in `invariants.md` and `guarantees.md`, propose
how `instructed` realises it (or deliberately drops it).

**Approach:** column-per-entry table or one section per entry, with:

- Commanded mechanism.
- Proposed `instructed` mechanism (schema element, stored proc, SDK
  convention, or "dropped").
- Whether the resulting guarantee is *tighter*, *equivalent*, or *looser*
  than Commanded's, and why that is acceptable.

This phase will force us to make many small decisions. Each one that
isn't obvious gets a new entry in `decisions.md`. Each one we can't yet
resolve goes to `open-questions.md`.

**Artifact:** `docs/mapping.md`.

**Done when:** every Commanded invariant/guarantee has either a proposed
realisation or an explicit decision to drop it.

---

## Phase 5 — Non-goals, made explicit — **done**

Produced `docs/non-goals.md`. Fourteen non-goals (NG-0001..NG-0014)
consolidated from Phase 4 mapping work, organised by concern
(coordination, delivery, strong consistency, data model,
convenience). The document includes a cross-reference table back to
`mapping.md` so every "dropped" / "looser by elimination" verdict
there is accounted for, and an explicit "what is *not* a non-goal"
section distinguishing non-goals from `maybe-later.md` entries and
from open questions.

**Goal:** write down what `instructed` does *not* do. Things like:

- No in-cluster registry of running aggregates.
- No push subscriptions; pull only.
- No synchronous strong-consistency on dispatch without a deliberate wait
  in the SDK.
- No in-memory caching of aggregate state between commands.
- Anything else that emerges from Phase 4.

Writing this down early prevents accidental drift back toward
Commanded-shaped solutions.

**Artifact:** `docs/non-goals.md`.

**Done when:** the document has been cross-checked against `mapping.md`
and any "we're not doing X" mentioned there has a corresponding entry.

---

## Phase 6 — Saga / process manager strategy — **done**

Produced `docs/sagas.md` in three review passes (survey absurd /
lay out candidates; take a position; schema and SDK implications
plus Phase 7 inputs). **D-0011** records the verdict: candidate 3
— PMs are the saga primitive, compensation is a command, side
effects bridge to absurd via events. No new tables; PM-011's
handle → dispatch → apply → persist → ack ordering is unchanged
(compensating commands dispatch in step 3 alongside forward
commands). PM-030 in `mapping.md` now has a verdict and a
cross-reference to D-0011. Eight forward-pointing constraints
handed to Phase 7 — most notably that `event_id` must stay
caller-supplied and `:duplicate_event` must be promoted from
reference-only to a public error so the absurd-bridge idempotency
pattern works.

**Goal:** decide how `instructed` handles long-running, multi-step
workflows with the ability to express compensating actions.

**Key open question (already surfaced):** absurd's task/step model is
forward-only — checkpoints cache completed work but there is no native
"undo" primitive. CQRS/ES sagas often need compensation
(book-hotel → book-flight-fails → cancel-hotel). Three candidates:

1. Punt entirely to absurd; compensation is the application's problem.
2. Add a small saga abstraction inside `instructed` with explicit
   forward-step / compensation-step pairing.
3. Process managers in `instructed` are pure (events in → commands out
   + state); side-effecting workflows are delegated to absurd by
   spawning tasks.

This is a design question, not a research one, so it lands here once we
understand the invariants well enough to evaluate the candidates.

**Artifact:** `docs/sagas.md`.

**Done when:** a choice is made and recorded in `decisions.md`, and the
implications for the schema and SDK are written into `mapping.md`.

---

## Phase 7 — First-cut SQL contract — **done**

Produced `sql/instructed.sql` (schema + 12 plpgsql procedures with
full docstrings and working bodies), `docs/sql-contract.md` (human-
oriented reference: error-code catalogue, lock-ordering summary,
recommended call patterns), and `sql/migrations/` (absurd-style
directory; empty until first tag). Resolved OQ-0001 via D-0012
(`$all`-as-stream with row-level lock); no new decisions or open
questions in Pass 3. Validated against a live Postgres: clean
install into an empty schema, every error SQLSTATE in the closed
catalogue (`IS001..IS006, IS010, IS020, IS022, 22023`) triggered
and caught, and two-session concurrent `any_version` appends
serialised correctly through the `$all` row lock.

**Goal:** the SQL file is the spec. Before any SDK code, produce a
schema and a set of stored procedure signatures with their pre/post
conditions, error codes, and lock ordering documented in comments —
in the same style as `absurd/sql/absurd.sql`.

**Approach:**

- Schema: events, snapshots, subscriptions, process manager instances,
  (saga tables if Phase 6 went that way).
- Procedures: `append_to_stream`, `read_stream`, `read_all`,
  `record_snapshot`, `read_snapshot`, `claim_subscription`,
  `advance_subscription`, `release_subscription`, and whatever Phase 6
  introduced.
- Each proc: documented invariants, error SQLSTATEs, lock-acquisition
  order (cf. absurd's `await_event`/`emit_event`).
- Migration story: follow absurd's pattern — single canonical SQL file
  plus a `migrations/` directory.

**Artifact:** `sql/instructed.sql` plus `docs/sql-contract.md`
(human-oriented reference).

**Done when:** the SQL file installs cleanly into an empty Postgres,
and every procedure has a docstring covering inputs, outputs, error
cases, and lock ordering.

---

## Phase 8 — Reference SDK

**Goal:** a single-language SDK that exercises the SQL contract and lets
us write the first end-to-end example (bank account, the canonical CQRS
demo).

**Language:** to be decided in Phase 7 or early Phase 8. Likely
TypeScript or Python to match absurd's existing stack.

**Scope:**

- Aggregate load-execute-append loop with optimistic-locking retry.
- Snapshot policy hook.
- Subscription worker loop (claim/process/advance/release).
- Process manager worker (subscription + state + emit commands).
- Polling-based wait-for-projection helper for pseudo-strong-consistency
  on dispatch.

**Artifact:** `sdks/<lang>/` plus a worked example.

**Done when:** the bank account example runs against Postgres, with
projections and at least one process manager.

---

## Phase 9 — Conformance harness

**Goal:** port (or mirror) the Commanded adapter conformance tests so
that we can demonstrate `instructed` provides the invariants from
Phase 2. This is the moment where we discover whether the "looser"
guarantees from Phase 4 cause real problems.

**Artifact:** `tests/conformance/`.

**Done when:** the harness runs green, and any deviations from the
Commanded conformance set are explained in `non-goals.md`.

---

## Beyond

- Additional SDKs (Python, Go, Elixir).
- `instructedctl` tool, modelled on `absurdctl`.
- A habitat-equivalent UI, if useful.
- Performance work: index strategy, partition strategy for the events
  table at scale, snapshot cadence.

These are out of scope until Phase 9 is done.
