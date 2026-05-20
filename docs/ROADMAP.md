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
(D-0008, later **superseded by D-0016 in Phase 8** — the
user-handler is now opaque to the SDK and idempotency is the
application's concern, matching Commanded's contract; only the
SDK-internal snapshot+ack pair for PMs remains co-transactional)
and schema-enforced reservations; the principal
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

## Phase 8 — Reference SDK — **done**

Design signed off (D-0013 — TypeScript / Node 18+ / pg 8.x;
D-0015 — API shape; D-0016 — handlers opaque to SDK; D-0017 —
causation/correlation defaulting; D-0018 — worker lifecycle).
`docs/sdk-design.md` is the authoritative design; `sql/instructed.sql`
is the spec it wraps. Implementation sequencing comes from
`sdk-design.md` §10 (eight steps; each lands as its own commit
with tests against the docker-compose Postgres).

**Status:**

- **Step 1/8 — done.** `Client` (Layer 0) + `errors.ts`. Dual ESM/CJS
  build wiring, one method per stored procedure, SQLSTATE → typed
  `Error` subclass translation. 28 cases covering every procedure and
  every SQLSTATE in the closed catalogue (IS001–IS010, IS020–IS022,
  IS006, 22023).
- **Step 2/8 — done.** `aggregate.ts` (Layer 1) + OCC-retry test.
  `runCommand` snapshot-load → page through events folding `apply` →
  `execute` → `appendToStream(expected.exact(V))` with retry on IS001
  (D-0005 / mapping.md AGG-010). Causation/correlation defaulting
  (§11.8); `apply` receives the §11.3 DomainEvent shape;
  `everyN(n)` snapshot policy. D-0019 records four non-obvious
  choices (retryBudget semantics, explicit-expectedVersion no-retry,
  snapshot-write-failure non-fatal, internal page size = 500).
- **Step 3/8 — done.** `subscription.ts` (Layer 2) +
  heartbeat-lease-loss test. `startProjection` realises §3 layer 2 /
  §8 / §11.1 / §11.5 / §11.9 / D-0016 / D-0018: handler opaque
  (`ctx = { workerId, position, signal }`), two short SDK-owned
  transactions bracket the handler, abort fires immediately on
  `close()` / lease loss while the SDK still awaits the in-flight
  handler, no advance after abort, heartbeat retries once with 250ms
  before escalating, handler-throws backoff = [250,500,1k,2k,4k,8k,16k]
  capped at 30s with no max-attempts in v1.

- **Step 4/8 — done.** `process-manager.ts` (Layer 3) +
  single-event smoke test. PM routes per-event-type, loads instance
  state from a snapshot, runs `handle` outside any SDK tx (D-0016),
  dispatches each command via `runCommand` on the separate
  `dispatchClient` (D-0011 / D-0012) with causation = triggering
  event id and correlation propagated (§11.8 / D-0017), then
  writes snapshot + advance in one short SDK-internal tx via the
  new `internal/with-transaction.ts` helper. `{kind:'start'}` is
  lenient (§11.4); same-`Client` persist/dispatch throws at
  construction (D-0011 / D-0012). Causation override hook added to
  `RunCommandOptions` (§11.8). Aggregate runner now treats
  `StreamNotFound` during load as an empty stream when
  `version == 0` so PMs can drive first-ever commands to fresh
  aggregates. Choices recorded in D-0020.

**Remaining:**

- **Step 5/8 — done.** `consistency.ts` (Layer 4)
  `waitForProjection` + timeout test. Polls
  `readSubscriptionPosition` until each named subscription's
  `lastSeen >= target` (event_number for `$all`; stream_version
  per-stream). Throws `ConsistencyTimeout({waitedMs, missing})`
  on timeout. Treats `IS020` (subscription not yet created) as
  "not caught up" and keeps polling. No `:strong` shorthand
  (D-0010). Also fixed a latent bug in `subscription.ts` /
  `process-manager.ts`: per-stream subscriptions now advance the
  cursor using `stream_version`, not `event_number` (sql/instructed.sql
  :: advance_subscription). Existing projection tests only covered
  `$all` so this hadn't surfaced.
- **Step 6/8 — done.** `instructed.ts` (Layer 5 facade). Thin
  composition over 0–4: `registerAggregate` /
  `registerProjection` / `registerProcessManager`, `dispatch`
  (by-name aggregate lookup raising `UnknownAggregateType`),
  `startWorker` (fans out to one subscription loop per registered
  projection / PM under one handle). `dispatch` accepts a
  `consistency` list — bare strings as sugar for `$all`
  subscriptions — and delegates the wait to
  `waitForProjection` (no `:strong` shorthand, D-0010). Dispatch
  pool materialises eagerly on `registerProcessManager` (or via
  `dispatchClient()`); a process that only registers aggregates
  never opens a second pool. Owned pools are closed by
  `close()`; user-supplied pools / Queryables are left alone.
  Tests: registry lookup + miss, fan-out, lazy materialisation
  with both connection-string and Pool inputs, consistency wait.
- **Step 7/8 — done.** `sdks/typescript/examples/bank-account/`
  ships an `Account` and a `Transfer` aggregate, a `Balances`
  projection on `$all`, and a `TransferProcessManager` realising
  D-0011 compensation-by-refusal: `WithdrawalRefused` stops the PM,
  no compensating command needed; `Withdrawn` → `Deposited` is the
  forward path. `main.ts` runs end-to-end against the
  docker-compose Postgres and prints the final balances
  (700 / 300). `test/bank-account.test.ts` exercises the same
  modules under `npm test` (77 total).
- **Step 8/8 — done.** Top-level `README.md` updated: status flipped
  to "Phase 8 done", all five layers summarised with their D- entries,
  bank-account example called out with its run command.

**Language:** TypeScript / Node 18+, `pg` 8.x peer dependency
(recorded as D-0013).

**Artifact:** `sdks/typescript/` plus `sdks/typescript/examples/bank-account/`.

**Done when:** the bank-account example runs against Postgres, with
the `Balances` projection and the `TransferProcessManager` working
end-to-end, and `README.md` points at both.

---

## Phase 9 — Conformance harness — **done**

**Goal:** port (or mirror) the Commanded adapter conformance tests so
that we can demonstrate `instructed` provides the invariants from
Phase 2. This is the moment where we discover whether the "looser"
guarantees from Phase 4 cause real problems.

**Artifact:** `tests/conformance/`.

**Done when:** the harness runs green, and any deviations from the
Commanded conformance set are explained in `non-goals.md`. ✅

**Status:**

- 113 active cases + 3 skipped (deferred per D-0024) green and stable
  across repeated runs. Total `node --test` line: 116 tests, 113
  pass, 3 skipped, 0 fail.
- INV-coverage matrix (`npm run coverage`): 54 covered, 3 deferred,
  7 dropped, 1 above-line, 0 missing across Parts B–F. The reporter
  exits non-zero on any "missing" row so regressions surface in CI.
- Non-goals reconcile: the existing cross-check table in
  `non-goals.md` already accounted for every dropped / looser-by-
  elimination verdict in `mapping.md`; the harness surfaced no new
  divergences and no NG-* entries needed to be added.
- D-0021..D-0024 (the design pass) survived implementation
  unchanged. The hand-rewrite-by-INV approach (D-0022) paid off:
  organising cases by invariant rather than by Elixir source-file
  made the coverage matrix mechanical, and the SQL-only substrate
  (D-0021) means future SDKs inherit adapter-line conformance with
  zero porting work.

**Design pass — done.** Four shape decisions landed as D-0021..D-0024
(resolving OQ-0004..OQ-0007):

- **D-0021** — SQL-only harness in `tests/conformance/`, TypeScript +
  `node --test` + `pg`, no SDK in the loop. The conformance contract
  belongs to the store.
- **D-0022** — Hand-rewrite cases organised by INV-* identifier; the
  Elixir `*_test_case.ex` files act as a coverage checklist in
  `tests/conformance/COVERAGE.md`. INV-coverage matrix printed on
  green run.
- **D-0023** — Adapter-line scope only (Parts B–F of `invariants.md`).
  AGG-* / HND-* / PM-* / CON-* / DSP-* stay in the SDK test suite.
- **D-0024** — INV-SUB-P-040..042 (partitioned consumers) land as
  `test.skip` with `// deferred — ML-0001 / D-0002` tags so the
  coverage matrix distinguishes deferred from missing.

**Sub-step sequencing.** Each step lands as its own commit, tagged
with the INV-* identifiers it covers.

- **Step 1/8.** `tests/conformance/` workspace package skeleton:
  `package.json`, `tsconfig.json`, `fixtures.ts` (lifted pattern
  from `sdks/typescript/test/fixtures.ts`), an empty
  `coverage-report.ts` reporter, and an initial `COVERAGE.md`
  cross-referencing the three Elixir `*_test_case.ex` files. One
  smoke test that runs `truncateAll` and asserts the schema is
  installed. No invariant cases yet.
- **Step 2/8.** Part B — Append. `append.test.ts` covers
  INV-APPEND-001..007 (identity, ordering, atomicity),
  INV-APPEND-010..014 (expected-version semantics, all four shapes),
  INV-APPEND-020..022 (concurrent appends — two-session
  tests against the docker-compose Postgres),
  INV-APPEND-030 (duplicate `event_id`),
  INV-APPEND-040..041 (immutability triggers, no hard-delete in v1).
- **Step 3/8.** Part C — Read. `read.test.ts` covers
  INV-READ-001..004 (`stream_not_found`, order, inclusive start,
  exhaustiveness),
  INV-READ-005..008 (`$all` semantics — original stream identity
  echoed via `read_all`, global ordering by `event_number`),
  INV-READ-020 (reader/append concurrency, MVCC behaviour).
- **Step 4/8.** Part D — Snapshots. `snapshot.test.ts` covers
  INV-SNAP-001..006 (at-most-one-per-source, upsert semantics,
  `snapshot_not_found`, idempotent delete, no history, advisory).
- **Step 5/8.** Part E persistent — `subscription-persistent.test.ts`
  covers INV-SUB-P-001..002 (identity + idempotent re-subscribe),
  INV-SUB-P-010..012 (single-active-subscriber under the lease
  model; failover after TTL),
  INV-SUB-P-020..021 (`start_from` honoured on first create, ignored
  thereafter),
  INV-SUB-P-030..034 (strictly-increasing delivery, at-least-once,
  cursor advance, monotonicity, out-of-order ack absorption),
  INV-SUB-P-060..062 (release — cursor stays; delete — cursor gone;
  delete-missing returns IS020 per D-0009).
- **Step 6/8.** Part E partitioned —
  `subscription-partitioned.test.ts` lands the three INV-SUB-P-040..042
  cases as `test.skip(...)` per D-0024.
- **Step 7/8.** Part E transient and Part F cross-cutting —
  `subscription-transient.test.ts` records INV-SUB-T-001..005 as
  *dropped* (NG-0006 / D-0007) via a single annotated case that
  documents the omission and asserts the omission shape (no
  `subscribe` procedure exists); the coverage reporter renders them
  as "dropped". INV-SUB-P-050 (selector) is annotated
  *above-adapter-line* in the same file — realised SDK-side per
  `mapping.md` Pass 2 / ML-0003 — and rendered separately in the
  coverage report. `cross-cutting.test.ts` covers INV-META-001
  (causation / correlation persisted and echoed), INV-META-010..011
  (event_type opaque, JSONB round-trip per the tighter v1
  encoding), INV-STREAM-001..003 (`$all` reserved at the schema
  level), INV-LINK-001 + INV-DELETE-001 as *dropped*.
- **Step 8/8.** Coverage report rendering + final green-run pass.
  `coverage-report.ts` walks the INV-* annotations from all test
  files, emits the B–F matrix (covered / deferred / dropped /
  above-line / missing), and exits non-zero if any "missing" rows
  remain. Annotation grammar (frozen in step 1/8): comment lines
  whose body starts with `INV-` are annotations; an optional
  `[parenthetical]` and an optional `:`-prefixed suffix follow.
  Multiple INV-* identifiers on the same line all receive the
  same classification. The grammar tolerates narrative mentions
  of an INV-* mid-line without false-positive-covering it.

  Cross-check pass: every "dropped" / "looser" verdict in
  `mapping.md` already had a corresponding entry in
  `non-goals.md` (the table at the bottom of NG already covered
  it); the harness surfaced no new divergences and no NG-*
  additions were needed.

  Final scoreboard (per `npm run coverage`):

  | Part | Identifiers | Covered | Deferred | Dropped | Above-line |
  |------|-------------|---------|----------|---------|------------|
  | B    | 18          | 18      | 0        | 0       | 0          |
  | C    | 9           | 9       | 0        | 0       | 0          |
  | D    | 6           | 6       | 0        | 0       | 0          |
  | E-T  | 5           | 0       | 0        | 5       | 0          |
  | E-P  | 19          | 15      | 3        | 0       | 1          |
  | F    | 8           | 6       | 0        | 2       | 0          |
  | **Σ**| **65**      | **54**  | **3**    | **7**   | **1**      |

  README pointed at the harness; Phase 9 marked done.

---

## Beyond

- Additional SDKs (Python, Go, Elixir).
- `instructedctl` tool, modelled on `absurdctl`.
- A habitat-equivalent UI, if useful.
- Performance work: index strategy, partition strategy for the events
  table at scale, snapshot cadence.

These are out of scope until Phase 9 is done.
