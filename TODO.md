# TODO

Follow-up tasks parked during the 2026-05-23 doc-tidy conversation.
Each item is its own piece of work, separate from the doc tidy that
prompted the list. Pick them up one at a time.

---

## 1. Fresh Commanded re-review — find load-bearing BEAM-isms we may have under-served

**Why this exists.** Our existing invariant catalogue (`docs/invariants.md`,
`docs/guarantees.md`) was derived from Commanded's adapter contract and
its conformance tests. The (I)/(B)/(C) classification in `guarantees.md`
Part G was our judgement on each item. The risk: we classified something
as **(B)** "BEAM mechanism, can be dropped" when it was actually
**(I) realised via a BEAM mechanism** — i.e. intrinsic CQRS/ES that
Commanded happened to do in OTP and we forgot to provide an equivalent.

**What to do.** Treat the catalogue as suspect and re-read Commanded
against a fresh review spec. For each Commanded mechanism flagged
`[beam-mechanism]` in `guarantees.md`, ask:

- Is there an *observable application-facing behaviour* this mechanism
  provides?
- If yes, does `instructed` provide that behaviour by some other means?
- If no, is the behaviour intrinsic to CQRS/ES (apps will break without
  it) or convenience (apps can live without it)?

Specific candidates that warrant the closest look:

- **AGG-011** — per-aggregate command serialisation via GenServer mailbox.
  We replaced this with OCC retry (D-0005). The semantic *outcome* is the
  same, but is there a behavioural difference under sustained contention
  (e.g. retry storm vs. fair queue) that applications would notice?
- **AGG-025** — aggregate self-subscription. We dropped this because
  NG-0002 eliminates the in-memory cache. But the self-subscription also
  served as a *liveness signal* for "this aggregate exists somewhere".
  Re-check whether anything in Commanded depended on that signal beyond
  the cache-coherence story.
- **CON-002/003/013** — pubsub + ETS for strong-consistency-on-dispatch.
  We replaced with polling (CON-010). Latency floor is the obvious
  trade-off. Is there a *correctness* difference under heavy concurrent
  ack traffic that we missed?
- **PM-040** — per-instance GenServer for process managers. We rely on
  the single-active-worker-per-subscription contract instead. Re-check
  whether Commanded's per-instance process gave any concurrency guarantee
  beyond "only one in-flight handle/3 at a time" that we don't reproduce.
- **HND-050 / INV-SUB-P-040..042** — partitioned consumers. Currently
  deferred (ML-0001). Re-check whether any single-worker workload is
  effectively blocked from working at all (vs. just slower than ideal).

**Sources to read.**

- `commanded/lib/commanded/aggregates/{aggregate,execution_context}.ex`
- `commanded/lib/commanded/event/handler.ex`
- `commanded/lib/commanded/process_managers/{process_manager_instance,process_router}.ex`
- `commanded/lib/commanded/subscriptions.ex`
- The same three adapter conformance tests already consumed
  (`commanded/test/event_store/support/{append_events,subscription,snapshot}_test_case.ex`)
  but this time looking for behaviours the *adapter* relies on rather
  than for invariants the adapter enforces.

**Note:** Commanded source is not currently checked out anywhere on this
machine. First step is `git clone https://github.com/commanded/commanded`
into a scratch location, plus `commanded/eventstore` for the reference
Postgres adapter.

**Output.** Either "no gaps found" (recorded as a short addendum to
whatever the post-tidy invariant/guarantee doc is), or a list of new
INV-* / new ADR entries with realisation plans.

---

## 2. SDK restructuring — core vs. idiomatic-convenience split

**Why this exists.** Conversation on 2026-05-23 settled a framing:
each SDK should have a small **core** that drives the SQL contract, plus
one or more **convenience packages** that offer idiomatic APIs over that
core. The split lets us be clear-eyed about what a new-language SDK port
must reproduce (the core) vs. what can be idiomatic per language (the
conveniences).

**Core (every SDK must provide):**

- Wrappers over the SQL procedures with SQLSTATE → typed-error translation.
- The aggregate load-execute-append loop with OCC retry (the AGG-001..010
  semantics; `runCommand` in the TS SDK).
- A persistent-subscription worker loop with lease + heartbeat + cursor
  advance (the HND-* semantics; `startProjection` in the TS SDK).
- Saga state persistence via the snapshot primitive.
- A way to subscribe to the event stream.

**Conveniences (each SDK may offer in whatever shape fits the language):**

- Routing events to projectors / process managers / command handlers
  (the TS SDK's `Instructed` facade with `registerAggregate` /
  `registerProjection` / `registerProcessManager`).
- Saga workflow conveniences — rollback / compensation / steps / tasks
  on top of the PM primitive (currently we have none; D-0011 settled on
  "compensation is just a command" but a convenience layer for the
  common saga shapes is plausible).
- PM `interested?` conveniences.
- "Wait for strong consistency" on dispatch (currently the
  `consistency: [...]` option on `Instructed.dispatch`; this is
  convenience because `LISTEN/NOTIFY` may eventually change the
  mechanism — see ML-0002).

**What to do in the TS SDK.**

- Decide whether to physically split into two packages
  (`@instructed/core` + `@instructed/runtime` or similar) or keep one
  package with documented sub-paths (`instructed-sdk/core` vs.
  `instructed-sdk`).
- Either way, get `src/index.ts` to re-export the core and the
  conveniences separately so a port author can read just the core surface.
- Update the SDK design doc (if one survives the tidy) to describe the
  split.

**Output.** A restructured TS SDK and a clear "porting checklist" for
new-language SDKs. The checklist is what a Python/Go/Elixir port reads
to know "what do I have to build to be a conforming SDK" — distinct
from `tests/conformance/` which tests the *store*.

---

## 3. Concurrent correctness + load/soak harness

**Why this exists.** Previous Commanded ports passed point-by-point
conformance and then fell over the moment real concurrent traffic
showed up. The fear is a bug class that only surfaces when several
mechanisms race against each other — aggregate OCC retries happening
at the same time as projector lease churn happening at the same time
as PM dispatch. The existing conformance and SDK suites *do* cover
concurrency, but each test exercises one verb at a time (two
appenders race; two claims race; two writers race for an aggregate).
Nothing currently runs the composed system.

This splits into two pieces of work with different value:

### 3a. Composed-concurrency correctness tests — **DONE**

Landed in `sdks/typescript/test/concurrent.test.ts`. Three
deterministic-ish scenarios that wire several mechanisms together
and assert end-to-end invariants, all running in well under a
second:

- **Aggregate OCC × projector.** N=12 concurrent dispatchers force
  OCC retries on one aggregate; a projector on `$all` follows.
  Asserts the projector's folded value equals the re-folded
  aggregate value, stream is gapless 1..N, projector saw monotone
  `event_number`.
- **Two projectors, one subscription.** Two workers compete for
  the same `(stream, name)`; short leases; mid-flight lease theft
  via direct SQL. Asserts `last_seen` monotone (sampled
  continuously), every event ack'd at least once across the union,
  no two handlers ran the same `event_id` concurrently.
- **PM × appender × projector.** PM dispatches `add` commands to
  aggregate B while a concurrent appender fires `Triggered` events;
  projector on `$all`. Asserts B's re-folded value equals sum of
  triggers, projector saw `2N` events monotone, **PM-020** holds
  (`snapshot.source_version` == event_number of last Triggered),
  **PM-024** holds (`source_version <= last_seen`).

Writing these caught a real wording bug in the PM-024 invariant
("doubles as last_seen" overstated the relationship) — fix landed
in the same commit. See item #9 for the docs follow-up.
- **Crash-mid-handler.** Already partially covered by
  `subscription.test.ts` "heartbeat-lease-loss"; the composed
  version is harder to add value to and may be subsumed by the two
  scenarios above.

Lives in `sdks/typescript/test/concurrent.test.ts` (or split if it
grows). Uses the existing fixtures and docker-compose Postgres.

### 3b. Load / soak harness (lower priority, performance-focused)

The original sketch of TODO #3 was really this: a longer-running
workload generator + worker farm + failure injection that runs
continuous invariant checks. Reframe its purpose as **performance
gauge + invariant fuzzer over time**, not the primary correctness
story. Runs nightly / on demand, not per-PR.

The SQL contract (`tests/conformance/`)
tests *correctness*: two appenders race, one wins, one gets `IS001`.
It doesn't test correctness *at scale*. We want a soak harness that
runs N dispatchers, M projectors, K process managers, optional
crashing nodes, against a representative workload, and asserts no
invariant violations occur (no gaps in `event_number`, no out-of-order
deliveries, no double-acked events past the cursor, no two workers
holding the same lease at the same time).

**Initial sketch (subject to design).**

- Lives in `tests/smoke/`.
- Workload generator: configurable rate of `Transfer` commands against
  N accounts; configurable mix of `:any_version` and exact-version
  appends.
- Worker farm: M concurrent projector processes claiming the same
  subscription (only one wins at a time per the lease model);
  K process managers; lease TTLs short enough to force regular
  rebalances.
- Failure injection: kill workers mid-batch; kill workers between
  read-batch and advance; partition the network (sleep their tx
  before commit).
- Invariant checks (run at end and continuously):
  - `event_number` is gapless across the global stream.
  - For every stream, `stream_version` is gapless and starts at 1.
  - For every subscription, `last_seen` is monotone over time.
  - For every event, the projector saw it at most once *past the
    cursor* (i.e. redeliveries are fine, but the durable cursor
    never went backward and never advanced past an event the
    handler did not see).
  - For every PM, snapshot `source_version` equals the
    subscription's `last_seen` (PM-024).
  - No event was permanently undelivered to a healthy projector.

**Output (3b only — 3a is done).**

- A `tests/soak/` (or `tests/load/`) harness with its own README
  and an interpretation guide for each invariant check.
- Should reuse the SDK test fixtures' Postgres setup and the
  invariant IDs from `docs/invariants.md` so a soak-detected
  failure can be cross-referenced to a contract clause.
- Performance numbers (commands/sec, projector lag at
  steady-state, lease churn rate) reported alongside the
  invariant report — the soak harness doubles as a perf gauge.

**Starting points for a new session on 3b.**

- The 3a composed tests in `sdks/typescript/test/concurrent.test.ts`
  are the closest existing analogue; the soak harness scales them
  out, adds time, and adds failure injection.
- ML-0005 (just added to `maybe-later.md`) notes that PMs currently
  ack each ignored event individually. The soak harness should
  measure ignored-event ack overhead so we know whether ML-0005 is
  worth implementing or just theoretical.
- The composed-test scenarios that bit me on first run (OCC
  retry-budget exhaustion at N=12, PM-024 wording) are the kind of
  thing the soak harness should surface at higher N. Worth
  parametrising N as a CLI flag.

**Depends on.** 3b benefits from the doc tidy settling invariant
wording first so the soak checks reference stable IDs.

---

## 4. OCC enforcement in SQL — review the strength of what we enforce

**Why this exists.** Conversation surfaced the question: optimistic
locking is the SDK's job, but does the SQL contract *enforce* it
strongly enough that a misbehaving SDK can't silently corrupt an
aggregate?

**Current position.** The SQL boundary enforces:

- `append_to_stream` with `expected_version_type = 'exact'` and a stale
  V raises `IS001` (the unique constraint on
  `stream_events (stream_id, stream_version)` is the mechanism, per
  INV-APPEND-022).
- An SDK can deliberately pass `'any_version'` and bypass OCC. This is
  intentional — `:any_version` is part of the Commanded contract for
  legitimate non-aggregate use cases (e.g. appending exogenous events).
- An SDK that *forgets* to specify `'exact'` and defaults to `'any_version'`
  *could* corrupt aggregate semantics. We rely on the SDK to choose the
  right expected-version mode per call.

**Open question to answer.** Do we want a stronger enforcement — e.g.
a per-stream flag that says "this is an aggregate stream, reject
`:any_version` appends"? Pros: makes "aggregate stream" a first-class
concept in the store, eliminates a class of SDK bug. Cons: introduces
domain knowledge into the store; complicates the contract; doesn't
match Commanded.

**Output.** Either "we leave as-is and document the SDK's responsibility
crisply" or "we add the flag and a new SQLSTATE". My current lean is
the former, but worth a deliberate re-read.

---

## 5. SNAP-002 — snapshot module versioning in the SDK

**Why this exists.** `guarantees.md` SNAP-002 records that snapshots
carry a `snapshot_module_version` in their metadata, and that readers
MUST reject mismatched-version snapshots and fall back to full event
replay. The SQL contract provides the metadata column. The TypeScript
SDK does *not* implement the reject-and-fall-back policy — the
application using `instructed` would have to do it themselves.

**What to do.** Decide whether SNAP-002 enforcement belongs in the
core SDK (which would make it part of the "what every SDK port must
provide" checklist in TODO #2) or stays as application-level concern
documented in usage examples.

**Output.** Either an SDK feature + tests, or a short addendum to
whatever doc replaces `guarantees.md` that says "this is your problem".

---

## 6. Additional language SDKs

**Why this exists.** The TypeScript SDK is the reference. The
library's value is multiplied by SDKs in each language an
application might be written in. The conformance harness lives
SQL-side, so each new SDK inherits adapter-line conformance for
free by pointing at a conformant Postgres; the work is the SDK
shape itself.

**Initial language list (rough priority order):**

- **Python** — ubiquitous, common pairing with Postgres-native
  services, idiomatic to write async or sync depending on the
  caller.
- **Go** — popular for service workloads; `pgx` is the obvious
  driver; goroutines map well to the polling worker loop.
- **Elixir** — the natural home of CQRS/ES (Commanded users
  might bridge here); `postgrex` is the driver; the worker loop
  is a `GenServer`-shaped fit.
- **Rust** — candidate if there's demand from systems-level
  workloads.

**Depends on.** TODO #2 (SDK core vs. conveniences split). Each
language SDK should implement the **core** in a shape idiomatic
to its ecosystem; conveniences are optional and may differ per
language. The core surface (procedure wrappers + aggregate loop
+ subscription worker + snapshot primitives + consistency-wait
read) is the porting checklist.

**Output per language:** a package in `sdks/<lang>/` with its
own README documenting the core/convenience layout, tests
running against the docker-compose Postgres, and at least one
worked example under `examples/<lang>/`.

---

## 7. `instructedctl` — administrative CLI

**Why this exists.** Operators need to inspect and manage an
`instructed` deployment without writing ad-hoc SQL. Modelled on
[`absurdctl`](https://github.com/earendil-works/absurd) which
provides the analogous surface for absurd.

**Likely functionality (first cut):**

- **Schema lifecycle:** install / migrate / status. Reports the
  installed schema version and any pending migrations under
  `sql/migrations/`.
- **Stream inspection:** list streams, show stream head /
  version, read a range of events from a stream (or from `$all`)
  in a human-readable form.
- **Subscription inspection:** list subscriptions with their
  cursors, claimed-by, lease-expires-at; show how far behind a
  subscription is relative to `$all`'s head.
- **Subscription lifecycle:** `release` a stuck claim (where the
  worker is known dead but lease hasn't expired); `delete` a
  subscription by name; `claim` for diagnostic purposes.
- **Snapshot inspection:** show a snapshot by `source_uuid`.
- **Health:** a quick "is the store sound" check — `$all`
  contiguous, no orphaned `stream_events` rows, no expired-lease
  zombies.

**Likely shape:** a small Go or Rust binary so it can be
distributed as a single static executable. Connects directly to
Postgres; does not depend on any application SDK.

**Output.** A `tools/instructedctl/` directory with the source,
build instructions, and a short user guide. The tool should be
able to do everything a production operator currently does by
opening psql.

---

## 8. Examples — reorganise by language and cross-language

**Why this exists.** The current `examples/bank-account/` is
TypeScript-only and lives flat at the top level. With more SDKs
coming (TODO #6), we need an organising scheme.

**What to do:**

- **Re-shape to `examples/<language>/<example-name>/`.** Move
  `examples/bank-account/` to `examples/typescript/bank-account/`.
  Add equivalents for each new SDK as they land.
- **Add cross-language / mixed-language examples** under
  `examples/mixed/<scenario>/` for cases where two SDKs cooperate
  through the same Postgres (typical workload: a TypeScript web
  service dispatches commands; a Python worker runs a heavy
  projection; a Go scheduler runs the process managers).
- **Drop relative imports.** Currently `examples/bank-account/`
  imports from `../../sdks/typescript/src/index.ts`. Move every
  example to importing the SDK as a package
  (`import { Instructed } from "instructed-sdk"`, or the
  language-equivalent). This makes examples real consumer-style
  references and removes the implicit tie to repo layout.
- **Lean on docker where needed.** Mixed-language examples and
  any example that needs multiple long-running processes should
  ship a `docker-compose.yaml` of their own (or extend the
  repo-root one), so running an example is one `docker compose
  up`. The repo-root `docker-compose.yaml` stays as the test
  database for the SDK and conformance suites.

**Output.** Reorganised `examples/` tree, a top-level
`examples/README.md` that indexes by scenario, and every example
runnable with a documented one-liner. Each SDK's tests that
currently import from `examples/` (e.g. `bank-account.test.ts`)
move to using the SDK's own test fixtures and stop crossing the
example boundary.

**Depends on.** Publishing the SDK as a package (or at least
workspace-linking it under a stable name) so examples can
import without relative paths.

---

## 9. Documentation — next pass

**Outstanding follow-ups from the 2026-05-23 concurrent-tests work:**

- PM-024 was reworded in `invariants.md`, `architecture.md`, and
  `sql-contract.md` during the 3a work to fix the "doubles as
  last_seen" misstatement. A short *worked example* in
  `architecture.md` or `concepts.md` showing the two markers
  (`last_seen`, `source_version`) diverging across a couple of
  ignored events would help the reader internalise the new
  wording. Currently the new wording is correct but abstract.
- `architecture.md` now mentions that a PM's subscription is
  shared across all its process instances and a poison event
  stalls the whole PM type. This is a one-paragraph callout; if
  it gets reader feedback as surprising, promote it to a sized
  section with a recovery-pattern recipe ("how to skip a poison
  event in practice").
- ML-0005 (coalesce ignored-event acks) was added to
  `maybe-later.md`. If the soak harness (3b) shows it matters,
  promote it from ML-0005 to an implementation task.

---


**Why this exists.** The 2026-05-23 tidy collapsed work-in-
progress framing and got the doc set down to a workable size,
but a serious next pass is warranted on writing quality, depth,
and worked content.

**What to do:**

- **More examples in the docs.** `concepts.md` introduces the
  primitives but mostly in prose; `architecture.md` describes
  mechanisms but rarely shows code. Add concrete code snippets
  to every concept ("here's what an aggregate looks like";
  "here's what an apply function returns when an event arrives";
  "here's the worker loop in pseudocode and in TypeScript").
  Aim for at least one code block per concept introduction.
- **Writing quality pass.** Tighten the prose. Remove residual
  designed-by-committee phrasing. Read each doc aloud and edit
  for flow. Use shorter sentences where the technical content
  allows. Consistent terminology ("event log" vs. "event store"
  vs. "the store" — pick one per audience and stick to it).
- **Definitions and glossary.** A `docs/glossary.md` (or a
  glossary section in `concepts.md`) defining every term used
  load-bearing in the docs: aggregate, command, event, stream,
  `$all`, projection, process manager, subscription, cursor,
  lease, snapshot, causation, correlation, OCC, dispatch.
  Cross-link from first mention in every doc.
- **Context and motivation.** Each doc currently starts "this is
  X"; add a paragraph or two of "and why you'd care" up front.
  `architecture.md` particularly would benefit from a "when you
  would choose this design vs. a coordinator-based one" framing.
- **Cleaner separation by audience.** The README points each
  audience at the right docs, but the docs themselves still mix
  audiences in places. `invariants.md` is for porters and the
  conformance harness, but it's the densest doc and could lose a
  casual reader who arrived from `concepts.md`. Consider
  per-audience entry pages (a "start here" for each: app
  developer, library evaluator, SDK porter) that thread the
  reader through the right sequence of docs.
- **End-to-end walkthrough.** A `docs/walkthrough.md` or
  similar: build a small application from scratch, step by step,
  exercising every concept. The bank-account example is good but
  it's a finished artifact; a step-by-step has different value.

**Output.** A docs revision that a first-time reader can use to
go from "never heard of CQRS" to "I just shipped an aggregate"
in a focused afternoon, plus a separate path from "I write Go,
show me what to port" to "my Go SDK passes the conformance
harness".

**Depends on.** TODO #6 partially — once a second SDK exists,
writing will need to stop using TypeScript as the default
example language in every code snippet.

---

## Done items (delete on confirmation)

*(none yet — log resolutions here as TODO items close)*
