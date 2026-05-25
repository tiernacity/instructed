# TODO

Follow-up tasks parked during the 2026-05-23 doc-tidy conversation.
Each item is its own piece of work, separate from the doc tidy that
prompted the list. Pick them up one at a time.

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
- **Projection rebuild (was PRJ-D in the SUB-A working files).**
  Reset a projection by name: `delete_subscription` cascades the
  work-queue rows, then a fresh claim from `start_from = 'origin'`
  re-routes the whole history. The read-store wipe (truncate the
  projection's tables, flush its Redis namespace, drop its
  Elasticsearch index, etc.) is application-domain and is the
  operator's responsibility before the rebuild command runs --
  the SDK doesn't know where the read-store is. `instructedctl`
  provides the framework-side half: a one-shot "forget this
  subscription's state" that's safe to run while the worker is
  stopped.
- **Work-item operator surface.** Inspect the SUB-A work queue
  per subscription (counts by state; oldest in-flight item;
  list `failed` rows with their `error_text`). One
  `skip_work_item_with_audit` command that moves a stuck
  `failed` row to a terminal state with an operator-supplied
  audit note. The default error policy never produces `failed`
  rows, so this surface is the dedicated escape hatch for the
  future `quarantineAfter` convenience wrapper (SUB-B) and for
  manual operator action on poison events. Per
  [INV-SUB-W-013](docs/invariants.md), `failed` rows are
  operator-only; this is the operator's tool.
- **Snapshot inspection:** show a snapshot by `source_uuid`.
  A future `force-snapshot` command for aggregates is sketched
  in ML-0009.
- **Health:** a quick "is the store sound" check — `$all`
  contiguous, no orphaned `stream_events` rows, no expired-lease
  zombies, no orphaned `subscription_work_items` rows.

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

**Smaller follow-ups carried over from the 2026-05-23
concurrent-tests work** (do alongside the broader pass, or
opportunistically):

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

---

## 12. Apply re-review outcomes

**Status: SUB-A / PM-F / PM-C / PRJ-A landed. Re-review outcomes
broken out into the open items below.**

The 2026-05-23 re-review of the invariant catalogue produced
a body of follow-up work originally tracked across working
files under `docs/todo/`. The four SUB-A-adjacent working
files (`subscriptions.md`, `process-manager.md`,
`projections.md`, `sub-a-implementation.md`) closed when SUB-A
landed across SDK slices 1-12; their decided content migrated
into `docs/architecture.md`, `docs/concepts.md`,
`docs/decisions.md`, `docs/invariants.md`, and
`docs/sql-contract.md`. Pre-release migration guidance for the
breaking-change-at-the-SDK-surface bits lives in
`docs/upgrade-notes/pm-f.md`.

What's still open from the re-review:

- `docs/todo/doc-patches.md` — three small wording patches
  (DOC-A handler purity, DOC-B D-0004 one-liner, DOC-C D-0010
  "Why" tighten). Independent of SUB-A; pick up when convenient.
- `docs/todo/consistency.md` — CON-A (`exclude` mechanism for
  `dispatch`). CON-B (`waitForProjection` cross-stream guard)
  landed as part of TODO #11.
- The follow-on items still hanging off this index entry
  (`ML-0006..0012` in `docs/maybe-later.md`, the SDK-level
  PM-fan-out non-goal, PM-E for deterministic event IDs on
  PM-dispatched commands).

---

Item numbers are stable: closed items are removed from the body and
recorded below rather than renumbered, so existing in-tree references
(e.g. `TODO #3a`, `TODO #10` in code comments and docs) keep their
meaning. Gaps in the numbering are expected.

## Done items (delete on confirmation)

- **#13 `streams_stream_uuid_key` race in `append_to_stream`.**
  Fixed by wrapping the `'exact'` V=0 missing-stream-create
  INSERT in `sql/instructed.sql` with a `unique_violation`
  handler that translates to `IS001`, parallel to the existing
  `'no_stream'` → `IS002` translation and INV-APPEND-022's
  `stream_events` translation. Audit of all four `streams`
  INSERT sites confirmed the `'any'` (ON CONFLICT), `'no_stream'`
  (already handled), and `$all` bootstrap paths were race-safe;
  only the V=0 path was missing the handler. Deterministic
  conformance test added in
  `tests/conformance/test/append.test.ts` using two dedicated
  `pg.Client` connections with explicit BEGIN to hold session
  1's transaction open past session 2's SELECT FOR UPDATE —
  verified to fail with `23505 !== IS001` without the fix and
  pass with it. INV-APPEND-014 in `docs/invariants.md` updated
  to call out the translation. Full conformance suite (164/164,
  3 pre-existing D-0024 skips) and full TS SDK suite (129/129)
  green.
- **#1 Fresh re-review of the invariant catalogue against the
  reference event-sourcing library.** Walked the catalogue
  end-to-end on 2026-05-23. Headline outcomes:
  - Two correctness-class findings: the `waitForProjection`
    cross-stream silent wrong-answer bug (still tracked in
    `docs/todo/consistency.md` CON-B; still open) and the PM
    multi-command-on-redelivery duplicate-dispatch risk (PM-E;
    open, no working file -- see `docs/invariants.md` "Honest
    gaps in v1" entry 2).
  - A larger architectural finding: the single-cursor subscription
    model was insufficient for the PM-instance and
    concurrent-projection cases. Resolved in SUB-A across SDK
    slices 1–12; see `docs/decisions.md` D-0002 and
    `docs/architecture.md` "How a worker runs". The original
    ML-0001 partitioned-consumers entry has been removed
    (capability now provided by SUB-A's partition shape).
  - Several smaller items routed to `docs/maybe-later.md`
    (ML-0006..0009) and `docs/non-goals.md` (SDK-level PM
    fan-out).
  - Conformance-test re-evaluation parked behind SUB-A; tracked
    as item #11.
  - Master index of the follow-up work: item #12 above.
- **#3 Concurrent correctness + load/soak harness.**
  - **#3a composed-concurrency correctness tests** — landed in
    `sdks/typescript/test/concurrent.test.ts` (aggregate OCC ×
    projector; two projectors / one subscription with lease theft;
    PM × appender × projector). Also caught a PM-024 wording bug
    that was fixed in the same commit.
  - **#3b load / soak harness** — landed in `tests/soak/` with a
    CLI workload generator, worker farm, failure injection, and
    continuous + final invariant checks (INV-APPEND-003,
    INV-APPEND-022, INV-SUB-P-008, INV-SUB-P-LEASE-UNIQ, PM-024,
    PM-FORWARD-TOTAL, REFOLD-MATCH). Interpretation guide in
    `tests/soak/README.md`. Deferred follow-ups (network partition
    injection, OCC retry-count surfacing, multi-process
    orchestration) are documented in that README's *Known gaps*.
- **#10 PM ignored-event ack coalescing (ex-ML-0005)** — shipped
  in `sdks/typescript/src/process-manager.ts`; new tests in
  `sdks/typescript/test/pm-ack-coalescing.test.ts`; ML-0005 stub
  removed from `docs/maybe-later.md`; architecture / soak docs
  updated. Full TS SDK suite (84 tests) green. Soak re-baseline
  deferred to next harness run.
- **#11 Conformance criteria — revisit once the subscription
  model stabilises.** Walked the 2026-05-23 §4 gap list and
  TODO #11 step-2 SUB-A acceptance set against the landed
  subscription model. Outcome:
  - Four new tests landed:
    `subscription-persistent.test.ts` gains scope isolation
    (per-stream A doesn't deliver B) and the composed
    lease-expiry → takeover → IS022 case for INV-SUB-P-011;
    `subscription-work-items-procedures.test.ts` gains mixed
    route_batch (cursor jumps past ignored event_numbers; no
    work-item written) for INV-SUB-P-033 and the composed
    delete-with-queued-items → re-claim from `:origin` →
    re-route case for INV-SUB-P-061.
  - One re-label (no behaviour change) on SP "redelivery:
    crash-before-advance is recovered by re-claim" to clarify
    it pins the routing-layer no-auto-ack contract; the
    application-facing redelivery contract under SUB-A is the
    work-item lease takeover already covered at SWP.
  - Six §4 cases dropped as already covered: `:current`
    start_from, `$all` original-identity echo (per
    INV-READ-006/007), release-preserves-cursor,
    monotone cumulative ack, plus the SUB-A step-2 items for
    per-partition ordering, failed-row partition blocking,
    atomic route_batch, work-item lease leasing/expiry/loss,
    and `waitForProjection` end-to-end (the latter already
    covered three ways at
    `sdks/typescript/test/consistency.test.ts` "SUB-A
    work-item conjunct").
  - CON-B `waitForProjection` cross-stream guard landed
    alongside: new `ConsistencyTargetError`, synchronous (fast
    pre-await) rejection in `waitForProjection`, four tests in
    `sdks/typescript/test/consistency.test.ts` (positive,
    negative, mixed-list, `$all`-exempt). Required adding
    `stream_uuid` to the SDK's `AppendedEvent` shape
    (populated client-side; no SQL change). `docs/todo/
    consistency.md` CON-B marked landed; CON-A still open.
  - INV catalogue `[mechanism-only]` split deemed already
    correct (INV-SUB-P-011, INV-SUB-W-003, INV-APPEND-022,
    INV-STREAM-002 already carry the marker); the
    application-facing vs routing-mechanism split is now
    explicit in `tests/conformance/COVERAGE.md` under "TODO
    #11 / SUB-A re-fit notes".
  - ML-0001 (removed during the 2026-05-23 doc tidy) replaced
    with ML-0013 (multi-routing-worker subscriptions /
    `concurrency_limit > 1`) in `docs/maybe-later.md` to
    capture the forward-looking work that the three skipped
    INV-SUB-P-040/041/042 conformance tests are parked behind.
    The 22 stale `ML-0001` references across
    `sql/instructed.sql`, `sdks/typescript/src/types.ts`,
    `sdks/typescript/README.md`, and `tests/conformance/
    COVERAGE.md` updated to ML-0013.
  - Architecture / guarantees docs gained the one-sentence
    per-stream-target guard callout per CON-B step 5.
  - Full conformance suite (171/171 active, +4 new; 3 skipped
    as deferred per ML-0013) and full TS SDK suite (133/133,
    +4 new CON-B tests) green.
