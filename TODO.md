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

## 11. Conformance criteria — revisit once the subscription model stabilises

**Why this exists.** The 2026-05-23 Commanded re-review (TODO #1)
produced a list of conformance-test gaps in `§4` of the review
document — cases that the conformance harness should cover but
probably doesn't (subscription scope isolation, lease takeover
without admin action, monotone cumulative ack, etc.). Those gaps
were written against the **current** single-cursor subscription
model.

During the same review walkthrough, the subscription model itself
became a parked design question. The leading candidate (SUB-A
Design 3 in `docs/todo/subscriptions.md`) replaces the
`subscriptions` cursor + per-event ack model with a
routing-cursor + work-queue model. Under that model, several of
the §4 conformance cases either no longer apply (the cursor
behaviours they test are internal routing-layer details, not
application-facing contract) or change shape (per-partition
ordering, work-item state transitions, claim semantics over
`SELECT … FOR UPDATE SKIP LOCKED`).

Rather than codify conformance cases against a model we are
actively redesigning, the work is parked.

**What to do (when SUB-A is decided):**

1. Walk the gap list below with the chosen SUB-A design in mind.
   For each conformance case decide:
   - Still applies as written → land the test.
   - Applies in modified form → reshape and land.
   - No longer meaningful (was about cursor mechanics that are now
     internal) → drop, record why.
2. Add the new conformance cases SUB-A introduces:
   - Per-partition ordering under concurrent claimants.
   - Failed work item blocks subsequent items for the same
     partition; does not block other partitions.
   - Routing-cursor advance is atomic with the batch of work-item
     INSERTs.
   - `waitForProjection` catch-up predicate (both conditions:
     routing cursor past target AND no outstanding work items ≤
     target).
   - Cross-stream `waitForProjection` guard (review §2.3.2; the
     guard itself ships earlier against today's schema but the
     test is part of the SUB-A acceptance set).
   - Claim leasing, lease expiry takeover, lease-loss detection at
     the work-item layer.
3. Decide the v1 conformance surface. The current `INV-*`
   catalogue mixes "intrinsic store contract" with "single-cursor
   subscription mechanism". Under SUB-A, only the former survives
   as the application-facing contract; the latter becomes
   `[mechanism-only]` on the routing-layer internals. The split
   is part of this work.

**Depends on.** `docs/todo/subscriptions.md` SUB-A reaching a
decision. Until then, do not modify `tests/conformance/`.

**Original gap list (against today's model).** Tests worth adding
in the current shape; some will translate, some won't, per the
walk above.

- Subscription scope isolation: a subscription on stream A does
  not deliver events appended to stream B. (`INV-READ-006`,
  `INV-SUB-P-030` imply it; not explicitly tested.)
- `:current` `start_from` skips all currently-existing events.
  (`INV-SUB-P-020`.)
- Resume from last successful ack: an un-ack'd event redelivers.
  (`INV-SUB-P-031`.)
- `$all` event rows carry their **original** `stream_id` /
  `stream_version`. (`INV-READ-006/007`.)
- Delete-subscription + re-subscribe from `:origin` redelivers
  from the start. (`INV-SUB-P-061`.)
- `release_subscription` preserves cursor; subsequent claim
  resumes from `last_seen`. (`INV-SUB-P-060`.)
- Lease expiry without administrative action; takeover by another
  worker; original worker's next op raises `IS022`.
  (`INV-SUB-P-012`.)
- Monotone cumulative-ack absorbs a lower-position advance
  silently. (`INV-SUB-P-034`.)

**Depends on.** `docs/todo/subscriptions.md` SUB-A reaching a
decision. Until then, do not modify `tests/conformance/`.

**Output.** An updated `tests/conformance/` plus a clear statement
of what the conformance harness verifies and why each case is in
or out, reflecting the post-SUB-A subscription model.

---

## 12. Apply re-review outcomes

**Why this exists.** The 2026-05-23 re-review of the invariant
catalogue (closed as item #1, in the Done list below) produced
a body of follow-up work split across four working files under
`docs/todo/`. This item is the index — the place to come for
"what's left from the re-review and where does each piece live?".

**Pre-release, small / contained:**

- `docs/todo/doc-patches.md` — three small wording patches to
  `guarantees.md`, `concepts.md`, and `decisions.md`. ~30 minutes
  each.
- `docs/todo/consistency.md` — two coordinated SDK changes to the
  consistency-wait path: an `exclude` mechanism for `dispatch`
  (with PM self-deadlock auto-prevention and warning log), and a
  cross-stream guard for `waitForProjection`. Focused
  implementations, each one or two files plus tests.

**Pre-release, design first:**

- `docs/todo/subscriptions.md` — the subscription substrate
  redesign (SUB-A: routing-vs-processing architecture / work
  queue), with the handler error-policy surface (SUB-B) and
  routing-side batching (SUB-C) folded in. The big architectural
  question of this cycle; ML-0001 partitioned-consumers collapses
  into it.
- `docs/todo/process-manager.md` — PM-specific items that ride on
  top of the subscription substrate (handle/apply split,
  deterministic event IDs for PM-dispatched commands,
  fan-out-as-modelling-pattern, simplified routing surface
  collapsing the Commanded directive set). PM-A, PM-C, PM-E,
  PM-F can proceed in parallel with SUB-A; PM-B is the slice
  that depends on the subscription decision (and now consists
  mostly of a constraint-mapping that's already written into
  the SUB-A proposed design).
- `docs/todo/projections.md` — projection-side items that ride
  on the subscription substrate (registration surface for
  partition modes, why projections don't take PM-C's
  apply/handle split, read-model transactionality, rebuild as
  an operator action). All items pre-release; depend on SUB-A
  landing for the substrate, but the API-surface design can
  proceed in parallel.

**Post-release / depends on others:**

- TODO #11 above — conformance criteria revisit, blocked on SUB-A.
- The new `ML-0006..0009` entries in `docs/maybe-later.md` — the
  general consistency-wait predicate, the Aggregate Multi-step
  convenience, aggregate state/version introspection,
  force-snapshot admin.
- The new entry in `docs/non-goals.md` — SDK-level PM fan-out is
  explicitly out.

**Output.** Each working file closes when its items land in the
live docs and SDK; this index item closes when all four working
files are empty (or themselves closed).

---

Item numbers are stable: closed items are removed from the body and
recorded below rather than renumbered, so existing in-tree references
(e.g. `TODO #3a`, `TODO #10` in code comments and docs) keep their
meaning. Gaps in the numbering are expected.

## Done items (delete on confirmation)

- **#1 Fresh re-review of the invariant catalogue against the
  reference event-sourcing library.** Walked the catalogue
  end-to-end on 2026-05-23. Headline outcomes:
  - Two correctness-class findings: the `waitForProjection`
    cross-stream silent wrong-answer bug (tracked in
    `docs/todo/consistency.md` CON-B) and the PM
    multi-command-on-redelivery duplicate-dispatch risk (tracked
    in `docs/todo/process-manager.md` PM-E).
  - A larger architectural finding: the single-cursor subscription
    model is insufficient for the PM-instance and
    concurrent-projection cases. Three candidate designs
    identified; decision parked for a dedicated design pass
    (`docs/todo/subscriptions.md` SUB-A). ML-0001 collapses into
    it.
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
