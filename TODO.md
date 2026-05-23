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

## 3. Taxing concurrent smoke test

**Why this exists.** The conformance harness (`tests/conformance/`)
tests *correctness*: two appenders race, one wins, one gets `IS001`.
It doesn't test correctness *at scale*. We want a smoke test that
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

**Output.** A runnable smoke harness that surfaces any failed/missing
invariants under load, plus an interpretation guide for each invariant
check.

**Depends on.** Doc tidy ideally settles the wording on each invariant
first so the smoke test's checks reference stable IDs.

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

## Done items (delete on confirmation)

*(none yet — log resolutions here as TODO items close)*
