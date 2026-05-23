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

### 3b. Load / soak harness — **DONE**

Landed in `tests/soak/`. CLI-driven workload generator + worker farm
+ failure injection + continuous-and-final invariant checks. See
`tests/soak/README.md` for the interpretation guide. Exits 0 on no
violations and 1 otherwise; performance facts (commands/sec, events/sec,
respawn count, lease theft count, $all head) always print.

What the harness exercises:

- Counter aggregate × OCC retry under `--dispatchers` concurrent
  writers, plus a `--any-version-fraction` knob mixing exact-version
  and `:any_version` appends.
- Multi-slot competition for one subscription with short leases
  (default 3s) and periodic lease theft via direct SQL.
- Forwarder PM on `$all` dispatching to the same accounts the
  dispatchers write to, so PM acks against `Added` events become a
  measurable share of the load (ML-0005 surfaces here as PM lag).
- Worker respawn injection: a random slot is bounced every
  `--respawn-every-ms`; a keep-alive supervisor re-creates it with a
  250ms minimum lifetime so a perpetually-losing slot doesn't busy-loop.

Invariants checked (final + continuous sampler), each tagged with its
`docs/invariants.md` ID:

- `INV-APPEND-003` — `$all` event_number gapless 1..head.
- `INV-APPEND-022` — per-stream stream_version gapless.
- `INV-SUB-P-008` — subscription `last_seen` monotone and ≤ head.
- `INV-SUB-P-LEASE-UNIQ` — ≤ 1 unexpired claim per (stream, name)
  at every sample tick.
- `PM-024` — PM `source_version` ≤ `last_seen`.
- `PM-FORWARD-TOTAL` — total PM `forwarded` count equals trigger count.
- `REFOLD-MATCH` — projector's running balance equals a fresh re-fold.

Follow-ups deferred (out of scope for the first cut, documented in
`tests/soak/README.md` § Known gaps):

- Network partition injection (`sleep tx before commit`).
- Direct OCC retry-count surfacing — the SDK doesn't expose them
  yet; visible indirectly via attempted-vs-completed delta.
- Multi-process orchestration (everything runs in one Node process).

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

## 10. PM ignored-event ack coalescing (ex-ML-0005)

**Why this exists.** The soak harness (TODO #3b) measured PM drain
throughput at ~140 events/sec sustained against a local docker
Postgres. Profiling the run pinned the bottleneck at the
`advance_subscription` round-trip cost: the PM acks **every**
event on its subscription individually, both the routed ones
(inside the persist-and-ack tx) and the ignored ones (a
standalone `ackOnly` round-trip each).

For a PM subscribed to `$all` on a busy store this is brutal. The
soak's reported workload — 7629 routed `Triggered` events plus
~22000 ignored `Added` events from concurrent dispatchers —
produced ~30000 advance round-trips in the drain phase, which is
why the harness needs ~216s of drain after a 60s active workload.
The projection code path already coalesces (one
`advance_subscription` per batch); only the PM is per-event.

Promoted from `docs/maybe-later.md` ML-0005 with the soak
measurement as the trigger.

### Observation that makes the change cheap

A routed event's persist-and-ack tx already advances the cursor
to that event's position. That advance **implicitly covers every
ignored event at a smaller position** because
`advance_subscription` is monotone (`last_seen = greatest(last_seen,
p_up_to_position)`). So the optimisation is not "batch ignored
acks together" — it's **"don't ack ignored events at all when a
routed event follows them in the same batch; the routed event's
tx covers them for free"**. The only remaining round-trip is one
standalone `advance_subscription` per batch for the tail of
ignored events that have no routed event following them in this
batch.

### Worked example

Batch of 5 events on `$all`, position numbers in brackets:

```
  E[100]  Added      (ignored by PM)
  E[101]  Added      (ignored by PM)
  E[102]  Triggered  (routed)
  E[103]  Added      (ignored)
  E[104]  Triggered  (routed)
```

Today (5 round-trips):

```
  advance_subscription(100)                       ← ackOnly
  advance_subscription(101)                       ← ackOnly
  BEGIN; record_snapshot(sv=102);
         advance_subscription(102); COMMIT        ← routed tx
  advance_subscription(103)                       ← ackOnly
  BEGIN; record_snapshot(sv=104);
         advance_subscription(104); COMMIT        ← routed tx
```

After ML-0005 (2 round-trips):

```
  (E[100], E[101]: tracked as pendingIgnoredTo=101, no round-trip)
  BEGIN; record_snapshot(sv=102);
         advance_subscription(102); COMMIT        ← covers 100..102
  (E[103]: pendingIgnoredTo=103, no round-trip)
  BEGIN; record_snapshot(sv=104);
         advance_subscription(104); COMMIT        ← covers 103..104
```

Trailing-ignored case: same batch, but E[104] is also ignored.
After routing E[100..103] as above (1 routed tx covering 100..102),
E[103] and E[104] are both ignored and at the end of the batch —
flush them with one `advance_subscription(104)` after the loop.
Total 2 round-trips for 5 events instead of 5.

### Implementation plan

All changes are in `sdks/typescript/src/process-manager.ts`. No
SQL contract change, no invariant change.

1. **Split `processEvent`.** The current function does both route
   resolution and the routed work. Extract the
   handle/dispatch/persist-and-ack body into
   `processRoutedEvent(event, routed): Promise<boolean>` so the
   poll loop can call it directly when a routed event is found.
2. **Replace the per-event loop with a coalescing walker.** In
   the existing poll loop where today we do
   `for (const event of batch) { await processEvent(event); }`,
   carry a `pendingIgnoredTo: bigint | null` across iterations:

   ```ts
   let pendingIgnoredTo: bigint | null = null;
   const positionOf = (e: RecordedEvent<E>) =>
     stream === "$all" ? e.event_number : e.stream_version;

   for (const event of batch) {
     if (closing || aborted) break;
     const routeFn = def.routes[event.event_type];
     const routed = routeFn
       ? normaliseRoute(routeFn(event))
       : { kind: "ignore" as const };
     if (routed.kind === "ignore") {
       pendingIgnoredTo = positionOf(event);
       continue;
     }
     const ok = await processRoutedEvent(event, routed);
     if (!ok) break;
     pendingIgnoredTo = null; // covered by routed tx's advance
   }

   // End-of-batch trailing ignored flush.
   if (pendingIgnoredTo !== null && !aborted) {
     try {
       await client.advanceSubscription(
         stream, def.name, workerId, pendingIgnoredTo,
       );
     } catch (err) {
       if (isLeaseLoss(err)) markAborted(err as Error);
       else safeOnError(err as Error);
     }
   }
   ```

3. **Delete `ackOnly`.** After step 2 it has no remaining caller.
   (Keep the import-graph clean rather than leave dead code.)
4. **Projections are unchanged.** `subscription.ts` already does
   one advance per batch at the end of the loop.

### Edge cases to verify in tests

- **Crash mid-batch with `pendingIgnoredTo` not yet flushed.** The
  cursor stays at the last-flushed position. On redelivery the
  ignored events re-arrive, the route fn returns ignore again,
  the pending pointer accumulates again, flush on the new
  worker's batch boundary. No data loss, no double-handle.
- **Lease loss during the end-of-batch flush.** `advance` raises
  IS022 → `markAborted` → next iteration's `if (closing ||
  aborted) break` exits the poll loop. Same semantics as today's
  ackOnly lease-loss path.
- **Pure-ignored batch (no routed event).** Walker accumulates
  `pendingIgnoredTo` through the whole batch; one flush at end.
  This is the case where the optimisation pays the most.
- **Routed-then-ignored at end of batch.** After the routed tx,
  `pendingIgnoredTo` is null; the trailing ignored events set
  it; end-of-batch flush issues one advance. One routed tx + one
  flush.
- **Ignored-then-routed (no trailing ignored).** Walker
  accumulates `pendingIgnoredTo`, then the routed event's tx
  covers it; reset to null; end of batch; no flush. One routed
  tx, zero extra calls.

### Invariants that must still hold

- **INV-SUB-P-008** (`last_seen` monotone): unchanged.
  `advance_subscription` is monotone and the walker only ever
  moves the cursor forward.
- **PM-020** (snapshot `source_version` equals the event_number
  of the last routed event): unchanged. The routed event's tx
  still sets `source_version = routed_event.event_number`.
- **PM-024** (`source_version <= last_seen`): unchanged but the
  inequality becomes strict more often. After ML-0005, once a PM
  has seen any ignored event after its last routed event,
  `last_seen > source_version`. This is already permitted by
  PM-024 ("<=", not "=="); the existing `concurrent.test.ts` PM
  scenario uses `assert.ok(snap.sourceVersion <= pmPos.lastSeen)`
  which still holds.
- **At-least-once delivery**: unchanged. The contract was always
  "redelivery is acceptable"; ML-0005 makes ignored-event
  redelivery slightly more likely (a crash between the last
  routed event and the end-of-batch flush will redeliver the
  intervening ignored run) but the route fn returning ignore is
  idempotent by construction.

### Test additions

All in `sdks/typescript/test/subscription.test.ts` or a new
`sdks/typescript/test/pm-ack-coalescing.test.ts`:

- **All-ignored batch advances cursor to last event.** Append N
  events of a type the PM doesn't route. Start the PM. Assert
  `last_seen` reaches event N after one batch, with exactly **one**
  `advance_subscription` call (instrument the client or count via
  `pg_stat_statements` / a wrapped Client).
- **Routed event covers prior ignored.** Append `[ignored, ignored,
  routed, ignored, ignored, routed]`. Assert exactly two
  `advance_subscription` calls happen (both inside routed txs); no
  standalone advance.
- **Trailing ignored flush.** Append `[routed, ignored, ignored]`.
  Assert one routed tx + one standalone advance. Assert
  `last_seen` reaches the trailing ignored.
- **Crash between routed tx and trailing flush.** Use
  `subscription.test.ts`'s heartbeat-lease-loss fixture: hold the
  PM right after the routed tx commits and before the flush; steal
  the lease; assert the trailing ignored events get redelivered and
  flushed by the new worker, with `last_seen` ultimately reaching
  the head.

### Acceptance check

The soak harness already measures this. After ML-0005:

- A 60s active run with the default workload should drain in
  **≈70s** (down from ~216s). That's the soak's pre/post baseline.
- The PM-FORWARD diagnostic block's `dispatched (causation)`
  and `forwarded (snapshots)` rows are unchanged (correctness is
  invariant); only the *time to reach* those numbers changes.
- Re-running the full SDK + conformance suites passes unchanged
  (no contract surface changes).

### Once shipped

- Delete the ML-0005 entry from `docs/maybe-later.md`.
- Add a one-line note in `docs/architecture.md` under the PM
  section: "ignored events on a PM's subscription are not
  individually acked; the next routed event's persist-and-ack tx
  covers them implicitly, and a single trailing
  `advance_subscription` per batch covers any tail of unrouted
  events."
- Update `tests/soak/README.md` *Performance baselining* section
  with the new expected drain time.

---

## Done items (delete on confirmation)

*(none yet — log resolutions here as TODO items close)*
