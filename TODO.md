# TODO

Outstanding follow-up tasks. Each item is its own piece of work;
pick them up one at a time.

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

**Depends on.** The SDK core-vs-conveniences split (landed: the
TypeScript SDK ships `instructed-sdk` and `instructed-sdk/core`, and
`sdks/porting-checklist.md` is the porting surface). Each
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
- **Projection rebuild.** Reset a projection by name:
  `delete_subscription` cascades the
  work-queue rows, then a fresh claim from `start_from = 'origin'`
  re-routes the whole history. The read-store wipe (truncate the
  projection's tables, flush its Redis namespace, drop its
  Elasticsearch index, etc.) is application-domain and is the
  operator's responsibility before the rebuild command runs --
  the SDK doesn't know where the read-store is. `instructedctl`
  provides the framework-side half: a one-shot "forget this
  subscription's state" that's safe to run while the worker is
  stopped.
- **Work-item operator surface.** Inspect the work queue per
  subscription (counts by state; oldest in-flight item; list
  `failed` rows with their `error_text`). One
  `skip_work_item_with_audit` command that moves a stuck
  `failed` row to a terminal state with an operator-supplied
  audit note. The default error policy never produces `failed`
  rows, so this surface is the dedicated escape hatch for a
  future `quarantineAfter` convenience wrapper and for manual
  operator action on poison events. Per
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

## 9. Documentation — next pass

**Why this exists.** The doc set is at a workable size but a
serious pass is warranted on writing quality, depth, and worked
content.

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

**Smaller opportunistic follow-ups:**

- A short *worked example* in `architecture.md` or `concepts.md`
  showing the two PM markers (`last_seen`, `source_version`)
  diverging across a couple of ignored events would help the
  reader internalise PM-024. The current wording is correct but
  abstract.
- The callout that a PM's subscription is shared across all its
  process instances and that a poison event stalls only its own
  partition could be promoted to a sized section with a
  recovery-pattern recipe ("how to skip a poison event in
  practice") if reader feedback flags it as surprising.

---

## 15. `claim_subscription` returns nullable diagnostic fields the SDK mistypes [SUPERSEDED]

**Why this exists.** Surfaced during the 2026-05-26 bank-account
multi-process work. Starting a second `pm:transfer` worker against
a steady-running first one prints one noisy line in the first
worker's log:

    [PM error] expected Date, got object

The PM keeps making progress; the next routing-worker poll
succeeds and produces no further error. So this is
benign-but-ugly, in the same class as TODO #14.

**Diagnosis.**

- `instructed.claim_subscription` (`sql/instructed.sql:1485-1505`)
  legitimately returns `claim_expires_at IS NULL` (and
  `claimed_by IS NULL`) in the `'already_claimed'` outcome under
  one specific race: the `FOR UPDATE SKIP LOCKED` step returns
  zero rows (some other tx holds the row lock right now), and
  the diagnostic unlocked re-read sees the row in the released
  `(NULL, NULL)` state -- which is exactly the steady-state
  routing-worker between-batches state under D-0025's per-batch
  claim/release. A second routing worker booting up trips this
  window because that's the only moment two workers race on the
  same subscription row in steady state. The SQL comment on the
  branch is explicit: *"the cursor value reported back uses the
  MVCC snapshot if we have one; otherwise NULL claimed_by /
  claim_expires_at carry the 'unknown' signal forward."*

- `Client.claimSubscription` (`sdks/typescript/src/client.ts:324`)
  unconditionally calls `toDate(r.claim_expires_at)` regardless
  of the `result` field. `toDate(null)` throws
  `expected Date, got object` because `typeof null === 'object'`.
  The thrown error surfaces via the registration's `onError`
  hook (in the bank-account example, prefixed `[PM error]`).

- No code path in the SDK consumes `claimExpiresAt` in the
  `'already_claimed'` branch; the field is purely diagnostic.
  The routing worker reacts to `'already_claimed'` by sleeping
  and retrying, and by the next poll worker 2's claim has
  committed so the pre-check returns clean fields.

**Class of bug.** SDK type contract narrower than the SQL
contract. Every language SDK port that mirrored the current
TypeScript shape would re-implement the same off-by-one
nullability. Material for TODO #2 -- the `Client.claimSubscription`
wrapper is exactly the thin procedure-binding the *core* layer
should get right once.

**What to do.**

- `Client.claimSubscription` return type: `claimExpiresAt:
  Date | null` (conditional `toDate`) and `claimedBy:
  string | null`.
- Public `ClaimSubscriptionResult` interface change in
  `sdks/typescript/src/client.ts` and re-exports through
  `index.ts`.
- Audit other procedure wrappers in `client.ts` for the same
  shape -- in particular the work-item analogues -- and confirm
  no other call sites are silently null-mistyped.
- Conformance test covering the contention-race branch (assert
  the SQL function returns NULL fields when the
  FOR UPDATE SKIP LOCKED step finds zero rows). The branch is
  currently described only in the SQL function's inline comments;
  promote it to the contract.
- One-sentence note in `docs/sql-contract.md` next to
  `claim_subscription` calling out the nullable diagnostic
  fields in the `'already_claimed'` outcome.

**Status.** Landed: `ClaimResult` is now a discriminated union
in `sdks/typescript/src/types.ts` (`'claimed'` arm carries
populated fields; `'already_claimed'` arm widens to nullable);
`Client.claimSubscription` branches on `result` and guards the
`toDate()` call; conformance test in
`tests/conformance/test/subscription-persistent.test.ts`;
`docs/sql-contract.md` notes the nullable diagnostic fields.
No SQL change. Section retained because TODO #15 is referenced
from in-tree code comments.

---

## 14. `claim_work_item` IS020 noise on first-startup race

**Why this exists.** Surfaced during the 2026-05-26 bank-account
example work. On a fresh DB, `Instructed.startWorker()` launches
the routing worker and the processing worker concurrently. The
processing worker's first `claim_work_item` typically runs before
the routing worker has created the subscription row, so the SQL
function raises `IS020` (`subscription X on $all (shard 0) not
found`). The SDK's processing worker catches `SubscriptionNotFound`
and treats it as "queue empty -- sleep and retry"; by the next
poll the subscription exists and life continues. No application
impact, but every fresh-DB startup logs a Postgres `ERROR` line
that looks alarming.

**Decision needed.** Pick one of:

1. **SQL contract change (preferred).** In `claim_work_item`,
   collapse "subscription does not exist" into "no candidate row
   available": return an empty result instead of raising IS020.
   The processing-worker semantics for both cases are identical.
   Side benefits: SDK loses a special-case catch; one fewer
   SQLSTATE for every language port to translate; Postgres log
   stays clean.

   This is a real contract change. IS020 stays alive for call
   sites where it's diagnostic (e.g. `release_claim`,
   `delete_subscription`); only `claim_work_item` softens. Needs
   an invariant update (the relevant `INV-SUB-W-*` entries), a
   `docs/decisions.md` entry, conformance-test updates, and a
   pass over the SDK's `processing-worker.ts` and `errors.ts` to
   drop the now-unreachable `SubscriptionNotFound` branch in the
   claim loop (but keep the class for the other call sites).

2. **SDK sequencing.** `startWorker()` `await`s the routing
   worker's first `claim_subscription` before launching the
   processing worker. Removes the race in the common case but
   doesn't help stand-alone processing workers running against a
   fresh DB. Doesn't address the contract-debt of "raise for a
   condition that is semantically not-an-error from the only
   caller that hits it."

3. **Doc-only.** Add a README note that the line is expected
   and benign on first startup. Cheapest, ugliest, doesn't
   improve the contract for SDK porters.

My lean is (1). Discuss before changing SQL.

**Output.** Either the SQL + invariants + decision-record change
plus SDK simplification, or a clear note in `docs/sql-contract.md`
about why we left it as-is.

---

## 12. `exclude` mechanism for `dispatch(..., { consistency: [...] })`

**Why.** A PM dispatching a command with
`consistency: [...own_subscription_name...]` self-deadlocks:
the wait needs the PM's subscription cursor to advance past the
dispatched events, but the cursor cannot advance until the PM's
`handle` returns, which cannot happen until the wait returns.
Hard self-deadlock until `consistencyTimeout` fires.

**What lands.**

1. **Public API.** `Instructed.dispatch` accepts an optional
   `exclude?: string[] | SubscriptionRef[]`, normalised the
   same way `consistency` is normalised (`string[]` defaults to
   `{ stream: "$all", name }`). References in `exclude` are
   removed from the resolved consistency wait set before
   `waitForProjection` is called.
2. **Default behaviour when dispatching from inside a PM.** The
   PM worker's internal `runCommand` call passes
   `exclude: [{ stream: <pm subscription stream>, name: <pm
   name> }]` automatically.
3. **Warning log on auto-exclusion.** When the PM worker
   auto-excludes its own subscription, emit a warning via the
   configured logger describing what was excluded and why.
   Include the PM name so the application can identify and
   remove the self-reference. **Do not silently drop** — that
   teaches bad habits.
4. **Explicit `exclude` from the application.** Honoured
   without warning — it's an explicit caller decision.
5. **Tests.** PM concurrent-tests cases: (a) PM dispatching
   with `consistency: [own_name]` does not deadlock and the
   operation completes; (b) the warning fires once per dispatch
   that triggered auto-exclusion; (c) explicit `exclude`
   suppresses the warning.
6. **Docs.** Short paragraph in `docs/architecture.md` "Strong
   consistency on dispatch" describing the `exclude` option and
   the PM auto-exclusion. Frame the constraint as "a
   subscription cannot wait for itself to make progress while
   it is the active processor".

**Related open items.** Follow-ons referenced from this index
entry: `ML-0006..0012` in `docs/maybe-later.md`, the SDK-level
PM-fan-out non-goal, and PM-E (deterministic event IDs on
PM-dispatched commands; see `docs/invariants.md` "Honest gaps
in v1").

---

Item numbers are stable: closed items are removed from the body
rather than renumbered, so existing in-tree references (e.g.
`TODO #3a`, `TODO #10` in code comments and docs) keep their
meaning. Gaps in the numbering are expected.
