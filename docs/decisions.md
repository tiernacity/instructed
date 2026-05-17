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

## D-0008 — Cursor advance is co-transactional with handler writes

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
