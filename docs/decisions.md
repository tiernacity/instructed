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
