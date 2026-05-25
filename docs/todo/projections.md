# Projections — outstanding work

**Origin:** 2026-05-23 re-review of the subscription model
(SUB-A in [`subscriptions.md`](./subscriptions.md)). Under the
SUB-A proposed design (Design 3 + the PM-F routing shape),
projections are not a separate primitive — they are a subscription
kind that uses the same `subscription_work_items` mechanism with
a different `partition_by` policy. The mechanism is in SUB-A;
the projection-specific API surface and the projection-shaped
concerns are here.

This file collects projection-side work that rides on the SUB-A
substrate. It does *not* re-state the SUB-A design; read SUB-A
first.

Status legend: **Decided** / **Designed, awaiting confirmation** /
**Open**. Release relevance: **Pre-release** / **Post-release**.

---

## PRJ-A — Projection registration surface

**Status:** Decided. Three-mode `PartitionBy` (`sequential` /
`per-event` / `per-key`) as the v1 SDK surface, default
`sequential`. A future refinement toward a less-opinionated
surface (e.g. a free-form partition function) is deferred to
ML-0011; not in scope for the initial implementation.
**Release-relevance:** Pre-release (rides on SUB-A).

### Problem

Today's `registerProjection(name, handler)` assumes a single
cursor, a single worker, strict-sequential delivery. Under the
SUB-A proposed design, every subscription has a `partition_by`
policy and concurrency follows from it. Projections need to
express this.

### Proposal

```ts
type PartitionBy =
  | { kind: "sequential" }                          // one partition, ordered, one worker
  | { kind: "per-event" }                           // each event its own partition; full parallelism
  | { kind: "per-key", key: (event) => string }     // parallel across keys, ordered within

registerProjection(name, {
  partitionBy?: PartitionBy,                        // default: { kind: "sequential" }
  handler: (event, ctx) => Promise<void>,
})
```

Default is `sequential` so existing projections continue to work
without code change. The three modes map directly onto SUB-A's
generalisation table:

| `partitionBy`                           | Concurrency                                    | Application responsibility                 |
| --------------------------------------- | ---------------------------------------------- | ------------------------------------------ |
| `{ kind: "sequential" }`                | 1                                              | none beyond today                          |
| `{ kind: "per-event" }`                 | up to `min(workers, events_in_flight)`         | handler must be commutative                |
| `{ kind: "per-key", key }`              | up to `min(workers, distinct_active_keys)`     | handler ordered within a key only          |

The `key` function is pure, runs at the routing layer (same place
PMs' `RouteFn` runs), and may not access projection state — by
construction routing is stateless w.r.t. processing state, same
as PMs.

### Why three modes and not a free-form function (v1 stance)

A free-form `partitionBy: (event) => string | null` would
collapse sequential and per-key into one shape. For v1 we keep
them distinct because:

- `sequential` is the most common projection shape today and
  warrants the cheapest default with no application code.
- `per-event` is a distinct *intent* ("I am commutative; route
  for maximum parallelism") that benefits from being explicit
  rather than expressed as `(e) => e.event_id`. Tooling
  (`instructedctl`, soak harness) can recognise the kind and
  surface it.
- `per-key` is the case where a function makes sense; isolating
  it to that branch keeps the other two free of accidental
  complexity.

This is a v1 stance, not a permanent one. A future SDK
revision may collapse to a less-opinionated surface; see
ML-0011. Widening singular → free-form is backwards-compatible
(existing three-mode usage maps to the free-form equivalents);
narrowing is not.

### What lands

1. Type definitions for `PartitionBy` in the TS SDK.
2. `registerProjection` accepts the optional `partitionBy`;
   default = `sequential`.
3. The convenience layer translates `partitionBy` into the
   routing-layer `RouteFn` that the SUB-A core expects.
4. Worked examples in `docs/concepts.md` (or wherever
   projections are documented) showing each of the three modes.
5. One-line callout in the porting checklist (TODO #2): the
   *core* SDK only needs the routing-layer primitive; the
   `PartitionBy` ergonomics are convenience-layer surface and
   may differ per language.

---

## PRJ-B — No `apply` / `handle` split for projections

**Status:** Decided.
**Release-relevance:** Pre-release (clarifies PRJ-A).

### What's settled

PM-C splits the PM callback into `apply` (pure state fold) and
`handle` (commands only) because:

1. PM state is a snapshot; rebuilding it via replay must not
   re-dispatch commands.
2. SNAP-002 version mismatch must be recoverable losslessly.

Neither reason applies to projections:

1. A projection's state is the external **read model** (the
   application's own table, document store, etc.) — not a
   framework-managed snapshot. The framework neither writes nor
   reads projection state.
2. There is no `snapshot_module_version` for projections;
   there's no SNAP-002 sharp edge to remove.

Projections therefore keep the single-callback shape:

```ts
handler(event, ctx) → Promise<void>
```

The handler writes to the read model; framework records the
work-item as `done` on successful return.

### Consequence for rebuilds

Rebuilding a projection from scratch is *not* a framework-level
replay through `apply`. It is an operator-level action: wipe the
read model, reset the subscription (delete work-items, reset the
routing cursor), restart. The handler runs against every event
from origin. This is the same shape projections have today,
just adapted to the new schema. See PRJ-D.

---

## PRJ-E — Work-item lifecycle: immediate delete on success

**Status:** Decided.
**Release-relevance:** Pre-release (codifies the SUB-A
"Work-item lifecycle by subscription kind" contract for the
projection side).

### What's settled

On handler success, the framework DELETEs the work-item row
via `complete_work_item_projection`. The DELETE runs as its
own short SDK-owned transaction *after* the handler returns;
the handler is opaque to the SDK and may target any store
(see D-0016 in `docs/decisions.md`, and PRJ-C below for why
the earlier "same tx as read-model write" wording was
dropped). No `done` row is ever persisted for a projection.

Rationale:

- A projection's state is the external read model; the
  framework neither owns nor reconstructs it (PRJ-B). The
  `done` row would serve no purpose post-handler.
- This makes the catch-up predicate simpler in the projection
  case: any work-item with `event_number <= T` for the
  subscription means work is outstanding. (The unified
  `state IN ('pending','claimed','failed')` filter in the
  predicate is redundant-but-harmless for projections; it's
  load-bearing for PMs.)
- Audit / ops-visibility, if needed, lives in the application's
  audit log rather than in framework state.

### Race safety at the start of `waitForProjection`

A caller that appends event N and immediately calls
`waitForProjection(S, N)` must not observe a spurious
"caught-up" before the routing worker has had a chance to
process N. This holds **iff the routing cursor advance and
the work-item INSERTs commit in a single transaction** —
which is exactly what the SUB-A routing hot path does.

The property is independent of immediate-delete (it would
apply equally if we retained `done` rows), but immediate-delete
makes the predicate's intent clearer: "any row at or below
the target means work is still outstanding".

### Configurable retention

Deferred: applications that want a brief ops-visibility window
("keep `done` rows for K minutes so I can see what processed
recently in `instructedctl`") would need a `keepDoneFor`
option on `registerProjection`. Tracked as a maybe-later
entry; default ships as immediate DELETE.

---

## PRJ-C — Read-model transactionality (DROPPED, see D-0016)

**Status:** Dropped. Back-reference only.
**Release-relevance:** N/A.

### Why this is dropped

An earlier version of this section proposed that the
framework thread its own work-item-DELETE transaction
through the projection handler's `ctx` so applications could
opt into a same-tx pattern with the read-model write. That
proposal contradicts the established design decision D-0016
(`docs/decisions.md`): the projection handler is opaque to
the SDK; it does not receive a Postgres connection, an ORM
handle, or any other framework-owned resource. Projection
targets are application-domain (Postgres, Elasticsearch,
Redis, BigQuery, HTTP APIs, ...) and only a Postgres-in-the-
same-database target could possibly share a transaction with
the framework's DELETE. Paying that plumbing cost on every
handler signature to benefit one narrow target was rejected
in D-0016 and remains rejected here.

### What replaces it

- Delivery is at-least-once. Handlers MUST be idempotent
  against redelivery (UPSERT, `_id` keyed on `event_id`,
  `SETNX`, or whatever the target store offers). This is the
  same contract D-0016 already specifies for the legacy
  single-cursor projection worker.
- The framework runs `complete_work_item_projection` as its
  own short SDK-owned transaction *after* the handler
  returns. If the handler throws, the DELETE never runs and
  redelivery happens via lease expiry / re-claim.
- The `ctx` passed to the projection handler stays opaque
  (`workerId`, `partitionKey`, `eventNumber`, `attempt`,
  `signal`). No tx, no `Queryable`, no read-model writer.

See also: `docs/non-goals.md` on projection targets;
`docs/concepts.md` and `docs/sql-contract.md` for the
at-least-once / idempotent-handler contract.

---

## PRJ-D — Projection rebuild as an operator action

**Status:** Designed, awaiting confirmation.
**Release-relevance:** Pre-release (small).

### What's settled

A rebuild is wipe-and-replay:

1. Application wipes its read model (its responsibility — the
   framework doesn't know the read model schema).
2. Operator invokes `instructedctl rebuild-projection <name>`
   (or equivalent), which:
   - Deletes all `subscription_work_items` rows for the
     subscription.
   - Resets `subscriptions.last_seen` to 0 (or `:origin`).
3. Routing worker re-routes from origin; processing workers
   re-run the handler against every event.

### Mechanics under SUB-A

Step (2) is the only framework-side action. The SQL is trivial:

```sql
BEGIN;
DELETE FROM subscription_work_items WHERE subscription_id = $1;
UPDATE subscriptions SET last_seen = 0 WHERE subscription_id = $1;
COMMIT;
```

Concurrency caveat: the routing worker and any processing
worker for this subscription MUST be quiesced (lease released)
before the reset, otherwise the cursor moves forward
immediately and the reset is a no-op. The `instructedctl`
command should refuse to run if any active leases exist for
the subscription, with a documented way for the operator to
force-release leases first.

### What lands

- `instructedctl rebuild-projection` command (when
  `instructedctl` ships, TODO #7).
- Programmatic equivalent in the TS SDK for tests and
  controlled deployments.
- Docs callout: "A rebuild is destructive of work-item history;
  if you need an audit trail of past failures, archive
  `subscription_work_items` first."

---

## Concerns that live elsewhere

These are projection-relevant but tracked in other files. Cross-
referenced here so the projection reader knows where to look.

- **Strict-sequential cost.** A projection with
  `partitionBy: { kind: "sequential" }` pays one INSERT + one
  UPDATE per event under the SUB-A proposed design (vs.
  today's pure-cursor one UPDATE per batch). The
  one-mechanism-vs-two-mechanisms question is **decided** in
  SUB-A: one mechanism. PRJ-A's `sequential` mode is
  implemented over work-items; the per-event cost is accepted
  as the price of architectural uniformity.

- **Cross-stream `waitForProjection` guard.** CON-B in
  [`consistency.md`](./consistency.md) documents a
  silent-wrong-answer bug when `waitForProjection` is called
  for a projection on a stream different from the one just
  appended to. The fix is a separate artifact that ships
  against today's schema and re-fits to SUB-A's
  catch-up predicate.

- **`INV-SUB-*` triage.** Several invariants that currently
  describe single-cursor mechanics will change shape or become
  `[mechanism-only]` under SUB-A. The triage is a SUB-A
  deferred artifact; projection-related invariants
  (`INV-SUB-P-020`, `INV-SUB-P-031`, etc.) are part of it.

- **Conformance harness updates.** TODO #11 covers the
  conformance-test re-fit and is blocked on SUB-A landing.
  Projection-shaped tests (per-partition ordering under
  concurrent claimants, commutative-projection invariants,
  catch-up predicate for `waitForProjection`) belong there.

---

## Connections to other files

- The substrate (work-items, routing, claim semantics,
  catch-up predicate) lives in
  [`subscriptions.md`](./subscriptions.md) SUB-A.
- PM-specific concerns (RouteFn shape, `apply`/`handle` split,
  deterministic command IDs, lifecycle) live in
  [`process-manager.md`](./process-manager.md). PRJ-B is the
  projection-side decision to *not* take PM-C's split.
- `waitForProjection` shape and the cross-stream guard live
  in [`consistency.md`](./consistency.md) CON-B.
