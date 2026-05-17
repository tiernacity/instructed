# Non-goals

Things `instructed` deliberately does *not* do. Each entry exists to
prevent accidental drift back toward a Commanded-shaped solution
during implementation. They are positioning statements — they say
what `instructed` *is*, by saying clearly what it is not.

This is distinct from [`maybe-later.md`](maybe-later.md): non-goals
are things we are not going to do. Maybe-laters are things we
expect to do, just not in v1. Where a non-goal has a neighbouring
maybe-later (e.g. "no push, but `LISTEN`/`NOTIFY` may come back as
a latency optimisation"), the cross-reference is explicit.

Each entry is sourced from one or more decisions in
[`decisions.md`](decisions.md). The non-goals here have all been
implied by Phase 4 mapping work; this document is a consolidation
exercise, not a new round of decisions.

---

## Coordination and process model

### NG-0001 — No in-cluster registry of running aggregates

There is no Commanded-style `Application.Registry` of active
aggregate processes. The SDK does not enumerate or coordinate
\"which aggregates are currently in flight\"; Postgres is the only
source of truth about an aggregate's version and state.

**Why:** the entire hypothesis of `instructed` is that Postgres is
the coordination point. A registry is the first piece of state
that would need its own coherence story across SDK nodes.

**What serves the same use case:** every command goes through the
same load-execute-append loop against the store. Two concurrent
commands on the same aggregate are serialised by INV-APPEND-013
(unique constraint on `(stream_id, stream_version)`); the loser
retries per AGG-010.

**Sources:** D-0004, D-0005.

### NG-0002 — No in-memory caching of aggregate state between commands

Each command runs a fresh load (snapshot + events) against the
store, executes against that state, and appends. There is no
GenServer (or equivalent SDK-side object) holding the
post-command state in memory for the next caller.

**Why:** an in-memory cache reintroduces the registry/coordination
problems we set out to avoid. Snapshotting (Part D of the
invariants) is the load-cost mitigation; it is more honest than
caching because it does not need coherence across processes.

**What serves the same use case:** snapshot policy tuning
(`snapshot_every: N` per aggregate). For hot aggregates the
load cost is one snapshot read plus a small tail of recent events.

**Sources:** D-0004. This non-goal also drops AGG-004 (in-memory
state), AGG-025 (self-subscription), and AGG-030 (`aggregate_lifespan`)
as mechanisms.

### NG-0003 — No advisory-lock-based per-aggregate command serialisation

The SQL contract does not take a per-aggregate advisory lock for
the duration of load-execute-append. Optimistic-lock retry is the
only mechanism by which two concurrent commands on the same
aggregate are linearised.

**Why:** advisory locks tied to a session leak responsibility into
connection-pool behaviour. Keeping the only locking inside
`append_to_stream` makes lock ordering trivial to reason about.

**Cross-reference:** D-0005 leaves the door cracked for an
opt-in advisory-lock variant later as a tuning option for
write-hot aggregates, but the v1 contract does not include it.

**Sources:** D-0005.

---

## Delivery model

### NG-0004 — No push delivery; no `LISTEN`/`NOTIFY` in the SQL contract

Every subscriber polls. `append_to_stream` does not call
`pg_notify`, and no correctness property depends on it. Workers
discover new events by calling `read_subscription_batch` on a
poll interval.

**Why:** notifications are not durable. They would have to be a
strict optimisation on top of polling, which means two code paths
and two sets of bugs. Polling alone is simpler and operationally
honest about its latency floor.

**Cross-reference:** [ML-0002](maybe-later.md) leaves room for
`LISTEN`/`NOTIFY` to be added later as a pure wake-up
optimisation; if it is, every notification must be droppable
without affecting correctness.

**Sources:** D-0003.

### NG-0005 — No transient subscriptions

There is no fire-and-forget pub/sub primitive. The
`subscribe(meta, stream)` callback from Commanded's adapter
contract has no `instructed` equivalent. Every event-delivery path
goes through a persistent, leased, cursored subscription.

**Why:** the two internal Commanded uses (aggregate self-watch,
strong-consistency notifications) are gone in `instructed`
(NG-0002 and NG-0006 respectively). The external "live tail" use
case is satisfied by a persistent subscription with
`start_from: :current` and a teardown call. Keeping a single
delivery primitive means there is one place to reason about
ordering, ack, and redelivery.

**What serves the same use case:** persistent subscription +
`start_from: :current` + `delete_subscription` on teardown. The
SDK may offer a `tail(stream, fn)` helper that wraps the pattern.

**Sources:** D-0007. Drops INV-SUB-T-001..005 from the realised
contract.

---

## Strong consistency

### NG-0006 — No `consistency: :strong` shorthand on dispatch

`Application.dispatch(command, consistency: :strong)` — Commanded's
"wait for all strongly-consistent handlers in this application" —
is not part of the v1 contract. The dispatcher names the
subscriptions it wants to wait for, explicitly:
`consistency: ["AccountBalanceProjector", "OrderPositionsProjector"]`.

**Why:** the shorthand requires a registry of strongly-consistent
handlers, which is the kind of cross-process coordinated state
NG-0001 rules out. The explicit list is the only shape that
needs no extra schema and no cross-process state — and it is the
more honest API anyway, because the caller almost always knows
exactly which projections they want to see catch up.

**What serves the same use case:** the explicit name list, plus
an SDK-level convenience that auto-collects names when the
dispatcher and the handlers run in the same SDK instance. The
SQL contract is unaffected by that convenience.

**Sources:** D-0010.

### NG-0007 — No synchronous strong consistency without a deliberate SDK wait

Because there is no push (NG-0004), `consistency: [names]` is
realised by polling subscription cursors after append. There is
no in-VM `receive` blocking on pubsub. The dispatcher's call
returns when polling shows every named subscription's `last_seen`
has caught up, or `{:error, :consistency_timeout}` when the
bound elapses.

**Why:** polling is the only mechanism consistent with NG-0004.
The latency floor is the SDK's poll interval.

**What serves the same use case:** tuning the poll interval and
`consistency_timeout` for the workload. The SDK exposes both.

**Sources:** D-0003, D-0010. (This is the realisation note of
CON-010, called out separately here because it is a positioning
statement about *what the dispatch wait feels like*.)

---

## Data model

### NG-0008 — No hard-delete of events

The `events` table is append-only. There is no gated
`enable_hard_deletes` setting and no procedure that removes event
rows. Triggers raise on `UPDATE` and `DELETE`.

**Why:** the reference adapter's hard-delete escape hatch is
optional in the contract anyway. Removing it tightens the
append-only model and removes a footgun. Operators who need
GDPR-style erasure must use external tooling and accept that
they are breaking the model.

**Cross-reference:** a first-class privacy/erasure path is a
candidate for `maybe-later.md` once we understand the deployment
shape. The current non-goal is specifically *Commanded-style
session-toggled hard-delete*, not erasure in general.

**Sources:** INV-DELETE-001 dropped in Pass 1.

### NG-0009 — No user-facing event linking

The reference adapter's ability to "link" an existing event into
additional streams (via `stream_events` rows with
`original_stream_id != stream_id`) is not exposed. Linking exists
internally only to the extent that the `$all` realisation chosen
in OQ-0001 requires it.

**Why:** the Commanded adapter contract doesn't expose linking
either; the reference adapter uses it only internally. Exposing
it would invite "category streams as a built-in" before we have
the schema story for it.

**Sources:** INV-LINK-001 dropped in Pass 1.

### NG-0010 — No binary-blob payloads; JSONB only

`events.data` and `events.metadata` are `JSONB`. The store does
not accept opaque `bytea` payloads. Applications that need to
store binary data must base64-encode it into the JSONB
structure.

**Why:** JSONB lets the store be queried directly (useful for
admin/inspection tooling later), avoids the serializer-coupling
problem `bytea` has, and lets Postgres handle compression. The
cost of forcing base64 for genuinely-binary payloads is real but
small.

**Cross-reference:** a `data_bin BYTEA` column could be added
later as a non-breaking schema change if a real workload demands
it. Not currently tracked in `maybe-later.md` — revisit when
the first SDK example surfaces.

**Sources:** INV-META-011 mapping in Pass 1.

### NG-0011 — `$all` is reserved at the schema level

The `streams` table has `CHECK (stream_uuid <> '$all')`. A
caller cannot create a user-named stream that collides with the
global stream name.

**Why:** the reference adapter relies on convention; we prefer a
constraint. This is a small tightening, listed here for symmetry
with the other data-model entries.

**Sources:** INV-STREAM-003 mapping in Pass 1.

---

## Convenience layers

### NG-0012 — Middleware is an SDK-level decoration, not part of the contract

Commanded's `pipeline_before` / `pipeline_after` /
`pipeline_after_failure` middleware exists at the application
layer. `instructed` SDKs may offer equivalent decorations on the
dispatch helper, but the SQL contract knows nothing about it.

**Why:** middleware is convenience, not semantics. Treating it as
SDK-only keeps the store contract small and lets different SDKs
make different choices.

**Sources:** DSP-005 mapping in Pass 3.

### NG-0013 — `aggregate_lifespan` is not a concept

There are no per-aggregate process lifetime controls (no
`:hibernate`, no inactivity timeout, no `:stop` lifespan return
value). With NG-0002 there is no process to hibernate or stop.

**Sources:** AGG-030 dropped in Pass 1. Subsumed by NG-0002;
called out separately because Commanded users may look for the
knob.

### NG-0015 — No co-transactional persist-and-ack; handlers own idempotency

**Source:** D-0016 (supersedes D-0008).

**What we are not doing:** the SDK does not run projection /
process-manager handlers inside a transaction that also advances
the subscription cursor. The handler receives the event and
returns; the SDK advances the cursor in a separate short
transaction after the handler returns successfully. There is no
transaction, connection, or any other SDK-owned resource passed
to the handler.

**Why:** the projection target is application-domain. Real
projections target Elasticsearch, ClickHouse, Redis, BigQuery,
an external HTTP API, an in-memory cache, a different database,
or any combination. None of these can share a Postgres
transaction with `instructed`. The plumbing required to keep the
co-transactional property for the Postgres case was paid by
every user and reaped only by Postgres-targeted projections that
happened to write to the same database the event store sits in.
Inverted bargain; reversed.

**Consequences:** delivery is at-least-once. A worker that
crashes between handler-return and cursor-advance will
redeliver the event on next claim. Applications are responsible
for handler idempotency — typically via an idempotent UPSERT
(Postgres), an `_id` keyed on `event_id` (Elasticsearch),
`SETNX` (Redis), or whatever the target's idempotency story is.
This is the same contract Commanded provides.

**Future variant not precluded:** an opt-in flag that re-enables
co-transactional persist-and-ack for the narrow case of
projecting into the same Postgres database is a plausible v2
feature. The SQL contract already supports it (`advance_subscription`
is callable inside any well-formed transaction). Not tracked as
an `ML-` entry until a real workload demands it.

---

### NG-0014 — `delete_subscription` on a missing subscription returns an error

Not a positioning statement on its own, but a contract-level
non-goal worth pinning: we do not adopt the reference adapter's
silent `:ok`. Missing-on-delete is `subscription_not_found`.

**Sources:** D-0009.

---

## What is *not* a non-goal

A few things that look like they might belong here but don't:

- **Partitioned-consumer subscriptions** (`concurrency_limit > 1`,
  `partition_by`). Tracked as [ML-0001](maybe-later.md) — we
  expect to add this in a future version. Forward-compat
  constraints on the v1 schema are recorded there.
- **`LISTEN`/`NOTIFY` push optimisation.** Tracked as
  [ML-0002](maybe-later.md). The non-goal (NG-0004) is on the
  *correctness contract*; an optional wake-up optimisation is
  compatible with that.
- **Compensation as a first-class saga primitive.** Tracked as
  D-0001 / Phase 6 — we *will* address it, with a mechanism to
  be determined.
- **Server-side selector evaluation.** Tracked as OQ-0003 —
  open question, deferred to Phase 8 informed by the first SDK
  example.
- **First-class privacy/erasure.** Not currently tracked
  formally; NG-0008 is specifically about Commanded-style
  hard-delete, not about erasure in general.

These are deliberately kept out of `non-goals.md` so the document
stays a list of things we are *not* doing, distinct from things
we are *not yet* doing.

---

## Cross-check against `mapping.md`

Every \"dropped\" / \"non-goal candidate\" / \"looser by
elimination\" verdict in `mapping.md` has a corresponding entry
above:

| `mapping.md` ID            | Non-goal |
|----------------------------|----------|
| INV-APPEND-040/041 (hard-delete) | NG-0008 |
| INV-META-011 (JSONB only)        | NG-0010 |
| INV-STREAM-003 (`$all` reserved) | NG-0011 |
| INV-LINK-001 (linking)           | NG-0009 |
| INV-DELETE-001                   | NG-0008 |
| INV-SUB-T-001..005 (transient)   | NG-0005 |
| INV-SUB-P-062 (delete strictness)| NG-0014 |
| AGG-004 (in-memory cache)        | NG-0002 |
| AGG-011 (mailbox serialisation)  | NG-0001, NG-0003 |
| AGG-025 (self-subscription)      | NG-0002 |
| AGG-030 (lifespan)               | NG-0013 |
| CON-002/003/013 (pubsub+ETS)     | NG-0004, NG-0006, NG-0007 |
| CON-011 (`consistency: :strong`) | NG-0006 |
| DSP-001 (process registry)       | NG-0001 |
| DSP-005 (middleware)             | NG-0012 |
| PM-040 (per-instance GenServer)  | NG-0001, NG-0002 |
