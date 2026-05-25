# Non-goals

What `instructed` deliberately does not do, and why. These are
positioning statements — they pin the shape of the library by
saying what it isn't. They're distinct from
[`maybe-later.md`](maybe-later.md), which records things we expect
to add eventually.

---

## No in-cluster registry of running aggregates

There is no in-memory or out-of-band registry of "which aggregates
are currently in flight". Postgres is the only source of truth
about an aggregate's version and state.

The hypothesis of `instructed` is that Postgres is the
coordination point. A registry is the first piece of state that
would need its own coherence story across SDK nodes.

Two concurrent commands on the same aggregate are serialised by
the unique constraint on `stream_events (stream_id, stream_version)`
— one wins, the other retries. See [D-0004](decisions.md#d-0004),
[D-0005](decisions.md#d-0005).

## No in-memory caching of aggregate state between commands

Each command runs a fresh load (snapshot + events) → execute →
append. There is no GenServer-equivalent holding post-command
state for the next caller.

Snapshotting is the load-cost mitigation. It's more honest than
caching because it doesn't need coherence across processes.
See [D-0004](decisions.md#d-0004).

## No per-aggregate advisory locks

The SQL contract does not take a per-aggregate advisory lock for
the duration of load-execute-append. Optimistic-lock retry is the
only mechanism by which two concurrent commands on the same
aggregate linearise.

Advisory locks tied to a session leak responsibility into
connection-pool behaviour. Keeping the only locking inside
`append_to_stream` makes lock ordering trivial.
See [D-0005](decisions.md#d-0005).

## No push delivery

Every subscriber polls. `append_to_stream` does not call
`pg_notify`, and no correctness property depends on it.

Notifications are not durable. Treating them as primary delivery
means two code paths to keep consistent. The polling-only
contract is operationally honest about its latency floor.

A wake-up optimisation via `LISTEN`/`NOTIFY` may be added later
as a pure optimisation on top of polling; every notification
must be droppable without affecting correctness.
See [D-0003](decisions.md#d-0003), ML-0002.

## No transient (fire-and-forget) subscriptions

Every event-delivery path goes through a persistent, leased,
cursored subscription. There is no separate "pubsub from now
onwards, no ack" primitive.

Keeping a single delivery primitive means one place to reason
about ordering, ack, and redelivery. A "live tail" use case is
served by a persistent subscription started at `start_from:
'current'` and deleted on teardown.

## No `:strong` consistency shorthand on dispatch

A dispatcher that wants strong consistency passes an explicit
list of subscription names:
`dispatch(..., { consistency: ["BalancesProjector"] })`. There
is no "wait for everything" shorthand.

A shorthand requires a registry of strongly-consistent handlers,
which is the kind of cross-process coordinated state the design
rejects. The explicit list also matches the question the caller
usually has in mind: "which projection do I want to read my own
write against?"
See [D-0010](decisions.md#d-0010).

## No hard-delete of events

The `events` table is append-only. There is no procedure that
removes event rows. Triggers raise on `UPDATE` and `DELETE`.

This tightens the append-only model and removes a footgun.
Operators who need GDPR-style erasure must use external tooling
and accept that they are breaking the model. A first-class
privacy/erasure path is a candidate for future work once we
understand deployment shapes.

## No user-facing event linking

The internal `stream_events` table supports "linking" an event
into multiple streams (this is how `$all` is populated). The
SQL contract does not expose linking to callers; user-named
streams contain only events appended directly to them.

Exposing linking would invite "category streams as a built-in"
without a clear schema story for them.

## No binary-blob event payloads; JSONB only

`events.data` and `events.metadata` are JSONB. Applications that
need to store binary data base64-encode it into the JSONB
structure.

JSONB lets the store be queried directly (useful for
administrative tooling), avoids the serializer-coupling problem
opaque `bytea` has, and lets Postgres handle compression.

## No middleware in the contract

A middleware/pipeline mechanism (`before` / `after` /
`afterFailure`) may be a useful SDK-level decoration on the
dispatch helper. The SQL contract knows nothing about it.

## `$all` is reserved at the schema level

The `streams` table has `CHECK (stream_uuid <> '$all' OR
stream_id = 0)`. A caller cannot create a user-named stream
that collides with the global stream name. The single seeded
row with `stream_id = 0` is the only `$all`.

## No co-transactional persist-and-ack

The SDK does not run projection or process-manager handlers
inside a transaction that also performs the terminal-success
step (DELETE work item for projections; UPDATE work item +
UPSERT snapshot for PMs). The handler runs outside any SDK
transaction; the terminal step runs in a separate short
transaction after the handler returns.

Projection targets are application-domain — Elasticsearch,
Redis, ClickHouse, HTTP APIs, in-memory maps. None can share a
Postgres transaction with `instructed`. Idempotency on
redelivery is the application's concern.

The SQL contract still supports a co-transactional terminal
step — `complete_work_item_projection` and
`complete_work_item_pm` are callable inside any well-formed
transaction — so a narrow Postgres-projecting-into-same-database
opt-in is achievable in a future SDK version. See
[D-0016](decisions.md#d-0016) and
[ML-0004](maybe-later.md#ml-0004).

## No SDK-level fan-out of one event to many PM instances

A PM `routeFn` returns at most one `{ partitionKey }` per event
(or `"ignore"`). The SDK does not parallelise an event across
multiple PM partitions of the same type. A future widening to
`{ partitionKey }[]` is reserved as a design option (see
[D-0018](decisions.md#d-0018)); v1 ships with the singular
return.

Applications that want fan-out (a `BatchApproved` event
affecting every `Order` in the batch) model it with composition:
a first PM consumes `BatchApproved` and emits per-instance
events (`OrderApprovalRequested × N`); a second PM consumes
those per-instance events.

The trade is performance/optimisation for expressivity and
flexibility. Modelling fan-out in domain terms keeps the
fan-out semantics (parallel? sequential? what if one fails?
what about ordering across the fanned-out events?) in
application code where the decisions belong, rather than
baking SDK-level assumptions every application then has to
override.
