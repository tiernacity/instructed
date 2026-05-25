# Concepts

A short primer on CQRS / event sourcing as `instructed` uses the
terms, and how the pieces fit together when you build an application.
For the formal contract see [`invariants.md`](invariants.md); for
the implementation see [`architecture.md`](architecture.md).

---

## The shape

```
                ┌──────────────┐
   command  ──▶ │  aggregate   │ ──▶ events ──┐
                └──────────────┘              │
                       ▲                      ▼
                       │              ┌───────────────┐
                       │              │   event log   │
                       │              │   (Postgres)  │
                       │              └───────────────┘
                       │                      │
                       │ commands             │ events
                       │                      ▼
                ┌──────────────┐      ┌───────────────┐
                │   process    │◀─────│  subscription │
                │   manager    │      │    (cursor)   │
                └──────────────┘      └───────────────┘
                                              │
                                              ▼
                                      ┌───────────────┐
                                      │  projection   │
                                      │  (read model) │
                                      └───────────────┘
```

The event log is the system of record. Everything else is a fold
over it.

## Events and streams

An **event** is an immutable record of something that happened, in
the past tense (`AccountOpened`, `MoneyDeposited`,
`TransferRequested`). Once written, events are never modified or
deleted.

A **stream** is an ordered sequence of events, identified by an
opaque string. The two common stream conventions:

- **One stream per aggregate instance.** `account-{uuid}` is the
  stream for one account; appending an event to that stream means
  "this happened to this account". Streams created this way are
  the source of truth for the aggregate's state.
- **`$all`** is the global stream that contains every event ever
  written, across every stream, in commit order. You don't write
  to `$all` directly; appending to any stream automatically
  registers the event in `$all`. Subscribers typically read from
  `$all`.

Every event has:

- `event_id` — globally unique UUID.
- `stream_id` + `stream_version` — its position within its
  originating stream, starting at 1.
- `event_number` — its position within `$all`, globally
  monotonic, gapless.
- `event_type` — a caller-chosen string (typically the type name).
- `data` and `metadata` — JSONB payloads.
- `causation_id` and `correlation_id` — optional UUIDs threading
  events to the command that produced them and to a wider
  conversation.
- `created_at` — UTC timestamp.

## Aggregates and commands

A **command** is a request to change state, in the imperative
(`OpenAccount`, `DepositMoney`, `RequestTransfer`). Commands may
fail; events may not.

An **aggregate** is a unit of business logic that decides whether
a command succeeds and, if so, what events it produces. An
aggregate has:

- **An identity** — a UUID that names this instance.
- **A state** — the fold of all events in its stream.
- **Command handlers** — functions of `(state, command) → events`
  that either return events or throw / return an error.
- **Event appliers** — functions of `(state, event) → state`
  that update the state when an event is applied. These must
  never fail; they run on stored events during hydration.

The command-handling cycle is:

1. Read the aggregate's stream from the log.
2. Fold the events into state via the appliers.
3. Run the command handler against that state.
4. If it produces events, append them to the stream with
   `expected_version = currently_observed_version`.
5. If the append is rejected because someone else appended first,
   start over.

This last step — the retry on version conflict — is **optimistic
concurrency control**. It's how two concurrent commands on the
same aggregate are serialised: at most one of them wins each round
of the race; the loser re-reads and tries again.

## Projections

A **projection** is a read model — typically a query-shaped view
of the data that the events imply. A `Balances` projection
tracking account balances. A `OpenOrders` projection of orders
not yet shipped.

Projections are built by **subscribing** to the event log,
processing each event in turn, and writing to the read model.
The projection's storage is the application's concern: it might
be a SQL table, an Elasticsearch index, a Redis hash, an
in-memory map.

Projections are **eventually consistent**: there's a gap between
a command appending an event and a projection processing it.
The SDK provides a "wait until this projection has caught up"
helper when an application needs to read its own writes
synchronously.

A projection declares how it wants its events partitioned:

- **`{ kind: 'sequential' }`** (the default) — every event lands
  on one partition; the handler runs strictly in `event_number`
  order with no parallelism. The closest match to the
  one-cursor-one-worker shape.
- **`{ kind: 'per-event' }`** — each event becomes its own
  partition; max parallelism.
- **`{ kind: 'per-key', key: (e) => string }`** — the user
  supplies a partition key; events sharing a key are delivered
  in order, events on different keys run in parallel.

Projections that need routing-side filtering (the projection
ignores some event types entirely) skip the `partitionBy` sugar
and pass a `routeFn` directly: `(event) => { partitionKey } |
"ignore"`.

## Subscriptions and the work queue

A **subscription** is the framework's bookkeeping for a
projection or process manager. Each subscription is split
internally into two cooperating halves:

- A **routing cursor** (`last_seen`) advanced by a single
  *routing worker* that reads new events past the cursor, runs
  the user's `routeFn` on each, and atomically writes both the
  new cursor and the per-partition **work items** for the
  events that route to a partition.
- A **work queue** (`subscription_work_items`) consumed by N
  *processing workers* in parallel. A processing worker claims
  one item at a time via `claim_work_item`, runs the user's
  handler, and writes the terminal-success step. Within a
  partition, items are processed serially in `event_number`
  order; across partitions, processing is concurrent.

The two halves are isolated. The routing-side lease lives on the
`subscriptions` row; the processing-side lease lives on each
work-item row. A processing worker does not take a subscription
lease, and the routing worker does not claim work items. A slow
or poison handler stalls only its own partition; other
partitions keep draining.

Subscriptions are **persistent** (cursors and queue survive
restarts), **single-active on routing** (one routing worker per
subscription at a time), **parallel on processing** (any number
of processing workers per subscription), and **polled** (no
`LISTEN`/`NOTIFY` in the contract).

Delivery is **at-least-once**: a processing worker that crashes
between handler-return and the terminal-success call will see
the item redelivered when its lease expires. Handlers must be
idempotent.

When an application needs to wait for a subscription to catch
up (read-your-own-writes), the SDK polls a server-side
**catch-up predicate**: the routing cursor has reached the
target AND no work item below the target is still in flight.
Either condition alone is insufficient; the atomic write of the
route step makes both observable together.

## Process managers

A **process manager** coordinates multiple aggregates over time.
It subscribes to the event log like a projection, but instead of
writing to a read model it **dispatches commands**.

The canonical example: a money transfer. A `TransferRequested`
event arrives; the PM dispatches a `Withdraw` to the source
account; on `Withdrawn` it dispatches a `Deposit` to the
destination. If the withdraw is refused, the PM observes the
`WithdrawalRefused` event and completes the partition — there's
nothing to undo because the debit never happened.

A process manager has three callbacks:

- **`routeFn(event) -> { partitionKey } | "ignore"`** — decides
  which PM-instance partition an event belongs to (or skips it
  entirely). Pure; no state parameter.
- **`apply(state, event) -> state`** — pure state fold. Runs
  during PM-state rebuild (after a missing snapshot or a
  module-version mismatch) and on the triggering event before
  `handle`.
- **`handle(state, event) -> { commands?, complete? }`** — the
  side-effectful step. Returns commands to dispatch and an
  optional `complete: true` flag that DELETEs the PM
  partition's snapshot + every remaining work item in one
  transaction.

Process manager state is persisted as a snapshot keyed by the
PM's partition. The split between `apply` (pure fold) and
`handle` (side effects + lifecycle) means rebuild can replay
state cheaply without re-dispatching commands. PMs express
sagas as event-driven state machines.

A single PM type can have any number of partitions running
concurrently — each `partitionKey` value is an independent
instance. A poison event on one partition does not block
another.

## Snapshots

A **snapshot** is the cached state of an aggregate or process
manager at a particular version, used to skip replay of old
events on load. Snapshots are:

- **Advisory** — the event stream is always the source of
  truth; snapshots can be deleted at any time without affecting
  correctness.
- **At-most-one** per source — `record_snapshot` is an upsert.

The library provides the storage primitive. *When* to snapshot
(every N events? every M minutes?) is the application's policy
choice.

## Strong consistency on dispatch

By default, a command returns as soon as its events are
durably appended. Projections catch up on their own time.

Sometimes a caller needs to read its own write — dispatch a
command, then immediately query a projection and see the
results. The SDK supports this by letting `dispatch` block
until named projections have caught up to the appended events:

```ts
await app.dispatch("Account", aliceStream, deposit, {
  consistency: ["Balances"],
});
// Balances has processed every event from the deposit.
const row = await db.query("SELECT * FROM balances WHERE …");
```

The list of subscriptions to wait for is **explicit** — there's
no "wait for everything" shorthand. The wait polls the
subscription's catch-up predicate (routing cursor at-or-past the
appended events AND no in-flight work items below them), so the
latency floor is the poll interval; the timeout is configurable.

## Causation and correlation

Two UUIDs threaded through events for traceability:

- **`causation_id`** — the id of the immediate cause. A
  command's events all share `causation_id = command_id`. A
  process manager's dispatched commands carry
  `causation_id = triggering_event_id`.
- **`correlation_id`** — a conversation id that spans a whole
  multi-step workflow. Set once at the top of a workflow;
  propagated through every command and event the SDK threads.

`instructed` propagates both automatically across dispatch
boundaries when you use the high-level API. They're plain
opaque UUIDs to the store; the store assigns no meaning.

---

## How to write an application with `instructed`

A typical application has:

1. **A connection to Postgres** with the `instructed` schema
   installed (one-off; `psql -f sql/instructed.sql`).
2. **Aggregate definitions** — for each aggregate type: its
   initial state, its command handlers, and its event appliers.
3. **Projection definitions** — for each read model: which
   subscription to use, and a handler `(event, ctx) => Promise<void>`.
4. **Process manager definitions** — for each saga: a router
   that maps events to `start` / `continue` / `stop` / `ignore`,
   plus a handle function that produces commands.
5. **An `Instructed` instance** that registers all of the above
   and exposes `dispatch` for commands and `startWorker` to run
   the projections and process managers.

The shape, in TypeScript:

```ts
import { Instructed } from "instructed-sdk";

const app = new Instructed({ db: pool });

app.registerAggregate(Account);             // { type, initialState, execute, apply }
app.registerAggregate(Transfer);

app.registerProjection("Balances", {
  // Default partitioning is `{ kind: 'sequential' }`. Use a
  // routeFn if you also want routing-side filtering.
  handler: async (event) => { /* fold into read store */ },
});

app.registerProcessManager("TransferProcessManager", {
  routeFn: (event) => {
    const id = (event.data as { transferId?: string }).transferId;
    return id ? { partitionKey: id } : "ignore";
  },
  initialState: () => ({ stage: "starting" }),
  apply: (state, event) => /* pure state fold */ state,
  handle: async (state, event) => {
    // Return commands and/or { complete: true } to terminate.
    return { commands: [/* ... */], complete: false };
  },
});

const worker = await app.startWorker();     // claims subs, spawns workers

await app.dispatch("Account", aliceStream, { kind: "Open", owner: "alice" });
await app.dispatch("Account", aliceStream,
                   { kind: "Deposit", amount: 1000 },
                   { consistency: ["Balances"] });
await app.dispatch("Transfer", transferStream,
                   { kind: "Request", from: "alice", to: "bob", amount: 300, transferId });

await worker.close();                       // graceful shutdown
```

See [`examples/bank-account/`](../examples/bank-account/) for a
working end-to-end version of exactly this shape, including the
aggregate / projection / PM definitions.

## What `instructed` does and doesn't do for you

**Does:**

- Store events durably with global and per-stream ordering.
- Run aggregate command cycles with optimistic concurrency.
- Run projection and process manager workers with leased,
  cursored subscriptions.
- Persist process manager state as snapshots.
- Wait for named projections to catch up before returning from
  dispatch.
- Translate every error into a typed exception.

**Doesn't:**

- Cache aggregate state in memory between commands — every
  dispatch reloads from the store (snapshots make this cheap).
- Provide a saga DSL — process managers dispatch commands,
  and that's the saga primitive. Conveniences for
  rollback/compensation/step-tracking belong in optional
  packages, not the core.
- Push events to subscribers — workers poll. Latency floor is
  the poll interval.
- Filter events server-side — the SDK reads a batch and applies
  the selector locally before invoking the handler.
- Decide *when* to snapshot — that's a per-aggregate policy
  choice the application makes.
- Distribute one subscription across multiple workers — single
  active worker per cursor. Throughput scaling is by splitting
  into multiple named subscriptions.
- Provide handler-side transactional atomicity with the cursor
  advance — handlers are opaque to the SDK, and projections
  often target stores other than Postgres anyway. Idempotency
  is the handler's job.
