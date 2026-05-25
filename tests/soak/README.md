# Soak harness (`tests/soak/`)

Performance gauge + invariant fuzzer over time. Composes several
mechanisms (aggregate OCC, projector subscriptions, process-manager
dispatch) against a single Postgres for a configurable duration with
periodic failure injection (worker respawn + lease theft), then runs
invariant checks that mirror the per-PR composed-concurrency tests
— but at higher N, over longer wall-clock, with deliberately churned
leases.

Under SUB-A (slice 11 re-baseline), each projector / PM "slot"
actually owns a *pair* of workers: a routing worker (single-active
per subscription — turns events into `subscription_work_items`
rows) and a processing worker (claims work items and runs the
handler — multiple may be active concurrently per subscription).
The slot-respawn and lease-theft failure injections operate on the
pair; lease theft specifically affects only the routing-side lease,
which is the only place a subscription-level lease lives under
SUB-A.

Scope:

- **Not** part of the per-PR test path. Runs nightly / on demand.
- Reuses the SDK-test Postgres (`docker compose up -d postgres`) but
  on its own database (`instructed_soak` by default) so it can't
  clobber the SDK or conformance fixtures.
- Exits 0 if every invariant check passed, 1 otherwise. Performance
  facts always print.

This file doubles as the interpretation guide called out in TODO #3b:
each invariant check below names the ID (or the descriptive tag if
none exists) that will appear in a `VIOLATION` line, and what the
likely cause is.

## Running

```sh
docker compose up -d postgres
cd tests/soak
npm install
npm start -- --duration 60 --accounts 8 --dispatchers 6 \
             --projectors 3 --pms 2 --trigger-streams 4
```

Type-check (no run):

```sh
npm run type-check
```

## Flags

All optional. Defaults are tuned for a 30s smoke run that should
finish well inside a minute on a developer laptop.

| Flag                       | Default | Meaning                                                                   |
| -------------------------- | ------- | ------------------------------------------------------------------------- |
| `--duration`               | `30`    | Seconds of active workload.                                               |
| `--accounts`               | `6`     | Counter-aggregate streams the workload writes to.                         |
| `--trigger-streams`        | `3`     | Streams the trigger appenders write `Triggered` events to.                |
| `--dispatchers`            | `4`     | Concurrent dispatchers running `runCommand(add)` against random accounts. |
| `--trigger-appenders`      | `2`     | Concurrent tasks appending `Triggered` events for the PM to forward.      |
| `--projectors`             | `2`     | Projector slots competing for the `p-balances` subscription on `$all`.    |
| `--pms`                    | `2`     | PM slots competing for the `pm-forwarder` subscription on `$all`.         |
| `--lease-seconds`          | `3`     | Subscription lease TTL. Short to force regular rebalances.                |
| `--poll-interval-ms`       | `50`    | Worker idle-poll interval.                                                |
| `--think-time-ms`          | `20`    | Upper bound on workload task think-time between iterations.               |
| `--sample-interval-ms`     | `100`   | Continuous-sampler tick.                                                  |
| `--drain-timeout-sec`      | `300`   | Max seconds to wait for full drain after workload stops. See below.       |
| `--respawn-every-ms`       | `2000`  | Force-bounce a random worker on this cadence. `0` disables.               |
| `--steal-every-ms`         | `3000`  | Backdate a random subscription's lease. `0` disables.                     |
| `--any-version-fraction`   | `0.2`   | Probability a dispatcher writes via `expected.any` instead of OCC.        |

Environment overrides match the rest of the repo: `PGHOST`, `PGPORT`,
`PGUSER`, `PGPASSWORD`, `PGDATABASE` (default `instructed_soak`).

## What it exercises

Domain (deliberately minimal — see `domain.ts`):

- **Counter aggregate.** Per-account stream taking `add{n}` commands.
- **Forwarder process manager.** Subscribed to `$all`, routes
  `Triggered{n, target}` events to a per-target PM partition
  (PM-F: `partitionKey = target`) and dispatches `add{n}` to the
  `target` account. The PM is long-lived (never returns
  `{ complete: true }`) so the final PM-FORWARD-TOTAL check can sum
  the `forwarded` field across every partition's snapshot. A poison
  event would stall only its own target partition under SUB-A
  (per-partition predicate) — the harness's workload doesn't inject
  poisons.
- **Balances projection.** Subscribed to `$all` with sequential
  partitioning (`partitionKey = '_default'`); folds `Added.n` into
  an in-memory `account → balance` map. The final report compares
  it against a fresh re-fold from the events table.

Mechanisms composed under SUB-A:

- Aggregate OCC retry on `runCommand` (`--any-version-fraction`
  mixes in exogenous `expected.any` writes so both append modes get
  tested).
- **Routing-side rebalancing.** Multi-slot competition for the
  single routing lease per subscription, with short lease TTL and
  active lease theft. Only one routing worker per subscription is
  active at a time; the others wait.
- **Processing-side parallelism.** All `--projectors` /
  `--pms` slots run their processing workers simultaneously, racing
  for work items via `claim_work_item` (`FOR UPDATE SKIP LOCKED`).
  Per-partition ordering is enforced by the SUB-A claim predicate;
  parallelism is real across partitions (per-target for the PM, a
  no-op for the sequential projection which has one partition).
- PM dispatch over a separate connection pool (D-0011 / D-0012).
- Process death + restart on every slot type (kills both the
  routing and processing worker of the affected slot).

## Drain and quiescence

After the active workload finishes, the harness **drains**: it waits
for `$all` head to stop growing and every subscription to satisfy
the SUB-A catch-up predicate (`is_subscription_caught_up`): both the
routing cursor at or past head AND zero in-flight work items at or
below head. The PM creates new events while processing triggers, so
head keeps growing until the PM is itself caught up. A larger
workload or `--drain-timeout-sec` may still be needed if you crank
`--trigger-appenders` and `--dispatchers` together.

Two invariants — **PM-FORWARD-TOTAL** and **REFOLD-MATCH** — only hold
at quiescence. If `--drain-timeout-sec` is hit before drain completes,
the harness reports those as **INCONCLUSIVE** rather than
`VIOLATIONS`. The gaplessness / monotonicity / lease-uniqueness /
failed-work-item checks are valid in either case.

A run that prints `drain: completed` and `lag=0` on every
subscription is a clean run. A run that prints `drain: INCOMPLETE
(timeout)` with non-zero lag should be re-run with a larger
`--drain-timeout-sec` or a smaller workload; the diagnostic output
(below) makes the lag visible so it's clear when this is happening.

## PM-FORWARD diagnostic

Every report prints a five-line counter block that pins any
`forwarded ≠ triggered` discrepancy to a single SDK code path. The
counters are maintained by the harness's route and handle hooks (in
`domain.ts`) plus one final SQL query, and cost ~zero stdout noise.

```
PM-FORWARD diagnostic:
  triggers_total          7629
  route(Triggered) calls  7632
  handle calls            7632
  handle returns          7632
  dispatched (causation)  7632
  forwarded (snapshots)   7629
```

Reading the block top-to-bottom:

| Step | What it means |
| --- | --- |
| `triggers_total` | `count(*) WHERE event_type='Triggered'` — the ground truth. |
| `route(Triggered) calls` | Times the SDK invoked our route function. Should be `≥ triggers_total`; the excess is redeliveries. |
| `handle calls` | Times handle was entered. A gap below route calls means events routed but aborted before handle. |
| `handle returns` | Times handle returned normally (not threw / not aborted mid-body). Synchronous handle: should equal handle calls. |
| `dispatched (causation)` | `count(Added WHERE causation_id IN Triggered.event_id)` — PM dispatches that committed to an aggregate stream. |
| `forwarded (snapshots)` | `sum(snapshot.data.forwarded)` across all Forwarder instances. Should equal `triggers_total` modulo redelivery slop. |

At clean quiescence the bottom row equals `triggers_total`. The
intermediate rows can exceed it by a few events under lease theft
(at-least-once redelivery). A gap *between* rows pinpoints the bug:
route < triggers means the SDK isn't invoking the route fn; handle
< route means SDK loses events after routing; dispatched < handle
means `runCommand` is reporting success without committing; etc.

## Invariants checked

| Code                       | Where                                              | What it catches                                                                       |
| -------------------------- | -------------------------------------------------- | ------------------------------------------------------------------------------------- |
| `INV-APPEND-003`           | Final, on `$all`.                                  | A gap in `event_number` — the global head says N but fewer than N rows exist.         |
| `INV-APPEND-022`           | Final, per stream.                                 | A per-stream `stream_version` gap.                                                    |
| `INV-SUB-P-008`            | Continuous sampler + final.                        | Routing cursor (`subscriptions.last_seen`) went backwards, or advanced past the head. |
| `INV-SUB-P-LEASE-UNIQ`     | Continuous sampler.                                | Two unexpired claims on the same subscription — the routing-side lease. The SQL contract forbids this. |
| `INV-SUB-W-LEASE-UNIQ`     | Continuous sampler.                                | (SUB-A) Two unexpired processing-worker claims on the same `(subscription, partition, event_number)` work item. |
| `INV-SUB-W-NO-FAILED`      | Final.                                             | (SUB-A) A `failed` row appeared in `subscription_work_items`. The soak workload never injects poisons, so a `failed` row indicates an SDK bug. |
| `PM-024`                   | Final.                                             | A PM snapshot's `source_version` exceeded the subscription's `last_seen`.             |
| `PM-FORWARD-TOTAL`         | Final.                                             | The Forwarder snapshots' total `forwarded` count differs from the trigger count.      |
| `REFOLD-MATCH`             | Final.                                             | Per-account: the projector's running balance differs from a fresh re-fold of `Added` events. |

The continuous sampler runs every `--sample-interval-ms` while the
harness is live; transient lease-uniqueness or non-monotone cursor
violations would be invisible to a final-only scan.

The two `INV-SUB-W-*` codes are SUB-A-shaped and don't yet have a
corresponding entry in `docs/invariants.md`; slice 12's `INV-SUB-*`
triage pass will reconcile naming and add formal definitions.

## Reading the report

Sample output (truncated):

```
=========================== SOAK REPORT ===========================
elapsed:                    30.4s
commands attempted:         1840
commands completed:         1832
commands failed:            8
triggers appended:          612
worker respawns:            45
lease thefts:               10
sampler ticks:              304
$all head:                  2450
Added events total:         2444
Triggered events total:     612
commands/sec (completed):   60.3
events/sec ($all):          80.6

OK — no invariant violations observed.
===================================================================
```

Things to look for:

- **`commands/sec (completed)` vs. `(attempted)`** — gap is OCC retry
  failures that exhausted their budget plus appender errors. Should
  be small (<1%) in a healthy run; a large gap usually means
  `--dispatchers` is too high relative to `--accounts`.
- **`worker respawns`** — should be approximately
  `duration_sec * 1000 / --respawn-every-ms` plus the lease-theft
  cascade. If it's much smaller, slots are crash-looping rather than
  staying up (look for stack traces above the report).
- **`Added events total = #dispatcher writes + #triggers_appended`**.
  If `Added` is short, either a dispatcher write or a PM forward was
  lost. This is the workload-side correctness check.
- **`Triggered events total = forwarded by PM`** is asserted by
  `PM-FORWARD-TOTAL`; deviation means the PM dropped or duplicated a
  routed event.

## Performance baselining

The same flags produce the same workload shape across runs. A
suggested baseline for "did we regress?" tracking:

```sh
npm start -- --duration 60 --accounts 12 --dispatchers 8 \
             --projectors 3 --pms 3 --trigger-streams 4 \
             --trigger-appenders 3 --respawn-every-ms 0 \
             --steal-every-ms 0
```

(failure injection disabled isolates the perf number from churn
cost). Record `commands/sec (completed)` and `events/sec ($all)` per
run; these are the gauge.

The PM used to ack every `Added` event it ignored; the coalescing
optimisation (TODO #10 / ex-ML-0005) replaced that with one
`advance_subscription` per batch tail of ignored events, with
routed-event txs covering prior ignored runs implicitly. The
ignored-event ack overhead is therefore bounded by batch boundary
count rather than ignored event count. To stress PM throughput end
to end, bump the trigger appender count and watch PM throughput vs.
trigger count:

```sh
npm start -- --duration 60 --trigger-appenders 6 --trigger-streams 8 \
             --dispatchers 12 --any-version-fraction 0.5
```

If PM lag relative to `$all` head grows under this load, the
remaining cost is in the routed-event path (handle + dispatch +
persist-and-ack tx), not in ignored-event acks.

## Known gaps

The harness currently does **not**:

- Inject network partitions (`sleep transaction before commit`) —
  the TODO #3b sketch mentions this as a stretch goal; we settled
  for worker-process kill + lease theft as a first cut.
- Surface OCC retry counts from `runCommand` — the SDK doesn't
  expose them. Indirectly visible as `commandsAttempted -
  commandsCompleted - commandsFailed` ≈ 0 (i.e. retries succeed
  silently); a future SDK observability hook (separate work) would
  let us count retries directly.
- Run multiple processes. Everything is in-process, which is fine
  for invariant-fuzzing intent (the `pg` library does real
  network I/O so the concurrency is genuine) but means the harness
  doesn't exercise multi-host clock skew.
- Exercise SUB-B convenience wrappers (`retryUpTo`,
  `quarantineAfter`, etc.) — those ship after SUB-A. The default
  error policy (exponential backoff, retry forever) is what the
  soak workload runs against; the `INV-SUB-W-NO-FAILED` check
  relies on this (a workload that injects a poison event under a
  `quarantineAfter` policy would legitimately produce `failed`
  rows).
- Inject failures at the *processing*-worker lease boundary
  specifically. The current lease-theft path backdates
  `subscriptions.claim_expires_at`, which is the routing lease.
  A future stretch goal would backdate
  `subscription_work_items.lease_expires_at` to force a
  processing-worker takeover (today these only happen indirectly
  when a respawn kills the processing worker before its in-flight
  item completes).

See TODO `#3b` for the long-form motivation and the framing this
harness landed against.
