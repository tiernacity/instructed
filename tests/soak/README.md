# Soak harness (`tests/soak/`)

Performance gauge + invariant fuzzer over time. Composes several
mechanisms (aggregate OCC, projector subscriptions, process-manager
dispatch) against a single Postgres for a configurable duration with
periodic failure injection (worker respawn + lease theft), then runs
the same invariant checks the per-PR composed-concurrency tests run
in `sdks/typescript/test/concurrent.test.ts` — but at higher N, over
longer wall-clock, with deliberately churned leases.

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
| `--respawn-every-ms`       | `2000`  | Force-bounce a random worker on this cadence. `0` disables.               |
| `--steal-every-ms`         | `3000`  | Backdate a random subscription's lease. `0` disables.                     |
| `--any-version-fraction`   | `0.2`   | Probability a dispatcher writes via `expected.any` instead of OCC.        |

Environment overrides match the rest of the repo: `PGHOST`, `PGPORT`,
`PGUSER`, `PGPASSWORD`, `PGDATABASE` (default `instructed_soak`).

## What it exercises

Domain (deliberately minimal — see `domain.ts`):

- **Counter aggregate.** Per-account stream taking `add{n}` commands.
- **Forwarder process manager.** Subscribed to `$all`, routes
  `Triggered{n, target}` events to a per-target process instance and
  dispatches `add{n}` to the `target` account. Many PM instances
  share the one subscription, so a poison event stalls the whole
  type — that's intentional and matches v1's PM model.
- **Balances projection.** Subscribed to `$all`, folds `Added.n` into
  an in-memory `account → balance` map. The final report compares it
  against a fresh re-fold from the events table.

Mechanisms composed:

- Aggregate OCC retry on `runCommand` (`--any-version-fraction` mixes
  in exogenous `expected.any` writes so both append modes get tested).
- Multi-slot competition for one subscription, with short leases and
  active lease theft.
- PM dispatch over a separate connection pool (D-0011 / D-0012).
- Process death + restart on every slot type.

## Invariants checked

| Code                       | Where                                              | What it catches                                                                       |
| -------------------------- | -------------------------------------------------- | ------------------------------------------------------------------------------------- |
| `INV-APPEND-003`           | Final, on `$all`.                                  | A gap in `event_number` — the global head says N but fewer than N rows exist.         |
| `INV-APPEND-022`           | Final, per stream.                                 | A per-stream `stream_version` gap.                                                    |
| `INV-SUB-P-008`            | Continuous sampler + final.                        | `last_seen` went backwards, or advanced past the head.                                |
| `INV-SUB-P-LEASE-UNIQ`     | Continuous sampler.                                | Two unexpired claims on the same subscription. The SQL contract forbids this.         |
| `PM-024`                   | Final.                                             | A PM snapshot's `source_version` exceeded the subscription's `last_seen`.             |
| `PM-FORWARD-TOTAL`         | Final.                                             | The Forwarder snapshots' total `forwarded` count differs from the trigger count.      |
| `REFOLD-MATCH`             | Final.                                             | Per-account: the projector's running balance differs from a fresh re-fold of `Added` events. |

The continuous sampler runs every `--sample-interval-ms` while the
harness is live; transient lease-uniqueness or non-monotone cursor
violations would be invisible to a final-only scan.

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

To stress ignored-event ack overhead (relevant to ML-0005), bump the
trigger appender count and watch PM throughput vs. trigger count:

```sh
npm start -- --duration 60 --trigger-appenders 6 --trigger-streams 8 \
             --dispatchers 12 --any-version-fraction 0.5
```

The PM acks every `Added` event it ignores; if that overhead matters
in practice, this configuration will surface it as PM lag relative
to `$all` head.

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

See TODO `#3b` for the long-form motivation and the framing this
harness landed against.
