# instructed

An exploration of building a CQRS/ES library — functionally comparable to
[Commanded](https://github.com/commanded/commanded) — using the
"lean hard on Postgres" pattern pioneered by
[absurd](https://github.com/earendil-works/absurd).

The hypothesis: the CQRS/ES core (event-sourced aggregates with optimistic
locking, projections, process managers, sagas) can be expressed as a small
schema and a handful of stored procedures in Postgres, with thin, pull-based
SDKs in application languages. No coordinator service. No language-runtime
guarantees required from the host (no BEAM, no OTP).

## Status

**Phase 8 — Reference SDK — done.**

- The SQL contract is the spec: see [`sql/instructed.sql`](sql/instructed.sql).
  Schema, 12 plpgsql procedures with full docstrings, closed SQLSTATE
  catalogue, lock-acquisition order documented inline. Human-oriented
  reference in [`docs/sql-contract.md`](docs/sql-contract.md).
- The reference TypeScript SDK lives in
  [`sdks/typescript/`](sdks/typescript/). All five layers per
  [`docs/sdk-design.md`](docs/sdk-design.md) §3 are implemented and
  tested against the docker-compose Postgres (77 cases):
    - **Layer 0** `Client` — thin wrappers around every stored procedure,
      SQLSTATE → typed `Error` subclass.
    - **Layer 1** `runCommand` — aggregate load-execute-append loop with
      OCC retry (D-0005 / D-0019).
    - **Layer 2** `startProjection` — persistent-subscription worker with
      lease heartbeat, abort-aware shutdown, handler-throws backoff
      (D-0016 / D-0018).
    - **Layer 3** `startProcessManager` — routing PM with separate
      dispatch session, snapshot + cursor-advance in one short SDK-internal
      transaction (D-0011 / D-0012 / D-0020).
    - **Layer 4** `waitForProjection` — consistency-on-dispatch wait
      (D-0010, explicit list, no `:strong` shorthand).
    - **Layer 5** `Instructed` facade — `registerAggregate` /
      `registerProjection` / `registerProcessManager`, `dispatch`,
      `startWorker`, lazy dispatch-pool materialisation.
- The bank-account example in
  [`sdks/typescript/examples/bank-account/`](sdks/typescript/examples/bank-account/)
  is the Phase 8 done-criterion target: an `Account` and a `Transfer`
  aggregate, a `Balances` projection, and a `TransferProcessManager`
  realising compensation by refusal (D-0011). `main.ts` runs end-to-end
  against the docker-compose Postgres and prints final balances.
- Per-step history is in [`docs/ROADMAP.md`](docs/ROADMAP.md) "Phase 8";
  non-obvious choices are pinned in
  [`docs/decisions.md`](docs/decisions.md) (most recently D-0019 —
  aggregate-runner semantics, and D-0020 — PM-worker / runner
  fresh-stream details).

Phase 9 (the cross-language conformance harness) is the next major
milestone.

## Layout

```
sql/                     -- the spec
  instructed.sql         -- schema + 12 procedures with docstrings
  migrations/            -- absurd-style migrations directory
sdks/
  typescript/            -- reference SDK (Node 18+, pg 8.x peer dep)
    src/                 -- layers 0..5 of the design
    test/                -- node --test fixtures against docker-compose
docs/
  ROADMAP.md             -- phase-by-phase plan and status
  sdk-design.md          -- the SDK design (authoritative)
  sql-contract.md        -- human-oriented SQL reference
  decisions.md           -- running log of design decisions
  invariants.md          -- the Commanded-derived invariant catalogue
  mapping.md             -- how each invariant lands in instructed
  guarantees.md          -- application-facing guarantees
  non-goals.md           -- what we deliberately do not do
  maybe-later.md         -- deferred capabilities (ML-xxxx)
  open-questions.md      -- unresolved design questions (OQ-xxxx)
  sagas.md               -- process-manager / saga strategy
  sdk-usage-sketch.md    -- user-facing sketches that drove the design
docker-compose.yaml      -- the test Postgres
```

## Running the SDK tests

The tests run against the Postgres provided by `docker-compose`:

```sh
docker compose up -d
cd sdks/typescript
npm install
npm test
```

The fixtures connect to `instructed_test`, create the database if missing,
install [`sql/instructed.sql`](sql/instructed.sql), and truncate the
`instructed.*` tables between cases.

## Running the bank-account example

```sh
docker compose up -d
cd sdks/typescript
node --experimental-strip-types examples/bank-account/main.ts
```

See [`sdks/typescript/examples/bank-account/README.md`](sdks/typescript/examples/bank-account/README.md)
for what the example demonstrates.

## Further reading

- [`docs/ROADMAP.md`](docs/ROADMAP.md) — the plan, phase by phase.
- [`docs/decisions.md`](docs/decisions.md) — running design-decision log.
- [`docs/sdk-design.md`](docs/sdk-design.md) — the SDK design pass.
