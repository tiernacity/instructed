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

**Phase 9 — Conformance harness — done.**

- A SQL-only conformance harness lives in
  [`tests/conformance/`](tests/conformance/) (D-0021). It drives the
  procedures in `sql/instructed.sql` directly via `pg`, with no SDK
  in the loop — the conformance contract belongs to the store, so a
  future Python/Go/Elixir SDK inherits adapter-line conformance for
  free by pointing at a conformant Postgres.
- 113 active cases organised by INV-* (D-0022) across
  `append.test.ts`, `read.test.ts`, `snapshot.test.ts`,
  `subscription-persistent.test.ts`, `cross-cutting.test.ts`, plus
  three `test.skip` slots for INV-SUB-P-040..042 (deferred per
  D-0024 / ML-0001) and an omission shell for the dropped INV-SUB-T-*
  family (NG-0005). Scope is adapter-line only (D-0023); SDK-layer
  behaviours stay in `sdks/typescript/test/`.
- Coverage of the Phase 2 invariant catalogue (Parts B–F) is
  complete: 54 covered, 3 deferred, 7 dropped, 1 above-line, 0
  missing. The `npm run coverage` reporter prints the matrix and
  exits non-zero if anything regresses. Annotation grammar is
  documented in `tests/conformance/README.md`.

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

Beyond Phase 9: additional SDKs (Python, Go, Elixir),
`instructedctl` tool, performance work — see
[`docs/ROADMAP.md`](docs/ROADMAP.md) "Beyond".

## Layout

```
sql/                     -- the spec
  instructed.sql         -- schema + 12 procedures with docstrings
  migrations/            -- absurd-style migrations directory
sdks/
  typescript/            -- reference SDK (Node 18+, pg 8.x peer dep)
    src/                 -- layers 0..5 of the design
    test/                -- node --test fixtures against docker-compose
tests/
  conformance/           -- Phase 9 SQL-only conformance harness (D-0021)
    test/                -- one *.test.ts per INV-* family
    coverage-report.ts   -- INV-coverage matrix reporter (D-0022)
    COVERAGE.md          -- Elixir-source <-> INV-* checklist
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

## Running the conformance harness

```sh
docker compose up -d
cd tests/conformance
npm install
npm test            # 113 active cases + 3 skipped (deferred)
npm run coverage    # INV-* matrix, exits non-zero on any 'missing' row
```

The harness uses the same `instructed_test` database as the SDK
tests, with a fresh schema per process and `truncateAll` between
cases. No SDK dependency.

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
