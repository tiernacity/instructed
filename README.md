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

**Phase 8 — Reference SDK — in progress.**

- The SQL contract is the spec: see [`sql/instructed.sql`](sql/instructed.sql).
  Schema, 12 plpgsql procedures with full docstrings, closed SQLSTATE
  catalogue, lock-acquisition order documented inline. Human-oriented
  reference in [`docs/sql-contract.md`](docs/sql-contract.md). Phases 1–7
  are done.
- The reference SDK lives in [`sdks/typescript/`](sdks/typescript/). Steps
  1–3 of the eight-step Phase 8 sequencing (see
  [`docs/sdk-design.md`](docs/sdk-design.md) §10) have landed:
    - **Layer 0** `Client` — thin wrappers around every stored procedure,
      SQLSTATE → typed `Error` subclass.
    - **Layer 1** `runCommand` — aggregate load-execute-append loop with
      OCC retry (D-0005).
    - **Layer 2** `startProjection` — persistent-subscription worker with
      lease heartbeat, abort-aware shutdown, and handler-throws backoff.
- Layers 3–5 (process manager, consistency wait, facade), the bank-account
  example, and a follow-up README update are still ahead. The detailed
  per-step status is in [`docs/ROADMAP.md`](docs/ROADMAP.md) "Phase 8".

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

## Further reading

- [`docs/ROADMAP.md`](docs/ROADMAP.md) — the plan, phase by phase.
- [`docs/decisions.md`](docs/decisions.md) — running design-decision log.
- [`docs/sdk-design.md`](docs/sdk-design.md) — the SDK design pass.
