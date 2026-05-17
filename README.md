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

Status: **exploration**. No code yet. The current focus is extracting the
invariants and guarantees that a CQRS/ES system must provide, so that we can
deliberately decide which to keep, which to drop, and how each surviving
guarantee is realised in Postgres.

See [`docs/ROADMAP.md`](docs/ROADMAP.md) for the plan, and
[`docs/decisions.md`](docs/decisions.md) for the running log of design
decisions.
