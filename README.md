# instructed

A CQRS / event sourcing library that lives almost entirely in
Postgres. The schema and a small set of stored procedures are the
spec; SDKs are thin clients.

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

What's there:

- Optimistic-concurrency-checked appends with gapless per-stream
  and global ordering.
- Persistent, leased, single-active-worker subscriptions over
  named streams or `$all`.
- Aggregates with OCC retry; projections with at-least-once
  delivery; process managers with snapshot-persisted state.
- Strong consistency on dispatch via explicit subscription-name
  waits.
- No coordinator service. No in-memory aggregate cache. No push
  delivery in the contract.

## Quickstart

```sh
docker compose up -d                          # the test Postgres
cd sdks/typescript
npm install
npm test                                      # 77 tests
```

End-to-end example (two aggregates, one projection, one process
manager):

```sh
node --experimental-strip-types examples/bank-account/main.ts
```

The conformance harness:

```sh
cd tests/conformance
npm install
npm test            # 113 active cases + 3 deferred
npm run coverage    # INV-* coverage matrix
```

## Repository layout

```
README.md                  -- this file
TODO.md                    -- parked follow-ups (Commanded re-review,
                              SDK restructuring, smoke test, …)
docs/                      -- conceptual + reference documentation
examples/                  -- worked examples
sdks/typescript/           -- the reference SDK (Node 18+, pg 8.x)
sql/                       -- the spec (schema + stored procedures)
tests/conformance/         -- SQL-only conformance harness
docker-compose.yaml        -- the test Postgres
```

## Documentation

For app developers:

- **[`docs/concepts.md`](docs/concepts.md)** — CQRS / ES primer
  and how to write an `instructed` application.
- **[`docs/guarantees.md`](docs/guarantees.md)** — what the library
  promises to applications, in plain language.
- **[`examples/`](examples/)** — worked examples.

For library users going deeper:

- **[`docs/architecture.md`](docs/architecture.md)** — how
  `instructed` realises CQRS/ES on Postgres: mechanisms,
  concurrency model, lock ordering.
- **[`docs/non-goals.md`](docs/non-goals.md)** — what the library
  deliberately doesn't do, and why.
- **[`docs/maybe-later.md`](docs/maybe-later.md)** — capabilities
  deferred for a future version, with forward-compatibility
  constraints on v1.

For SDK porters and the conformance harness:

- **[`docs/invariants.md`](docs/invariants.md)** — formal
  catalogue of constraints (INV-*, AGG-*, HND-*, PM-*, CON-*,
  DSP-*, SNAP-*).
- **[`docs/sql-contract.md`](docs/sql-contract.md)** — reading
  guide for `sql/instructed.sql`, the closed SQLSTATE catalogue,
  recommended call patterns.
- **[`docs/decisions.md`](docs/decisions.md)** — the
  architectural decisions worth preserving.

## SDKs

| SDK | Status |
|---|---|
| **[TypeScript](sdks/typescript/)** | reference SDK; tracks the SQL contract |

Additional SDKs (Python, Go, Elixir) and an `instructedctl`
administrative tool are tracked in [`TODO.md`](TODO.md).

## How the contract is structured

`sql/instructed.sql` installs the `instructed` schema and ~12
stored procedures into an existing Postgres database. The schema
is **append-only** for events; mutation goes only through the
procedures. The error set per procedure is **closed** — each
raises one of a documented set of custom SQLSTATEs in class `IS`,
which SDKs translate to typed errors.

A new SDK in any language gets adapter-line conformance for free
by pointing at a conformant Postgres; the conformance harness
runs entirely SQL-side and proves the procedures meet the
contract.
