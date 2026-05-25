# `tests/conformance/`

Conformance harness for the `instructed` SQL contract (Phase 9,
**D-0021**). SQL-only: drives the procedures in
`sql/instructed.sql` directly via `pg`. No SDK in the loop.

The harness's job is to demonstrate that `instructed` realises the
invariants in [`docs/invariants.md`](../../docs/invariants.md) Parts
B–F (adapter-line scope, **D-0023**). The four design decisions
that fix the shape are recorded in
[`docs/decisions.md`](../../docs/decisions.md): **D-0021** (SQL-only
substrate), **D-0022** (hand-rewrite by INV-*), **D-0023** (scope),
**D-0024** (partitioned-consumer slot).

## Running

```sh
docker compose up -d postgres   # from the repo root
cd tests/conformance
npm install
npm test
```

Environment overrides (same as the SDK fixture):

| Variable    | Default     |
|-------------|-------------|
| `PGHOST`    | `127.0.0.1` |
| `PGPORT`    | `5432`      |
| `PGUSER`    | `postgres`  |
| `PGPASSWORD`| `postgres`  |
| `PGDATABASE`| `instructed_test` |

Each test process drops and reinstalls `sql/instructed.sql`. Cases
within a process share the schema; `truncateAll` runs between them.

## Layout

```
tests/conformance/
├── package.json
├── tsconfig.json
├── coverage-report.ts        # placeholder; full impl in step 8/8
├── COVERAGE.md               # Elixir-source ↔ INV-* checklist
└── test/
    ├── fixtures.ts           # pool + schema-install + truncateAll
    └── smoke.test.ts         # step 1/8 only: schema present?
```

Step 2/8 onwards adds `append.test.ts`, `read.test.ts`,
`snapshot.test.ts`, `subscription-persistent.test.ts`,
`subscription-transient.test.ts` (dropped-shape documentation),
and `cross-cutting.test.ts`. See `docs/ROADMAP.md` Phase 9 for the
full sequencing. (A previous `subscription-partitioned.test.ts`
slot held skipped placeholders for sharded-routing cases; those
are unbuilt — see `docs/maybe-later.md` ML-0013 — and the file
has been removed.)

## Annotation grammar

Each `test(...)` carries one or more INV-* tags on the lines
immediately preceding it. The reporter scrapes these — keep them
faithful to the case's actual assertions.

```ts
// INV-APPEND-013: V mismatch returns IS001 wrong_expected_version
test("integer V against a stream at the wrong version fails", async () => {
  // …
});
```

Recognised forms:

- `// INV-FOO-NNN` — covered.
- `// INV-FOO-NNN: <prose>` — covered, with case-local context.
- `// INV-FOO-NNN: dropped — see NG-XXXX` — deliberately dropped.
- `// INV-FOO-NNN: deferred — see ML-NNNN` — deferred; `test.skip`
  expected.
- `// INV-FOO-NNN: above adapter line — see <ref>` — realised in
  SDK code; out of scope per **D-0023**.

## What does *not* live here

SDK-layer guarantees (`AGG-*`, `HND-*`, `PM-*`, `CON-*`, `DSP-*`)
stay in `sdks/typescript/test/` per **D-0023**. A future
Python/Go/Elixir SDK inherits adapter-line conformance by virtue
of pointing at a conformant store; its own test suite covers its
own SDK-layer realisation.
