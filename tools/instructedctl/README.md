# instructedctl

An administrative CLI for an `instructed` deployment. It lets an operator inspect and
manage a store — schema lifecycle, streams, subscriptions, work items, snapshots, and
health — without writing ad-hoc SQL.

It connects **directly to PostgreSQL** and does **not** depend on any application SDK.
Modelled on [`absurdctl`](https://github.com/earendil-works/absurd), but written in
TypeScript on [Deno](https://deno.com) so it ships as a single, dependency-free static
executable.

## Status

Early. The architecture is in place — a reusable **core** (`src/core`) and a thin
[Cliffy](https://cliffy.io)-based **CLI** (`src/cli`) — with the `schema` and
`subscriptions` command groups landed. The rest of the surface (streams, work items,
snapshots, health) lands incrementally against the same shape.

## Architecture

The package is split so the behaviour is reusable beyond the CLI (e.g. a future web UI):

- **`src/core`** — the consumable API. Pure functions that take a `Db` and typed
  parameters and **return data**. No output formatting, no process exit, no CLI
  framework, no concrete driver import. This is the testing boundary: tests call core
  functions directly against a throwaway database. Exported as the package's `./core`
  entry.
- **`src/cli`** — a thin wrapper. [Cliffy](https://cliffy.io) declares the command tree
  and global options, adapts `@db/postgres` to the core `Db` interface, and renders core
  results as a table or JSON. Commands contain no behaviour beyond parse → call core →
  format.

The `Db` boundary (`src/core/db.ts`) is a two-method interface (`query` / `exec`). The
CLI supplies a `@db/postgres`-backed adapter; any other consumer supplies its own.

### A note on the TypeScript SDK

instructedctl does **not** reuse the `instructed-sdk`. The SDK's `Client` is the
application-runtime procedure surface (append / claim / complete / fail); the operator
surface here is mostly inspection, listing, lag, work-item state counts, and health —
which the SDK deliberately does not cover. The SDK is also built on `pg`, whereas this
tool uses Deno's `@db/postgres`; reusing it would force a second Postgres stack plus an
adapter shim with type-decoding risk. Like the conformance harness (D-0021), the tool
drives SQL directly. Revisit if the SDK grows an admin sub-surface.

## Command structure

Commands are organised **noun-first** (resource → verb), the modern convention used by
`gh`, `stripe`, and `docker`:

```sh
instructedctl <group> <verb> [args] [options]
```

A bare group name runs its default verb (usually `list`/`status`). Global DB options and
`--json` are accepted at any level.

```sh
instructedctl schema                 # default: status
instructedctl schema status
instructedctl schema version
instructedctl schema install [--force]

instructedctl subscriptions          # default: list   (alias: subs)
instructedctl subscriptions list     # alias: ls
instructedctl subscriptions get <name>
```

## Running

During development, with Deno installed:

```sh
# from tools/instructedctl
deno task dev schema status
deno task dev subscriptions list
deno run -A src/cli/main.ts --help
```

Build a standalone executable (no Deno required at runtime):

```sh
deno task compile        # produces ./instructedctl
./instructedctl schema status
```

`deno compile` bundles the Postgres driver, Cliffy, and the embedded schema into the
binary, so the artifact is self-contained. When the web UI lands, the same
`deno
compile` path produces the server binary.

## Connecting to the database

Connection settings resolve with this precedence (matching absurdctl):

1. The `--database` flag (a database name **or** a full connection URI).
2. `INSTRUCTED_DATABASE_URL` (a connection URI).
3. `PGDATABASE` (a name or a URI).
4. The default `postgresql://localhost/instructed`.

When the resolved value is not a URI, discrete fields fill in from flags then the
standard libpq variables:

| Flag             | Env var      | Default      |
| ---------------- | ------------ | ------------ |
| `-d, --database` | `PGDATABASE` | `instructed` |
| `-h, --host`     | `PGHOST`     | `localhost`  |
| `-p, --port`     | `PGPORT`     | `5432`       |
| `-U, --user`     | `PGUSER`     | `$USER`      |
| —                | `PGPASSWORD` | —            |

These are **global options**: they work on any command. `--verbose` prints the resolved
configuration (password redacted) to stderr; `--json` switches output to JSON.

```sh
export INSTRUCTED_DATABASE_URL="postgresql://user:pass@localhost:5432/mydb"
instructedctl schema status --json
```

## `schema install`

Applies the embedded `sql/instructed.sql` to the target database (analogous to
`absurdctl init`). The schema creates tables with bare `create table`, so it is meant
for a clean database: if the `instructed` schema already exists, install refuses unless
`--force` is given. `--force` drops the schema (`CASCADE`) and reinstalls, destroying
all data.

The schema is **embedded into the binary** so install is self-contained. The mechanism:
`src/core/instructed.sql` is a symlink to the repo-root `sql/instructed.sql` (single
source of truth, no drift), declared in `deno.json`'s `compile.include`.
`src/core/schema-sql.ts` reads it via `import.meta.dirname + "/instructed.sql"`, which
resolves through the symlink in dev and from the embedded copy in the compiled binary.
The symlink is co-located with its reader because `deno compile` only materialises
included files reachable without traversing above the reading module's directory.

## Tests

```sh
deno task test        # core + CLI smoke tests (needs Postgres)
deno task check
deno task lint
deno task fmt
```

- **Core tests** (`tests/core/*`) call core functions directly against a **throwaway
  database** — the clean boundary. The harness (`tests/support.ts`) creates a
  uniquely-named database, loads `sql/instructed.sql`, runs, and drops it on teardown,
  leaving no residue.
- **CLI smoke tests** (`tests/cli_test.ts`) drive the Cliffy tree via `parse()` against
  a throwaway database to confirm the wrapper wires global options + commands to core.

Tests require the docker-compose Postgres (`docker compose up -d postgres`). The admin
connection resolves from the standard `PG*` variables (defaults: `127.0.0.1:5432`,
user/password `postgres`).

## Planned command surface

Derived from TODO #7 — "everything a production operator currently does by opening
psql." Now grouped noun-first. ✅ = landed.

| Group           | Verbs                                                               |
| --------------- | ------------------------------------------------------------------- |
| `schema`        | `status` ✅, `version` ✅, `install` ✅, `migrate`                  |
| `streams`       | `list`, `get <uuid>`, `read <uuid> [--from --count]`                |
| `all`           | `read [--from --count]` (the global `$all` stream)                  |
| `subscriptions` | `list` ✅, `get <name>` ✅, `release`, `delete`, `claim`, `rebuild` |
| `work-items`    | `list [--subscription]`, `skip <key> --audit <note>`                |
| `snapshots`     | `get <source_uuid>`                                                 |
| `health`        | (bare) `$all` contiguity, orphans, expired-lease zombies            |

Absurd commands with no instructed analogue (queue/task/cron concepts): `create-queue`,
`drop-queue`, `queue-policy`, `cron`, `cleanup`, `spawn-task`, `retry-task`,
`emit-event`, `dump-task`, `install-skill`.

## Layout

```
tools/instructedctl/
  deno.json            # tasks, import map, compile.include, exports (. and ./core)
  src/
    core/              # consumable API — the testing boundary
      db.ts            # Db interface (query / exec) — no driver import
      types.ts         # typed return shapes
      schema.ts        # schemaPresent / getSchemaVersion / getStatus / installSchema
      subscriptions.ts # listSubscriptions / getSubscription
      schema-sql.ts    # reads the embedded schema
      instructed.sql   # symlink -> ../../../../sql/instructed.sql (embedded at compile)
      index.ts         # re-exports the core API
    cli/               # thin Cliffy wrapper
      main.ts          # command tree + global options
      db.ts            # connection resolution + @db/postgres -> Db adapter
      options.ts       # global-options shape + runWith helper
      output.ts        # table / JSON / key-value formatting
      commands/
        schema.ts
        subscriptions.ts
  tests/
    support.ts         # throwaway-db harness + core Db + stdout capture
    cli_test.ts        # CLI smoke tests via Cliffy parse()
    core/
      schema_test.ts
      subscriptions_test.ts
  README.md
```

Adding a command group:

1. Write core functions in `src/core/<group>.ts` (take a `Db`, return data) and export
   them from `src/core/index.ts`.
2. Write a Cliffy builder in `src/cli/commands/<group>.ts` that calls core and formats.
3. Mount it in `src/cli/main.ts`.
4. Add core tests in `tests/core/<group>_test.ts`.
