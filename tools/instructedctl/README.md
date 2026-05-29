# instructedctl

An administrative CLI for an `instructed` deployment. It lets an operator inspect and
manage a store — schema lifecycle, streams, subscriptions, work items, snapshots, and
health — without writing ad-hoc SQL.

It connects **directly to PostgreSQL** and does **not** depend on any application SDK.
Modelled on [`absurdctl`](https://github.com/earendil-works/absurd), but written in
TypeScript on [Deno](https://deno.com) so it ships as a single, dependency-free static
executable.

## Status

Skeleton. The framework (argument parsing, command registry, connection resolution,
verbose config) is in place, with two working commands — `status` and `schema-version`.
The remaining command surface is sketched below and lands incrementally.

## Running

During development, with Deno installed:

```sh
# from tools/instructedctl
deno task dev status
deno task dev schema-version
deno run -A src/main.ts help
```

Build a standalone executable (no Deno required at runtime):

```sh
deno task compile        # produces ./instructedctl
./instructedctl status
```

`deno compile` bundles the Postgres driver and all dependencies into the binary, so the
artifact is self-contained. When the web UI lands, the same `deno compile` path produces
the server binary.

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

`-v, --verbose` prints the resolved configuration (with the password redacted) to stderr
before running.

```sh
export INSTRUCTED_DATABASE_URL="postgresql://user:pass@localhost:5432/mydb"
instructedctl status
```

## Tests

```sh
deno task test        # unit + integration
deno task check       # type-check
deno task lint
deno task fmt
```

Unit tests (`tests/cli_test.ts`, `tests/db_test.ts`) cover argument parsing and
connection-config resolution and need no database. Integration tests
(`tests/commands_test.ts`) run each command against a **throwaway database**: the
harness (`tests/support.ts`) creates a uniquely-named database, loads
`sql/instructed.sql` into it, runs the command, and drops it on teardown — so tests are
isolated and leave no residue.

The integration tests require the docker-compose Postgres to be running
(`docker compose up -d postgres`). The admin connection used to create and drop
throwaway databases resolves from the standard `PG*` variables (defaults:
`127.0.0.1:5432`, user/password `postgres`).

## Commands today

- `status` — schema version, `$all` head, and high-level row counts.
- `schema-version` — print the recorded schema version.
- `help`, `--version`.

## Planned command surface

Derived from TODO #7. The goal is "everything a production operator currently does by
opening psql." Grouped by area, with the absurd analogue where one exists. The
instructed domain is an event store (streams / `$all` / subscriptions / work items /
snapshots), not a task queue, so most surface is new rather than a rename of an absurd
command.

### Schema lifecycle

| instructedctl         | absurdctl        | Notes                                     |
| --------------------- | ---------------- | ----------------------------------------- |
| `schema-version` ✅   | `schema-version` | Done.                                     |
| `status` ✅ (partial) | —                | Summary; grows into health.               |
| `install`             | `init`           | Apply `sql/instructed.sql` to a fresh DB. |
| `migrate`             | `migrate`        | Apply pending `sql/migrations/`.          |

### Stream inspection

| instructedctl  | absurdctl     | Notes                                     |
| -------------- | ------------- | ----------------------------------------- |
| `list-streams` | `list-queues` | Streams with head version + event count.  |
| `show-stream`  | —             | Head / version for one stream.            |
| `read-stream`  | —             | Human-readable event range from a stream. |
| `read-all`     | —             | Event range from `$all`.                  |

### Subscription inspection and lifecycle

| instructedctl         | absurdctl | Notes                                            |
| --------------------- | --------- | ------------------------------------------------ |
| `list-subscriptions`  | —         | Cursor, `claimed_by`, lease, lag vs `$all` head. |
| `show-subscription`   | —         | Detail for one subscription.                     |
| `release`             | —         | Release a stuck claim (dead worker, live lease). |
| `delete-subscription` | —         | Delete by name; cascades work-item rows.         |
| `claim`               | —         | Diagnostic claim.                                |

### Projection rebuild

| instructedctl | absurdctl | Notes                                                                                                                                                                                                                                   |
| ------------- | --------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `rebuild`     | —         | Framework-side "forget this subscription's state": `delete-subscription` then re-claim from `origin`. Read-store wipe is the operator's responsibility (the SDK does not know where it lives). Safe to run while the worker is stopped. |

### Work-item operator surface

| instructedctl               | absurdctl     | Notes                                                                                                               |
| --------------------------- | ------------- | ------------------------------------------------------------------------------------------------------------------- |
| `list-work-items`           | `list-tasks`  | Counts by state; oldest in-flight; `failed` rows with `error_text`.                                                 |
| `skip-work-item-with-audit` | `cancel-task` | Move a stuck `failed` row to terminal with an operator audit note. `failed` rows are operator-only (INV-SUB-W-013). |

### Snapshot inspection

| instructedctl   | absurdctl | Notes                                                            |
| --------------- | --------- | ---------------------------------------------------------------- |
| `show-snapshot` | —         | Snapshot by `source_uuid`. (`force-snapshot` is ML-0009, later.) |

### Health

| instructedctl | absurdctl | Notes                                                                                                            |
| ------------- | --------- | ---------------------------------------------------------------------------------------------------------------- |
| `health`      | —         | `$all` contiguous; no orphaned `stream_events`; no expired-lease zombies; no orphaned `subscription_work_items`. |

### Absurd commands with no instructed analogue

`create-queue`, `drop-queue`, `queue-policy`, `cron`, `cleanup`,
`list-detach-candidates`, `detach-candidate`, `spawn-task`, `retry-task`, `emit-event`,
`dump-task`, `install-skill` — these are queue/task/cron concepts specific to absurd's
domain and have no direct instructed counterpart. `install-skill` may return if
instructed ships an agent skill.

## Layout

```
tools/instructedctl/
  deno.json            # tasks (dev/check/fmt/compile), import map
  src/
    main.ts            # entry point + help + dispatch
    cli.ts             # arg parsing, command registry types, shared db options
    db.ts              # connection-config resolution + query helper
    registry.ts        # the list of commands
    commands/
      status.ts
      schema-version.ts
  tests/
    support.ts           # throwaway-db harness + stdout capture
    cli_test.ts          # arg parsing (no db)
    db_test.ts           # connection-config resolution (no db)
    commands_test.ts     # commands against a throwaway db
  README.md
```

Adding a command: create `src/commands/<name>.ts` exporting a `Command`, then add it to
the list in `src/registry.ts`. Help text and dispatch derive from that list
automatically.
