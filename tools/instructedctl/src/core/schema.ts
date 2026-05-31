// Core: schema lifecycle and store status. Pure data-in/data-out over a `Db`.

import type { Db } from "./db.ts";
import type { EnsureResult, InstallResult, StoreStatus } from "./types.ts";
import { SCHEMA_SQL } from "./schema-sql.ts";

// The schema version the *embedded* SQL would install, extracted from the
// body of `instructed.get_schema_version()` in SCHEMA_SQL. This is the
// target `ensureSchema` compares a live store against. Parsing (rather than
// hardcoding) keeps the tool and the SQL spec from drifting: the version
// lives in exactly one place, the SQL file.
export function bundledSchemaVersion(): string {
  const m = SCHEMA_SQL.match(
    /create\s+or\s+replace\s+function\s+instructed\.get_schema_version[\s\S]*?\bselect\s+'([^']*)'::text/i,
  );
  if (!m) {
    throw new Error(
      "could not determine the bundled schema version from the embedded SQL " +
        "(instructed.get_schema_version body did not match the expected shape)",
    );
  }
  return m[1];
}

// Whether the `instructed` schema exists in the target database.
export async function schemaPresent(db: Db): Promise<boolean> {
  const rows = await db.query<{ present: boolean }>(
    `select exists(
       select 1 from information_schema.schemata
       where schema_name = 'instructed'
     ) as present`,
  );
  return rows[0].present;
}

// The recorded schema version (instructed.get_schema_version()).
export async function getSchemaVersion(db: Db): Promise<string> {
  const rows = await db.query<{ version: string }>(
    "select instructed.get_schema_version() as version",
  );
  return rows[0].version;
}

// A high-level store summary.
export async function getStatus(db: Db): Promise<StoreStatus> {
  const rows = await db.query<{
    version: string;
    all_head: string;
    streams: string;
    events: string;
    subscriptions: string;
  }>(
    `select
       instructed.get_schema_version() as version,
       (select stream_version from instructed.streams where stream_id = 0) as all_head,
       (select count(*) from instructed.streams where stream_id <> 0) as streams,
       (select count(*) from instructed.events) as events,
       (select count(*) from instructed.subscriptions) as subscriptions`,
  );
  const r = rows[0];
  return {
    schemaVersion: r.version,
    allHead: Number(r.all_head),
    streams: Number(r.streams),
    events: Number(r.events),
    subscriptions: Number(r.subscriptions),
  };
}

// Thrown by installSchema when the schema already exists and force is not set.
export class SchemaAlreadyInstalled extends Error {
  constructor() {
    super("the 'instructed' schema already exists");
    this.name = "SchemaAlreadyInstalled";
  }
}

// Install the schema. On a fresh database this applies sql/instructed.sql. If
// the schema already exists, installSchema throws SchemaAlreadyInstalled
// unless `force` is true, in which case it drops the schema (CASCADE) and
// reinstalls — destroying all data.
export async function installSchema(
  db: Db,
  options: { force?: boolean } = {},
): Promise<InstallResult> {
  const existed = await schemaPresent(db);

  if (existed && !options.force) {
    throw new SchemaAlreadyInstalled();
  }
  // Install atomically: drop-then-create (force) or create (fresh) runs in
  // one transaction, so a failure leaves the database untouched rather than
  // half-installed. This is what lets `ensureSchema` trust "schema present"
  // to mean "schema fully installed".
  await db.transaction(async (tx) => {
    if (existed && options.force) {
      await tx.exec("drop schema instructed cascade");
    }
    await tx.exec(SCHEMA_SQL);
  });

  return {
    alreadyExisted: existed,
    action: existed ? "reinstalled" : "installed",
    schemaVersion: await getSchemaVersion(db),
  };
}

// Thrown by ensureSchema when a schema is already installed but at a version
// other than the one this binary would install. ensureSchema never mutates an
// existing schema, so this is the boundary where a future `schema migrate`
// takes over.
export class SchemaVersionMismatch extends Error {
  constructor(
    readonly running: string,
    readonly bundled: string,
  ) {
    super(
      `the installed instructed schema is version '${running}', but this ` +
        `tool installs version '${bundled}'. ensureSchema will not modify an ` +
        `existing schema; migration is not yet supported.`,
    );
    this.name = "SchemaVersionMismatch";
  }
}

// Idempotent, non-destructive install. Safe to run on every deploy / CI run:
//
//   - schema absent           -> install it (transactional)   -> 'installed'
//   - schema present, same ver -> no-op                        -> 'already-current'
//   - schema present, diff ver -> throw SchemaVersionMismatch  (needs migrate)
//
// Unlike installSchema (which errors if present) and installSchema({ force })
// (which drops all data), ensureSchema never errors on an up-to-date store and
// never destroys data.
export async function ensureSchema(db: Db): Promise<EnsureResult> {
  if (!(await schemaPresent(db))) {
    const result = await installSchema(db);
    return { action: "installed", schemaVersion: result.schemaVersion };
  }

  const running = await getSchemaVersion(db);
  const bundled = bundledSchemaVersion();
  if (running !== bundled) {
    throw new SchemaVersionMismatch(running, bundled);
  }
  return { action: "already-current", schemaVersion: running };
}
