// Core: schema lifecycle and store status. Pure data-in/data-out over a `Db`.

import type { Db } from "./db.ts";
import type { InstallResult, StoreStatus } from "./types.ts";
import { SCHEMA_SQL } from "./schema-sql.ts";

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
  if (existed && options.force) {
    await db.exec("drop schema instructed cascade");
  }

  await db.exec(SCHEMA_SQL);

  return {
    alreadyExisted: existed,
    action: existed ? "reinstalled" : "installed",
    schemaVersion: await getSchemaVersion(db),
  };
}
