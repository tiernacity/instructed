// Core: snapshot inspection.

import type { Db } from "./db.ts";
import type { Snapshot } from "./types.ts";

interface SnapshotRow {
  source_uuid: string;
  source_type: string;
  source_version: string;
  data: unknown;
  metadata: unknown;
  created_at: Date;
}

// Fetch a snapshot by source uuid, or null when there is none. (Reads the
// table directly rather than instructed.read_snapshot, which raises IS010 on
// miss; null is the friendlier shape for a `get` command.)
export async function getSnapshot(
  db: Db,
  sourceUuid: string,
): Promise<Snapshot | null> {
  const rows = await db.query<SnapshotRow>(
    `select source_uuid, source_type, source_version, data, metadata, created_at
       from instructed.snapshots
      where source_uuid = $1`,
    [sourceUuid],
  );
  if (rows.length === 0) return null;
  const r = rows[0];
  return {
    sourceUuid: r.source_uuid,
    sourceType: r.source_type,
    sourceVersion: Number(r.source_version),
    data: r.data,
    metadata: r.metadata,
    createdAt: r.created_at,
  };
}
