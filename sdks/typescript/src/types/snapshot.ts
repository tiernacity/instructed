/** Snapshot wire shapes: input to `record_snapshot` and the row
 *  returned by `read_snapshot`. */

/** Input to `record_snapshot`. */
export interface SnapshotInput<S = unknown> {
  sourceUuid: string;
  sourceType: string;
  sourceVersion: bigint;
  data: S;
  metadata?: unknown;
}

/** A snapshot row as returned by `read_snapshot`. */
export interface Snapshot<S = unknown> {
  sourceUuid: string;
  sourceType: string;
  sourceVersion: bigint;
  data: S;
  metadata: unknown;
  createdAt: Date;
}
