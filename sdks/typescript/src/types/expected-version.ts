/**
 * Expected-version tag passed to `append_to_stream`.
 *
 * See `sql/instructed.sql` :: append_to_stream for the SQL
 * contract; `src/core.ts` and `src/index.ts` for the SDK's
 * export inventory.
 */
export type ExpectedVersion =
  | { kind: "any" }
  | { kind: "noStream" }
  | { kind: "streamExists" }
  | { kind: "exact"; version: bigint };

/** Constructors mirroring the SQL `expected_version_type` values. */
export const expected = {
  any: { kind: "any" } as const,
  noStream: { kind: "noStream" } as const,
  streamExists: { kind: "streamExists" } as const,
  exact(version: bigint | number): ExpectedVersion {
    return { kind: "exact", version: BigInt(version) };
  },
};
