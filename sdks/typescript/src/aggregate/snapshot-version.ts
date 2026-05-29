/**
 * Shared SDK-reserved snapshot metadata key for module versioning
 * (SNAP-002).
 *
 * Aggregates and process managers both use this key when stamping
 * a `snapshot_module_version` string into a snapshot's metadata on
 * write, and when comparing on read. A mismatch (or "version
 * present on one side and absent on the other") triggers a fall
 * back to full replay: for aggregates, page from version 0; for
 * PMs, fold `apply` over `listPmRebuildEvents`.
 *
 * The key string is **part of the porting checklist** (see
 * `sdks/porting-checklist.md` §4.2). A port that disagrees on
 * the key string can't read snapshots written by another port;
 * the TypeScript value `"snapshot_module_version"` is the
 * canonical choice and every conformant port reproduces it.
 *
 * Lives in its own file so both `aggregate.ts` (L2) and
 * `pm-substrate.ts` (L2) import from one place. Re-exported from
 * `instructed-sdk/core` (the porting-checklist surface).
 */

export const SNAPSHOT_MODULE_VERSION_KEY = "snapshot_module_version";
