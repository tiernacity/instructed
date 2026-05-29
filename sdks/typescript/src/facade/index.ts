/**
 * L3 facade (barrel).
 *
 * This is a barrel: it contains nothing but re-exports.
 *
 *   - `instructed.ts`      — the `Instructed` facade.
 *   - `command-router.ts`  — `commandRouter` resolver.
 *   - `partition-by.ts`    — `PartitionBy` sugar over a `RoutingFn`.
 *   - `routing-helpers.ts` — `RoutingFn` combinators (`onlyTypes`).
 *
 * None of these are part of the porting-checklist surface; they are
 * re-exported only from the bare `instructed-sdk` entry (`src/index.ts`),
 * not from `instructed-sdk/core`.
 */

export {
  Instructed,
  DEFAULT_WORKER_OPTIONS,
} from "./instructed.ts";
export type {
  InstructedOptions,
  RegistrationOptions,
  WorkerOptions,
  PollOptions,
  ProjectionDefinition,
  ProcessManagerDefinition,
  DispatchOptions,
} from "./instructed.ts";

export { commandRouter } from "./command-router.ts";
export type { CommandRouter, CommandRoute } from "./command-router.ts";

export {
  routingFnForPartitionBy,
  SEQUENTIAL_PARTITION_KEY,
} from "./partition-by.ts";
export type { PartitionBy } from "./partition-by.ts";

export { onlyTypes } from "./routing-helpers.ts";
