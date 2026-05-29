/**
 * Process-manager workers — barrel.
 *
 *   - `pm-substrate.ts` (L2): snapshot+ack lifecycle, rebuild, lease mgmt.
 *   - `pm-worker.ts` (L3): by-value `commands`-list orchestration over
 *     the substrate.
 */

export { startPmSubstrate } from './pm-substrate.ts'
export type {
  PmSubstrateDefinition,
  PmSubstrateHandleResult,
  PmSubstrateHandlerContext,
  PmSubstrateOptions,
} from './pm-substrate.ts'

export { startPmWorker } from './pm-worker.ts'
export type {
  PmDefinition,
  PmHandleResult,
  PmHandlerContext,
  PmWorkerOptions,
  DispatchedCommand,
  DispatchedCommandExplicit,
} from './pm-worker.ts'
