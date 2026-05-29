/**
 * Processing worker (L2 kind-agnostic poll loop + SUB-B error policy) —
 * barrel. Also re-exports the L3 error-policy standard library
 * (`error-policies.ts`), which lives here because it composes directly
 * over the processing worker's `ErrorPolicy` contract.
 */

export {
  startProcessingWorker,
  DEFAULT_PROCESSING_LEASE_SECONDS,
  DEFAULT_PROCESSING_POLL_INTERVAL_MS,
  DEFAULT_ERROR_POLICY,
} from './processing-worker.ts'
export type {
  ProcessingHandler,
  ProcessingCompleter,
  ProcessingHandlerContext,
  ProcessingWorkerDefinition,
  ProcessingWorkerOptions,
  ErrorPolicy,
  ErrorPolicyDecision,
  ErrorPolicyContext,
  ErrorPolicyResult,
} from './processing-worker.ts'

// L3 error-policy standard library (composable wrappers over ErrorPolicy).
export { exponentialBackoff, linearBackoff, retryUpTo } from './error-policies.ts'
export type { ExponentialBackoffOptions, LinearBackoffOptions } from './error-policies.ts'
