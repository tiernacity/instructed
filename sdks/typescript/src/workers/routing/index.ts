/** Routing worker (L2, D-0025 per-batch claim/release) — barrel. */

export {
  startRoutingWorker,
  DEFAULT_ROUTING_BATCH_SIZE,
  DEFAULT_ROUTING_LEASE_SECONDS,
  DEFAULT_ROUTING_POLL_INTERVAL_MS,
} from "./routing-worker.ts";
export type {
  RoutingDecision,
  RoutingFn,
  RoutingDefinition,
  RoutingWorkerOptions,
} from "./routing-worker.ts";
