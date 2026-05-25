/**
 * SUB-A slice 6 — projection processing worker.
 *
 * Thin adapter over `startProcessingWorker` (slice 5) that supplies the
 * projection-side terminal-success step: a single call to
 * `complete_work_item_projection`. The DELETE runs as its own short
 * SDK-owned tx *after* the user handler returns. Per D-0016
 * (`docs/decisions.md`) the projection handler is opaque to the SDK:
 * it receives the event and a minimal context (`workerId`,
 * `partitionKey`, `eventNumber`, `attempt`, `signal`); it does **not**
 * receive a Postgres connection, an ORM handle, or any other
 * framework-owned resource. Projection targets are application-domain
 * (Postgres, Elasticsearch, Redis, BigQuery, HTTP APIs, ...) and the
 * SDK does not assume one. At-least-once delivery; the handler MUST be
 * idempotent against redelivery.
 *
 * This module also owns the `PartitionBy` -> `RoutingFn` translation
 * helper (`routingFnForPartitionBy`). The translation is used by the
 * slice-9 facade and by slice-6 tests; it produces a `RoutingFn` that
 * never returns `"ignore"`. A projection that needs routing-side
 * filtering (the legacy `selector` parameter's role) uses a raw
 * `routeFn: RoutingFn` directly via the slice-9 facade -- see
 * `docs/todo/projections.md` PRJ-A under option (c) for the unified
 * routing surface.
 *
 * Not yet re-exported from `src/index.ts`. The layer-5 facade in
 * slice 9 will wire `registerProjection` here; tests import the
 * module directly.
 */

import type { Client } from "./client.ts";
import type { RecordedEvent } from "./types.ts";
import type { RoutingFn } from "./routing-worker.ts";
import {
  startProcessingWorker,
  type ErrorPolicy,
  type ProcessingHandlerContext,
  type ProcessingWorkerOptions,
} from "./processing-worker.ts";
import type { RunningWorker } from "./internal/running-worker.ts";

// ============================================================================
// PartitionBy + RoutingFn translation (sugar)
// ============================================================================

/**
 * Three-mode partitioning sugar over a routing-layer `RoutingFn`. None of
 * the modes can emit `"ignore"`; if you need to filter at routing time,
 * pass a raw `RoutingFn` (slice-9 facade). PRJ-A.
 */
export type PartitionBy<E = unknown> =
  | { kind: "sequential" }
  | { kind: "per-event" }
  | { kind: "per-key"; key: (event: RecordedEvent<E>) => string };

/** The synthetic partition key used by `{ kind: "sequential" }`. */
export const SEQUENTIAL_PARTITION_KEY = "_default";

/**
 * Translate a `PartitionBy` into a routing-layer `RoutingFn`. The
 * returned function is pure and never produces `"ignore"`.
 *
 *   sequential -> always `{ partitionKey: '_default' }`.
 *   per-event  -> `{ partitionKey: String(event.event_number) }`.
 *   per-key    -> `{ partitionKey: key(event) }`.
 *
 * The per-key user function may throw or return a non-string value;
 * those errors propagate to the routing worker, which surfaces them
 * via `onError` and stalls (SUB-A "no silent skip"). No clever
 * recovery here.
 */
export function routingFnForPartitionBy<E>(pb: PartitionBy<E>): RoutingFn<E> {
  switch (pb.kind) {
    case "sequential":
      return () => ({ partitionKey: SEQUENTIAL_PARTITION_KEY });
    case "per-event":
      return (event) => ({ partitionKey: String(event.event_number) });
    case "per-key": {
      const key = pb.key;
      return (event) => ({ partitionKey: key(event) });
    }
  }
}

// ============================================================================
// Projection processing worker
// ============================================================================

/**
 * Context handed to a projection handler. A subset of the kind-agnostic
 * `ProcessingHandlerContext`; we re-export the same shape under the
 * projection-facing name so the public surface is self-contained.
 * Per D-0016: no tx, no Queryable, no framework-owned resource.
 */
export type ProjectionHandlerContext = ProcessingHandlerContext;

export type ProjectionHandler<E = unknown> = (
  event: RecordedEvent<E>,
  ctx: ProjectionHandlerContext,
) => Promise<void>;

export interface ProjectionDefinition<E = unknown> {
  /** Subscription name (must match the routing worker for the same sub). */
  name: string;
  /** Source stream; default `$all`. */
  stream?: string;
  /** User-supplied projection handler. Opaque to the SDK (D-0016). */
  handler: ProjectionHandler<E>;
  /** SUB-B error-policy hook. Defaults to exponential backoff, retry forever. */
  errorPolicy?: ErrorPolicy;
}

export type ProjectionWorkerOptions = ProcessingWorkerOptions;

/**
 * Start a projection processing worker. Wraps `startProcessingWorker`
 * (the kind-agnostic poll loop) with a `complete` callback that DELETEs
 * the work item via `complete_work_item_projection`.
 *
 * Caller is responsible for the routing-worker side (one
 * `startRoutingWorker` per subscription); the slice-9 facade glues
 * the two together at registration time.
 */
export function startProjectionWorker<E = unknown>(
  client: Client,
  def: ProjectionDefinition<E>,
  opts: ProjectionWorkerOptions = {},
): RunningWorker {
  const stream = def.stream ?? "$all";
  return startProcessingWorker<E>(
    client,
    {
      name: def.name,
      stream,
      errorPolicy: def.errorPolicy,
      handle: def.handler,
      complete: async (_event, ctx) => {
        // Own short SDK-owned tx (D-0016): single procedure call,
        // implicit single-statement tx server-side. No framework tx
        // wraps the handler.
        await client.completeWorkItemProjection(
          stream,
          def.name,
          ctx.workerId,
          ctx.partitionKey,
          ctx.eventNumber,
        );
      },
    },
    opts,
  );
}
