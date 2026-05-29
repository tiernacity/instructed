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
 * The `PartitionBy` sugar that pairs with this adapter (PRJ-A) lives
 * in its own file (`src/partition-by.ts`) so the file boundary matches
 * the layer boundary: this adapter is L2 and is re-exported from
 * `instructed-sdk/core`; the sugar is L3 and is re-exported only from
 * the bare `instructed-sdk` entry. A consumer of `core` writes a raw
 * `RoutingFn` directly. See `docs/architecture.md` "How a worker
 * runs" for the routing / processing split.
 */

import type { Client } from "./client/index.ts";
import type { Event, RecordedEvent } from "./types/index.ts";
import {
  startProcessingWorker,
  type ErrorPolicy,
  type ProcessingHandlerContext,
  type ProcessingWorkerOptions,
} from "./processing-worker.ts";
import type { RunningWorker } from "./internal/running-worker.ts";

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

export type ProjectionHandler<E extends Event = Event> = (
  event: RecordedEvent<E>,
  ctx: ProjectionHandlerContext,
) => Promise<void>;

export interface ProjectionWorkerDefinition<E extends Event = Event, PolicyState = undefined> {
  /** Subscription name (must match the routing worker for the same sub). */
  name: string;
  /** Source stream; default `$all`. */
  stream?: string;
  /** User-supplied projection handler. Opaque to the SDK (D-0016). */
  handler: ProjectionHandler<E>;
  /**
   * Retry/error-policy hook. Defaults to `DEFAULT_ERROR_POLICY`
   * (exponential backoff, retry forever). Type-parameterised by
   * `PolicyState` for callers writing stateful policies; defaults
   * to `ErrorPolicy<undefined>`.
   */
  errorPolicy?: ErrorPolicy<PolicyState>;
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
export function startProjectionWorker<E extends Event = Event, PolicyState = undefined>(
  client: Client,
  def: ProjectionWorkerDefinition<E, PolicyState>,
  opts: ProjectionWorkerOptions = {},
): RunningWorker {
  const stream = def.stream ?? "$all";
  return startProcessingWorker<E, PolicyState>(
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
