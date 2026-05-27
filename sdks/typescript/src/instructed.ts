/**
 * Layer 5: the `Instructed` facade (SUB-A slice 9 rewrite).
 *
 * Thin composition over Layers 0-4. `register*` declares what this
 * process can do; `startWorker()` fans out one **routing worker** +
 * one **processing worker** per registered projection / process
 * manager. `dispatch(aggregateType, ...)` resolves the aggregate
 * through the registry and delegates to `runCommandWithSnapshots`. `dispatch`
 * also accepts a `consistency` list and a `consistencyTimeout`
 * which, after the append commits, waits via {@link waitForProjection}
 * for the named subscriptions to catch up (D-0010: no `:strong`
 * shorthand).
 *
 * SUB-A registration surface (PRJ-A + PM-F, slice 9):
 *
 *   registerProjection(name, { partitionBy? | routeFn?, handler,
 *                              stream?, errorPolicy?, startFrom? },
 *                      opts?)
 *     - `partitionBy` and `routeFn` are mutually exclusive.
 *     - Default: `{ kind: 'sequential' }`.
 *     - The legacy `selector` parameter is removed; the same
 *       observable behaviour is recoverable via a `routeFn` that
 *       returns `"ignore"` for would-be-skipped events.
 *
 *   registerProcessManager(name, { routeFn, apply, handle,
 *                                  initialState, snapshotModuleVersion?,
 *                                  stream?, errorPolicy?, startFrom? },
 *                          opts?)
 *     - The old single-`handle` signature is **removed** (breaking;
 *       not deprecated).
 *
 * Pool management:
 *   - the client wraps the user's `db` (env-var or default when
 *     omitted); ownership tracked so `close()` ends owned pools.
 *   - per [D-0026](../../../docs/decisions.md#d-0026) there is one
 *     pool / one `Client` for the entire SDK. PM dispatch shares the
 *     same client as the persist-and-ack path; the two-pool model
 *     (`dispatchDb`, `dispatchClient()`) was retired — lock-set
 *     disjointness is a property of the SQL contract's per-procedure
 *     lock-acquisition orders, not of pool / client identity.
 */

import * as pg from "pg";
import { Client } from "./client.ts";
import {
  DEFAULT_RETRY_BUDGET,
  type AggregateDefinition,
  type DomainEvent,
  type RunCommandOptions,
} from "./aggregate.ts";
import { runCommandWithSnapshots } from "./aggregate-snapshots.ts";
import {
  startRoutingWorker,
  DEFAULT_ROUTING_BATCH_SIZE,
  DEFAULT_ROUTING_LEASE_SECONDS,
  DEFAULT_ROUTING_POLL_INTERVAL_MS,
  type RoutingFn,
} from "./routing-worker.ts";
import {
  startProjectionWorker,
  type ProjectionHandler,
} from "./projection-worker.ts";
import {
  routingFnForPartitionBy,
  type PartitionBy,
} from "./partition-by.ts";
import {
  startPmWorker,
  type PmDefinition,
} from "./pm-worker.ts";
import type { ErrorPolicy } from "./processing-worker.ts";
import {
  waitForProjection,
  type SubscriptionRef,
} from "./consistency.ts";
import { UnknownAggregateType } from "./errors.ts";
import type {
  AppendedEvent,
  ExpectedVersion,
  Queryable,
  StartFrom,
} from "./types.ts";
import type { RunningWorker } from "./internal/running-worker.ts";

// ============================================================================
// Public surface
// ============================================================================

export interface InstructedDefaults {
  /** Lease (seconds) for both the routing and processing workers. */
  leaseSeconds?: number;
  /** Routing-worker batch size (events per `route_batch` call). */
  batchSize?: number;
  /** Poll interval (ms) used by both worker kinds. */
  pollInterval?: number;
  /** Aggregate retry budget on conflict. */
  retryBudget?: number;
}

export interface InstructedOptions {
  /** `pg.Pool`, a connection string, or any Queryable. */
  db?: pg.Pool | Queryable | string;
  defaults?: InstructedDefaults;
}

/** Per-registration knobs (applied to both routing and processing). */
export interface RegistrationOptions {
  batchSize?: number;
  leaseSeconds?: number;
  heartbeatInterval?: number;
  pollInterval?: number;
  onError?: (err: Error) => void;
}

export interface DispatchOptions {
  /**
   * Either a list of subscription names (sugar for `$all` subs) or
   * an explicit `[{stream, name}]` list. The list is always explicit
   * -- no `:strong` shorthand (D-0010).
   */
  consistency?: string[] | SubscriptionRef[];
  /** Total budget for the consistency wait in ms. Default 5_000. */
  consistencyTimeout?: number;
  retryBudget?: number;
  expectedVersion?: ExpectedVersion;
}

/**
 * Projection registration shape (PRJ-A, SUB-A slice 9).
 *
 * `partitionBy` and `routeFn` are mutually exclusive. Default is
 * `{ kind: 'sequential' }`. A projection that needs routing-side
 * filtering (the legacy `selector` parameter's role) passes
 * `routeFn: (event) => 'ignore' | { partitionKey }`.
 */
export interface RegisterProjectionInput<E = unknown> {
  /** Source stream; default `$all`. */
  stream?: string;
  /** Sugar over a `RoutingFn`. */
  partitionBy?: PartitionBy<E>;
  /** Raw routing function escape hatch (mutually exclusive with `partitionBy`). */
  routeFn?: RoutingFn<E>;
  /** Honoured only on the first claim that creates the subscription. */
  startFrom?: StartFrom;
  /** User-supplied projection handler. Opaque to the SDK (D-0016). */
  handler: ProjectionHandler<E>;
  /** SUB-B error-policy hook. Default: exponential backoff, retry forever. */
  errorPolicy?: ErrorPolicy;
}

/**
 * Process-manager registration shape (PM-F + PM-C, SUB-A slice 9).
 *
 * The legacy single-`handle` signature is removed. `routeFn` is the
 * PM-F routing primitive (`'ignore' | { partitionKey }`); `apply` is
 * the PM-C pure state fold; `handle` produces commands and/or signals
 * partition completion (`complete: true`).
 */
export interface RegisterProcessManagerInput<S, E = unknown>
  extends Omit<PmDefinition<S, E>, "name"> {
  /** PM-F routing decision per event. */
  routeFn: RoutingFn<E>;
  /** Honoured only on the first claim that creates the subscription. */
  startFrom?: StartFrom;
}

interface RegisteredProjection {
  name: string;
  stream: string;
  input: RegisterProjectionInput<any>;
  opts: RegistrationOptions;
}

interface RegisteredProcessManager {
  name: string;
  stream: string;
  input: RegisterProcessManagerInput<any, any>;
  opts: RegistrationOptions;
}

// ============================================================================
// Implementation
// ============================================================================

export class Instructed {
  private readonly persistPool: pg.Pool | Queryable;
  private readonly persistOwned: boolean;

  private readonly persistClient_: Client;

  private readonly defaults: Required<InstructedDefaults>;

  private readonly aggregates = new Map<
    string,
    AggregateDefinition<any, any, any>
  >();
  private readonly projections: RegisteredProjection[] = [];
  private readonly processManagers: RegisteredProcessManager[] = [];

  private worker: RunningWorker | null = null;
  private closed = false;

  constructor(opts: InstructedOptions | string = {}) {
    const o: InstructedOptions = typeof opts === "string" ? { db: opts } : opts;

    // Pool. Single pool for the whole SDK under D-0026.
    let dbArg: pg.Pool | Queryable | string | undefined = o.db;
    if (dbArg === undefined) {
      dbArg = process.env.INSTRUCTED_DATABASE_URL || undefined;
    }
    if (dbArg === undefined) {
      this.persistPool = new pg.Pool();
      this.persistOwned = true;
    } else if (typeof dbArg === "string") {
      this.persistPool = new pg.Pool({ connectionString: dbArg });
      this.persistOwned = true;
    } else {
      this.persistPool = dbArg;
      this.persistOwned = false;
    }
    this.persistClient_ = new Client(this.persistPool as Queryable);

    this.defaults = {
      leaseSeconds: o.defaults?.leaseSeconds ?? DEFAULT_ROUTING_LEASE_SECONDS,
      batchSize: o.defaults?.batchSize ?? DEFAULT_ROUTING_BATCH_SIZE,
      pollInterval: o.defaults?.pollInterval ?? DEFAULT_ROUTING_POLL_INTERVAL_MS,
      retryBudget: o.defaults?.retryBudget ?? DEFAULT_RETRY_BUDGET,
    };
  }

  // ---- registry ----

  registerAggregate<S, C, E extends DomainEvent = DomainEvent>(
    def: AggregateDefinition<S, C, E>,
  ): void {
    if (this.aggregates.has(def.type)) {
      throw new Error(
        `Instructed.registerAggregate: aggregate type "${def.type}" already registered`,
      );
    }
    this.aggregates.set(def.type, def);
  }

  registerProjection<E = unknown>(
    name: string,
    input: RegisterProjectionInput<E>,
    opts: RegistrationOptions = {},
  ): void {
    if (input.partitionBy !== undefined && input.routeFn !== undefined) {
      throw new Error(
        `Instructed.registerProjection("${name}"): \`partitionBy\` and \`routeFn\` are mutually exclusive`,
      );
    }
    this.projections.push({
      name,
      stream: input.stream ?? "$all",
      input,
      opts,
    });
  }

  registerProcessManager<S, E = unknown>(
    name: string,
    input: RegisterProcessManagerInput<S, E>,
    opts: RegistrationOptions = {},
  ): void {
    this.processManagers.push({
      name,
      stream: input.stream ?? "$all",
      input,
      opts,
    });
  }

  // ---- dispatch ----

  async dispatch<C>(
    aggregateType: string,
    streamUuid: string,
    command: C,
    opts: DispatchOptions = {},
  ): Promise<AppendedEvent[]> {
    const def = this.aggregates.get(aggregateType);
    if (!def) throw new UnknownAggregateType(aggregateType);

    const runOpts: RunCommandOptions = {
      retryBudget: opts.retryBudget ?? this.defaults.retryBudget,
    };
    if (opts.expectedVersion !== undefined) {
      runOpts.expectedVersion = opts.expectedVersion;
    }
    const appended = await runCommandWithSnapshots(
      this.persistClient_,
      def,
      streamUuid,
      command,
      runOpts,
    );

    if (opts.consistency && opts.consistency.length > 0 && appended.length > 0) {
      const refs = normaliseConsistency(opts.consistency);
      await waitForProjection(this.persistClient_, appended, refs, {
        timeout: opts.consistencyTimeout,
      });
    }
    return appended;
  }

  // ---- worker ----

  async startWorker(opts: { workerId?: string } = {}): Promise<RunningWorker> {
    if (this.worker) {
      throw new Error("Instructed.startWorker: a worker is already running");
    }
    if (this.projections.length === 0 && this.processManagers.length === 0) {
      throw new Error(
        "Instructed.startWorker: no projections or process managers registered",
      );
    }

    const workers: RunningWorker[] = [];

    // Projections: one routing worker + one processing worker per
    // registration. Both honour the same per-registration knobs.
    for (const p of this.projections) {
      const routeFn = this.resolveProjectionRouteFn(p);
      const routingOpts = this.routingOpts(p.opts, opts.workerId);
      const processingOpts = this.processingOpts(p.opts, opts.workerId);
      const routing = startRoutingWorker(this.persistClient_, {
        name: p.name,
        stream: p.stream,
        routeFn,
        ...(p.input.startFrom !== undefined
          ? { startFrom: p.input.startFrom }
          : {}),
      }, routingOpts);
      const processing = startProjectionWorker(this.persistClient_, {
        name: p.name,
        stream: p.stream,
        handler: p.input.handler,
        ...(p.input.errorPolicy !== undefined
          ? { errorPolicy: p.input.errorPolicy }
          : {}),
      }, processingOpts);
      workers.push(routing, processing);
    }

    // PMs: one routing worker + one processing worker per registration.
    // The processing worker takes the dispatch client too (D-0011).
    for (const pm of this.processManagers) {
      const routingOpts = this.routingOpts(pm.opts, opts.workerId);
      const processingOpts = this.processingOpts(pm.opts, opts.workerId);
      const routing = startRoutingWorker(this.persistClient_, {
        name: pm.name,
        stream: pm.stream,
        routeFn: pm.input.routeFn,
        ...(pm.input.startFrom !== undefined
          ? { startFrom: pm.input.startFrom }
          : {}),
      }, routingOpts);
      const pmDef: PmDefinition<any, any> = {
        name: pm.name,
        stream: pm.stream,
        initialState: pm.input.initialState,
        apply: pm.input.apply,
        handle: pm.input.handle,
        ...(pm.input.snapshotModuleVersion !== undefined
          ? { snapshotModuleVersion: pm.input.snapshotModuleVersion }
          : {}),
        ...(pm.input.errorPolicy !== undefined
          ? { errorPolicy: pm.input.errorPolicy }
          : {}),
      };
      const processing = startPmWorker(
        this.persistClient_,
        pmDef,
        processingOpts,
      );
      workers.push(routing, processing);
    }

    const composite: RunningWorker = {
      stopped: Promise.all(workers.map((w) => w.stopped)).then(() => {}),
      close: async () => {
        // Parallel close: routing-worker dropping mid-batch is the
        // same observable behaviour as a crash (ML-0012); processing
        // workers honour the AbortSignal and finish their in-flight
        // item before exiting.
        await Promise.all(workers.map((w) => w.close()));
      },
    };
    this.worker = composite;
    return composite;
  }

  // ---- escape hatches ----

  client(): Client {
    return this.persistClient_;
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    if (this.worker) {
      try {
        await this.worker.close();
      } catch {
        // ignore
      }
      this.worker = null;
    }
    if (this.persistOwned && isPool(this.persistPool)) {
      await this.persistPool.end();
    }
  }

  // ---- internals ----

  private resolveProjectionRouteFn(
    p: RegisteredProjection,
  ): RoutingFn<unknown> {
    if (p.input.routeFn) return p.input.routeFn;
    const pb: PartitionBy<unknown> =
      p.input.partitionBy ?? { kind: "sequential" };
    return routingFnForPartitionBy(pb);
  }

  private routingOpts(
    o: RegistrationOptions,
    workerId: string | undefined,
  ) {
    const out: Parameters<typeof startRoutingWorker>[2] = {
      batchSize: o.batchSize ?? this.defaults.batchSize,
      leaseSeconds: o.leaseSeconds ?? this.defaults.leaseSeconds,
      pollInterval: o.pollInterval ?? this.defaults.pollInterval,
    };
    // Note: `heartbeatInterval` is intentionally ignored for routing
    // workers under D-0025 (per-batch claim/release; no heartbeat).
    // The option is preserved on `RegistrationOptions` because
    // processing workers still use it.
    if (o.onError !== undefined) out.onError = o.onError;
    if (workerId !== undefined) out.workerId = workerId;
    return out;
  }

  private processingOpts(
    o: RegistrationOptions,
    workerId: string | undefined,
  ) {
    // The processing worker has no `batchSize` knob (it claims one
    // item at a time); the other knobs map 1:1.
    const out: Parameters<typeof startProjectionWorker>[2] = {
      leaseSeconds: o.leaseSeconds ?? this.defaults.leaseSeconds,
      pollInterval: o.pollInterval ?? this.defaults.pollInterval,
    };
    if (o.heartbeatInterval !== undefined) out.heartbeatInterval = o.heartbeatInterval;
    if (o.onError !== undefined) out.onError = o.onError;
    if (workerId !== undefined) out.workerId = workerId;
    return out;
  }

}

function normaliseConsistency(
  list: string[] | SubscriptionRef[],
): SubscriptionRef[] {
  return list.map((entry) =>
    typeof entry === "string"
      ? ({ stream: "$all", name: entry })
      : entry,
  );
}

function isPool(con: unknown): con is pg.Pool {
  return (
    typeof con === "object" &&
    con !== null &&
    typeof (con as { end?: unknown }).end === "function" &&
    typeof (con as { connect?: unknown }).connect === "function"
  );
}
