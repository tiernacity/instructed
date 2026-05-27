/**
 * Layer 5: the `Instructed` facade (SUB-A slice 9 rewrite).
 *
 * Thin composition over Layers 0-4. `register*` declares what this
 * process can do; `poll()` fans out one **routing worker** +
 * one **processing worker** per registered projection / process
 * manager. `dispatch(aggregateType, ...)` resolves the aggregate
 * through the registry and delegates to `runCommandWithSnapshots`. `dispatch`
 * also accepts a `consistency` list and a `consistencyTimeout`
 * which, after the append commits, waits via {@link waitForProjection}
 * for the named subscriptions to catch up (D-0010: no `:strong`
 * shorthand).
 *
 * Registration surface: a single chainable `register(arg, opts?)`
 * that takes any of the four registerable shapes and dispatches
 * structurally to the right private handler. See the per-method
 * doc-comment on `register` for the overload set.
 *
 *   - `AggregateDefinition` (has `execute`): no `opts`.
 *   - `CommandRouter` (a function): no `opts`; at most one.
 *   - `ProjectionDefinition` (has `handler`): `opts` for
 *     lease/poll/heartbeat/onError. `partitionBy` and `routeFn`
 *     are mutually exclusive; default `{ kind: 'sequential' }`.
 *   - `ProcessManagerDefinition` (has `handle` + `routeFn`): same
 *     `opts` shape as projections.
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
  prefixType,
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
import type { CommandRouter } from "./command-router.ts";
import type { ErrorPolicy } from "./processing-worker.ts";
import {
  waitForProjection,
  type SubscriptionRef,
} from "./consistency.ts";
import { UnknownAggregateType } from "./errors.ts";
import type {
  Event,
  Command,
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
  /**
   * The database handle. A `pg.Pool` (typical), a `pg.PoolClient`,
   * or anything else conforming to `Queryable`. **The application
   * owns this connection** — it must end / release it itself when
   * tearing down (the facade does not). This makes the pool's
   * lifecycle explicit and lets the application share the pool
   * with non-SDK code (read-store queries, ad-hoc admin work).
   *
   * No env-var fallback and no string-URL convenience: the
   * application is responsible for constructing the pool exactly
   * as it wants.
   */
  db: pg.Pool | Queryable;
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
 * Projection definition (PRJ-A, SUB-A slice 9). Identified by
 * `type`, which doubles as the subscription name.
 *
 * `partitionBy` and `routeFn` are mutually exclusive. Default is
 * `{ kind: 'sequential' }`. A projection that needs routing-side
 * filtering passes `routeFn: (event) => 'ignore' | { partitionKey }`.
 */
export interface ProjectionDefinition<E extends Event = Event> {
  /** Projection type — doubles as the subscription name. */
  type: string;
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
  /**
   * Retry/error-policy hook. Default: exponential backoff, retry
   * forever. The facade stores policies as `ErrorPolicy<any>` so
   * users may register stateful policies (with a `PolicyState`
   * other than `undefined`); type-safety on the state slot is
   * forfeit at this layer. Callers wanting strong typing of
   * `PolicyState` use `startProjectionWorker` directly.
   */
  errorPolicy?: ErrorPolicy<any>;
}

/**
 * Process-manager definition (PM-F + PM-C, SUB-A slice 9). Identified
 * by `type`, which doubles as the subscription name and the snapshot
 * source_type prefix — same role as `AggregateDefinition.type`.
 *
 * `routeFn` is the PM-F routing primitive (`'ignore' | { partitionKey }`);
 * `apply` is the PM-C pure state fold; `handle` produces commands
 * and/or signals partition completion (`complete: true`).
 */
export interface ProcessManagerDefinition<S, E extends Event = Event>
  extends Omit<PmDefinition<S, E, any>, "type" | "streamName"> {
  /** PM type — doubles as the subscription name. */
  type: string;
  /** Optional source_uuid encoding (see {@link PmDefinition.streamName}). */
  streamName?(partitionKey: string): string;
  /** PM-F routing decision per event. */
  routeFn: RoutingFn<E>;
  /** Honoured only on the first claim that creates the subscription. */
  startFrom?: StartFrom;
}

interface RegisteredProjection {
  stream: string;
  def: ProjectionDefinition<Event>;
  opts: RegistrationOptions;
}

interface RegisteredProcessManager {
  stream: string;
  def: ProcessManagerDefinition<unknown, Event>;
  opts: RegistrationOptions;
}

// ============================================================================
// Implementation
// ============================================================================

export class Instructed {
  private readonly persistClient_: Client;

  private readonly defaults: Required<InstructedDefaults>;

  private readonly aggregates = new Map<
    string,
    AggregateDefinition<unknown, unknown, DomainEvent>
  >();
  private readonly projections: RegisteredProjection[] = [];
  private readonly processManagers: RegisteredProcessManager[] = [];
  private commandRouter_: CommandRouter | null = null;

  constructor(opts: InstructedOptions) {
    // The application owns the pool. The facade just wraps it.
    this.persistClient_ = new Client(opts.db);

    this.defaults = {
      leaseSeconds: opts.defaults?.leaseSeconds ?? DEFAULT_ROUTING_LEASE_SECONDS,
      batchSize: opts.defaults?.batchSize ?? DEFAULT_ROUTING_BATCH_SIZE,
      pollInterval:
        opts.defaults?.pollInterval ?? DEFAULT_ROUTING_POLL_INTERVAL_MS,
      retryBudget: opts.defaults?.retryBudget ?? DEFAULT_RETRY_BUDGET,
    };
  }

  // ---- registry ----

  /**
   * Unified, chainable registration. One public method registers all
   * four SDK extension points; the overload TS picks is determined
   * by the argument's structural shape:
   *
   *   - `CommandRouter`: a function `(Command) => { aggregateType,
   *     aggregateId }`. At most one per facade.
   *   - `AggregateDefinition`: object with an `execute` method.
   *     Indexed by `def.type`; duplicate types raise.
   *   - `ProjectionDefinition`: object with a `handler` method.
   *     `opts` configures lease / poll / heartbeat / onError.
   *   - `ProcessManagerDefinition`: object with `handle` + `routeFn`.
   *     Same `opts` as projections.
   *
   * Returns `this` so registrations chain:
   *
   *     new Instructed({ db })
   *       .register(Account)
   *       .register(Transfer)
   *       .register(appCommandRouter)
   *       .register(balancesProjection(pool), { pollInterval: 50 })
   *       .register(transferProcessManager(), { pollInterval: 50 });
   *
   * `opts` is rejected for aggregate and router registrations (it's
   * meaningless there) — silent acceptance would hide typos.
   *
   * Internally delegates to the per-kind private methods
   * (`#registerAggregate`, `#registerProjection`, etc.); those keep
   * named, narrowly-typed signatures for the implementation and for
   * any future SDK-internal callers that want to bypass the
   * structural dispatch.
   */
  register(router: CommandRouter): this;
  register<S, C, E extends DomainEvent = DomainEvent>(
    def: AggregateDefinition<S, C, E>,
  ): this;
  register<E extends Event = Event>(
    def: ProjectionDefinition<E>,
    opts?: RegistrationOptions,
  ): this;
  register<S, E extends Event = Event>(
    def: ProcessManagerDefinition<S, E>,
    opts?: RegistrationOptions,
  ): this;
  register(
    arg:
      | CommandRouter
      | AggregateDefinition<unknown, unknown, DomainEvent>
      | ProjectionDefinition<Event>
      | ProcessManagerDefinition<unknown, Event>,
    opts?: RegistrationOptions,
  ): this {
    // Discriminate structurally. The four registerable shapes are
    // disjoint by these markers (TS narrows the union through each
    // `in` check, so no casts are needed):
    //   typeof === function   -> CommandRouter
    //   has "execute"          -> AggregateDefinition
    //   has "handle" + routeFn -> ProcessManagerDefinition
    //   has "handler"          -> ProjectionDefinition
    if (typeof arg === "function") {
      if (opts !== undefined) {
        throw new Error(
          "Instructed.register: a CommandRouter takes no options",
        );
      }
      return this.registerCommandRouter(arg);
    }
    if ("execute" in arg) {
      if (opts !== undefined) {
        throw new Error(
          "Instructed.register: an AggregateDefinition takes no options",
        );
      }
      return this.registerAggregate(arg);
    }
    if ("handle" in arg && "routeFn" in arg) {
      return this.registerProcessManager(arg, opts);
    }
    if ("handler" in arg) {
      return this.registerProjection(arg, opts);
    }
    throw new Error(
      "Instructed.register: argument doesn't match any registerable shape " +
        "(CommandRouter / AggregateDefinition / ProjectionDefinition / " +
        "ProcessManagerDefinition)",
    );
  }

  // ---- per-kind registration (private; reached via `register()`) ----

  private registerAggregate<S, C, E extends DomainEvent = DomainEvent>(
    def: AggregateDefinition<S, C, E>,
  ): this {
    if (this.aggregates.has(def.type)) {
      throw new Error(
        `Instructed.register: aggregate type "${def.type}" already registered`,
      );
    }
    this.aggregates.set(def.type, def);
    return this;
  }

  private registerProjection<E extends Event = Event>(
    def: ProjectionDefinition<E>,
    opts: RegistrationOptions = {},
  ): this {
    if (def.partitionBy !== undefined && def.routeFn !== undefined) {
      throw new Error(
        `Instructed.register("${def.type}"): \`partitionBy\` and \`routeFn\` are mutually exclusive`,
      );
    }
    this.projections.push({
      stream: def.stream ?? "$all",
      def,
      opts,
    });
    return this;
  }

  private registerProcessManager<S, E extends Event = Event>(
    def: ProcessManagerDefinition<S, E>,
    opts: RegistrationOptions = {},
  ): this {
    this.processManagers.push({
      stream: def.stream ?? "$all",
      def,
      opts,
    });
    return this;
  }

  private registerCommandRouter(router: CommandRouter): this {
    if (this.commandRouter_ !== null) {
      throw new Error(
        "Instructed.register: a command router is already registered",
      );
    }
    this.commandRouter_ = router;
    return this;
  }

  // ---- dispatch ----

  /**
   * Dispatch a command. Two overloads:
   *
   *   - **Lean:** `dispatch(command, opts?)` — the registered
   *     command router resolves the command to
   *     `(aggregateType, aggregateId)`. Recommended.
   *   - **Explicit:** `dispatch(aggregateType, id, command, opts?)`
   *     — caller names the aggregate and id directly; bypasses
   *     the router.
   *
   * In both cases the underlying stream name is derived from the
   * aggregate definition (`def.streamName(id)`, defaulting to
   * {@link prefixType}). Application code identifies aggregates
   * by `(type, id)`; stream names are a storage-layer concern.
   */
  dispatch<C extends Command>(
    command: C,
    opts?: DispatchOptions,
  ): Promise<AppendedEvent[]>;
  dispatch<C>(
    aggregateType: string,
    id: string,
    command: C,
    opts?: DispatchOptions,
  ): Promise<AppendedEvent[]>;
  async dispatch(
    a: unknown,
    b?: unknown,
    c?: unknown,
    d?: unknown,
  ): Promise<AppendedEvent[]> {
    let aggregateType: string;
    let id: string;
    let command: unknown;
    let opts: DispatchOptions;

    if (typeof a === "string") {
      // Explicit overload: dispatch(type, id, command, opts?)
      aggregateType = a;
      id = b as string;
      command = c;
      opts = (d as DispatchOptions | undefined) ?? {};
    } else {
      // Lean overload: dispatch(command, opts?). Route via router.
      if (!this.commandRouter_) {
        throw new Error(
          "Instructed.dispatch: no command router registered. Call " +
            "`register(router)` first, or use the explicit " +
            "`dispatch(aggregateType, id, command, opts?)` overload.",
        );
      }
      const route = this.commandRouter_(a as Command);
      aggregateType = route.aggregateType;
      id = route.aggregateId;
      command = a;
      opts = (b as DispatchOptions | undefined) ?? {};
    }

    const def = this.aggregates.get(aggregateType);
    if (!def) throw new UnknownAggregateType(aggregateType);

    const streamUuid = (def.streamName ?? prefixType(def.type))(id);

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

  /**
   * Start polling the work queue. Spins up one routing worker +
   * one processing worker per registered projection / process
   * manager and returns a composite handle covering all of them.
   *
   * The application owns the returned `RunningWorker`: it must
   * call `.close()` to stop, and may keep a reference to
   * `.stopped` for a clean shutdown wait. The facade does *not*
   * track or auto-stop returned workers.
   *
   * Calling `poll()` more than once on the same `Instructed`
   * starts independent worker sets — unusual, but not forbidden.
   * Multiple processes pointing the same registration at the
   * same database is the normal HA story; multiple `poll()`s
   * inside one process is mostly useful for tests.
   */
  async poll(opts: { workerId?: string } = {}): Promise<RunningWorker> {
    if (this.projections.length === 0 && this.processManagers.length === 0) {
      throw new Error(
        "Instructed.poll: no projections or process managers registered",
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
        name: p.def.type,
        stream: p.stream,
        routeFn,
        ...(p.def.startFrom !== undefined
          ? { startFrom: p.def.startFrom }
          : {}),
      }, routingOpts);
      const processing = startProjectionWorker(this.persistClient_, {
        name: p.def.type,
        stream: p.stream,
        handler: p.def.handler,
        ...(p.def.errorPolicy !== undefined
          ? { errorPolicy: p.def.errorPolicy }
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
        name: pm.def.type,
        stream: pm.stream,
        routeFn: pm.def.routeFn,
        ...(pm.def.startFrom !== undefined
          ? { startFrom: pm.def.startFrom }
          : {}),
      }, routingOpts);
      const pmDef: PmDefinition<unknown, Event> = {
        type: pm.def.type,
        stream: pm.stream,
        initialState: pm.def.initialState,
        apply: pm.def.apply,
        handle: pm.def.handle,
        ...(pm.def.streamName !== undefined
          ? { streamName: pm.def.streamName }
          : {}),
        ...(pm.def.snapshotModuleVersion !== undefined
          ? { snapshotModuleVersion: pm.def.snapshotModuleVersion }
          : {}),
        ...(pm.def.errorPolicy !== undefined
          ? { errorPolicy: pm.def.errorPolicy }
          : {}),
      };
      // Wire the L3 routing helpers in so PM `handle` can return
      // lean commands (bare `Command`s) that the worker resolves
      // via the registered router + aggregate registry.
      const pmOpts = {
        ...processingOpts,
        ...(this.commandRouter_ !== null
          ? { router: this.commandRouter_ }
          : {}),
        aggregates: this.aggregates,
      };
      const processing = startPmWorker(
        this.persistClient_,
        pmDef,
        pmOpts,
      );
      workers.push(routing, processing);
    }

    return {
      stopped: Promise.all(workers.map((w) => w.stopped)).then(() => {}),
      close: async () => {
        // Parallel close: routing-worker dropping mid-batch is the
        // same observable behaviour as a crash (ML-0012); processing
        // workers honour the AbortSignal and finish their in-flight
        // item before exiting.
        await Promise.all(workers.map((w) => w.close()));
      },
    };
  }

  // ---- escape hatches ----

  client(): Client {
    return this.persistClient_;
  }

  // ---- internals ----

  private resolveProjectionRouteFn(
    p: RegisteredProjection,
  ): RoutingFn {
    if (p.def.routeFn) return p.def.routeFn;
    const pb: PartitionBy =
      p.def.partitionBy ?? { kind: "sequential" };
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


