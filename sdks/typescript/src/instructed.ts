/**
 * Layer 5: the `Instructed` facade.
 *
 * Thin composition over Layers 0–4 (sdk-design.md §3 layer 3.5).
 * `register*` declares what this process can do; `startWorker()`
 * fans out to one subscription loop per registered projection /
 * process manager. `dispatch(aggregateType, ...)` resolves the
 * aggregate definition through the registry and delegates to
 * `runCommand`. `dispatch` also accepts a `consistency` list and
 * a `consistencyTimeout` which, after the append commits, waits
 * via {@link waitForProjection} for the named subscriptions to
 * catch up — no `:strong` shorthand (D-0010).
 *
 * Pool management:
 *   - the persist client wraps the user's `db` (env-var or default
 *     when omitted); ownership tracked so `close()` ends owned pools.
 *   - the dispatch client wraps `dispatchDb` (or a sibling pool with
 *     the same connection string); **materialised lazily** — a process
 *     that only registers aggregates and dispatches never opens it.
 *     The first `registerProcessManager` or PM-driven dispatch
 *     triggers materialisation (whichever comes first). v1 considers
 *     `dispatch` itself NOT a PM-driven dispatch — it uses the persist
 *     pool. The PM worker is the only path that needs the dispatch
 *     pool today.
 */

import * as pg from "pg";
import { Client } from "./client.ts";
import {
  runCommand,
  DEFAULT_RETRY_BUDGET,
  type AggregateDefinition,
  type DomainEvent,
  type RunCommandOptions,
} from "./aggregate.ts";
import {
  startProjection,
  DEFAULT_BATCH_SIZE,
  DEFAULT_LEASE_SECONDS,
  DEFAULT_POLL_INTERVAL_MS,
  type ProjectionDefinition,
  type ProjectionWorkerOptions,
  type RunningWorker,
} from "./subscription.ts";
import {
  startProcessManager,
  type ProcessManagerDefinition,
  type ProcessManagerWorkerOptions,
} from "./process-manager.ts";
import {
  waitForProjection,
  type SubscriptionRef,
} from "./consistency.ts";
import { UnknownAggregateType } from "./errors.ts";
import type {
  AppendedEvent,
  ExpectedVersion,
  Queryable,
} from "./types.ts";

export interface InstructedDefaults {
  leaseSeconds?: number;
  batchSize?: number;
  pollInterval?: number;
  retryBudget?: number;
}

export interface InstructedOptions {
  /** `pg.Pool`, a connection string, or any Queryable. */
  db?: pg.Pool | Queryable | string;
  /** Separate pool for PM dispatch; defaults to a sibling Pool. */
  dispatchDb?: pg.Pool | Queryable | string;
  defaults?: InstructedDefaults;
}

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
   * — no `:strong` shorthand (D-0010).
   */
  consistency?: string[] | SubscriptionRef[];
  /** Total budget for the consistency wait in ms. Default 5_000. */
  consistencyTimeout?: number;
  retryBudget?: number;
  expectedVersion?: ExpectedVersion;
}

interface RegisteredProjection {
  def: ProjectionDefinition<any>;
  opts: RegistrationOptions;
}
interface RegisteredProcessManager {
  def: ProcessManagerDefinition<any, any>;
  opts: RegistrationOptions;
}

export class Instructed {
  private readonly persistPool: pg.Pool | Queryable;
  private readonly persistOwned: boolean;
  /** Lazily materialised on the first PM registration. */
  private dispatchPool: pg.Pool | Queryable | null;
  private dispatchOwned: boolean;
  /** The user-supplied dispatchDb (kept until materialisation). */
  private readonly dispatchSource: pg.Pool | Queryable | string | undefined;
  /** The connection string used for the persist pool, if any. */
  private readonly persistConnString: string | undefined;

  private readonly persistClient_: Client;
  private dispatchClient_: Client | null = null;

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

    // Persist pool.
    let dbArg: pg.Pool | Queryable | string | undefined = o.db;
    if (dbArg === undefined) {
      // Mirror absurd's default: env var or a localhost fallback. We
      // use PGDATABASE-style env vars (pg picks them up automatically
      // when we instantiate a Pool with no connection options).
      dbArg = process.env.INSTRUCTED_DATABASE_URL || undefined;
    }
    if (dbArg === undefined) {
      this.persistPool = new pg.Pool();
      this.persistOwned = true;
      this.persistConnString = undefined;
    } else if (typeof dbArg === "string") {
      this.persistPool = new pg.Pool({ connectionString: dbArg });
      this.persistOwned = true;
      this.persistConnString = dbArg;
    } else {
      this.persistPool = dbArg;
      this.persistOwned = false;
      this.persistConnString = undefined;
    }
    this.persistClient_ = new Client(this.persistPool as Queryable);

    this.dispatchSource = o.dispatchDb;
    this.dispatchPool = null;
    this.dispatchOwned = false;

    this.defaults = {
      leaseSeconds: o.defaults?.leaseSeconds ?? DEFAULT_LEASE_SECONDS,
      batchSize: o.defaults?.batchSize ?? DEFAULT_BATCH_SIZE,
      pollInterval: o.defaults?.pollInterval ?? DEFAULT_POLL_INTERVAL_MS,
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

  registerProjection<E>(
    def: ProjectionDefinition<E>,
    opts: RegistrationOptions = {},
  ): void {
    this.projections.push({ def, opts });
  }

  registerProcessManager<S, E>(
    def: ProcessManagerDefinition<S, E>,
    opts: RegistrationOptions = {},
  ): void {
    this.processManagers.push({ def, opts });
    // Materialise the dispatch pool eagerly when a PM is registered.
    // This is the contract from §3 layer 3.5 ("materialised on the
    // first registerProcessManager or first PM-driven dispatch").
    this.ensureDispatchClient();
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
    const appended = await runCommand(
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
    for (const p of this.projections) {
      workers.push(
        startProjection(this.persistClient_, p.def, this.projOpts(p.opts, opts.workerId)),
      );
    }
    for (const pm of this.processManagers) {
      workers.push(
        startProcessManager(
          this.persistClient_,
          this.ensureDispatchClient(),
          pm.def,
          this.pmOpts(pm.opts, opts.workerId),
        ),
      );
    }

    const composite: RunningWorker = {
      stopped: Promise.all(workers.map((w) => w.stopped)).then(() => {}),
      close: async () => {
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

  dispatchClient(): Client {
    return this.ensureDispatchClient();
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
    if (this.dispatchOwned && this.dispatchPool && isPool(this.dispatchPool)) {
      await this.dispatchPool.end();
    }
  }

  // ---- internals ----

  private projOpts(
    o: RegistrationOptions,
    workerId: string | undefined,
  ): ProjectionWorkerOptions {
    const out: ProjectionWorkerOptions = {
      batchSize: o.batchSize ?? this.defaults.batchSize,
      leaseSeconds: o.leaseSeconds ?? this.defaults.leaseSeconds,
      pollInterval: o.pollInterval ?? this.defaults.pollInterval,
    };
    if (o.heartbeatInterval !== undefined) out.heartbeatInterval = o.heartbeatInterval;
    if (o.onError !== undefined) out.onError = o.onError;
    if (workerId !== undefined) out.workerId = workerId;
    return out;
  }

  private pmOpts(
    o: RegistrationOptions,
    workerId: string | undefined,
  ): ProcessManagerWorkerOptions {
    return this.projOpts(o, workerId);
  }

  private ensureDispatchClient(): Client {
    if (this.dispatchClient_) return this.dispatchClient_;

    let src: pg.Pool | Queryable | string | undefined = this.dispatchSource;
    if (src === undefined) {
      // Default: sibling Pool with the same connection string as
      // persist, when persist was opened from a string; otherwise
      // a default Pool that picks up PG* env vars. The crucial
      // invariant is that this is a *different* pool from the
      // persist pool (D-0011 / D-0012).
      if (this.persistConnString !== undefined) {
        this.dispatchPool = new pg.Pool({
          connectionString: this.persistConnString,
        });
        this.dispatchOwned = true;
      } else if (this.persistOwned) {
        // Persist was a default Pool (env-var-driven); spin a sibling.
        this.dispatchPool = new pg.Pool();
        this.dispatchOwned = true;
      } else {
        // The user handed us a Pool / Queryable for persist. We have
        // no connection string and cannot safely guess. The caller
        // must supply `dispatchDb` if they want process managers.
        throw new Error(
          "Instructed: cannot materialise a dispatch pool — when `db` is a Pool/Queryable, `dispatchDb` must also be supplied (D-0011 / D-0012)",
        );
      }
    } else if (typeof src === "string") {
      this.dispatchPool = new pg.Pool({ connectionString: src });
      this.dispatchOwned = true;
    } else {
      this.dispatchPool = src;
      this.dispatchOwned = false;
    }

    this.dispatchClient_ = new Client(this.dispatchPool as Queryable);
    return this.dispatchClient_;
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
