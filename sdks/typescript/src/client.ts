/**
 * Layer 0: thin procedure wrappers around `instructed.*` stored procedures.
 *
 * One method per procedure; argument shapes match the SQL contract verbatim;
 * results are typed rows. No retry, no SDK-opened transactions, no
 * cached state. The only translation is SQLSTATE → typed Error subclass.
 *
 * See sql/instructed.sql for the spec.
 */

import type * as pg from "pg";
import {
  mapPgError,
  SnapshotNotFound,
  type MapPgErrorContext,
} from "./errors.ts";
import type {
  AppendOptions,
  AppendedEvent,
  ClaimResult,
  ClaimSubscriptionOptions,
  ExpectedVersion,
  NewEvent,
  Queryable,
  RecordedEvent,
  Snapshot,
  SnapshotInput,
  SubscriptionShardOption,
} from "./types.ts";

export interface ClientOptions {
  /** Reserved. */
}

function isQueryable(value: unknown): value is Queryable {
  return (
    typeof value === "object" &&
    value !== null &&
    typeof (value as Queryable).query === "function"
  );
}

/** Some Postgres ints (int8) come back as strings. Coerce to bigint. */
function toBigInt(v: unknown): bigint {
  if (typeof v === "bigint") return v;
  if (typeof v === "number") return BigInt(v);
  if (typeof v === "string") return BigInt(v);
  throw new Error(`expected bigint-like, got ${typeof v}: ${String(v)}`);
}

function toDate(v: unknown): Date {
  if (v instanceof Date) return v;
  if (typeof v === "string") return new Date(v);
  throw new Error(`expected Date, got ${typeof v}`);
}

function packEvent<E>(e: NewEvent<E>): Record<string, unknown> {
  const out: Record<string, unknown> = {
    event_id: e.event_id,
    event_type: e.event_type,
    data: e.data ?? null,
  };
  if (e.metadata !== undefined) out.metadata = e.metadata;
  if (e.causation_id !== undefined) out.causation_id = e.causation_id;
  if (e.correlation_id !== undefined) out.correlation_id = e.correlation_id;
  return out;
}

function expectedVersionParams(
  ev: ExpectedVersion,
): { type: string; version: string | null } {
  switch (ev.kind) {
    case "any":
      return { type: "any", version: null };
    case "noStream":
      return { type: "no_stream", version: null };
    case "streamExists":
      return { type: "stream_exists", version: null };
    case "exact":
      return { type: "exact", version: ev.version.toString() };
  }
}

interface RawEventRow {
  event_id: string;
  event_number: string | number;
  stream_uuid: string;
  stream_version: string | number;
  event_type: string;
  causation_id: string | null;
  correlation_id: string | null;
  data: unknown;
  metadata: unknown;
  created_at: Date | string;
}

function mapRecordedEvent<E>(row: RawEventRow): RecordedEvent<E> {
  return {
    event_id: row.event_id,
    event_number: toBigInt(row.event_number),
    stream_uuid: row.stream_uuid,
    stream_version: toBigInt(row.stream_version),
    event_type: row.event_type,
    causation_id: row.causation_id,
    correlation_id: row.correlation_id,
    data: row.data as E,
    metadata: row.metadata,
    created_at: toDate(row.created_at),
  };
}

/** Layer 0: a thin wrapper over the Postgres procedures. */
export class Client {
  readonly con: Queryable;

  constructor(con: Queryable | pg.Pool, _opts: ClientOptions = {}) {
    if (!isQueryable(con)) {
      throw new TypeError("Client requires a pg.Pool, pg.Client, or Queryable");
    }
    this.con = con;
  }

  /** Run a single query against the underlying connection. */
  private async run<T extends pg.QueryResultRow = any>(
    sql: string,
    params: unknown[],
    ctx: MapPgErrorContext = {},
  ): Promise<pg.QueryResult<T>> {
    try {
      return await this.con.query<T>(sql, params as any[]);
    } catch (err) {
      throw mapPgError(err, ctx);
    }
  }

  // ---- events ----

  async appendToStream<E = unknown>(
    streamUuid: string,
    expected: ExpectedVersion,
    events: NewEvent<E>[],
    _options: AppendOptions = {},
  ): Promise<AppendedEvent[]> {
    if (!Array.isArray(events) || events.length === 0) {
      // Mirror the SQL contract: empty array is invalid_parameter_value
      // (22023). The procedure raises it; we let it through. But we'd
      // rather raise client-side for a marginally better stack.
      throw new (await import("./errors.ts")).InvalidParameterValue(
        "appendToStream: events must be a non-empty array",
        { code: "22023" },
      );
    }
    const ev = expectedVersionParams(expected);
    // The SDK fills event_id on omission per §11.2. We also fill it
    // here as the lowest layer so direct Client users get the same
    // convenience as runCommand users.
    const filled = events.map((e) => ({
      ...packEvent(e),
      event_id: e.event_id ?? globalThis.crypto.randomUUID(),
    }));
    const res = await this.run<{
      event_id: string;
      stream_version: string | number;
      event_number: string | number;
      created_at: Date | string;
    }>(
      `SELECT event_id, stream_version, event_number, created_at
         FROM instructed.append_to_stream($1, $2, $3, $4::jsonb, $5::jsonb)`,
      [
        streamUuid,
        ev.type,
        ev.version,
        JSON.stringify(filled),
        JSON.stringify({}),
      ],
      { streamUuid },
    );
    return res.rows.map((r) => ({
      event_id: r.event_id,
      stream_version: toBigInt(r.stream_version),
      event_number: toBigInt(r.event_number),
      created_at: toDate(r.created_at),
    }));
  }

  async readStream<E = unknown>(
    streamUuid: string,
    fromVersion: bigint,
    qty: number,
  ): Promise<RecordedEvent<E>[]> {
    const res = await this.run<RawEventRow>(
      `SELECT event_id, event_number, stream_uuid, stream_version,
              event_type, causation_id, correlation_id, data, metadata, created_at
         FROM instructed.read_stream($1, $2, $3, $4::jsonb)`,
      [streamUuid, fromVersion.toString(), qty, JSON.stringify({})],
      { streamUuid },
    );
    return res.rows.map((r) => mapRecordedEvent<E>(r));
  }

  async readAll<E = unknown>(
    fromEventNumber: bigint,
    qty: number,
  ): Promise<RecordedEvent<E>[]> {
    const res = await this.run<RawEventRow>(
      `SELECT event_id, event_number, stream_uuid, stream_version,
              event_type, causation_id, correlation_id, data, metadata, created_at
         FROM instructed.read_all($1, $2, $3::jsonb)`,
      [fromEventNumber.toString(), qty, JSON.stringify({})],
    );
    return res.rows.map((r) => mapRecordedEvent<E>(r));
  }

  // ---- snapshots ----

  async recordSnapshot<S = unknown>(snap: SnapshotInput<S>): Promise<void> {
    await this.run(
      `SELECT instructed.record_snapshot($1, $2, $3, $4::jsonb, $5::jsonb, $6::jsonb)`,
      [
        snap.sourceUuid,
        snap.sourceType,
        snap.sourceVersion.toString(),
        JSON.stringify(snap.data),
        snap.metadata === undefined ? null : JSON.stringify(snap.metadata),
        JSON.stringify({}),
      ],
      { sourceUuid: snap.sourceUuid },
    );
  }

  /** Throws SnapshotNotFound (IS010) if no snapshot exists. */
  async readSnapshot<S = unknown>(sourceUuid: string): Promise<Snapshot<S>> {
    const res = await this.run<{
      source_uuid: string;
      source_type: string;
      source_version: string | number;
      data: unknown;
      metadata: unknown;
      created_at: Date | string;
    }>(
      `SELECT source_uuid, source_type, source_version, data, metadata, created_at
         FROM instructed.read_snapshot($1, $2::jsonb)`,
      [sourceUuid, JSON.stringify({})],
      { sourceUuid },
    );
    // The SQL function raises IS010 when missing, so res.rows.length is
    // never 0 here; guard for safety.
    if (res.rows.length === 0) {
      throw new SnapshotNotFound(`snapshot ${sourceUuid} not found`, {
        code: "IS010",
        sourceUuid,
      });
    }
    const r = res.rows[0];
    return {
      sourceUuid: r.source_uuid,
      sourceType: r.source_type,
      sourceVersion: toBigInt(r.source_version),
      data: r.data as S,
      metadata: r.metadata,
      createdAt: toDate(r.created_at),
    };
  }

  /** Idempotent: deleting a missing snapshot succeeds silently (INV-SNAP-004). */
  async deleteSnapshot(sourceUuid: string): Promise<void> {
    await this.run(
      `SELECT instructed.delete_snapshot($1, $2::jsonb)`,
      [sourceUuid, JSON.stringify({})],
      { sourceUuid },
    );
  }

  // ---- subscriptions ----

  async claimSubscription(
    streamUuid: string,
    subscriptionName: string,
    workerId: string,
    leaseSeconds: number,
    options: ClaimSubscriptionOptions = {},
  ): Promise<ClaimResult> {
    const opts: Record<string, unknown> = {};
    if (options.startFrom !== undefined) {
      opts.start_from =
        typeof options.startFrom === "bigint"
          ? options.startFrom.toString()
          : typeof options.startFrom === "number"
            ? String(options.startFrom)
            : options.startFrom;
    }
    if (options.shard !== undefined) opts.shard = options.shard;
    const res = await this.run<{
      result: "claimed" | "already_claimed";
      last_seen: string | number;
      claimed_by: string;
      claim_expires_at: Date | string;
    }>(
      `SELECT result, last_seen, claimed_by, claim_expires_at
         FROM instructed.claim_subscription($1, $2, $3, $4, $5::jsonb)`,
      [
        streamUuid,
        subscriptionName,
        workerId,
        leaseSeconds,
        JSON.stringify(opts),
      ],
      {
        streamUuid,
        subscriptionName,
        shard: options.shard,
      },
    );
    const r = res.rows[0];
    return {
      result: r.result,
      lastSeen: toBigInt(r.last_seen),
      claimedBy: r.claimed_by,
      claimExpiresAt: toDate(r.claim_expires_at),
    };
  }

  async extendSubscriptionClaim(
    streamUuid: string,
    subscriptionName: string,
    workerId: string,
    leaseSeconds: number,
    options: SubscriptionShardOption = {},
  ): Promise<{ claimExpiresAt: Date }> {
    const opts: Record<string, unknown> = {};
    if (options.shard !== undefined) opts.shard = options.shard;
    const res = await this.run<{ claim_expires_at: Date | string }>(
      `SELECT claim_expires_at
         FROM instructed.extend_subscription_claim($1, $2, $3, $4, $5::jsonb)`,
      [
        streamUuid,
        subscriptionName,
        workerId,
        leaseSeconds,
        JSON.stringify(opts),
      ],
      { streamUuid, subscriptionName, shard: options.shard },
    );
    return { claimExpiresAt: toDate(res.rows[0].claim_expires_at) };
  }

  async releaseSubscription(
    streamUuid: string,
    subscriptionName: string,
    workerId: string,
    options: SubscriptionShardOption = {},
  ): Promise<void> {
    const opts: Record<string, unknown> = {};
    if (options.shard !== undefined) opts.shard = options.shard;
    await this.run(
      `SELECT instructed.release_subscription($1, $2, $3, $4::jsonb)`,
      [streamUuid, subscriptionName, workerId, JSON.stringify(opts)],
      { streamUuid, subscriptionName, shard: options.shard },
    );
  }

  async readSubscriptionBatch<E = unknown>(
    streamUuid: string,
    subscriptionName: string,
    workerId: string,
    qty: number,
    options: SubscriptionShardOption = {},
  ): Promise<RecordedEvent<E>[]> {
    const opts: Record<string, unknown> = {};
    if (options.shard !== undefined) opts.shard = options.shard;
    const res = await this.run<RawEventRow>(
      `SELECT event_id, event_number, stream_uuid, stream_version,
              event_type, causation_id, correlation_id, data, metadata, created_at
         FROM instructed.read_subscription_batch($1, $2, $3, $4, $5::jsonb)`,
      [
        streamUuid,
        subscriptionName,
        workerId,
        qty,
        JSON.stringify(opts),
      ],
      { streamUuid, subscriptionName, shard: options.shard },
    );
    return res.rows.map((r) => mapRecordedEvent<E>(r));
  }

  async advanceSubscription(
    streamUuid: string,
    subscriptionName: string,
    workerId: string,
    upToPosition: bigint,
    options: SubscriptionShardOption = {},
  ): Promise<{ lastSeen: bigint }> {
    const opts: Record<string, unknown> = {};
    if (options.shard !== undefined) opts.shard = options.shard;
    const res = await this.run<{ last_seen: string | number }>(
      `SELECT last_seen
         FROM instructed.advance_subscription($1, $2, $3, $4, $5::jsonb)`,
      [
        streamUuid,
        subscriptionName,
        workerId,
        upToPosition.toString(),
        JSON.stringify(opts),
      ],
      { streamUuid, subscriptionName, shard: options.shard },
    );
    return { lastSeen: toBigInt(res.rows[0].last_seen) };
  }

  async readSubscriptionPosition(
    streamUuid: string,
    subscriptionName: string,
    options: SubscriptionShardOption = {},
  ): Promise<{ lastSeen: bigint }> {
    const opts: Record<string, unknown> = {};
    if (options.shard !== undefined) opts.shard = options.shard;
    const res = await this.run<{ last_seen: string | number }>(
      `SELECT last_seen
         FROM instructed.read_subscription_position($1, $2, $3::jsonb)`,
      [streamUuid, subscriptionName, JSON.stringify(opts)],
      { streamUuid, subscriptionName, shard: options.shard },
    );
    return { lastSeen: toBigInt(res.rows[0].last_seen) };
  }

  async deleteSubscription(
    streamUuid: string,
    subscriptionName: string,
    options: SubscriptionShardOption = {},
  ): Promise<void> {
    const opts: Record<string, unknown> = {};
    if (options.shard !== undefined) opts.shard = options.shard;
    await this.run(
      `SELECT instructed.delete_subscription($1, $2, $3::jsonb)`,
      [streamUuid, subscriptionName, JSON.stringify(opts)],
      { streamUuid, subscriptionName, shard: options.shard },
    );
  }
}
