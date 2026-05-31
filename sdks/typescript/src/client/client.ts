/**
 * Layer 0: thin procedure wrappers around `instructed.*` stored procedures.
 *
 * One method per procedure; argument shapes match the SQL contract verbatim;
 * results are typed rows. No retry, no SDK-opened transactions, no
 * cached state. The only translation is SQLSTATE → typed Error subclass.
 *
 * See sql/instructed.sql for the spec.
 */

import type * as pg from 'pg'

import { mapPgError, SnapshotNotFound, type MapPgErrorContext } from '../errors/index.ts'
import type {
  Event,
  AppendOptions,
  AppendedEvent,
  ClaimResult,
  ClaimSubscriptionOptions,
  ClaimedWorkItem,
  CompletePmInstanceResult,
  ExpectedVersion,
  NewEvent,
  Queryable,
  RecordedEvent,
  RouteBatchResult,
  RouteDecision,
  Snapshot,
  SnapshotInput,
} from '../types/index.ts'
import { packEvent, expectedVersionParams } from './pack-event.ts'
import {
  toBigInt,
  toDate,
  mapRecordedEvent,
  READ_EVENT_COLUMNS,
  type RawEventRow,
} from './row-mappers.ts'

export interface ClientOptions {
  /** Reserved. */
}

function isQueryable(value: unknown): value is Queryable {
  return (
    // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- structural type-guard probe on an unknown value.
    typeof value === 'object' && value !== null && typeof (value as Queryable).query === 'function'
  )
}

/** Layer 0: a thin wrapper over the Postgres procedures. */
export class Client {
  readonly con: Queryable

  constructor(con: Queryable | pg.Pool, _opts: ClientOptions = {}) {
    if (!isQueryable(con)) {
      throw new TypeError('Client requires a pg.Pool, pg.Client, or Queryable')
    }
    this.con = con
  }

  /** Run a single query against the underlying connection. */
  private async run<T extends pg.QueryResultRow = pg.QueryResultRow>(
    sql: string,
    params: unknown[],
    ctx: MapPgErrorContext = {},
  ): Promise<pg.QueryResult<T>> {
    try {
      return await this.con.query<T>(sql, params)
    } catch (err) {
      throw mapPgError(err, ctx)
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
      throw new (await import('../errors/index.ts')).InvalidParameterValue(
        'appendToStream: events must be a non-empty array',
        { code: '22023' },
      )
    }
    const ev = expectedVersionParams(expected)
    // The SDK fills event_id on omission per §11.2. We also fill it
    // here as the lowest layer so direct Client users get the same
    // convenience as runCommand users.
    const filled = events.map((e) => ({
      ...packEvent(e),
      event_id: e.event_id ?? globalThis.crypto.randomUUID(),
    }))
    const res = await this.run<{
      event_id: string
      stream_version: string | number
      event_number: string | number
      created_at: Date | string
    }>(
      `SELECT event_id, stream_version, event_number, created_at
         FROM instructed.append_to_stream($1, $2, $3, $4::jsonb, $5::jsonb)`,
      [streamUuid, ev.type, ev.version, JSON.stringify(filled), JSON.stringify({})],
      { streamUuid },
    )
    return res.rows.map((r) => ({
      event_id: r.event_id,
      stream_uuid: streamUuid,
      stream_version: toBigInt(r.stream_version),
      event_number: toBigInt(r.event_number),
      created_at: toDate(r.created_at),
    }))
  }

  async readStream<E extends Event = Event>(
    streamUuid: string,
    fromVersion: bigint,
    qty: number,
  ): Promise<RecordedEvent<E>[]> {
    const res = await this.run<RawEventRow>(
      `SELECT ${READ_EVENT_COLUMNS}
         FROM instructed.read_stream($1, $2, $3, $4::jsonb)`,
      [streamUuid, fromVersion.toString(), qty, JSON.stringify({})],
      { streamUuid },
    )
    return res.rows.map((r) => mapRecordedEvent<E>(r))
  }

  async readAll<E extends Event = Event>(
    fromEventNumber: bigint,
    qty: number,
  ): Promise<RecordedEvent<E>[]> {
    const res = await this.run<RawEventRow>(
      `SELECT ${READ_EVENT_COLUMNS}
         FROM instructed.read_all($1, $2, $3::jsonb)`,
      [fromEventNumber.toString(), qty, JSON.stringify({})],
    )
    return res.rows.map((r) => mapRecordedEvent<E>(r))
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
    )
  }

  /** Throws SnapshotNotFound (IS010) if no snapshot exists. */
  async readSnapshot<S = unknown>(sourceUuid: string): Promise<Snapshot<S>> {
    const res = await this.run<{
      source_uuid: string
      source_type: string
      source_version: string | number
      data: unknown
      metadata: unknown
      created_at: Date | string
    }>(
      `SELECT source_uuid, source_type, source_version, data, metadata, created_at
         FROM instructed.read_snapshot($1, $2::jsonb)`,
      [sourceUuid, JSON.stringify({})],
      { sourceUuid },
    )
    // The SQL function raises IS010 when missing, so res.rows.length is
    // never 0 here; guard for safety.
    if (res.rows.length === 0) {
      throw new SnapshotNotFound(`snapshot ${sourceUuid} not found`, {
        code: 'IS010',
        sourceUuid,
      })
    }
    const r = res.rows[0]
    return {
      sourceUuid: r.source_uuid,
      sourceType: r.source_type,
      sourceVersion: toBigInt(r.source_version),
      // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- DB-row boundary: snapshot data column asserted to the caller's state type S.
      data: r.data as S,
      metadata: r.metadata,
      createdAt: toDate(r.created_at),
    }
  }

  /** Idempotent: deleting a missing snapshot succeeds silently (INV-SNAP-004). */
  async deleteSnapshot(sourceUuid: string): Promise<void> {
    await this.run(
      `SELECT instructed.delete_snapshot($1, $2::jsonb)`,
      [sourceUuid, JSON.stringify({})],
      { sourceUuid },
    )
  }

  // ---- subscriptions ----

  async claimSubscription(
    streamUuid: string,
    subscriptionName: string,
    workerId: string,
    leaseSeconds: number,
    options: ClaimSubscriptionOptions = {},
  ): Promise<ClaimResult> {
    const opts: Record<string, unknown> = {}
    if (options.startFrom !== undefined) {
      opts.start_from =
        typeof options.startFrom === 'bigint'
          ? options.startFrom.toString()
          : typeof options.startFrom === 'number'
            ? String(options.startFrom)
            : options.startFrom
    }
    const res = await this.run<{
      result: 'claimed' | 'already_claimed'
      last_seen: string | number
      claimed_by: string | null
      claim_expires_at: Date | string | null
    }>(
      `SELECT result, last_seen, claimed_by, claim_expires_at
         FROM instructed.claim_subscription($1, $2, $3, $4, $5::jsonb)`,
      [streamUuid, subscriptionName, workerId, leaseSeconds, JSON.stringify(opts)],
      {
        streamUuid,
        subscriptionName,
      },
    )
    const r = res.rows[0]
    // 'already_claimed' under one specific contention race returns
    // (NULL, NULL) for the diagnostic fields -- see the
    // claim_subscription SQL function and ClaimResult docs. Guard the
    // toDate() call so we don't synthesise an `expected Date, got
    // object` error from a legitimate contract outcome.
    if (r.result === 'claimed') {
      return {
        result: 'claimed',
        lastSeen: toBigInt(r.last_seen),
        // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- DB-row boundary: claimed_by is non-null on the 'claimed' branch.
        claimedBy: r.claimed_by as string,
        // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- DB-row boundary: claim_expires_at column shape from pg.
        claimExpiresAt: toDate(r.claim_expires_at as Date | string),
      }
    }
    return {
      result: 'already_claimed',
      lastSeen: toBigInt(r.last_seen),
      claimedBy: r.claimed_by,
      claimExpiresAt: r.claim_expires_at === null ? null : toDate(r.claim_expires_at),
    }
  }

  async releaseSubscription(
    streamUuid: string,
    subscriptionName: string,
    workerId: string,
  ): Promise<void> {
    const opts: Record<string, unknown> = {}
    await this.run(
      `SELECT instructed.release_subscription($1, $2, $3, $4::jsonb)`,
      [streamUuid, subscriptionName, workerId, JSON.stringify(opts)],
      { streamUuid, subscriptionName },
    )
  }

  async deleteSubscription(streamUuid: string, subscriptionName: string): Promise<void> {
    const opts: Record<string, unknown> = {}
    await this.run(
      `SELECT instructed.delete_subscription($1, $2, $3::jsonb)`,
      [streamUuid, subscriptionName, JSON.stringify(opts)],
      { streamUuid, subscriptionName },
    )
  }

  // ---- SUB-A work queue ----

  /**
   * Atomically advance the routing cursor to `newCursor` and insert one
   * work item per decision. Re-running with the same decisions hits
   * `ON CONFLICT DO NOTHING` on the work-items PK and is therefore
   * crash-replay safe. The caller MUST hold the subscription's lease;
   * lease loss raises `SubscriptionLeaseLost` (IS022).
   *
   * The cursor advance is monotone (`greatest(last_seen, newCursor)`).
   */
  async routeBatch(
    streamUuid: string,
    subscriptionName: string,
    workerId: string,
    newCursor: bigint,
    decisions: RouteDecision[],
  ): Promise<RouteBatchResult> {
    const opts: Record<string, unknown> = {}
    const payload = decisions.map((d) => ({
      partition_key: d.partitionKey,
      // event_number must be a JSON number (the SQL contract requires
      // jsonb_typeof = 'number'). bigint -> Number is safe in v1: the
      // store uses bigint event_numbers but no realistic deployment
      // exceeds 2^53 in v1. If that ever becomes a real concern the
      // SQL contract can be widened to accept a string; documented as
      // a known gap here.
      event_number: Number(d.eventNumber),
    }))
    const res = await this.run<{
      inserted_count: string | number
      new_last_seen: string | number
    }>(
      `SELECT inserted_count, new_last_seen
         FROM instructed.route_batch($1, $2, $3, $4, $5::jsonb, $6::jsonb)`,
      [
        streamUuid,
        subscriptionName,
        workerId,
        newCursor.toString(),
        JSON.stringify(payload),
        JSON.stringify(opts),
      ],
      { streamUuid, subscriptionName },
    )
    const r = res.rows[0]
    return {
      insertedCount: toBigInt(r.inserted_count),
      newLastSeen: toBigInt(r.new_last_seen),
    }
  }

  /**
   * Claim the next available work item for `subscriptionName`, enforcing
   * per-partition ordering (no row is claimable while an earlier row for
   * the same partition is still non-terminal). Includes the
   * lease-takeover branch: a `claimed` row with `lease_expires_at <
   * now()` is eligible.
   *
   * Returns `null` when the queue is empty (the SDK is expected to poll).
   */
  async claimWorkItem(
    streamUuid: string,
    subscriptionName: string,
    workerId: string,
    leaseSeconds: number,
  ): Promise<ClaimedWorkItem | null> {
    const opts: Record<string, unknown> = {}
    const res = await this.run<{
      partition_key: string
      event_number: string | number
      claimed_by: string
      lease_expires_at: Date | string
      was_takeover: boolean
      prior_claimed_by: string | null
    }>(
      `SELECT partition_key, event_number, claimed_by, lease_expires_at,
              was_takeover, prior_claimed_by
         FROM instructed.claim_work_item($1, $2, $3, $4, $5::jsonb)`,
      [streamUuid, subscriptionName, workerId, leaseSeconds, JSON.stringify(opts)],
      { streamUuid, subscriptionName },
    )
    if (res.rows.length === 0) return null
    const r = res.rows[0]
    return {
      partitionKey: r.partition_key,
      eventNumber: toBigInt(r.event_number),
      claimedBy: r.claimed_by,
      leaseExpiresAt: toDate(r.lease_expires_at),
      wasTakeover: r.was_takeover,
      priorClaimedBy: r.prior_claimed_by,
    }
  }

  /**
   * Heartbeat for a work-item claim. Extends `lease_expires_at` to
   * `now() + leaseSeconds`. Raises `WorkItemLeaseLost` (IS030) if the
   * row is gone, no longer in `'claimed'` state, or claimed by a
   * different worker -- in any of those cases the SDK must stop
   * processing the item.
   */
  async extendWorkItemClaim(
    streamUuid: string,
    subscriptionName: string,
    workerId: string,
    partitionKey: string,
    eventNumber: bigint,
    leaseSeconds: number,
  ): Promise<{ leaseExpiresAt: Date }> {
    const opts: Record<string, unknown> = {}
    const res = await this.run<{ lease_expires_at: Date | string }>(
      `SELECT lease_expires_at
         FROM instructed.extend_work_item_claim($1, $2, $3, $4, $5, $6, $7::jsonb)`,
      [
        streamUuid,
        subscriptionName,
        workerId,
        partitionKey,
        eventNumber.toString(),
        leaseSeconds,
        JSON.stringify(opts),
      ],
      {
        streamUuid,
        subscriptionName,
        partitionKey,
        eventNumber,
      },
    )
    return { leaseExpiresAt: toDate(res.rows[0].lease_expires_at) }
  }

  /**
   * Projection terminal success (PRJ-E): DELETE the work item. Called by
   * the SDK in its own short tx *after* the handler returns; the handler
   * is opaque to the SDK (D-0016 in `docs/decisions.md`) and may target
   * any store. No read-model locks are taken here.
   *
   * Raises `WorkItemLeaseLost` (IS030) if the caller is not (or no
   * longer) the row's claimant.
   */
  async completeWorkItemProjection(
    streamUuid: string,
    subscriptionName: string,
    workerId: string,
    partitionKey: string,
    eventNumber: bigint,
  ): Promise<void> {
    const opts: Record<string, unknown> = {}
    await this.run(
      `SELECT instructed.complete_work_item_projection($1, $2, $3, $4, $5, $6::jsonb)`,
      [
        streamUuid,
        subscriptionName,
        workerId,
        partitionKey,
        eventNumber.toString(),
        JSON.stringify(opts),
      ],
      {
        streamUuid,
        subscriptionName,
        partitionKey,
        eventNumber,
      },
    )
  }

  /**
   * PM non-terminal success (PM-C / PM-F): UPDATE the work item to `done`
   * AND UPSERT the PM's snapshot, in one transaction.
   *
   * Raises `WorkItemLeaseLost` (IS030) if the caller is not the row's
   * claimant.
   */
  async completeWorkItemPm<S = unknown>(
    streamUuid: string,
    subscriptionName: string,
    workerId: string,
    partitionKey: string,
    eventNumber: bigint,
    snapshot: SnapshotInput<S>,
  ): Promise<void> {
    const opts: Record<string, unknown> = {}
    await this.run(
      `SELECT instructed.complete_work_item_pm(
         $1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb, $11::jsonb)`,
      [
        streamUuid,
        subscriptionName,
        workerId,
        partitionKey,
        eventNumber.toString(),
        snapshot.sourceUuid,
        snapshot.sourceType,
        snapshot.sourceVersion.toString(),
        JSON.stringify(snapshot.data),
        snapshot.metadata === undefined ? null : JSON.stringify(snapshot.metadata),
        JSON.stringify(opts),
      ],
      {
        streamUuid,
        subscriptionName,
        partitionKey,
        eventNumber,
        sourceUuid: snapshot.sourceUuid,
      },
    )
  }

  /**
   * PM terminal success (`handle` returned `{ complete: true }`, PM-F):
   * DELETE the snapshot AND every work item for the partition in one
   * transaction. Idempotent: a second call returns zero counts and does
   * not raise.
   */
  async completePmInstance(
    streamUuid: string,
    subscriptionName: string,
    partitionKey: string,
    snapshotUuid: string,
  ): Promise<CompletePmInstanceResult> {
    const opts: Record<string, unknown> = {}
    const res = await this.run<{
      work_items_deleted: string | number
      snapshot_deleted: boolean
    }>(
      `SELECT work_items_deleted, snapshot_deleted
         FROM instructed.complete_pm_instance($1, $2, $3, $4, $5::jsonb)`,
      [streamUuid, subscriptionName, partitionKey, snapshotUuid, JSON.stringify(opts)],
      {
        streamUuid,
        subscriptionName,
        partitionKey,
        sourceUuid: snapshotUuid,
      },
    )
    const r = res.rows[0]
    return {
      workItemsDeleted: toBigInt(r.work_items_deleted),
      snapshotDeleted: r.snapshot_deleted,
    }
  }

  /**
   * Move a claimed work item to `failed`. The row blocks subsequent
   * work items for its partition only; other partitions are unaffected.
   * `failed` rows are never auto-skipped or auto-deleted; operator action
   * (deferred to `instructedctl`) is required to clear them.
   *
   * Raises `WorkItemLeaseLost` (IS030) if the caller is not the claimant.
   */
  async failWorkItem(
    streamUuid: string,
    subscriptionName: string,
    workerId: string,
    partitionKey: string,
    eventNumber: bigint,
    errorText: string | null,
  ): Promise<void> {
    const opts: Record<string, unknown> = {}
    await this.run(
      `SELECT instructed.fail_work_item($1, $2, $3, $4, $5, $6, $7::jsonb)`,
      [
        streamUuid,
        subscriptionName,
        workerId,
        partitionKey,
        eventNumber.toString(),
        errorText,
        JSON.stringify(opts),
      ],
      {
        streamUuid,
        subscriptionName,
        partitionKey,
        eventNumber,
      },
    )
  }

  /**
   * The SUB-A catch-up predicate: returns true iff the routing cursor is
   * at or past `target` AND no work item for the subscription with
   * `event_number <= target` is still in a non-terminal state. Used by
   * `waitForProjection` (slice 8).
   */
  async isSubscriptionCaughtUp(
    streamUuid: string,
    subscriptionName: string,
    target: bigint,
  ): Promise<boolean> {
    const opts: Record<string, unknown> = {}
    const res = await this.run<{ caught_up: boolean }>(
      `SELECT caught_up
         FROM instructed.is_subscription_caught_up($1, $2, $3, $4::jsonb)`,
      [streamUuid, subscriptionName, target.toString(), JSON.stringify(opts)],
      { streamUuid, subscriptionName },
    )
    return res.rows[0].caught_up
  }

  /**
   * Cold-path read for PM state rebuild (SUB-A slice 7 / PM-C). Returns
   * every `done` work-item for `(subscriptionName, partitionKey)` with
   * `event_number < exclusiveUpperBound`, in event-number order, in the
   * `read_all`-compatible recorded-event shape. The SDK uses this when
   * a PM partition's snapshot is missing (IS010) or carries a
   * `$instructed.snapshot_module_version` (in metadata) that no longer
   * matches the SDK's compiled-in version, to fold prior events through the PM's
   * `apply` callback from `initialState()`.
   *
   * Raises `SubscriptionNotFound` (IS020) if the subscription does not
   * exist.
   */
  async listPmRebuildEvents<E extends Event = Event>(
    streamUuid: string,
    subscriptionName: string,
    partitionKey: string,
    exclusiveUpperBound: bigint,
  ): Promise<RecordedEvent<E>[]> {
    const opts: Record<string, unknown> = {}
    const res = await this.run<RawEventRow>(
      `SELECT ${READ_EVENT_COLUMNS}
         FROM instructed.list_pm_rebuild_events($1, $2, $3, $4, $5::jsonb)`,
      [
        streamUuid,
        subscriptionName,
        partitionKey,
        exclusiveUpperBound.toString(),
        JSON.stringify(opts),
      ],
      {
        streamUuid,
        subscriptionName,
        partitionKey,
      },
    )
    return res.rows.map((r) => mapRecordedEvent<E>(r))
  }
}
