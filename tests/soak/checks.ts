/**
 * Invariant checks.
 *
 * Two surfaces: a **continuous sampler** that runs while the harness
 * is live (catches transient violations a final scan would miss), and
 * a **final report** that runs after the harness drains.
 *
 * Each check carries an invariant ID from `docs/invariants.md`
 * wherever one exists, so a soak failure points straight at the
 * contract clause that's broken.
 *
 * Continuous samples:
 *   - per-subscription `last_seen` monotonicity
 *     (INV-SUB-P-008 / advance-monotonicity)
 *   - subscription-level lease uniqueness: at most one *unexpired*
 *     claim per (stream, name) on `subscriptions` -- the SQL
 *     contract enforces this via a partial unique index; the
 *     sampler verifies it holds at every instant. Under SUB-A this
 *     covers the routing-worker lease only; processing workers do
 *     not take subscription leases.
 *   - work-item-level lease uniqueness (SUB-A): at most one
 *     processing worker should own an unexpired lease on any given
 *     `(subscription, partition_key, event_number)` work item.
 *     Tagged `INV-SUB-W-LEASE-UNIQ`; pending the `INV-SUB-*` triage
 *     in docs/invariants.md slice 12.
 *
 * Final checks:
 *   - `$all` event_number gapless 1..head (INV-APPEND-003)
 *   - per-stream stream_version gapless 1..head (INV-APPEND-022)
 *   - subscription cursors never advanced past head, never went
 *     backwards in the recorded sample stream (INV-SUB-P-008)
 *   - no `failed` work items remain (SUB-A: `failed` rows require
 *     operator action; the soak workload never injects poison
 *     events, so any `failed` row is an SDK bug). Tagged
 *     `INV-SUB-W-NO-FAILED`.
 *   - every Forwarder PM snapshot satisfies
 *     `source_version <= last_seen` (PM-024)
 *   - aggregate re-folds: balance per account == projector's
 *     view (cross-check that no event was silently dropped from
 *     either path)
 *   - PM forwarded count == total triggers (no triggers dropped)
 */

import type pg from 'pg'

import type { Client } from '../../sdks/typescript/src/index.ts'
import { type RecordedEvent } from '../../sdks/typescript/src/index.ts'

export interface InvariantViolation {
  /** Invariant ID where one exists; otherwise a descriptive tag. */
  code: string
  message: string
}

export interface CheckContext {
  pool: pg.Pool
  client: Client
  /** All subscription (stream, name) pairs the harness is using. */
  subscriptions: Array<{ stream: string; name: string }>
  /** All account stream UUIDs. */
  accounts: string[]
  /** Snapshot UUID prefix used by the Forwarder PM (`{pmName}-{processId}`). */
  forwarderName: string
}

// ---------------------------------------------------------------------------
// Continuous sampler
// ---------------------------------------------------------------------------

/**
 * Per-subscription last_seen history. Updated by `sampleOnce`,
 * inspected by `finalReport` for non-monotonicity. Public so the
 * harness can dump it to disk for post-mortem.
 */
export interface SamplerState {
  lastSeenByKey: Map<string, bigint[]>
  violations: InvariantViolation[]
  samples: number
}

export function newSamplerState(): SamplerState {
  return { lastSeenByKey: new Map(), violations: [], samples: 0 }
}

function subKey(stream: string, name: string): string {
  return `${stream}::${name}`
}

// Read a subscription's routing cursor directly. Replaces the removed
// `read_subscription_position` procedure (A2). Returns null when the
// subscription row does not exist yet (first claim hasn't happened).
async function readCursor(pool: pg.Pool, stream: string, name: string): Promise<bigint | null> {
  const r = await pool.query<{ last_seen: string }>(
    `SELECT s.last_seen::text AS last_seen
       FROM instructed.subscriptions s
       JOIN instructed.streams str ON str.stream_id = s.stream_id
      WHERE str.stream_uuid = $1 AND s.subscription_name = $2`,
    [stream, name],
  )
  return r.rows.length === 0 ? null : BigInt(r.rows[0].last_seen)
}

export async function sampleOnce(ctx: CheckContext, state: SamplerState): Promise<void> {
  state.samples += 1

  // --- last_seen monotonicity per subscription ---------------------------
  for (const sub of ctx.subscriptions) {
    const lastSeenOrNull = await readCursor(ctx.pool, sub.stream, sub.name)
    if (lastSeenOrNull === null) {
      // Subscription may not exist yet (first claim hasn't happened).
      continue
    }
    const lastSeen = lastSeenOrNull
    const key = subKey(sub.stream, sub.name)
    const history = state.lastSeenByKey.get(key) ?? []
    if (history.length > 0) {
      const prev = history[history.length - 1]!
      if (lastSeen < prev) {
        state.violations.push({
          code: 'INV-SUB-P-008',
          message: `last_seen went backwards for ${key}: ${prev} -> ${lastSeen}`,
        })
      }
    }
    history.push(lastSeen)
    state.lastSeenByKey.set(key, history)
  }

  // --- subscription-level lease uniqueness ------------------------------
  // Routing-worker lease lives on `subscriptions.claim_*`. The SQL
  // contract enforces uniqueness via a partial unique index. The
  // sample is a belt-and-braces correctness probe -- if it ever
  // triggers, the index is wrong, not the SDK.
  const dupes = await ctx.pool.query<{
    stream_id: number
    subscription_name: string
    n: string
  }>(
    `SELECT stream_id, subscription_name, count(*)::text AS n
       FROM instructed.subscriptions
      WHERE claim_expires_at IS NOT NULL
        AND claim_expires_at > now()
      GROUP BY stream_id, subscription_name
     HAVING count(*) > 1`,
  )
  for (const r of dupes.rows) {
    state.violations.push({
      code: 'INV-SUB-P-LEASE-UNIQ',
      message:
        `multiple unexpired claims on (stream_id=${r.stream_id}, ` +
        `name=${r.subscription_name}): n=${r.n}`,
    })
  }

  // --- work-item-level lease uniqueness (SUB-A) -------------------------
  // Per-work-item lease lives on `subscription_work_items.claimed_by` +
  // `lease_expires_at`. A row in state='claimed' with an unexpired
  // lease MUST have a unique `claimed_by`; multiple unexpired leases
  // on the same (sub, partition, event_number) would mean two
  // processing workers think they own the same item. The schema
  // models claimed_by as a single text column and the procedure
  // overwrites it on takeover, so by construction this can only
  // misbehave if a procedure update is non-atomic.
  const wiDupes = await ctx.pool.query<{
    subscription_name: string
    partition_key: string
    event_number: string
    n: string
  }>(
    `SELECT subscription_name, partition_key, event_number::text,
            count(*)::text AS n
       FROM instructed.subscription_work_items
      WHERE state = 'claimed'
        AND lease_expires_at IS NOT NULL
        AND lease_expires_at > now()
      GROUP BY subscription_name, partition_key, event_number
     HAVING count(*) > 1`,
  )
  for (const r of wiDupes.rows) {
    state.violations.push({
      code: 'INV-SUB-W-LEASE-UNIQ',
      message:
        `multiple unexpired work-item claims on (sub=${r.subscription_name}, ` +
        `pk=${r.partition_key}, event_number=${r.event_number}): n=${r.n}`,
    })
  }
}

// ---------------------------------------------------------------------------
// Final checks
// ---------------------------------------------------------------------------

export interface FinalReport {
  violations: InvariantViolation[]
  /** Findings that would be violations at quiescence but the harness drained incomplete. */
  inconclusive: InvariantViolation[]
  facts: {
    allHead: bigint
    streamCount: number
    triggersTotal: number
    addedTotal: number
    /**
     * Count of Added events on aggregate streams whose causation_id
     * is a Triggered event — i.e. PM dispatches that actually
     * committed. Compared against triggers_total + forwardedTotal +
     * the SDK-side handleCalls/handleReturns counters by the
     * harness's PM-FORWARD diagnostic block.
     */
    dispatchedViaCausation: number
    /** Sum of `forwarded` across every Forwarder PM snapshot. */
    forwardedTotal: number
    /** projector's running balance per account, if collected by the harness */
    projectorBalances?: Map<string, number>
  }
}

export interface FinalCheckOptions {
  /**
   * When false, the harness skips invariants that only hold at
   * quiescence (PM-FORWARD-TOTAL, REFOLD-MATCH). The harness's
   * report surfaces them under INCONCLUSIVE instead.
   */
  quiesced: boolean
}

export async function runFinalChecks(
  ctx: CheckContext,
  sampler: SamplerState,
  projectorBalances: Map<string, number> | undefined,
  options: FinalCheckOptions = { quiesced: true },
): Promise<FinalReport> {
  const violations: InvariantViolation[] = [...sampler.violations]
  const inconclusive: InvariantViolation[] = []
  const recordQuiescenceFinding = (v: InvariantViolation): void => {
    if (options.quiesced) violations.push(v)
    else inconclusive.push(v)
  }

  // --- $all event_number gapless 1..head --------------------------------
  // The seed row stream_id=0 has stream_version = head. If
  // count(stream_events where stream_id=0) != head, we have a gap.
  const allHeadR = await ctx.pool.query<{ head: string; n: string }>(
    `SELECT
       (SELECT stream_version::text FROM instructed.streams WHERE stream_id = 0) AS head,
       (SELECT count(*)::text FROM instructed.stream_events WHERE stream_id = 0) AS n`,
  )
  const allHead = BigInt(allHeadR.rows[0]!.head)
  const allCount = BigInt(allHeadR.rows[0]!.n)
  if (allCount !== allHead) {
    violations.push({
      code: 'INV-APPEND-003',
      message: `$all has ${allCount} events but head=${allHead} — gap in event_number`,
    })
  } else {
    // Also confirm contiguity of stream_version on stream_id=0.
    const gap = await ctx.pool.query<{ stream_version: string }>(
      `WITH s AS (
         SELECT stream_version,
                row_number() OVER (ORDER BY stream_version) AS rn
           FROM instructed.stream_events
          WHERE stream_id = 0
       )
       SELECT stream_version::text FROM s
        WHERE stream_version <> rn
        LIMIT 1`,
    )
    if (gap.rows.length > 0) {
      violations.push({
        code: 'INV-APPEND-003',
        message: `$all stream_version not contiguous; first gap at ${gap.rows[0]!.stream_version}`,
      })
    }
  }

  // --- per-stream stream_version gapless 1..version ---------------------
  const perStream = await ctx.pool.query<{
    stream_uuid: string
    head: string
    n: string
  }>(
    `SELECT s.stream_uuid,
            s.stream_version::text AS head,
            (SELECT count(*)::text FROM instructed.stream_events se
              WHERE se.stream_id = s.stream_id) AS n
       FROM instructed.streams s
      WHERE s.stream_id <> 0`,
  )
  for (const r of perStream.rows) {
    const head = BigInt(r.head)
    const n = BigInt(r.n)
    if (n !== head) {
      violations.push({
        code: 'INV-APPEND-022',
        message: `stream ${r.stream_uuid}: ${n} rows but version=${head} — gap`,
      })
    }
  }

  // --- no `failed` work items (SUB-A) -----------------------------------
  // `failed` rows are operator-only-resolution per SUB-A; the soak
  // workload doesn't inject poison events, so any failed row is an
  // SDK bug. Surface the count and a sample row.
  const failedR = await ctx.pool.query<{
    subscription_name: string
    partition_key: string
    event_number: string
    error_text: string | null
  }>(
    `SELECT subscription_name, partition_key, event_number::text,
            error_text
       FROM instructed.subscription_work_items
      WHERE state = 'failed'
      LIMIT 5`,
  )
  if (failedR.rowCount && failedR.rowCount > 0) {
    for (const r of failedR.rows) {
      violations.push({
        code: 'INV-SUB-W-NO-FAILED',
        message:
          `failed work item on (sub=${r.subscription_name}, ` +
          `pk=${r.partition_key}, event_number=${r.event_number}): ` +
          `${r.error_text ?? '(no error_text)'}`,
      })
    }
  }

  // --- subscription last_seen never past head ---------------------------
  for (const sub of ctx.subscriptions) {
    const lastSeenOrNull = await readCursor(ctx.pool, sub.stream, sub.name)
    if (lastSeenOrNull === null) {
      continue
    }
    const lastSeen = lastSeenOrNull
    // Per-stream subscription: cursor is a stream_version; per-`$all`
    // subscription: cursor is an event_number. Either way the head
    // is what we look up.
    let head: bigint
    if (sub.stream === '$all') {
      head = allHead
    } else {
      const r = await ctx.pool.query<{ head: string | null }>(
        `SELECT stream_version::text AS head FROM instructed.streams
          WHERE stream_uuid = $1`,
        [sub.stream],
      )
      head = BigInt(r.rows[0]?.head ?? '0')
    }
    if (lastSeen > head) {
      violations.push({
        code: 'INV-SUB-P-008',
        message:
          `subscription (${sub.stream}, ${sub.name}) last_seen=${lastSeen} ` + `> head=${head}`,
      })
    }
  }

  // --- PM snapshots: source_version <= last_seen (PM-024) ---------------
  const pmLastSeen = await readCursor(ctx.pool, '$all', ctx.forwarderName)
  const snaps = await ctx.pool.query<{
    source_uuid: string
    source_version: string
  }>(
    `SELECT source_uuid, source_version::text
       FROM instructed.snapshots
      WHERE source_type = $1`,
    [ctx.forwarderName],
  )
  let forwardedTotalLocal = 0
  for (const r of snaps.rows) {
    const sv = BigInt(r.source_version)
    if (pmLastSeen !== null && sv > pmLastSeen) {
      violations.push({
        code: 'PM-024',
        message: `PM snapshot ${r.source_uuid} source_version=${sv} ` + `> last_seen=${pmLastSeen}`,
      })
    }
    // Folded state is JSON; pull it to total forwarded.
    const dataR = await ctx.pool.query<{ data: { forwarded: number } }>(
      `SELECT data FROM instructed.snapshots WHERE source_uuid = $1`,
      [r.source_uuid],
    )
    forwardedTotalLocal += dataR.rows[0]?.data?.forwarded ?? 0
  }
  const forwardedTotal = forwardedTotalLocal

  // --- triggers vs forwarded --------------------------------------------
  const triggersR = await ctx.pool.query<{ n: string }>(
    `SELECT count(*)::text AS n FROM instructed.events
      WHERE event_type = 'Triggered'`,
  )
  const triggersTotal = Number(triggersR.rows[0]!.n)
  if (forwardedTotal !== triggersTotal) {
    recordQuiescenceFinding({
      code: 'PM-FORWARD-TOTAL',
      message:
        `PM forwarded ${forwardedTotal} triggers but ${triggersTotal} ` +
        `Triggered events exist (expect equality at quiescence)`,
    })
  }

  // --- aggregate re-folds vs projector view -----------------------------
  // The projector kept a running map (if the harness passed one in).
  // For each account, sum Added.n from the stream and compare. The
  // PM dispatches an `add` per trigger, so the re-folded value equals
  // (sum of direct `Added` events) + (sum of forwarded `add{n}` events
  // that landed). Both should match the projector — if not, an event
  // was dropped from one path.
  if (projectorBalances) {
    for (const acct of ctx.accounts) {
      const r = await ctx.pool.query<{ total: string | null }>(
        `SELECT sum((e.data->>'n')::int)::text AS total
           FROM instructed.events e
           JOIN instructed.stream_events se USING (event_id)
           JOIN instructed.streams s USING (stream_id)
          WHERE s.stream_uuid = $1 AND e.event_type = 'Added'`,
        [acct],
      )
      const refolded = Number(r.rows[0]?.total ?? '0')
      const projected = projectorBalances.get(acct) ?? 0
      if (refolded !== projected) {
        recordQuiescenceFinding({
          code: 'REFOLD-MATCH',
          message: `account ${acct}: refold=${refolded} projector=${projected}`,
        })
      }
    }
  }

  const addedR = await ctx.pool.query<{ n: string }>(
    `SELECT count(*)::text AS n FROM instructed.events
      WHERE event_type = 'Added'`,
  )

  // Count of Added events whose causation_id is a Triggered event:
  // the number of PM dispatches that actually landed on an aggregate
  // stream. If this matches `forwardedTotal`, dispatch is consistent
  // with the snapshot count; if it differs, persistence and dispatch
  // disagree about how many forwards happened.
  const dispatchedR = await ctx.pool.query<{ n: string }>(
    `SELECT count(*)::text AS n
       FROM instructed.events a
      WHERE a.event_type = 'Added'
        AND a.causation_id IN (
          SELECT t.event_id FROM instructed.events t
           WHERE t.event_type = 'Triggered'
        )`,
  )

  return {
    violations,
    inconclusive,
    facts: {
      allHead,
      streamCount: perStream.rows.length,
      triggersTotal,
      addedTotal: Number(addedR.rows[0]!.n),
      dispatchedViaCausation: Number(dispatchedR.rows[0]!.n),
      forwardedTotal,
      projectorBalances,
    },
  }
}

/**
 * Convenience: a projection handler that maintains an in-memory
 * `account -> balance` map plus a count of every event seen. The
 * harness wires this into the projector slots so `runFinalChecks`
 * can compare against the re-fold.
 *
 * Note: under at-least-once delivery a handler may run twice on the
 * same event if a worker crashed mid-batch. We track event_ids to
 * de-duplicate; the projector's *durable* cursor is still monotone,
 * but our in-memory map needs the de-dup to match the re-fold.
 */
export function makeBalanceProjector(): {
  balances: Map<string, number>
  seenEventIds: Set<string>
  handle: (e: RecordedEvent) => Promise<void>
} {
  const balances = new Map<string, number>()
  const seenEventIds = new Set<string>()
  return {
    balances,
    seenEventIds,
    async handle(event) {
      if (seenEventIds.has(event.event_id)) return
      seenEventIds.add(event.event_id)
      if (event.type !== 'Added') return
      const n = (event.data as { n: number }).n
      const cur = balances.get(event.stream_uuid) ?? 0
      balances.set(event.stream_uuid, cur + n)
    },
  }
}
