// Core: subscription inspection and lifecycle (release / delete / claim /
// rebuild).

import type { Db } from "./db.ts";
import type { ClaimResult, SubscriptionSummary } from "./types.ts";
import {
  sqlstateOf,
  SubscriptionLeaseLost,
  SubscriptionNotClaimed,
  SubscriptionNotFound,
} from "./errors.ts";

// Worker id stamped on the transient claim a rebuild uses to reset the cursor.
export const REBUILD_WORKER_ID = "instructedctl-rebuild";

// The default stream a subscription rides. SUB-A subscriptions are on `$all`.
const DEFAULT_STREAM = "$all";

export interface SubscriptionRef {
  name: string;
  streamUuid?: string;
}

// Translate the store's subscription SQLSTATEs into typed core errors.
function mapSubscriptionError(
  err: unknown,
  name: string,
  streamUuid: string,
): never {
  const code = sqlstateOf(err);
  if (code === "IS020") throw new SubscriptionNotFound(name, streamUuid);
  if (code === "IS022") throw new SubscriptionLeaseLost(name, streamUuid);
  throw err;
}

interface SubscriptionRow {
  subscription_name: string;
  stream_uuid: string;
  last_seen: string;
  lag: string;
  claimed_by: string | null;
  claim_expires_at: Date | null;
  created_at: Date;
}

function toSummary(r: SubscriptionRow): SubscriptionSummary {
  return {
    subscriptionName: r.subscription_name,
    streamUuid: r.stream_uuid,
    lastSeen: Number(r.last_seen),
    lag: Number(r.lag),
    claimedBy: r.claimed_by,
    claimExpiresAt: r.claim_expires_at,
    createdAt: r.created_at,
  };
}

// `lag` is measured against the head of the subscription's own stream
// (stream_version of the row in `streams` that the subscription is on). For
// the SUB-A subscriptions that ride `$all` (stream_id 0) this is the global
// head; the join keeps it correct for any stream.
const SELECT = `
  select
    s.subscription_name,
    st.stream_uuid,
    s.last_seen,
    (st.stream_version - s.last_seen) as lag,
    s.claimed_by,
    s.claim_expires_at,
    s.created_at
  from instructed.subscriptions s
  join instructed.streams st on st.stream_id = s.stream_id`;

// List every subscription, ordered by name then stream.
export async function listSubscriptions(db: Db): Promise<SubscriptionSummary[]> {
  const rows = await db.query<SubscriptionRow>(
    `${SELECT} order by s.subscription_name, st.stream_uuid`,
  );
  return rows.map(toSummary);
}

// Fetch subscriptions by name. The natural key is (stream_id,
// subscription_name), so a name can in principle exist on more than one
// stream; this returns all matches (usually one).
export async function getSubscription(
  db: Db,
  name: string,
): Promise<SubscriptionSummary[]> {
  const rows = await db.query<SubscriptionRow>(
    `${SELECT} where s.subscription_name = $1 order by st.stream_uuid`,
    [name],
  );
  return rows.map(toSummary);
}

// The current claim holder for one (stream, name), or undefined if the
// subscription does not exist.
async function currentHolder(
  db: Db,
  streamUuid: string,
  name: string,
): Promise<{ claimedBy: string | null } | undefined> {
  const rows = await db.query<{ claimed_by: string | null }>(
    `select s.claimed_by
       from instructed.subscriptions s
       join instructed.streams st on st.stream_id = s.stream_id
      where st.stream_uuid = $1 and s.subscription_name = $2`,
    [streamUuid, name],
  );
  if (rows.length === 0) return undefined;
  return { claimedBy: rows[0].claimed_by };
}

// Release a (stuck) claim. By default the current holder is detected
// automatically — the operator use case is "this worker is dead, free the
// lease". Pass `workerId` to release on behalf of a specific holder; the store
// raises SubscriptionLeaseLost if it no longer matches.
export async function releaseSubscription(
  db: Db,
  ref: SubscriptionRef & { workerId?: string },
): Promise<{ releasedFrom: string }> {
  const streamUuid = ref.streamUuid ?? DEFAULT_STREAM;
  let workerId = ref.workerId;

  if (workerId === undefined) {
    const holder = await currentHolder(db, streamUuid, ref.name);
    if (holder === undefined) {
      throw new SubscriptionNotFound(ref.name, streamUuid);
    }
    if (holder.claimedBy === null) {
      throw new SubscriptionNotClaimed(ref.name, streamUuid);
    }
    workerId = holder.claimedBy;
  }

  try {
    await db.query(
      "select instructed.release_subscription($1, $2, $3)",
      [streamUuid, ref.name, workerId],
    );
  } catch (err) {
    mapSubscriptionError(err, ref.name, streamUuid);
  }
  return { releasedFrom: workerId };
}

// Delete a subscription by name. Cascades its work-item rows. Raises
// SubscriptionNotFound if there is no such subscription (D-0009: deleting a
// missing subscription is an error, not a silent success).
export async function deleteSubscription(
  db: Db,
  ref: SubscriptionRef,
): Promise<void> {
  const streamUuid = ref.streamUuid ?? DEFAULT_STREAM;
  try {
    await db.query(
      "select instructed.delete_subscription($1, $2)",
      [streamUuid, ref.name],
    );
  } catch (err) {
    mapSubscriptionError(err, ref.name, streamUuid);
  }
}

// Diagnostic claim. Takes (or reports) the lease for a subscription. Creates
// the subscription if it does not exist, honouring `startFrom`
// ('origin' | 'current' | a non-negative integer).
export async function claimSubscription(
  db: Db,
  ref: SubscriptionRef & {
    workerId: string;
    leaseSeconds: number;
    startFrom?: string;
  },
): Promise<ClaimResult> {
  const streamUuid = ref.streamUuid ?? DEFAULT_STREAM;
  const options = ref.startFrom ? { start_from: ref.startFrom } : {};
  const rows = await db.query<{
    result: string;
    last_seen: string;
    claimed_by: string | null;
    claim_expires_at: Date | null;
  }>(
    "select * from instructed.claim_subscription($1, $2, $3, $4, $5)",
    [streamUuid, ref.name, ref.workerId, ref.leaseSeconds, JSON.stringify(options)],
  );
  const r = rows[0];
  return {
    result: r.result as ClaimResult["result"],
    lastSeen: Number(r.last_seen),
    claimedBy: r.claimed_by,
    claimExpiresAt: r.claim_expires_at,
  };
}

// Rebuild: the framework-side half of a projection rebuild (TODO #7). Forgets a
// subscription's state so a fresh worker re-routes the whole history from
// origin: delete the subscription (cascading its work items), then re-create it
// with the cursor reset to 0 and unclaimed. Atomic. The read-store wipe is the
// operator's responsibility and is out of scope here.
//
// Run with the subscription's worker stopped. (delete_subscription does not
// check the lease; a still-running worker's next call fails with IS020 and
// stops, but racing a live worker is not the intended use.)
export function rebuildSubscription(
  db: Db,
  ref: SubscriptionRef,
): Promise<{ existed: boolean }> {
  const streamUuid = ref.streamUuid ?? DEFAULT_STREAM;
  return db.transaction(async (tx) => {
    const holder = await currentHolder(tx, streamUuid, ref.name);
    const existed = holder !== undefined;

    if (existed) {
      await tx.query(
        "select instructed.delete_subscription($1, $2)",
        [streamUuid, ref.name],
      );
    }
    // Re-create at origin via a transient claim, then release so a real worker
    // resumes from 0 regardless of its configured start_from.
    await tx.query(
      "select * from instructed.claim_subscription($1, $2, $3, $4, $5)",
      [
        streamUuid,
        ref.name,
        REBUILD_WORKER_ID,
        1,
        JSON.stringify({ start_from: "origin" }),
      ],
    );
    await tx.query(
      "select instructed.release_subscription($1, $2, $3)",
      [streamUuid, ref.name, REBUILD_WORKER_ID],
    );
    return { existed };
  });
}
