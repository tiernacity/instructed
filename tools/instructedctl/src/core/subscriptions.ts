// Core: subscription inspection. Read-only for now; lifecycle actions
// (release / delete / claim / rebuild) land in later slices.

import type { Db } from "./db.ts";
import type { SubscriptionSummary } from "./types.ts";

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
