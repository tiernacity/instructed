// Core: work-queue inspection. Read-only. The operator escape hatch
// (skip-with-audit) is intentionally not here — it depends on a future
// `skip_work_item_with_audit` procedure (TODO #7).

import type { Db } from "./db.ts";
import type { FailedWorkItem, WorkItemCounts } from "./types.ts";

interface CountsRow {
  subscription_name: string;
  pending: string;
  claimed: string;
  failed: string;
  done: string;
  total: string;
  oldest_active: string | null;
}

// Per-subscription work-item counts by state, plus the oldest still-active
// event_number. Optionally filtered to one subscription.
export async function listWorkItemCounts(
  db: Db,
  subscription?: string,
): Promise<WorkItemCounts[]> {
  const where = subscription ? "where subscription_name = $1" : "";
  const params = subscription ? [subscription] : [];
  const rows = await db.query<CountsRow>(
    `select
       subscription_name,
       count(*) filter (where state = 'pending') as pending,
       count(*) filter (where state = 'claimed') as claimed,
       count(*) filter (where state = 'failed')  as failed,
       count(*) filter (where state = 'done')    as done,
       count(*) as total,
       min(event_number) filter (where state in ('pending','claimed','failed'))
         as oldest_active
     from instructed.subscription_work_items
     ${where}
     group by subscription_name
     order by subscription_name`,
    params,
  );
  return rows.map((r) => ({
    subscriptionName: r.subscription_name,
    pending: Number(r.pending),
    claimed: Number(r.claimed),
    failed: Number(r.failed),
    done: Number(r.done),
    total: Number(r.total),
    oldestActiveEventNumber: r.oldest_active === null ? null : Number(r.oldest_active),
  }));
}

interface FailedRow {
  subscription_name: string;
  partition_key: string;
  event_number: string;
  failed_at: Date | null;
  error_text: string | null;
}

// List failed work items with their error text. Per INV-SUB-W-013 these are
// operator-only and never auto-cleared. Optionally filtered to one
// subscription.
export async function listFailedWorkItems(
  db: Db,
  subscription?: string,
): Promise<FailedWorkItem[]> {
  const where = subscription
    ? "where state = 'failed' and subscription_name = $1"
    : "where state = 'failed'";
  const params = subscription ? [subscription] : [];
  const rows = await db.query<FailedRow>(
    `select subscription_name, partition_key, event_number, failed_at, error_text
       from instructed.subscription_work_items
       ${where}
       order by subscription_name, event_number`,
    params,
  );
  return rows.map((r) => ({
    subscriptionName: r.subscription_name,
    partitionKey: r.partition_key,
    eventNumber: Number(r.event_number),
    failedAt: r.failed_at,
    errorText: r.error_text,
  }));
}
