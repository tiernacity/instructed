// Core: store soundness checks. A quick "is the store sound" pass over the
// invariants an operator would otherwise eyeball in psql (TODO #7).

import type { Db } from "./db.ts";
import type { HealthCheck, HealthReport } from "./types.ts";

// Run all health checks. `ok` is true only when every check passes.
export async function checkHealth(db: Db): Promise<HealthReport> {
  const checks: HealthCheck[] = [
    await allContiguous(db),
    await noOrphanedStreamEvents(db),
    await noExpiredLeaseZombies(db),
    await noOrphanedWorkItems(db),
  ];
  return { ok: checks.every((c) => c.ok), checks };
}

// $all's event_numbers form a gapless 1..N sequence (INV-APPEND-003). For a
// contiguous sequence the row count equals the max event_number and the min is
// 1 (or the store is empty).
async function allContiguous(db: Db): Promise<HealthCheck> {
  const rows = await db.query<{ cnt: string; max_en: string; min_en: string }>(
    `select
       count(*) as cnt,
       coalesce(max(stream_version), 0) as max_en,
       coalesce(min(stream_version), 0) as min_en
     from instructed.stream_events where stream_id = 0`,
  );
  const cnt = Number(rows[0].cnt);
  const maxEn = Number(rows[0].max_en);
  const minEn = Number(rows[0].min_en);
  const ok = cnt === maxEn && (cnt === 0 || minEn === 1);
  return {
    name: "$all contiguous",
    ok,
    detail: ok
      ? `${cnt} events, head ${maxEn}`
      : `count=${cnt} max=${maxEn} min=${minEn} — gap or offset in $all`,
  };
}

async function noOrphanedStreamEvents(db: Db): Promise<HealthCheck> {
  const rows = await db.query<{ n: string }>(
    `select count(*) as n
       from instructed.stream_events se
       left join instructed.events e on e.event_id = se.event_id
      where e.event_id is null`,
  );
  const n = Number(rows[0].n);
  return {
    name: "no orphaned stream_events",
    ok: n === 0,
    detail: n === 0 ? "none" : `${n} stream_events rows with no event`,
  };
}

async function noExpiredLeaseZombies(db: Db): Promise<HealthCheck> {
  const rows = await db.query<{ n: string }>(
    `select count(*) as n
       from instructed.subscriptions
      where claimed_by is not null and claim_expires_at < now()`,
  );
  const n = Number(rows[0].n);
  return {
    name: "no expired-lease zombies",
    ok: n === 0,
    detail: n === 0
      ? "none"
      : `${n} subscription(s) claimed past lease expiry (dead worker?)`,
  };
}

async function noOrphanedWorkItems(db: Db): Promise<HealthCheck> {
  const rows = await db.query<{ n: string }>(
    `select count(*) as n
       from instructed.subscription_work_items wi
       left join instructed.subscriptions s
         on s.stream_id = wi.stream_id
        and s.subscription_name = wi.subscription_name
      where s.subscription_name is null`,
  );
  const n = Number(rows[0].n);
  return {
    name: "no orphaned work items",
    ok: n === 0,
    detail: n === 0 ? "none" : `${n} work item(s) with no subscription`,
  };
}
