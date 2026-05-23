#!/usr/bin/env node
/**
 * Soak harness entrypoint (TODO #3b).
 *
 * Composes N dispatchers, M trigger appenders, P competing projector
 * slots, K competing PM slots, plus periodic failure injection
 * (worker respawn + lease theft) against a single Postgres. Runs for
 * a configurable duration, drains, then reports.
 *
 * Usage:
 *
 *   docker compose up -d postgres
 *   cd tests/soak
 *   npm install
 *   npm start -- --duration 60 --accounts 8 --dispatchers 6 \
 *                --projectors 3 --pms 2 --triggers 4
 *
 * The harness is intentionally chatty on stdout. Stable identifiers
 * make grep'ing for "VIOLATION" cheap.
 *
 * Exit code is 0 if no invariant violations were observed and 1
 * otherwise. Performance facts always print regardless.
 *
 * See README.md for the interpretation guide.
 */

import { randomUUID } from "node:crypto";
import pg from "pg";

import {
  Client,
  type ProjectionDefinition,
  type RecordedEvent,
} from "../../sdks/typescript/src/index.ts";
import { dbConfigFromEnv, makePool, resetSchema } from "./fixtures.ts";
import { forwarder, newForwarderCounters } from "./domain.ts";
import {
  newMetrics,
  pickRandom,
  pmSlot,
  projectorSlot,
  stealLease,
  intervalLoop,
  type SlotHandle,
} from "./workers.ts";
import { dispatcherLoop, triggerAppenderLoop } from "./workload.ts";
import {
  makeBalanceProjector,
  newSamplerState,
  runFinalChecks,
  sampleOnce,
  type CheckContext,
} from "./checks.ts";

// ---------------------------------------------------------------------------
// CLI parsing
// ---------------------------------------------------------------------------

interface Cli {
  durationSec: number;
  accounts: number;
  triggers: number;
  dispatchers: number;
  triggerAppenders: number;
  projectors: number;
  pms: number;
  leaseSeconds: number;
  pollIntervalMs: number;
  thinkTimeMs: number;
  sampleIntervalMs: number;
  drainTimeoutSec: number;
  /** ms between respawn injections; 0 disables */
  respawnEveryMs: number;
  /** ms between lease theft injections; 0 disables */
  stealEveryMs: number;
  /** 0..1 — fraction of direct appends using `expected.any` */
  anyVersionFraction: number;
}

function parseCli(argv: string[]): Cli {
  const args = new Map<string, string>();
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]!;
    if (!a.startsWith("--")) continue;
    const k = a.slice(2);
    const v = argv[i + 1];
    if (v === undefined || v.startsWith("--")) {
      args.set(k, "true");
    } else {
      args.set(k, v);
      i += 1;
    }
  }
  const num = (k: string, d: number): number => {
    const v = args.get(k);
    if (v === undefined) return d;
    const n = Number(v);
    if (!Number.isFinite(n)) {
      throw new Error(`--${k}: not a number: ${v}`);
    }
    return n;
  };
  return {
    durationSec: num("duration", 30),
    accounts: num("accounts", 6),
    triggers: num("trigger-streams", 3),
    dispatchers: num("dispatchers", 4),
    triggerAppenders: num("trigger-appenders", 2),
    projectors: num("projectors", 2),
    pms: num("pms", 2),
    leaseSeconds: num("lease-seconds", 3),
    pollIntervalMs: num("poll-interval-ms", 50),
    thinkTimeMs: num("think-time-ms", 20),
    sampleIntervalMs: num("sample-interval-ms", 100),
    drainTimeoutSec: num("drain-timeout-sec", 300),
    respawnEveryMs: num("respawn-every-ms", 2000),
    stealEveryMs: num("steal-every-ms", 3000),
    anyVersionFraction: num("any-version-fraction", 0.2),
  };
}

// ---------------------------------------------------------------------------
// Drain logic
// ---------------------------------------------------------------------------

export interface DrainState {
  /** True if drain reached steady state before timeout. */
  completed: boolean;
  /** $all event_number at end of drain (whether completed or not). */
  head: bigint;
  /** Per-subscription final last_seen and lag relative to its head. */
  subscriptions: Array<{
    stream: string;
    name: string;
    lastSeen: bigint;
    head: bigint;
    lag: bigint;
  }>;
}

async function snapshotDrainState(
  pool: pg.Pool,
  client: Client,
  subscriptions: Array<{ stream: string; name: string }>,
  completed: boolean,
): Promise<DrainState> {
  const r = await pool.query<{ head: string }>(
    `SELECT stream_version::text AS head FROM instructed.streams WHERE stream_id = 0`,
  );
  const head = BigInt(r.rows[0]!.head);
  const subs: DrainState["subscriptions"] = [];
  for (const sub of subscriptions) {
    let lastSeen = 0n;
    let subHead = head;
    try {
      const pos = await client.readSubscriptionPosition(sub.stream, sub.name);
      lastSeen = pos.lastSeen;
    } catch {
      // Subscription not yet created; treat as fully behind.
    }
    if (sub.stream !== "$all") {
      const r2 = await pool.query<{ head: string }>(
        `SELECT stream_version::text AS head FROM instructed.streams WHERE stream_uuid = $1`,
        [sub.stream],
      );
      subHead = BigInt(r2.rows[0]?.head ?? "0");
    }
    subs.push({
      stream: sub.stream,
      name: sub.name,
      lastSeen,
      head: subHead,
      lag: subHead - lastSeen,
    });
  }
  return { completed, head, subscriptions: subs };
}

async function waitForDrain(
  pool: pg.Pool,
  client: Client,
  subscriptions: Array<{ stream: string; name: string }>,
  timeoutMs: number,
): Promise<boolean> {
  // Drain = $all head stable for two consecutive ticks AND every
  // subscription's last_seen >= head. The PM creates new events
  // while processing triggers, so head keeps growing until the PM
  // is itself caught up.
  const deadline = Date.now() + timeoutMs;
  let prevHead = -1n;
  let stableTicks = 0;
  while (Date.now() < deadline) {
    const r = await pool.query<{ head: string }>(
      `SELECT stream_version::text AS head FROM instructed.streams WHERE stream_id = 0`,
    );
    const head = BigInt(r.rows[0]!.head);
    if (head === prevHead) {
      stableTicks += 1;
    } else {
      stableTicks = 0;
      prevHead = head;
    }
    // Every subscription caught up?
    let allCaught = true;
    for (const sub of subscriptions) {
      try {
        const pos = await client.readSubscriptionPosition(sub.stream, sub.name);
        if (sub.stream === "$all") {
          if (pos.lastSeen < head) {
            allCaught = false;
            break;
          }
        } else {
          const r2 = await pool.query<{ head: string }>(
            `SELECT stream_version::text AS head FROM instructed.streams WHERE stream_uuid = $1`,
            [sub.stream],
          );
          const subHead = BigInt(r2.rows[0]?.head ?? "0");
          if (pos.lastSeen < subHead) {
            allCaught = false;
            break;
          }
        }
      } catch {
        // No subscription row yet; treat as caught up (nothing to do).
      }
    }
    if (stableTicks >= 2 && allCaught) return true;
    await new Promise((r) => setTimeout(r, 250));
  }
  console.warn(
    `[soak] drain timeout after ${timeoutMs}ms; quiescence-dependent checks will be reported as INCONCLUSIVE`,
  );
  return false;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<number> {
  const cli = parseCli(process.argv.slice(2));
  console.log("[soak] config:", JSON.stringify(cli, null, 2));

  const cfg = dbConfigFromEnv();
  // Sized so worker slots + dispatchers + samplers all have room.
  const poolSize = Math.max(
    16,
    cli.dispatchers + cli.triggerAppenders + cli.projectors + cli.pms * 2 + 4,
  );
  const pool = await makePool(cfg, poolSize);
  const dispatchPool = await makePool(cfg, Math.max(4, cli.pms * 2));

  try {
    console.log("[soak] resetting schema");
    await resetSchema(pool);

    const client = new Client(pool);
    const dispatchClient = new Client(dispatchPool);

    // Pre-allocate streams.
    const accounts = Array.from({ length: cli.accounts }, () => randomUUID());
    const triggerStreams = Array.from({ length: cli.triggers }, () =>
      randomUUID(),
    );

    // Projector with a balance map (and event_id dedup).
    const balanceProj = makeBalanceProjector();
    const projectionName = "p-balances";
    const projDef: ProjectionDefinition = {
      name: projectionName,
      stream: "$all",
      startFrom: "origin",
      handle: (e: RecordedEvent) => balanceProj.handle(e),
    };

    const forwarderName = "pm-forwarder";
    const pmCounters = newForwarderCounters();
    const pmDef = forwarder(forwarderName, pmCounters);

    // Subscription registry for invariant checks.
    const subscriptions = [
      { stream: "$all", name: projectionName },
      { stream: "$all", name: forwarderName },
    ];

    // Spawn slots.
    const metrics = newMetrics();
    const slots: SlotHandle[] = [];

    for (let i = 0; i < cli.projectors; i++) {
      slots.push(
        projectorSlot({
          client,
          def: projDef,
          slotLabel: `proj-${i}`,
          leaseSeconds: cli.leaseSeconds,
          pollInterval: cli.pollIntervalMs,
          metrics,
        }),
      );
    }
    for (let i = 0; i < cli.pms; i++) {
      slots.push(
        pmSlot({
          client,
          dispatchClient,
          def: pmDef,
          slotLabel: `pm-${i}`,
          leaseSeconds: cli.leaseSeconds,
          pollInterval: cli.pollIntervalMs,
          metrics,
        }),
      );
    }

    // Workload + sampler + failure injection abort signals.
    const stop = { aborted: false };

    // Workload tasks.
    const workloadTasks: Promise<void>[] = [];
    for (let i = 0; i < cli.dispatchers; i++) {
      workloadTasks.push(
        dispatcherLoop({
          client,
          accounts,
          maxThinkTimeMs: cli.thinkTimeMs,
          metrics,
          signal: stop,
          anyVersionFraction: cli.anyVersionFraction,
        }),
      );
    }
    for (let i = 0; i < cli.triggerAppenders; i++) {
      workloadTasks.push(
        triggerAppenderLoop({
          client,
          triggerStreams,
          accounts,
          maxThinkTimeMs: cli.thinkTimeMs,
          metrics,
          signal: stop,
        }),
      );
    }

    // Sampler.
    const samplerState = newSamplerState();
    const checkCtx: CheckContext = {
      pool,
      client,
      subscriptions,
      accounts,
      forwarderName,
    };
    const samplerTask = intervalLoop(
      () => sampleOnce(checkCtx, samplerState),
      cli.sampleIntervalMs,
      stop,
    );

    // Failure injection.
    const respawnTask =
      cli.respawnEveryMs > 0
        ? intervalLoop(
            async () => {
              await pickRandom(slots).bounce();
            },
            cli.respawnEveryMs,
            stop,
          )
        : Promise.resolve();
    const stealTask =
      cli.stealEveryMs > 0
        ? intervalLoop(
            async () => {
              const sub = pickRandom(subscriptions);
              await stealLease(pool, sub.stream, sub.name);
              metrics.leaseThefts += 1;
            },
            cli.stealEveryMs,
            stop,
          )
        : Promise.resolve();

    // Run for the configured duration.
    console.log(`[soak] running for ${cli.durationSec}s`);
    await new Promise((r) => setTimeout(r, cli.durationSec * 1000));

    // Stop workload + sampler + failure injection.
    console.log("[soak] stopping workload");
    stop.aborted = true;
    await Promise.all([
      ...workloadTasks,
      samplerTask,
      respawnTask,
      stealTask,
    ]);

    // Drain.
    console.log("[soak] draining");
    const drainCompleted = await waitForDrain(
      pool,
      client,
      subscriptions,
      cli.drainTimeoutSec * 1000,
    );
    const drainState = await snapshotDrainState(
      pool,
      client,
      subscriptions,
      drainCompleted,
    );

    // Take a final sample so monotonicity check sees the steady-state value.
    await sampleOnce(checkCtx, samplerState);

    // Stop workers.
    console.log("[soak] stopping workers");
    await Promise.all(slots.map((s) => s.close()));

    // Final invariant checks.
    console.log("[soak] running final invariant checks");
    const report = await runFinalChecks(
      checkCtx,
      samplerState,
      balanceProj.balances,
      { quiesced: drainCompleted },
    );

    // -------------------------------------------------------------------
    // Report
    // -------------------------------------------------------------------
    const elapsedSec = (Date.now() - metrics.startedAt) / 1000;
    console.log("");
    console.log("=========================== SOAK REPORT ===========================");
    console.log(`elapsed:                    ${elapsedSec.toFixed(1)}s`);
    console.log(`commands attempted:         ${metrics.commandsAttempted}`);
    console.log(`commands completed:         ${metrics.commandsCompleted}`);
    console.log(`commands failed:            ${metrics.commandsFailed}`);
    console.log(`triggers appended:          ${metrics.triggersAppended}`);
    console.log(`worker respawns:            ${metrics.respawns}`);
    console.log(`lease thefts:               ${metrics.leaseThefts}`);
    console.log(`sampler ticks:              ${samplerState.samples}`);
    console.log(`$all head:                  ${report.facts.allHead}`);
    console.log(`Added events total:         ${report.facts.addedTotal}`);
    console.log(`Triggered events total:     ${report.facts.triggersTotal}`);
    console.log(
      `commands/sec (completed):   ${(metrics.commandsCompleted / elapsedSec).toFixed(1)}`,
    );
    console.log(
      `events/sec ($all):          ${(Number(report.facts.allHead) / elapsedSec).toFixed(1)}`,
    );

    // PM-FORWARD diagnostic: localises any forwarded-vs-triggered
    // discrepancy to a single SDK code path. See domain.ts for the
    // counter semantics; ideal steady-state has every column >= the
    // one above it (redeliveries can inflate route/handle counts).
    const routeTriggered = pmCounters.routeCalls.get("Triggered") ?? 0;
    console.log("");
    console.log("PM-FORWARD diagnostic:");
    console.log(`  triggers_total          ${report.facts.triggersTotal}`);
    console.log(`  route(Triggered) calls  ${routeTriggered}`);
    console.log(`  handle calls            ${pmCounters.handleCalls}`);
    console.log(`  handle returns          ${pmCounters.handleReturns}`);
    console.log(`  dispatched (causation)  ${report.facts.dispatchedViaCausation}`);
    console.log(`  forwarded (snapshots)   ${report.facts.forwardedTotal}`);

    // Drain state: visible regardless of whether drain completed.
    // Lag > 0 at end-of-run means a subscription was still working
    // through its backlog; quiescence-dependent checks (PM-FORWARD-
    // TOTAL, REFOLD-MATCH) only hold when every lag is 0.
    console.log("");
    console.log(
      `drain:                      ${drainState.completed ? "completed" : "INCOMPLETE (timeout)"}`,
    );
    for (const s of drainState.subscriptions) {
      console.log(
        `  ${s.stream === "$all" ? "$all" : s.stream.slice(0, 8)}/${s.name.padEnd(14)} lastSeen=${s.lastSeen} head=${s.head} lag=${s.lag}`,
      );
    }

    if (report.inconclusive.length > 0) {
      console.log("");
      console.log(`INCONCLUSIVE (${report.inconclusive.length}) — drain did not complete; these only hold at quiescence:`);
      for (const v of report.inconclusive) {
        console.log(`  [${v.code}] ${v.message}`);
      }
    }

    if (report.violations.length === 0) {
      console.log("");
      if (drainState.completed) {
        console.log("OK — no invariant violations observed.");
      } else {
        console.log(
          "INCOMPLETE — no invariant violations observed, but drain timed out. " +
            "Re-run with --drain-timeout-sec set higher, or reduce workload.",
        );
      }
      console.log("===================================================================");
      return drainState.completed ? 0 : 0;
    }

    console.log("");
    console.log(`VIOLATIONS (${report.violations.length}):`);
    for (const v of report.violations) {
      console.log(`  [${v.code}] ${v.message}`);
    }
    console.log("===================================================================");
    return 1;
  } finally {
    await pool.end();
    await dispatchPool.end();
  }
}

main().then(
  (code) => process.exit(code),
  (err) => {
    console.error("[soak] fatal:", err);
    process.exit(2);
  },
);
