/**
 * Workload generator.
 *
 * Two task families:
 *
 *   - **Dispatcher** picks a random account and runs `add{n}` via
 *     the SDK's `runCommand` loop. Exact-version OCC means concurrent
 *     dispatchers on the same account fight; we sometimes target
 *     `:any_version` appends instead so the workload mixes both
 *     expected-version modes (the contract allows either).
 *
 *   - **Trigger appender** picks a random account and writes a
 *     `Triggered{n,target=account}` event to a designated trigger
 *     stream. The Forwarder PM picks these up and dispatches `add{n}`
 *     onto the account, exercising the PM dispatch path concurrently
 *     with direct dispatchers.
 *
 * Throttling is per-task: each task sleeps a small random interval
 * between iterations. The harness's `--rate-ms` flag sets the upper
 * bound on that sleep.
 */

import type {
  Client} from "../../sdks/typescript/src/index.ts";
import {
  expected,
  runCommand,
} from "../../sdks/typescript/src/index.ts";
import {
  counter,
  type CounterCommand,
  type TriggeredData,
} from "./domain.ts";
import type { SoakMetrics } from "./workers.ts";

export interface DispatcherOptions {
  client: Client;
  accounts: string[];
  /** Sleep up to this many ms between iterations. */
  maxThinkTimeMs: number;
  metrics: SoakMetrics;
  signal: { aborted: boolean };
  /** Probability (0..1) of using `:any_version` instead of OCC. */
  anyVersionFraction: number;
}

export async function dispatcherLoop(opts: DispatcherOptions): Promise<void> {
  const Counter = counter();
  while (!opts.signal.aborted) {
    const stream = pick(opts.accounts);
    const n = 1 + Math.floor(Math.random() * 5);
    opts.metrics.commandsAttempted += 1;
    try {
      if (Math.random() < opts.anyVersionFraction) {
        // Bypass OCC — append directly via the client, as a
        // misbehaving / exogenous writer would. Aggregate folds
        // still reconcile because `Added` events accumulate
        // commutatively.
        await opts.client.appendToStream(stream, expected.any, [
          { type: "Added", data: { n } },
        ]);
      } else {
        await runCommand(
          opts.client,
          Counter,
          stream,
          { kind: "add", n } as CounterCommand,
          // Soak runs deliberately wide; give OCC enough head room.
          { retryBudget: 64 },
        );
      }
      opts.metrics.commandsCompleted += 1;
    } catch {
      opts.metrics.commandsFailed += 1;
    }
    await jitterSleep(opts.maxThinkTimeMs, opts.signal);
  }
}

export interface TriggerAppenderOptions {
  client: Client;
  triggerStreams: string[];
  accounts: string[];
  maxThinkTimeMs: number;
  metrics: SoakMetrics;
  signal: { aborted: boolean };
}

export async function triggerAppenderLoop(
  opts: TriggerAppenderOptions,
): Promise<void> {
  while (!opts.signal.aborted) {
    const triggerStream = pick(opts.triggerStreams);
    const target = pick(opts.accounts);
    const n = 1 + Math.floor(Math.random() * 5);
    try {
      await opts.client.appendToStream(triggerStream, expected.any, [
        {
          type: "Triggered",
          data: { n, target } as TriggeredData,
        },
      ]);
      opts.metrics.triggersAppended += 1;
    } catch {
      // Tracked indirectly via final invariant checks; the trigger
      // stream is `expected.any` so the only failures here are
      // connectivity blips.
    }
    await jitterSleep(opts.maxThinkTimeMs, opts.signal);
  }
}

function pick<T>(xs: readonly T[]): T {
  if (xs.length === 0) throw new Error("pick: empty");
  return xs[Math.floor(Math.random() * xs.length)]!;
}

async function jitterSleep(
  maxMs: number,
  signal: { aborted: boolean },
): Promise<void> {
  if (maxMs <= 0) return;
  const ms = Math.floor(Math.random() * maxMs);
  if (ms === 0) return;
  await new Promise<void>((resolve) => {
    const t = setTimeout(resolve, ms);
    // Best-effort abort: poll every 50ms.
    const i = setInterval(() => {
      if (signal.aborted) {
        clearTimeout(t);
        clearInterval(i);
        resolve();
      }
    }, 50);
    t.unref?.();
    i.unref?.();
  });
}
