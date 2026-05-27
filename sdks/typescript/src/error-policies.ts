/**
 * Retry/error-policy standard library.
 *
 * Retry/error policy is one of the SDK's three named extension
 * points (see `sdks/porting-checklist.md` §4.3), following the
 * **contract + standard library + escape hatch** pattern. The
 * contract lives in `processing-worker.ts`
 * (`ErrorPolicy<PolicyState>`, `ErrorPolicyDecision`,
 * `ErrorPolicyContext`, `ErrorPolicyResult`). This file is the
 * **standard library** — shipped composable strategies for the
 * common cases. Users wanting something outside the cases below
 * use the escape hatch (write their own function obeying the
 * contract).
 *
 * The shipped helpers as of step-5 slice 3 (2026-05-27):
 *
 *   - `exponentialBackoff({ baseMs, capMs, jitter? })` — pure of
 *     `ctx.attempt`. Doubles the delay each attempt, capped.
 *     Optional full-jitter (random in [0, delay)) for
 *     thundering-herd avoidance.
 *   - `linearBackoff({ stepMs, capMs })` — pure of `ctx.attempt`.
 *     Linear growth, capped.
 *   - `retryUpTo(n, inner)` — wraps an inner policy. If
 *     `ctx.attempt > n`, returns `{ kind: 'stop' }`; otherwise
 *     delegates to `inner`. Preserves the inner policy's state.
 *
 * Composition is plain function wrapping; no separate helper
 * required:
 *
 * ```ts
 * import {
 *   exponentialBackoff,
 *   retryUpTo,
 * } from "instructed-sdk";
 *
 * const policy = retryUpTo(10, exponentialBackoff({
 *   baseMs: 100,
 *   capMs: 30_000,
 *   jitter: true,
 * }));
 * ```
 *
 * # Statefulness
 *
 * All three shipped helpers are *stateless* (pure functions of
 * `err` and `ctx.attempt`); they return `state: undefined` and
 * type-parameterise as `ErrorPolicy<undefined>`. The state slot
 * (`ErrorPolicyResult.state`) exists for user-written policies
 * that need to thread information forward — token-bucket
 * budgets, adaptive backoff, "back off harder if the last three
 * errors had the same SQLSTATE", etc. The slot is opaque to the
 * SDK and lifecycle is per-work-item (starts at `undefined`,
 * discarded on success).
 *
 * # `quarantineAfter`
 *
 * Not shipped. The helper that transitions a stuck work item to
 * the `'failed'` SQL state is parked pending TODO #7 co-design
 * with `instructedctl`'s operator surface (the consumer of
 * `failed` rows). Per `docs/invariants.md` INV-SUB-W-013,
 * `'failed'` is operator-only; shipping a producer in isolation
 * without the operator's `skip_work_item_with_audit` command
 * would leave operators with no recovery path.
 *
 * # Layer note
 *
 * This file is L3 (the standard library is idiomatic, not
 * required). The contract — `ErrorPolicy<PolicyState>` and its
 * supporting types — is L2 in `processing-worker.ts` and that's
 * what every port reproduces. A port may ship its own equivalent
 * helpers in whatever shape fits the language, or none at all.
 */

import type {
  ErrorPolicy,
  ErrorPolicyContext,
  ErrorPolicyResult,
} from "./processing-worker.ts";

// ============================================================================
// exponentialBackoff
// ============================================================================

export interface ExponentialBackoffOptions {
  /** Initial delay for attempt 1. Doubles per attempt. */
  baseMs: number;
  /** Upper bound on the computed delay. */
  capMs: number;
  /**
   * Full-jitter mode. When `true`, the returned delay is a
   * uniform sample in `[0, computedDelay)` rather than the
   * computed delay itself. Helps avoid thundering-herd retries
   * across many concurrently-failing workers. Default `false`.
   */
  jitter?: boolean;
}

/**
 * Exponential backoff, doubling per attempt, capped.
 *
 * `delay = min(capMs, baseMs * 2^(attempt - 1))`. With
 * `jitter: true`, returns a uniform sample in `[0, delay)`.
 *
 * Retries forever (never emits `stop`). Compose with `retryUpTo`
 * to cap attempts.
 */
export function exponentialBackoff(
  opts: ExponentialBackoffOptions,
): ErrorPolicy<undefined> {
  const { baseMs, capMs, jitter = false } = opts;
  if (!Number.isFinite(baseMs) || baseMs < 0) {
    throw new RangeError(
      `exponentialBackoff: baseMs must be a non-negative number, got ${baseMs}`,
    );
  }
  if (!Number.isFinite(capMs) || capMs < 0) {
    throw new RangeError(
      `exponentialBackoff: capMs must be a non-negative number, got ${capMs}`,
    );
  }
  return (_err, ctx, _state) => {
    // Clamp the exponent so we never compute an absurd
    // intermediate even if attempt is very large.
    const exp = Math.min(ctx.attempt - 1, 30);
    const raw = Math.min(capMs, baseMs * 2 ** exp);
    const delayMs = jitter ? Math.random() * raw : raw;
    return { decision: { kind: "retry-in", delayMs }, state: undefined };
  };
}

// ============================================================================
// linearBackoff
// ============================================================================

export interface LinearBackoffOptions {
  /** Delay for attempt 1; grows by stepMs each attempt. */
  stepMs: number;
  /** Upper bound on the computed delay. */
  capMs: number;
}

/**
 * Linear backoff: `delay = min(capMs, stepMs * attempt)`.
 * Retries forever (never emits `stop`). Compose with `retryUpTo`
 * to cap attempts.
 */
export function linearBackoff(
  opts: LinearBackoffOptions,
): ErrorPolicy<undefined> {
  const { stepMs, capMs } = opts;
  if (!Number.isFinite(stepMs) || stepMs < 0) {
    throw new RangeError(
      `linearBackoff: stepMs must be a non-negative number, got ${stepMs}`,
    );
  }
  if (!Number.isFinite(capMs) || capMs < 0) {
    throw new RangeError(
      `linearBackoff: capMs must be a non-negative number, got ${capMs}`,
    );
  }
  return (_err, ctx, _state) => ({
    decision: { kind: "retry-in", delayMs: Math.min(capMs, stepMs * ctx.attempt) },
    state: undefined,
  });
}

// ============================================================================
// retryUpTo
// ============================================================================

/**
 * Cap an inner policy at `maxAttempts` attempts. If
 * `ctx.attempt > maxAttempts`, return `{ kind: 'stop' }`;
 * otherwise delegate to `inner` (its `state` flows through
 * unchanged).
 *
 * `maxAttempts` counts attempts (the first failed attempt is
 * `attempt = 1`); `retryUpTo(3, inner)` allows attempts 1, 2,
 * and 3 to delegate, and stops at attempt 4.
 *
 * Typical use: cap an otherwise-infinite backoff:
 *
 * ```ts
 * const policy = retryUpTo(10, exponentialBackoff({
 *   baseMs: 100, capMs: 30_000,
 * }));
 * ```
 */
export function retryUpTo<PolicyState>(
  maxAttempts: number,
  inner: ErrorPolicy<PolicyState>,
): ErrorPolicy<PolicyState> {
  if (!Number.isInteger(maxAttempts) || maxAttempts < 1) {
    throw new RangeError(
      `retryUpTo: maxAttempts must be a positive integer, got ${maxAttempts}`,
    );
  }
  return async (err, ctx: ErrorPolicyContext, state) => {
    if (ctx.attempt > maxAttempts) {
      // `stop`: we discard the inner policy's chance to update
      // state, which is fine — the worker exits on `stop` and the
      // state slot is discarded anyway.
      return {
        decision: { kind: "stop" as const },
        // The state-of-record going into the stop is whatever was
        // last set; preserve it verbatim. Cast covers the `state`
        // parameter being `PolicyState | undefined`.
        state: state as PolicyState,
      };
    }
    const result: ErrorPolicyResult<PolicyState> = await inner(err, ctx, state);
    return result;
  };
}
