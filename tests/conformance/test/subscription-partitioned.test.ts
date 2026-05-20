/**
 * Part E — Partitioned-consumer conformance (Phase 9, step 6/8).
 *
 * All three cases here are SKIPPED per **D-0024** (ML-0001 / D-0002):
 * partitioned subscriptions are deferred from v1. The cases exist
 * now so that the INV-coverage matrix renders them as "deferred"
 * rather than "missing", and so the shape of the future
 * implementation is auditable.
 *
 * When partitioning lights up, two things must happen before these
 * tests are turned on:
 *
 *   1. `claim_subscription` must accept `concurrency_limit > 1`
 *      (currently fixed at 1 per D-0002), and emit `IS021
 *      too_many_subscribers` once that limit is hit.
 *   2. A partition-selector API must be chosen (server-side
 *      selector row on `subscriptions`, OR a `partition_key`
 *      argument on `read_subscription_batch`, OR a JSONB-path
 *      stored on the subscription per the ML-0003 space). The
 *      setup line of INV-SUB-P-041 below depends on that choice;
 *      the assertions are stable.
 *
 * The cases below are the canonical shape recorded in D-0024.
 * Maintainers landing partitioning should re-write each setup
 * to match the chosen API and remove the `.skip` modifier.
 *
 * See:
 *   - `docs/decisions.md` :: D-0024 (case shapes)
 *   - `docs/decisions.md` :: D-0002 (concurrency_limit = 1 in v1)
 *   - `docs/maybe-later.md` :: ML-0001 (partitioned consumers)
 *   - `docs/invariants.md` :: INV-SUB-P-040..042
 */

import { describe, test } from "node:test";

describe("subscriptions — partitioned consumers (deferred per D-0024 / ML-0001)", () => {
  // INV-SUB-P-040: deferred — see ML-0001 / D-0002
  //
  // When `concurrency_limit > 1`, every event MUST be delivered to
  // exactly one of the live subscribers; the total number of live
  // subscribers is capped at `concurrency_limit`.
  //
  // Setup (under the future API):
  //   - claim_subscription(stream, name, "worker-1", lease, { concurrency_limit: 3 })
  //   - claim_subscription(stream, name, "worker-2", lease)
  //   - claim_subscription(stream, name, "worker-3", lease)
  //   - claim_subscription(stream, name, "worker-4", lease)  →  IS021
  //   - append K events to `stream`.
  //   - Each of the three workers calls read_subscription_batch and
  //     advances after handling its batch.
  //
  // Assertions:
  //   - Union of event_ids received across workers has size K.
  //   - Pairwise intersection of those sets is empty.
  //   - A 4th claim returns IS021 too_many_subscribers.
  test.skip("INV-SUB-P-040: multi-subscriber distribution under concurrency_limit > 1", () => {
    // Intentionally empty — the assertions live in the comment above.
    // The harness skips this body entirely; it exists as a coverage
    // slot.
  });

  // INV-SUB-P-041: deferred — see ML-0001 / D-0002
  //
  // With a `partition_by` selector supplied, every event for which
  // `partition_by(event)` returns the same value MUST be delivered
  // to the same subscriber (modulo subscriber failure + rebalance).
  // Intra-partition order MUST equal the events' event_number order.
  //
  // Setup (under the future API — exact shape depends on which
  // selector mechanism lands; see file-level docstring):
  //   - Create the subscription with a selector that extracts a
  //     `partition_key` from each event (e.g. data.partition).
  //   - Three workers claim the subscription with concurrency_limit = 3.
  //   - Append events tagged with partition keys A, A, B, A, B in
  //     that global event_number order.
  //
  // Assertions:
  //   - Stickiness: the worker that received the first A-event
  //     received every A-event; same for B.
  //   - Order: each worker's A-events appear in
  //     event_number-ascending order; same for B.
  //
  // Rebalance variant:
  //   - The A-worker releases its lease (or its lease expires).
  //   - A fresh worker claims the subscription; the A partition is
  //     reassigned to it.
  //   - The new claimer resumes from the A-partition cursor (not
  //     from 0).
  //   - The still-live B-worker continues to see only B-events.
  test.skip("INV-SUB-P-041: partition_by stickiness, intra-partition order, and rebalance", () => {
    // Intentionally empty — see comment above.
  });

  // INV-SUB-P-042: deferred — see ML-0001 / D-0002
  //
  // Without a `partition_by` selector, the contract is silent on
  // which subscriber receives which event. The ONLY guarantee is
  // "every event is delivered to exactly one of the live subscribers".
  //
  // This is INV-SUB-P-040's weaker companion — it documents what we
  // are NOT promising. A future implementation that routes every
  // event to worker-1 would still be conformant for INV-SUB-P-042
  // (it would only fail INV-SUB-P-040's "distribute" wording, which
  // is the stronger of the two).
  //
  // Setup:
  //   - Three workers claim the subscription with concurrency_limit = 3
  //     and NO partition_by selector.
  //   - Append K events.
  //
  // Assertions (deliberately the weaker contract):
  //   - Union of event_ids received across workers has size K.
  //   - Pairwise intersection of those sets is empty.
  //   - NO assertion on stickiness, order across workers, or
  //     distribution fairness.
  test.skip("INV-SUB-P-042: no partition_by — exactly-once-among-live-subscribers, no stickiness", () => {
    // Intentionally empty — see comment above.
  });
});
