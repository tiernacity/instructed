# Consistency-wait — outstanding work

One remaining SDK change to the strong-consistency-on-dispatch
path settled during the 2026-05-23 Commanded re-review (CON-A).

CON-B (cross-stream `waitForProjection` guard) landed as part of
TODO #11's conformance-criteria revisit:
`ConsistencyTargetError` in `sdks/typescript/src/errors.ts`, the
synchronous validation in `waitForProjection`
(`sdks/typescript/src/consistency.ts`), and four tests in
`sdks/typescript/test/consistency.test.ts` under the
"cross-stream guard (CON-B)" describe block. The SUB-A underlying
compare is the catch-up predicate already; nothing further is
owed.

---

## CON-A — `exclude` mechanism for `dispatch(..., { consistency: [...] })`

**Why.** A PM dispatching a command with
`consistency: [...own_subscription_name...]` deadlocks: the wait
needs the PM's subscription cursor to advance past the dispatched
events, but the cursor cannot advance until the PM's `handle`
returns, which cannot happen until the wait returns. Hard
self-deadlock until `consistencyTimeout` fires.

**What lands.**

1. **Public API.** `Instructed.dispatch` accepts an optional
   `exclude?: string[] | SubscriptionRef[]`, normalised the same
   way `consistency` is normalised (`string[]` defaults to
   `{ stream: "$all", name }`). References in `exclude` are
   removed from the resolved consistency wait set before
   `waitForProjection` is called.

2. **Default behaviour when dispatching from inside a PM.** The
   PM worker's internal `runCommand` call (in
   `sdks/typescript/src/process-manager.ts`) passes
   `exclude: [{ stream: <pm subscription stream>, name: <pm
   name> }]` automatically. An application author who writes

   ```ts
   await dispatch(cmd, { consistency: [...projections..., "ThisPM"] });
   ```

   from inside `handle` does not deadlock — the PM's own
   subscription is filtered out before the wait begins.

3. **Warning log on auto-exclusion.** When the PM worker
   auto-excludes its own subscription, emit a warning via the
   configured logger / `onError`-adjacent channel describing
   what was excluded and why. The warning must include the PM
   name so the application can identify and remove the
   self-reference. **Do not silently drop** — that teaches bad
   habits.

4. **Explicit `exclude` from the application.** Honoured
   without warning — it's an explicit caller decision.

5. **Tests.** Cases in the PM concurrent-tests suite:
   (a) PM dispatching with `consistency: [own_name]` does not
   deadlock and the operation completes; (b) the warning fires
   once per dispatch that triggered auto-exclusion; (c) explicit
   `exclude` suppresses the warning.

6. **Docs.** Short paragraph in `docs/architecture.md` "Strong
   consistency on dispatch" describing the `exclude` option and
   the PM auto-exclusion. Frame the constraint as "a subscription
   cannot wait for itself to make progress while it is the active
   processor" — describe the property, not a comparison.

---

## CON-B — `waitForProjection` cross-stream guard (LANDED)

**Status:** shipped. Implementation and tests as described below;
the SUB-A re-fit collapsed into a no-op because the underlying
compare is the SUB-A catch-up predicate already. Retained here
for historical reference until the next docs tidy.

---

## Original CON-B brief

**Why.** A per-stream `SubscriptionRef` whose `stream` does not
match any appended event's `stream_uuid` is meaningless: the
subscription's `last_seen` lives in its own stream's
coordinate space, the target lives in the appended stream's
coordinate space, and comparing them silently produces wrong
answers. Documented bug in the current `waitForProjection`
implementation.

The typical case (`consistency: ["BalancesProjector"]` → bare
strings default to `{ stream: "$all", name }`) is unaffected.
The explicit-ref form (`consistency: [{ stream: "X", name: "Y" }]`)
is the buggy path.

### What lands now (today's schema)

1. **Predicate.** Every `SubscriptionRef` in the consistency
   list with `stream !== '$all'` must have its `stream` equal to
   one of the appended events' `stream_uuid` values.

2. **Error.** A new typed error — suggested name
   `ConsistencyTargetError`, shape consistent with the existing
   error taxonomy in `sdks/typescript/src/errors.ts`. Message
   must list:
   - the offending `SubscriptionRef` (stream + name),
   - the streams the append actually touched,
   - a one-line explanation that a per-stream subscription on
     stream X can only wait on appends to stream X.

3. **Where.** Inside `waitForProjection` in
   `sdks/typescript/src/consistency.ts`, *before* the polling
   loop starts. Throwing happens synchronously, not inside an
   `await`, so the error surfaces immediately rather than after
   `pollInterval`.

4. **Tests.** In whichever file covers `consistency.ts`:
   - positive: per-stream ref matching an appended stream
     resolves normally;
   - negative: per-stream ref differing from every appended
     stream throws `ConsistencyTargetError` synchronously;
   - mixed: a list containing one valid ref and one invalid ref
     throws (the invalid one prevents the wait from starting);
   - `$all` refs never rejected by this check, regardless of
     which streams were appended to.

5. **Docs.** One sentence in `docs/architecture.md` "Strong
   consistency on dispatch" and `docs/guarantees.md` "What
   `dispatch` returns":

   > A per-stream subscription target can only wait on appends
   > to its own stream. Passing a per-stream subscription target
   > for an append to a different stream raises a typed error
   > before the wait begins.

### What changes when SUB-A lands

The validation predicate stays. The *underlying compare* changes:

- **Today:** `last_seen >= target` per subscription.
- **Under SUB-A (work-queue model):** subscription S is caught
  up to target T iff **both**:
  1. The routing cursor for S has reached T (`routing_cursor[S]
     >= T`), AND
  2. No outstanding work items for S exist with `event_number
     <= T` (states `pending`, `claimed`, or `failed`).

Neither condition alone is sufficient. Same caller-facing API,
different SQL behind it. The cross-stream guard from this
ticket still applies — without it, a per-stream subscription
wait against an unrelated stream's append would vacuously
succeed under the work-queue model (router never enqueued
anything for that event, "no outstanding items" is trivially
true), a different flavour of wrong than today's silent
spurious result.

The SUB-A re-fit is tracked alongside the broader SUB-A work in
`subscriptions.md`; this file's CON-B closes when the cursor
compare is replaced.
