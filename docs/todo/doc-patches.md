# Small doc patches — outstanding

Small wording changes settled during the 2026-05-23 Commanded re-review.
None is a design question; each is a known patch waiting for someone with
30 minutes. All three are pre-release.

**Cross-cutting guidance.** Do not name any other library in the docs.
Attribution belongs once in the top-level `README.md`, not scattered
through the doc set. When the wording sketches below contrast our
behaviour with a previous frame, restate as a positive property of
`instructed` on its own terms.

---

## DOC-A — Handler purity (and the OCC-retry consequence)

**Where:** `docs/guarantees.md` "What commands do" (bullet) and
`docs/concepts.md` "Aggregates and commands" (paragraph).

**What.** Make explicit that aggregate command handlers and event
appliers are pure functions, and that the OCC-retry model means
a non-pure handler will be visibly broken under contention (the
handler runs again from scratch on each retry).

**Wording sketch (refine when implementing):**

> Command handlers and event appliers must be pure functions —
> no I/O, no metric emission, no UUID generation in the handler
> body, no clock reads whose value matters. Under contention, a
> command handler may be invoked more than once per logical
> command: each OCC retry re-loads the aggregate and re-runs the
> handler against the fresh state. A handler that produces side
> effects in its body will surface them once per attempt, not
> once per command. If you need a deterministic identifier on
> the resulting event, set it on the command before dispatch.

State the purity requirement as the rule. Note the
retry-re-runs-handler consequence as the *reason* the rule is
load-bearing in `instructed` specifically.

---

## DOC-B — D-0004 one-liner: no per-aggregate coherence side-channel

**Where:** `docs/decisions.md` D-0004 implications (alongside the
existing implications, not as a new sub-section).

**What.** One additional sentence recording that no per-aggregate
liveness/coherence side-channel is needed in the absence of an
in-memory cache.

**Wording sketch:**

> Without an in-memory cache, no per-aggregate coherence
> side-channel (subscription, broadcast, or other notification
> path) is required: every command's load reads the canonical
> sequence from the event log.

Frame as a positive consequence of D-0004.

---

## DOC-C — D-0010 "Why" tighten

**Where:** `docs/decisions.md` D-0010 "Why" paragraph (replace).

**What.** Distinguish "no shorthand" (a deliberate choice) from
"the shorthand is incoherent" (not true). The current wording
collapses the two.

**Wording sketch:**

> A "wait for every handler that opted in" shorthand is
> well-defined at the SDK layer — handlers register their
> consistency mode when the worker starts, and an SDK-local
> registry could collect them. But the store has no such
> metadata, and any SDK-level shorthand would only cover
> subscriptions managed through the same SDK instance — it would
> silently miss subscriptions held by other processes. The
> explicit list is also more honest: in practice the caller
> knows which projection(s) they need to read their own writes
> against. A per-SDK-instance auto-collection convenience
> remains compatible with v1 and may be added later; the store
> primitive stays explicit.

This tightens the reasoning and leaves the door open for an
opt-in SDK-side convenience without committing to it.
