# Lessons from building the complex example app

> **Status:** living log. As we build the ticketing example
> (`docs/plans/complex-example-app.md`), we record here every place
> the SDK or the contract fought the domain, surprised us, or
> taught us something worth acting on. Each entry is a candidate
> input to `TODO.md`, `docs/maybe-later.md`, or `docs/decisions.md`.
>
> Format per entry: **Observation → Impact → Recommendation →
> Status**. Keep them small and dated. Promote the actionable ones
> out to the real backlog and link back here.

---

## L-0001 — SDK type names are not namespaced (`Event`, `Command` collide with domain vocabulary)

**Date:** 2026-05-31 (surfaced during domain naming, before any code).

**Observation.** The store/SQL layer is well namespaced: the
reserved stream name is `$all` (the `$` sigil signals "reserved"),
the Postgres schema is `instructed`, and custom errors live in
SQLSTATE class `IS`. But the **TypeScript SDK exports bare
top-level interfaces named `Event` and `Command`** (`types/event.ts`,
`types/command.ts`), plus other unprefixed type names
(`RecordedEvent`, etc.). There is also one **bare reserved metadata
key**, `snapshot_module_version` (SNAP-002), with no reserved
prefix.

This came up immediately: the ticketing domain wants to model a
concert *Event* (a show) and naturally reaches for the noun
`Event` — which is also (a) the SDK's core type and (b) the DOM
`Event` global. The application must alias on import to model its
own domain.

**Impact.**
- **The SDK constrains the application's modelling vocabulary.** A
  CQRS/ES library is *especially* likely to collide here, because
  its core nouns (event, command, stream, aggregate, projection)
  are exactly the nouns a domain modeller wants to use. "Event" is
  the worst case — it's both our central type and an extremely
  common domain noun.
- Forced import aliases (`import { Event as EsEvent }`) are
  ceremony that cuts against the SDK's stated conciseness goal.
- The bare `snapshot_module_version` metadata key risks a silent
  collision with an application that writes its own metadata under
  the same name, and it's part of the cross-port contract, so any
  fix must be coordinated across SDKs.

**Audit of the public type surface (2026-05-31, P1).** Enumerated
both entry points (`index.ts` bare, `core.ts`). Findings, which
*narrow* the original worry:

- **Structural typing already insulates the app from the worst
  collision.** Application code defines its own events/commands as
  plain data (`export const X = 'X'; export type X = { type: typeof
  X; data: ... }`) and the SDK consumes them *structurally* via
  generic bounds (`E extends Event`, `RecordedEvent<E>`,
  `RoutingFn<E>`, `AggregateDefinition<S,C,E>`). The `bank-account`
  example **never imports bare `Event` or `Command`** — it models
  freely and the SDK's `Event`/`Command` stay internal constraints.
  So a domain `Show`/`Event` does *not* require aliasing the SDK
  type. The headline fear is largely already handled by design.
- **Names app code *does* import are well-suffixed / low-collision:**
  `AggregateDefinition`, `ProjectionDefinition`,
  `ProcessManagerDefinition`, `RoutingFn`, `RecordedEvent`,
  `DispatchedCommand`, `CommandRouter`, `commandRouter`, `onlyTypes`,
  `Instructed`. Good naming already.
- **Residual real risks** (value/type exports an app might import
  and shadow): `Client` (≈ "customer/client" domain noun), `Logger`
  (apps often have one), `Snapshot`, `DomainEvent`, the short value
  `expected`. Plus the bare `Event`/`Command` if someone *does*
  import them or uses a wildcard import.
- **The reserved metadata key is a genuine, if latent,
  *data*-level collision.** `SNAPSHOT_MODULE_VERSION_KEY =
  'snapshot_module_version'` is unprefixed. The high-level path
  *constructs* snapshot metadata itself (no merge with app
  metadata), so it's not an active bug today — but `SnapshotInput.
  metadata` is app-facing (`unknown`), and the key is part of the
  **cross-port contract** (SNAP-002). Namespace-import does *not*
  fix this; it's a data key, not a type name.

**Resolution (implemented 2026-05-31, see `docs/decisions.md`
D-0028).** Did 1, 2, and 4 below; 3 was a deliberate no-op.
1. **Blessed pattern: namespace import.** Document `import * as es
   from "instructed-sdk"` (→ `es.Event`, `es.Client`, …) as the
   recommended shape so *no* SDK name enters the app's flat
   namespace. Non-breaking, idiomatic, fully resolves "the SDK
   constrains my vocabulary." Primary fix.
2. **Namespace the reserved metadata key** (`snapshot_module_version`
   → a reserved-prefixed key, e.g. `$instructed.snapshot_module_
   version`). This is the one genuine data-level collision and is
   *contract* work: SQL/spec note, porting-checklist (SNAP-002),
   conformance, and a `docs/decisions.md` entry. The substantive
   piece of P1.
3. **No sweeping renames** of the type surface — the audit shows app
   code doesn't import the worst names. Revisit only if a concrete
   collision bites during the example build (candidates if so:
   `Client`, `Logger`).
4. **Document the namespacing posture for porters:** store layer
   uses `$` / `instructed` schema / `IS` SQLSTATE; language SDKs
   keep core type names out of the app's flat namespace (namespace
   import) and prefix reserved metadata keys.

**Status.** **Resolved (D-0028).** Namespace-import guidance landed
in the SDK README + `concepts.md`; the reserved key is now
`$instructed.snapshot_module_version` across SDK + invariants +
sql-contract + architecture + `sql/instructed.sql` + porting
checklist; SDK + conformance suites green (162 + snapshot). The
lighter-than-first-thought outcome is itself a finding — the SDK's
structural-typing design does more namespacing work than the raw
export list suggests.

---

## L-0002 — no schema migration; the only "upgrade" is destructive

**Date:** 2026-05-31 (surfaced planning the deployment/provisioning
story; before any code).

**Observation.** `instructedctl` can `schema install` (fresh) and
report `schema status` / `version`, and `schema migrate` is
explicitly *not implemented*. The only way to change an existing
store's schema today is `schema install --force`, which
`drop schema instructed cascade` then reinstalls — **it destroys all
events and all state.** There is a recorded schema version
(`instructed.get_schema_version()`), so versioning *exists*, but
there is no version-to-version migration mechanism.

**Impact.**
- An operator running a real `instructed` deployment cannot adopt a
  new `instructed` schema version without losing data — the worst
  possible property for an append-only event store, whose whole
  value proposition is that the log is the durable system of record.
- The example app, deployed for real, is the first place this bites
  in practice. It's a good forcing function: we'll *want* a
  non-destructive upgrade path the moment we bump anything.
- Migration of an event store is its own design problem: the
  schema (tables + ~12 stored procedures) can evolve, but events
  are immutable, so a migration is mostly DDL + procedure
  replacement + (rarely) additive backfill — *not* event rewriting.
  That constraint should make a forward-only migration tractable.

**Recommendation (to decide, not yet committed).**
- Design `instructedctl schema migrate`: forward-only, idempotent,
  driven by the recorded schema version; apply ordered migration
  steps from current → target; never rewrite events.
- Decide where migration steps live (in the `sql/` spec, versioned)
  and how an SDK/operator discovers the target version.
- Keep `--force` as the explicit destructive escape hatch, clearly
  separated from `migrate`.
- This is squarely a contract/tooling concern, not example-app
  work — the example just *reveals* the need. Promote to `TODO.md`
  (it already tracks `schema migrate` as remaining in the
  `instructedctl` README) and likely a `docs/decisions.md` entry on
  the migration model.

**Status.** Open / known gap. For the example we provision fresh
(`schema install`) and accept `--force` for re-provisioning; the
lesson is the prompt to design real migration. The smaller,
immediate slice — a **non-destructive idempotent init** ("ensure
installed; no-op if present") — is a prerequisite for the example's
CI/deploy lane (plan §9b, P2); full forward-only `migrate` is the
larger follow-on.

---

## L-0003 — time-based behaviour: passive expiry is fine; only *active* timed action is a real gap

**Date:** 2026-05-31 (surfaced designing the hold-expiry mechanism;
before any code). **Revised same day** after the
"expiry-as-derived-fact" framing — it *narrowed* the gap I first
claimed; recorded honestly here.

**First (overstated) framing.** "Modelling a hold that auto-releases
at `T+TTL` has no clean home: there's no scheduler/timer and no
event fan-out, so the obvious PM-driven designs are blocked."

**Correction — split time-based behaviour in *three*:**

- **(1) Passive expiry (a derived fact).** A hold carries
  `expiresAt`; "expired" is just `now > expiresAt`. **No primitive
  is needed.** ES supports this cleanly by treating *time as a
  decision input*: the edge stamps `expiresAt` onto the event and a
  decision-time `now` onto each command; the pure core compares
  them. `apply` accumulates hold facts; `execute` excludes expired
  holds when computing availability; reads filter by `expiresAt` at
  query time. Capacity self-heals, reads are always correct, no
  scheduler, no fan-out, no sweep, no release race. Only cost: state
  GC (prune opportunistically on the next command, or a non-critical
  janitor) — housekeeping, not correctness. **The better design;
  removes the difficulty I first attributed to the contract.**
- **(2) Client-side timed reaction.** "Tell *this user* their hold
  lapsed"; a checkout countdown. The client already has `expiresAt`,
  so it runs a *local* timer and updates its own UI. **No server
  scheduler — not an `instructed` concern at all.** A large slice of
  what looks like "timed reminders" is actually this.
- **(3) Server-originated active action.** Must *happen* server-side
  at a time, with no online actor and no triggering command: **offer
  a freed seat to the waitlist**; email a reminder 24h before the
  show. *Some* are handlable opportunistically (piggyback on the
  next passing command — best-effort timing); the irreducible
  residue ("fire at a precise time with nothing to ride on")
  genuinely needs an external trigger. **This — and only this — is
  the real, narrow gap.**

**What's actually missing, precisely.**
- A **scheduled / delayed command** primitive ("dispatch this command
  at/after time T") — the direct fix for active timed action.
- (Broader, separate) **fan-out / broadcast routing** (one event →
  many partitions) — not needed for expiry once it's derived, but it
  *is* the general blocker for "react across all live instances of
  X". Keep on the radar; not forced by the MVP.

**Impact.**
- Passive expiry is common and now has a clean, blessed pattern —
  good `docs/concepts.md` / walkthrough material ("model leases as
  derived state, carry time as a decision input").
- Category (3) is real but narrower than it first looks: (2) peels
  off most user-facing reminders to the client, and opportunistic
  piggybacking covers some of the rest. It maps onto scope: **MVP
  hold expiry is (1); the client handles (2); the stretch waitlist
  is the first feature that reaches irreducible (3).** So we feel
  the real gap exactly when we reach for the waitlist, not before.

**Recommendation (to decide, not yet committed).**
- Document derived/lease expiry as the blessed pattern for passive
  time-based rules. (Likely a `docs/concepts.md` addition.)
- Feed `docs/maybe-later.md` with the **scheduled/delayed command**
  primitive as the candidate for active timed action, weighed
  against the "no push / polling" stance and the design tension of
  reintroducing wall-clock time into a store that today knows only
  causal/commit order. Keep it optional and at the edge if added.
- Note fan-out/broadcast as a separate, lower-priority candidate.

**Status.** Open but *narrowed*. MVP uses derived expiry and needs
no primitive. Re-evaluate when the waitlist (active trigger) is
picked up — that's the honest test of whether a scheduled-command
primitive earns its place.

---
