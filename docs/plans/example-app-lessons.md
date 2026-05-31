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

**Recommendation (to decide, not yet committed).**
- **Audit the public type surface** for names that double as common
  domain nouns: `Event`, `Command`, `RecordedEvent`, `Aggregate`,
  `Projection`, `ProcessManager`, `Subscription`, `Snapshot`.
- **Prefer names the application is unlikely to want.** Options,
  roughly in increasing intrusiveness:
  1. Document the alias pattern and leave names as-is (cheapest;
     doesn't fix the root issue).
  2. Rename the collision-prone exports to qualified names
     (`StoredEvent`/`RecordedEvent` already exists; consider making
     `RecordedEvent` the canonical one and dropping/renaming the
     bare `Event`; similarly an aggregate-`Command` could be a more
     specific noun).
  3. Encourage namespace import in docs (`import * as es from
     "instructed-sdk"` → `es.Event`), so the bare names never enter
     the application's flat namespace. This is idiomatic and keeps
     the SDK names short *and* unambiguous at the use site.
- **Namespace the reserved metadata key.** Give it a reserved
  prefix that an application would never choose, e.g.
  `instructed.snapshot_module_version` or `__instructed_snapshot_
  module_version`. Coordinate the change across the porting
  checklist (SNAP-002) since the key string is part of the
  cross-port contract.
- **Document the namespacing posture explicitly** for SDK porters:
  "store-layer reserved names use the `$` sigil / `instructed`
  schema / `IS` SQLSTATE class; language SDKs should keep core type
  names out of the application's flat namespace (namespace import)
  and prefix any reserved metadata keys."

**Status.** Open. Captured here as the first lesson. Decide the
posture (likely: recommend namespace-import in docs *and* namespace
the reserved metadata key) before the second SDK port hardens the
type names. Cross-reference from `TODO.md` if/when promoted.

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
