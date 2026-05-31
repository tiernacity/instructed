# Plan: A more complex example application

> **Status:** living document — requirements gathering / domain exploration.
> Nothing here is committed. We are deciding (1) the domain, (2) the
> technology stack, (3) the deployment scenario, then producing a task list.

## 1. Why we're doing this

We have a working CQRS/ES library (Postgres-resident contract + thin
TypeScript SDK), a CLI (`instructedctl`), conformance + soak test
suites, and one small worked example (`bank-account`). The next thing
we want is a **larger, more demanding example application**. The aims,
restated from the kickoff:

1. **Exercise the SDK on a harder domain** and surface modelling
   pain-points — places where the SDK fights the domain, or where
   concise domain logic is hard to express.
2. **Assess the conciseness goal.** The SDK was designed to encourage
   concise, readable domain logic (pure `apply`/`execute`,
   `routeFn`/`handle`, no I/O in handlers). Does that hold up when the
   domain gets messy?
3. **Build confidence in robustness/consistency** under a scenario
   genuinely harder than `bank-account` — more aggregates, more
   sagas, real contention, real read-your-writes.
4. **Deploy in a realistic setting** — a mix of serverless and
   long-running server processes — and learn how well `instructed`
   supports each. We use Cloudflare a lot, so it's an obvious target.
5. **Realistic load testing** with many concurrent users — understand
   real-world latency and throughput, especially given our workers
   are polling processes (low lock contention by design, but polling
   has a latency floor).

The `bank-account` example is good but it is a *finished, minimal*
artifact: two aggregates, one PM, two projections. It doesn't stress
contention, doesn't model time, and runs as local foreground
processes. This new example is meant to be the opposite end: messy,
contended, time-aware, and deployed.

## 2. What the SDK gives us to model with (recap)

So the domain discussion stays grounded in what the SDK actually
offers (from `docs/concepts.md` and the `bank-account` example):

- **Aggregates** — `{ type, initialState, apply(state, event),
  execute(state, command) }`. Pure. OCC retry on append conflict.
  One stream per instance (`Account-<id>`).
- **Commands** — plain data with a discriminant `type`. A
  `commandRouter` maps command type → `{ aggregate, id(cmd) }`.
- **Events** — plain data; `data`/`metadata` JSONB; `event_number`
  (global, gapless), `stream_version`, causation/correlation.
- **Projections** — `{ type, stream, routeFn, handler(event) }`.
  Partitioning is `sequential` | `per-event` | `per-key`. Handler
  owns its own read store (no DB handle injected). At-least-once →
  handlers must be idempotent.
- **Process managers** — `{ type, stream, routeFn, initialState,
  apply, handle }`. `handle` returns `{ commands?, complete? }`.
  Snapshot-persisted state per partition. PMs *are* the saga
  primitive — no saga DSL.
- **Consistency waits** — `dispatch(..., { consistency: ["Proj"] })`
  blocks until named subscriptions catch up (read-your-writes).
- **Workers** — routing worker (single-active per subscription,
  per-batch lease) + N processing workers (parallel, per-item
  lease), all **polling**. No push, no `LISTEN/NOTIFY`, no
  coordinator service.
- **No scheduler.** The contract has no timers/delays. Time-based
  behaviour splits in two (see §5.3 / L-0003): **passive expiry**
  needs no primitive — carry time as a decision input and derive
  "expired" as a fact; only **active timed action** (something must
  *happen* at a time with no triggering command) needs external
  infrastructure. **A modelling pressure-point we deliberately
  exercise.**

## 3. Properties the domain should have

To meet the aims, the chosen domain should naturally produce:

- **Several aggregate types** (4–6) with non-trivial invariants, not
  just balance arithmetic.
- **Multiple process managers**, including at least one with
  **compensation/rollback** and at least one with **time-based**
  behaviour (expiry/timeout).
- **Multiple projection shapes** — at least one `sequential`, one
  `per-key`, one that filters heavily (`routeFn` → `ignore`), and one
  expensive/denormalised read model for a dashboard.
- **Genuine write contention** — "hot" aggregates many users hit at
  once, to stress OCC retry (this is the load-test centrepiece).
- **Read-your-writes** moments where `consistency:` waits matter and
  their latency is visible to a user.
- **A natural "many concurrent users" event** — a moment where load
  spikes (a flash sale, an on-sale, a surge) so load tests have a
  realistic shape, not just uniform traffic.

## 4. Domain options considered

| Domain | Aggregate variety | Contention | Time/expiry | Saga depth | "Many users" story | Relatability |
|---|---|---|---|---|---|---|
| **Event ticketing / on-sale** | High (Event, Section/Inventory, Hold, Order, Payment, Customer) | **Very high** (everyone wants the same seats) | **Strong** (holds expire) | Deep (checkout, refund, waitlist) | **Excellent** (on-sale moment) | High |
| Food delivery / ride-hailing | High (Order, Courier, Restaurant, Dispatch) | Medium (courier matching) | Strong (timeouts, ETAs) | Deep (matching, delivery, cancellation) | Good (dinner rush) | High |
| E-commerce order fulfilment | High (Cart, Order, Inventory, Payment, Shipment) | Medium (inventory on popular SKUs) | Medium (payment timeout) | Deep (checkout, fulfilment, returns) | Medium (Black Friday) | High |
| Seat/desk/room booking | Medium | High (popular slots) | Strong (holds) | Medium | Medium | Medium |
| Trading / order book | Medium | Very high | Low | Low (matching) | Good | Lower (niche) |

### Recommendation: **Event ticketing with a live on-sale**

It scores highest across every axis we care about, and the "on-sale"
moment is the single best load-test shape we could ask for: thousands
of users converging on a small, fixed pool of seats at a known instant.
That converts directly into:

- **OCC stress** — the seat-inventory aggregate(s) are the hottest
  objects in the system; many concurrent `Hold`/`Release` commands
  serialise through OCC retry. This is exactly the "real-world
  latency/throughput under contention" we said we want to measure,
  and it complements the soak harness (which fuzzes invariants but
  doesn't model a realistic hot-key distribution).
- **Time modelling** — seat **holds expire** after N minutes. There
  is no timer in the contract, so we *must* design an expiry
  mechanism. That will surface a real pain-point and probably a
  reusable pattern — derived/lease expiry, §5.3. Great signal for
  the library.)
- **Compensation** — a checkout saga that holds → charges → confirms,
  and **releases the hold** if payment fails or times out. Real
  rollback, not the bank-account "nothing to undo" case.
- **Read-your-writes** — "did I get the seats?" must reflect the hold
  immediately → `consistency:` wait on an availability projection,
  with user-visible latency.
- **Projection variety** — live availability map (hot, `per-key` by
  section), order history (`per-key` by customer), revenue/sales
  dashboard (denormalised, expensive), waitlist queue.

## 5. Proposed domain model (ticketing) — first cut

> Draft for discussion. Names and boundaries are negotiable.

### Aggregates

- **`Event`** *(the show, not an ES event — naming TBD; maybe `Show`
  to avoid the clash)* — lifecycle: announced → on-sale → closed.
  Holds metadata, on-sale time, total capacity references.
- **`Venue`** — owns the seating model for the shows held in it.
  **A venue's `seatingMode` selects the inventory granularity** (see
  below), so one example covers both contention regimes by hosting
  shows in different venue types. Decided: model both.
- **Inventory** — the hot path, modelled one of three ways per venue.
  Invariant in all three: `held + sold ≤ capacity`. Commands stay
  uniform at the boundary (`HoldSeats`, `ReleaseHold`, `ConfirmSale`);
  events `SeatsHeld` / `HoldReleased` / `SeatsSold` / `HoldRefused`.
  The **granularity spectrum**:
  - **(a) Counted `Section`** — one counter aggregate per section
    (`capacity/held/sold`), no seat identity. **Maximum contention on
    one hot aggregate — the pure OCC stress test.** `HoldSeats {
    sectionId, quantity }`.
  - **(b) Per-section `SeatMap`** *(optional middle)* — one aggregate
    owns every seat in a section: seat *identity* with section-level
    *contention*. Isolates "seat identity" from "contention
    granularity" as an experimental variable. `HoldSeats {
    sectionId, seatIds }` is still one command on one aggregate.
  - **(c) Per-seat `Seat`** — each seat is its own aggregate.
    Realistic, low per-seat contention — **but holding N specific
    seats touches N aggregates, and one command = one aggregate =
    no atomic multi-seat hold.** Holds must go one seat at a time
    and **compensate (release) on partial failure**. This is a real
    saga and a deliberate stress on the SDK's one-command-one-
    aggregate boundary.
  - **The design challenge / conciseness signal:** keep `Order`,
    `CheckoutSaga`, and the `Availability` projection **uniform**
    across (a)/(b)/(c), hiding granularity behind the hold command
    surface. How cleanly that's achievable is a primary finding.
    Venue `seatingMode` ∈ `{ counted, seatmap, seated }` chooses the
    backing aggregate at routing time.
- **`Order`** — a customer's purchase: pending → paid → confirmed →
  cancelled/refunded. Ties together holds across sections + payment.
- **`Payment`** — authorise → capture → fail → refund. (Simulated
  gateway with injectable latency/failure for load tests.)
- **`Customer`** — registration, contact, purchase limits.
- *(Maybe)* **`Waitlist`** — per show/section queue when sold out.

### Process managers

- **`CheckoutSaga`** — `OrderPlaced` → `HoldSeats` (each section) →
  on all held, `AuthorizePayment` → on captured, `ConfirmSale` +
  `ConfirmOrder`; on payment failure, `ReleaseHold` + `CancelOrder`.
  Hold *expiry* needs no saga step — it's a derived fact (§5.3): an
  unconfirmed hold simply stops counting after `expiresAt`.
  (Compensation path; the timeout is passive.)
- *(No `HoldExpiry` PM.)* Superseded by derived/lease expiry (§5.3)
  — there is nothing to *do*, so there is no process to run.
- **`WaitlistFulfilment`** *(stretch)* — the first feature that needs
  an **active** timed trigger: when a hold lapses, *offer* the freed
  seat to the next in queue (time-boxed offer → another expiry).
  Because a passive lapse emits no event, this needs an external
  trigger (the real L-0003 gap). Deferred with the rest of waitlist.
- **`RefundSaga`** — `OrderRefundRequested` → `RefundPayment` →
  `ReleaseSeats`. (Second compensation path.)

### Projections

- **`Availability`** — live seats-remaining per section. Hot,
  `per-key` by section, the read-your-writes target.
- **`OrderHistory`** — per customer, `per-key` by customer.
- **`SalesDashboard`** — denormalised revenue/sold/held per show;
  expensive, `sequential`; the "operator console".
- **`WaitlistView`** — queue positions.
- **`AuditLog`** — `$all`, near-passthrough; demonstrates a cheap
  catch-all projection.

### Time / expiry mechanism (the interesting bit)

**The problem.** A hold placed at `T` must stop counting against
capacity at `T+TTL` if not confirmed. The contract has **no
scheduler/timer**, the core is **pure** (no clock reads whose value
matters), and workers **poll**.

There are two *principally different* framings:

- **(I) Expiry as an action** — something periodically *makes* a
  hold expire (a scheduled task / sweeper dispatches `ReleaseHold`).
  Extra infrastructure; treats expiry imperatively.
- **(II) Expiry as a derived fact (lease / soft-state)** — a hold
  carries `expiresAt`; "expired" is simply `now > expiresAt`. Nobody
  *does* anything; expiry is computed wherever a decision or read
  needs it. **Recommended.**

**Why (II) is the right fit for ES — time as a decision *input*.**
Purity is preserved by *carrying* timestamps, not reading a clock
in the core:

- **Create:** the edge stamps `expiresAt = now + TTL` onto the
  `SeatsHeld` event — immutable, recorded, deterministic. (Could
  equivalently derive from the event's recorded `created_at + TTL`.)
- **Decide:** the `HoldSeats` command carries its decision-time
  `now`; `execute(state, cmd)` computes `available = capacity −
  sold − Σ(qty for holds where expiresAt > cmd.now)` and decides.
  Pure: same state + same command → same result.
- **Hydrate (`apply`):** accumulates `{holdId, qty, expiresAt}`
  facts; never deletes expired holds (that would need a clock).
  "Expired" is computed lazily at decision/read time.

The **only** clock reads are at the **edge** (stamp `expiresAt`;
stamp decision `now`) — exactly where reading the clock is already
allowed. The pure core never reads time.

**What (II) buys over (I):**

- **No scheduler, no periodic dispatch, no fan-out** — the entire
  L-0003 difficulty *evaporates for expiry*. Capacity is reclaimed
  automatically the instant the next buyer's `execute` sees a
  lapsed hold.
- **Lock-free, always-correct reads** — `Availability` computes
  `capacity − sold − live_holds(query_now)`, filtering by `expiresAt`
  at query time. No expiry events required for read correctness.
- **No sweep-vs-confirm race**; the hot path stays purely
  command-driven — cleaner for load testing.
- **`ConfirmSale` on a lapsed hold** is simply refused
  (`expiresAt ≤ now`) — textbook lease semantics.

**The one real cost: state growth → GC (housekeeping, not
correctness).** The aggregate retains per-hold records (not a bare
counter); expired ones linger until pruned. Bound it by emitting an
opportunistic `HoldsExpired { ids }` on the *next real command*
(piggybacked, not scheduled), which `apply` prunes; cold aggregates
just carry a little dead state (snapshots bound replay anyway). A
low-frequency janitor is optional and never on the critical path.

**Rejected framings (kept for the record).** A per-hold expiry PM
woken by a global tick *cannot work* — routing maps one event → one
partition, so a tick can't fan out to every open-hold partition
(L-0003). A single "clock" PM holding every deadline sidesteps
fan-out but is a hot, ever-growing, serialised state blob. Both are
*action* framings (I); (II) makes them unnecessary.

**The honest caveat — three categories of time-based behaviour.**
Derived expiry covers only the first; the genuine-scheduler residue
is narrow (see the revised L-0003):

1. **Passive expiry (derived fact)** — "the hold no longer counts
   after `expiresAt`." Solved by (II); no infrastructure.
2. **Client-side timed reaction** — "tell *this user* their hold
   lapsed"; countdown UI. The client already holds `expiresAt`, so
   it runs a local timer. **No server scheduler.** (Most
   user-facing "reminders about your own thing" land here.)
3. **Server-originated active action** — must *happen* server-side
   at a time, with no online actor and no triggering command:
   **offer a freed seat to the waitlist**, email a reminder 24h
   before the show. *Some* of these can be handled opportunistically
   (piggyback on the next passing command) at the cost of timing
   precision; the irreducible residue ("fire at a precise time with
   nothing else to ride on") genuinely needs framing (I) / a
   scheduler.

This maps onto scope: **MVP hold expiry = (1), no scheduler; the
client handles its own (2); only the stretch waitlist reaches (3).**
See the revised L-0003 in `example-app-lessons.md`.

### Hold command surface — uniformity across seating modes

The conciseness test (§4 decision): keep `Order`, `CheckoutSaga`,
and `Availability` from branching on `Venue.seatingMode`. Findings
from the model so far:

- **Uniform at the event + read-model layer.** All three inventory
  representations emit the **same event family** — `SeatsHeld`,
  `HoldReleased`, `SeatsSold`, `HoldRefused` (carrying `holdId`,
  `sectionId`, and either `qty` or `seatIds`). So `Availability`,
  `OpenHolds`, and the `Order` aggregate (which tracks holds
  abstractly by `holdId`) **do not branch on mode**. Good signal.
- **The saga necessarily branches — once.** The command router maps
  `commandType → { aggregate, id(cmd) }`; it can't send one
  `HoldSeats` to *either* `Section` or `Seat` based on mode without
  the mode leaking into the command. So `CheckoutSaga` is where the
  branch lives: it knows the venue and decides what to dispatch.
- **(a) counted and (b) seat-map collapse to "one aggregate per
  section".** Both are a single aggregate owning the section's
  inventory, so the saga dispatches **one** `HoldSeats { sectionId,
  selection }` per section and gets one result. Uniform.
- **(c) per-seat is the deliberate break.** Here holding N seats
  touches N `Seat` aggregates with no atomic multi-aggregate
  command, so the saga must **fan out N holds and gather**, then
  **compensate (release the partial set) if any seat is taken**.
  This is the multi-aggregate sub-saga the example exists to
  stress, and the explicit "here's what full granularity costs".

**Design lean:** treat (a) counted and (b) seat-map as the
*uniform* representations (one aggregate per section) and (c)
per-seat as the explicit contrast experiment. That keeps the MVP
saga simple (b is stretch anyway) while still letting us measure
the cost of full per-seat granularity. We'll judge whether the
single saga branch reads as "concise" or "ceremony" once written
(a primary §8 finding).

## 6. Technology choices

### Application framework

- **Next.js (App Router)** — gives us UI + server actions / route
  handlers for command dispatch out of the box, and deploys to
  Cloudflare via **`@opennextjs/cloudflare`** (OpenNext). This is the
  "lot of what we need out of the box" the kickoff mentioned.
- UI surfaces: (1) a public on-sale/checkout flow (the load-test
  surface); (2) an operator dashboard (the expensive-projection
  surface); (3) a **simple admin UI for seeding** — create venues
  (both seating modes), shows, and inventory, and mint synthetic
  customers. The admin UI dispatches the same domain commands as the
  rest of the app; it exists because seeding is *domain*-specific
  and so can't live in `instructedctl`.

### Database

- **Postgres** is mandatory (it *is* the contract). **Decided: Neon**
  (serverless Postgres). For local dev we reuse the existing
  `docker-compose.yaml` Postgres.
- **Hyperdrive applies only to Cloudflare-hosted compute.** It pools
  + caches connections *from Cloudflare Workers*. So it's relevant to
  the **Next.js dispatch path** (on Workers via OpenNext) → Neon.
  The **polling workers do not run on Cloudflare** (see Deployment:
  they run on Fly/Railway/containers), so they connect to Neon
  **directly via Neon's own pooler (PgBouncer)** — Hyperdrive is not
  in that path.
- **Connection model is a key finding area.** The SDK uses `pg` Pools
  and long-lived connections for the polling workers (Neon pooler);
  serverless dispatch wants short-lived/pooled connections
  (Hyperdrive). Compare: Hyperdrive vs. Neon pooler vs. direct.

### Database provisioning & operations — dogfood `instructedctl`

**Decided: use `instructedctl` for every store-lifecycle and
inspection task the deployment needs**, to exercise the tool in a
real setting (it's a Deno single static binary that connects
*directly* to Postgres — ideal for a deploy/CI step; it talks to Neon
via the direct connection string, *not* Hyperdrive).

- **Init.** `instructedctl schema install` against Neon initialises
  the `instructed` schema on first deploy. `schema status` /
  `schema version` as a post-deploy smoke check.
- **Migration — known gap.** `schema migrate` is **not implemented**;
  the only "upgrade" today is `schema install --force`, which drops
  the schema CASCADE and **destroys all data**. For a deployed
  example that we re-provision freshly this is tolerable, but it's a
  real gap a production operator would hit the moment `instructed`
  ships a schema bump. Logged as **L-0002** in
  `example-app-lessons.md`; the example is a good forcing function
  to design migration properly.
- **Operations + load-test analysis.** Reuse the inspection groups
  as a *server-side* complement to k6/OTel: `subscriptions`
  (cursor lag), `work-items` (state counts / backlog), `snapshots`,
  and `health` during and after on-sale runs. This dogfoods the
  read surface and gives us lag/backlog numbers the client-side
  load test can't see.

### Load testing & load generation

**The realism problem.** There is no pre-existing user base. Realism
comes not from real humans but from three things we model explicitly:
the **arrival process**, the **per-user behavioural state machine**,
and the **demand skew**. k6 handles all three; it is flexible enough
(scenarios, custom metrics, thresholds, `SharedArray` for
parametrised data, `setup`/`teardown`).

- **Arrival process — use an open model.** k6's
  `ramping-arrival-rate` executor injects iterations at a target rate
  *regardless of system response time* — exactly an on-sale: users
  arrive at T0 whether or not the system keeps up; a slow system
  builds backlog / sheds users. (A closed model — "N users looping"
  — would wrongly throttle arrivals when the system slows. Wrong
  shape for a flash sale.)
- **Behavioural state machine (the journey).** Each virtual user:
  list shows → check availability → attempt hold (may be refused →
  retry other seats or abandon) → checkout → pay (gateway
  latency/failure injected) → **poll order status until
  confirmed/cancelled**. The poll-for-outcome step is meaningful in
  itself: it's what a real client does against an eventually-
  consistent system, so we measure it (and compare against a
  `consistency:`-wait variant that makes the step synchronous).
- **Demand skew.** Zipf-ish distribution so a few shows are hot and
  front sections hotter than back. This is what *produces* OCC
  contention; uniform traffic would not.
- **Seeding (the missing-users answer).** Before a run, seed venues
  (both seating modes), shows, seat inventory, and a synthetic
  customer pool. **Decided: a simple Next.js admin UI** drives
  seeding via the domain command surface. *Not* `instructedctl` —
  that tool is `instructed`-specific (streams, subscriptions, work
  items, snapshots) and deliberately domain-agnostic; seeding
  ticketing data is a domain concern that belongs in the app. (k6
  doesn't seed either; it drives the on-sale through HTTP.)
- **k6 caveat.** k6 is not Node (own JS runtime, no arbitrary npm).
  We drive the system through the app's HTTP surface (desired) and
  keep DB seeding in a separate Node tool. Healthy separation.
- **Scenarios:** (1) on-sale spike (flagship, `ramping-arrival-
  rate`); (2) steady-state browse/buy background load; (3) endurance
  soak at moderate rate to watch projection lag + worker stability
  over time.
- **Capture:** dispatch latency, OCC-retry counts, hold-refusal
  rate, projection catch-up latency, read-your-writes wait time,
  end-to-end "got my ticket" time.

### Observability

- The SDK already emits via a configurable logger. Add OpenTelemetry
  traces around dispatch, OCC retries, worker poll loops, and
  consistency waits so the load-test findings are attributable.

## 7. Deployment scenarios (the core experiment)

The tension we most want to study: **command dispatch fits serverless;
polling workers do not (cleanly).** Key realisation: the workers'
only dependency is **a Postgres connection** — they hold no other
state and need no coordinator — **so they can run literally
anywhere** that can reach Neon. That makes breadth cheap. We lean
**breadth** and deploy the same app across several worker targets to
compare.

```
   Browser / k6
       │  HTTPS
       ▼
  ┌─────────────────────────┐
  │  Next.js on Cloudflare  │   command dispatch path  ── serverless
  │  (Workers, OpenNext)    │   (request/response, short-lived conns)
  └───────────┬─────────────┘
              │ Hyperdrive (CF-only path)
              ▼
        ┌───────────┐
        │  Postgres │  (Neon)  ◀── the instructed contract lives here
        └─────┬─────┘
              │ polling (Neon pooler, direct — NOT via Hyperdrive)
   ┌──────────┴───────────────────────────────┐
   │  WORKER PATH — "database only", runs anywhere │
   │  A. long-running container (baseline)     │  Fly.io / Railway / VM
   │  B. Cloudflare Cron Trigger + Worker      │  serverless polling
   │  C. Durable Object + alarm() loop         │  (likely NOT suitable)
   └───────────────────────────────────────────┘
```

### Worker deployment variants to compare

- **A — Long-running container (baseline). Lead target.** A normal
  Node process running `app.startWorker()` on **Fly.io / Railway**
  (accounts available). This is the shape the SDK was designed for
  and gives the latency/throughput reference numbers. Because the
  worker is "Postgres-only", the same image runs on any of
  Fly/Railway/a VM unchanged — breadth is nearly free here.
- **B — Cloudflare Cron + Worker.** A scheduled Worker that, on each
  invocation, drains the queue for a bounded time then exits. Tests
  "serverless polling". **Findings to expect:** Cron min interval is
  coarse (~1 min) → high latency floor; overlapping invocations
  exercise the routing-worker **per-batch lease** (good lease test);
  Worker CPU/wall limits bound how much each tick can drain.
- **C — Durable Object + `alarm()`. Probably NOT a good fit —
  investigate, don't assume.** DOs target coordination/stateful-
  edge workloads, not steady DB-polling loops; nothing in
  Cloudflare's docs/examples points this way, and per-DO billing +
  the `alarm()` cadence make a continuous poller awkward. Keep as a
  spike to confirm/deny, low priority. The single-active angle
  (one DO = natural single-active) is the only thing that makes it
  interesting; the container already gets single-active from the
  per-subscription lease.

Given the workers run anywhere, the **interesting contrast** is
**A (long-running) vs. B (serverless cron)** — i.e. the polling
latency-floor and lease behaviour of an ephemeral-invocation worker
vs. a continuous loop. That's the comparison worth investing in.

### What each variant should teach us

- Real dispatch latency (serverless cold/warm) and how Hyperdrive
  affects it.
- Polling latency floor per variant and its effect on read-your-writes
  (`consistency:` wait time).
- Lease behaviour when "workers" are ephemeral invocations rather than
  long-lived loops (the per-batch lease from D-0025 should make this
  benign — confirm it).
- Throughput ceiling under the on-sale spike per variant, and OCC
  retry amplification on the hot `Section` aggregate.
- Operational ergonomics: how painful is each to deploy/observe?

## 8. What we want to learn (explicit findings checklist)

- [ ] Modelling pain-points: where did the pure-core / no-I/O-in-handlers
      rules fight the domain? (expiry, cross-aggregate invariants,
      multi-section holds.)
- [ ] Conciseness: lines of domain logic vs. accidental ceremony
      (command router, event/type duplication, `onlyTypes` boilerplate).
- [ ] Time modelling: which expiry pattern won, and should the library
      offer a primitive? (feeds `docs/maybe-later.md`.)
- [ ] OCC under realistic hot-key contention: retry distribution,
      tail latency, throughput on the `Section` aggregate; per-seat vs.
      per-section counter trade-off.
- [ ] Polling latency floor across deployment variants; impact on
      read-your-writes UX.
- [ ] Lease robustness with ephemeral serverless workers.
- [ ] Connection management: Hyperdrive / pooler / direct for both
      dispatch and worker paths.
- [ ] Any new SQLSTATEs / error-translation gaps the SDK hits.
- [ ] `instructedctl` ergonomics for operating a real deployment
      (inspecting streams, subscriptions, work items, snapshots).
- [ ] `instructedctl schema install` as the deploy-time init step;
      and the **`schema migrate` gap** — what a schema upgrade against
      a live store should look like (L-0002).

## 9. Open questions (to resolve before building)

1. ~~**Domain confirmed?**~~ **Resolved (2026-05-31): ticketing/on-sale.**
2. ~~**`Section` granularity**~~ **Resolved (2026-05-31): model both,
   selected per `Venue.seatingMode`** — counted (a) and per-seat (c) as
   the two poles, per-section seat-map (b) optional. Keeping the upper
   layers uniform across modes is a primary finding.
3. ~~**Naming**~~ **Resolved (2026-05-31): `Show`** for the concert
   event (avoids the ES-`Event` clash). Also surfaced a broader
   SDK lesson — see `example-app-lessons.md` **L-0001** (SDK type
   names aren't namespaced).
4. **How real is payment?** Simulated gateway with injectable
   latency/failure is enough for load testing and saga compensation.
5. ~~**Deployment scope**~~ **Resolved (2026-05-31): lean breadth.**
   Workers are "database-only" so run anywhere; lead with **A**
   (Fly/Railway container), add **B** (CF Cron) for the
   serverless-polling contrast. **C** (Durable Object) is a
   low-priority spike — probably unsuitable, confirm don't assume.
6. ~~**Where does Postgres live?**~~ **Resolved (2026-05-31): Neon.**
   Hyperdrive is only in the Cloudflare-hosted dispatch path; the
   off-Cloudflare workers use Neon's pooler directly.
7. ~~**Repo location**~~ **Resolved (2026-05-31):
   `examples/typescript/ticketing/`.**
8. ~~**Scope discipline / MVP slice**~~ **Resolved (2026-05-31).**
   **MVP slice:** one venue of *each* seating mode (counted + per-seat)
   · `Show`, section inventory (counter / seat-map / per-seat),
   `Order`, `Payment` (simulated) aggregates · **`CheckoutSaga`**
   (hold → pay → confirm, with release-on-failure compensation) ·
   **derived/lease hold expiry** (§5.3, no scheduler) · **`Availability`**
   projection (read-your-writes target) + a minimal order-status
   read · admin/seeding UI + on-sale UI · k6 on-sale on **deployment
   A**, provisioned via `instructedctl`.
   **Stretch (out of MVP):** waitlist, refund saga, sales dashboard,
   per-section seat-map mode (b), deployment B/C.

## 9b. Prerequisite improvements (surfaced; do before/alongside)

The example exists to surface improvements — so we get on with the
blocking/ideally-first ones rather than working around them. Live
list here (example-scoped); **promote each to `TODO.md`** when we
commit to it (TODO.md is the stably-numbered canonical backlog).

| # | Improvement | Why first | Lesson | Priority |
|---|---|---|---|---|
| P1 | **SDK type/module namespacing** — ✅ **done (D-0028)**: namespace-import guidance in README + `concepts.md`; reserved key namespaced to `$instructed.snapshot_module_version` across SDK + contract docs + porting checklist; no sweeping renames. | Audit found structural typing already insulates the app from the worst (`Event`/`Command`) collision; the metadata key was the genuine residual. Suites green. | L-0001 | ~~Preferred-first~~ **Done** |
| P2 | **Non-destructive, idempotent `instructedctl` init** — an "ensure installed; no-op if present; never destructive" mode (today `install` errors if present and `--force` drops all data). | Needed for repeatable CI/deploy provisioning of Neon. Distinct from — and far smaller than — full `schema migrate`. | L-0002 | **Preferred-first** (blocker for the deploy automation lane, task 6) |

> Full forward-only `schema migrate` (L-0002) is the larger
> follow-on, **not** a prerequisite — the example provisions fresh.
> P2 is just the idempotent-init slice of it.

## 10. Proposed task list (draft — sequence once scope slice is locked)

0. **Prerequisites (§9b):** ~~P1 SDK namespacing~~ ✅ done (D-0028);
   P2 idempotent `instructedctl` init still to do.
1. Lock domain + model (this doc → "Decisions"). *(domain + MVP
   slice locked, §9.)*
2. Write the domain core (aggregates, commands, events, PMs,
   projections) as a *library-only* package, runnable locally against
   the docker-compose Postgres — mirrors `bank-account` layout. No UI
   yet. Prove the model with a scripted on-sale.
3. Decide + implement the expiry mechanism; document the friction.
4. Add the Next.js app: on-sale/checkout UI, operator dashboard,
   **admin/seeding UI**, and dispatch route handlers.
5. Add observability (OTel) and the k6 load script + Node seeding tool.
6. Provision Neon with `instructedctl schema install`; deploy variant
   A (Fly/Railway container worker + Next on Cloudflare w/ Hyperdrive
   on the dispatch path). Reference numbers. Use `instructedctl`
   inspection groups for server-side lag/backlog during runs.
7. Add variant B (CF Cron); compare A vs. B. Spike C (DO) only to
   confirm/deny suitability.
8. Write up findings → feed `example-app-lessons.md`, then promote to
   `TODO.md` / `docs/maybe-later.md` / `docs/decisions.md`.

## 11. Decisions log

> Append decisions here as we make them (date — decision — rationale).

- **2026-05-31 — Domain: event ticketing with a live on-sale.**
  Best fit across aggregate variety, contention, time/expiry, saga
  depth, and a natural "many concurrent users" shape.
- **2026-05-31 — Model both inventory granularities, chosen per
  `Venue.seatingMode`.** Counted-section (max OCC contention) and
  per-seat (multi-aggregate holds + compensation) as the two poles;
  per-section seat-map as an optional middle. Upper layers (`Order`,
  `CheckoutSaga`, `Availability`) stay uniform across modes.
- **2026-05-31 — Load generation: k6, open-model arrival
  (`ramping-arrival-rate`), behavioural-journey VUs, Zipf demand
  skew.** Realism comes from the modelled arrival process + journey
  + skew, not real users.
- **2026-05-31 — Naming: the concert event is `Show`.** Avoids the
  clash with the ES `Event` type. Surfaced lesson **L-0001**
  (`example-app-lessons.md`): SDK type names aren't namespaced.
- **2026-05-31 — Seeding via a simple Next.js admin UI, not
  `instructedctl`.** Seeding is a *domain* concern; `instructedctl`
  is deliberately domain-agnostic (operates on streams,
  subscriptions, work items, snapshots only).
- **2026-05-31 — Postgres: Neon. Hyperdrive only on the
  Cloudflare-hosted dispatch path; off-Cloudflare workers use
  Neon's pooler directly.**
- **2026-05-31 — Deployment: lean breadth, workers are
  "database-only".** Lead with container (Fly/Railway), add CF Cron
  for the serverless-polling contrast; Durable Object is a
  low-priority confirm/deny spike (likely unsuitable).
- **2026-05-31 — Repo location: `examples/typescript/ticketing/`.**
- **2026-05-31 — Dogfood `instructedctl` for deployment.** Use
  `schema install` for init and the inspection groups
  (`subscriptions`/`work-items`/`snapshots`/`health`) for ops +
  load-test analysis. Surfaced the migration gap — lesson **L-0002**.
- **2026-05-31 — Hold expiry: derived fact (lease / soft-state),
  §5.3.** Carry `expiresAt` on the event + decision-time `now` on
  the command; the pure core derives "expired"; no scheduler, sweep,
  or fan-out for the hot path; GC is opportunistic housekeeping.
  Reverses the earlier "edge sweep" lean. **Narrowed** lesson
  **L-0003**: passive expiry needs no primitive; only *active timed
  action* (e.g. stretch waitlist offers) is a real gap.
- **2026-05-31 — Seat-mode uniformity (§5.3).** Shared event family
  keeps `Order`/`Availability`/`OpenHolds` mode-agnostic; the saga
  branches once. (a) counted + (b) seat-map collapse to one
  aggregate per section; (c) per-seat is the explicit
  multi-aggregate + compensation contrast.
- **2026-05-31 — MVP slice locked (§9.8)** and **prerequisites P1/P2
  identified (§9b)**, to be promoted to `TODO.md` when started.

> A running log of SDK/contract lessons from this exercise lives in
> [`example-app-lessons.md`](./example-app-lessons.md).
```
