# Working plan — bottom-up SDK / schema cleanup

Status: **not started** (this doc written after a spike that was reverted).
Owner: rolling — designed to be picked up across multiple sessions.

This is a living document. After each slice: tick the box, note the
commit hash, and record anything that surprised you under "Session log".

---

## 0. Why this exists / terms of reference

We are working **bottom-up** on the TypeScript SDK and the SQL contract
it wraps. The original brief (verbatim intent):

1. Review the core `instructed` SQL schema and stored procedures in
   detail. List tables/columns that are unused or were added
   speculatively — candidates for removal. (There was early churn,
   notably the subscription-tracking model.)
2. List stored procedures that are no longer used (functionality
   replaced by others) — candidates for removal.
3. List opportunities to rationalise/centralise functionality that is
   duplicated across different logic.
4. If we change/remove anything in the schema, adapt the **lowest SDK
   layer (`core`)** accordingly and remove anything unused.
5. The TS SDK package is poorly structured. We want the **layers** and
   the **discrete functional categories** reflected in the
   directory/module structure:
   - modules above a size/complexity threshold live in their own
     directory with a **barrel `index.ts` that contains nothing but
     exports**;
   - each class/function in its own file named for it (closely-related
     utilities may be grouped);
   - types in their own files (`types.ts`, or `types/index.ts` +
     `types/<typename>.ts` when there are many).

**Hard constraint adopted after the spike:** no big-bang. Work in
small slices; commit after each; every commit leaves all test suites
green.

---

## 1. Decisions already taken (do not re-litigate)

| Topic | Decision | Rationale |
|---|---|---|
| `streams.created_at`, `subscriptions.created_at` | **KEEP** | Common, cheap, might want later. |
| `shard` column (on `subscriptions` and `subscription_work_items`) | **REMOVE everywhere** | Speculative (ML-0013); `partition_key` is what we actually use. Threaded through ~12 procedures + ~14 SDK methods as dead weight. |
| `subscription_work_items.failed_at` | **KEEP** | Timestamp of failure; useful for the forthcoming `instructedctl` failed-items view. (The boolean is redundant with `state='failed'`, but the timestamp isn't.) |
| `subscription_work_items.error_text` | **KEEP** | The handler error message; **not** derivable from `state`. Exactly what an operator needs to triage a stalled partition. |
| Legacy procedures `read_subscription_batch`, `advance_subscription`, `read_subscription_position`, `extend_subscription_claim` | **REMOVE** (+ Client bindings + conformance tests + doc refs) | Orphaned by the SUB-A model change (per-batch routing worker + work-item queue). Not called by any L2/L3 worker, the facade, or `consistency.ts`. |
| `delete_subscription` | **KEEP** | Admin op (no current consumer, but it's the `instructedctl`-era teardown primitive). Distinct from "legacy/replaced". |
| Conformance tests for removed mechanisms | **OK to delete/rewrite** | The harness was written against the schema; remove what no longer exists. |
| Docs (`docs/*.md`) | **OK to update** | They may contain vestiges of old mechanisms. |

---

## 2. Findings inventory (the review the brief asked for)

### 2.1 Tables / columns

- `streams`: all used except `created_at` (write-only) — **kept by decision**.
- `events`: all used (`causation_id`/`correlation_id` filled by the aggregate; `correlation_id` read by pm-worker).
- `stream_events`: all load-bearing.
- `snapshots`: all used (`source_type`, `metadata` carries `snapshot_module_version`, `created_at`).
- `subscriptions`: `shard` → **remove**; `created_at` write-only → **keep**.
- `subscription_work_items`: `shard` → **remove**; `failed_at`/`error_text` → **keep**.
- `claim_work_item` returns `was_takeover`/`prior_claimed_by` — consumed only for one log line in `processing-worker.ts`. Marginal; **keep**.

### 2.2 Procedures

**Active / keep:** `append_to_stream`, `read_stream`, `read_all`,
`record_snapshot`, `read_snapshot`, `delete_snapshot`,
`claim_subscription`, `release_subscription`, `delete_subscription`,
`route_batch`, `claim_work_item`, `extend_work_item_claim`,
`complete_work_item_projection`, `complete_work_item_pm`,
`complete_pm_instance`, `fail_work_item`, `is_subscription_caught_up`,
`list_pm_rebuild_events`, plus `get_schema_version`,
`raise_append_only`.

**Remove (legacy single-cursor model):** `read_subscription_batch`,
`advance_subscription`, `read_subscription_position`,
`extend_subscription_claim`.

### 2.3 Centralisation opportunities (for the later refactor slices)

SQL — repeated verbatim across the subscription/work-item procedures:
1. input-validation guards (null/empty stream/name/worker/lease/partition/event_number);
2. stream resolution (`select stream_id … ; if not found raise IS003/IS020`);
3. subscription existence check (`if not exists (…) raise IS020`);
4. shard extraction (disappears entirely once `shard` is gone);
5. lease-verify (`select claimed_by for update; verify; raise IS022/IS030`);
6. the recorded-event projection SELECT (read_stream / read_all / read_subscription_batch / list_pm_rebuild_events).

Note: (2) cannot be a single helper — read/append want `IS003`, the
subscription procedures want `IS020` ("no such stream"). (3) is
cleanly uniform across the work-item procedures and is the best first
extraction.

SDK (`client.ts`):
- `const opts = {}; if (options.shard !== undefined) opts.shard = options.shard;` repeated 14× → vanishes with `shard` removal.
- `toBigInt` / `toDate` / `mapRecordedEvent` / `RawEventRow` are reusable row mappers worth their own module.
- the read-event column-list string is duplicated across 4 methods.

### 2.4 SDK structure (the iteration-2 target)

Today `src/` is 24 flat files (only `internal/` grouped). Proposed
tree (barrels = `index.ts`, exports only):

```
src/
  index.ts                 # L1+L2+L3 barrel (currently index.ts)
  core/index.ts            # L1+L2 barrel (currently core.ts)
  types/                   # event, command, expected-version, snapshot,
                           #   subscription, work-item, queryable
  errors/                  # base, append, snapshot, subscription, work-item,
                           #   consistency, map-pg-error
  client/                  # client.ts + row-mappers.ts + pack-event.ts
  aggregate/               # run-command, snapshot-policy, snapshots (L3)
  workers/
    routing/  processing/  projection/  pm/
  consistency/
  facade/                  # instructed.ts, command-router, partition-by, routing-helpers
  logger/
  internal/                # as-is
```
Files over the own-directory threshold: `client.ts` (885),
`instructed.ts` (857), `aggregate.ts` (592), `processing-worker.ts`
(570), `errors.ts` (456), `routing-worker.ts` (410).

---

## 3. Slices

Order: workstream **A** (schema + core logic, bottom-up) first, then
workstream **B** (directory restructure — easier once the module set
is final). Each slice ends green.

### Workstream A — schema + core cleanup

- [x] **A1 — Remove `extend_subscription_claim`.**
      Smallest legacy proc; no SDK worker calls it; cleanest first cut.
      Provider-then-consumer or consumer-then-provider (see §4 ordering note).
- [x] **A2 — Remove `read_subscription_position`.**
      Consumers: `consistency.ts` already uses `isSubscriptionCaughtUp`
      (no change). Soak uses it (see §5 gotcha). Conformance:
      subscription-persistent + smoke + cross-cutting.
- [x] **A3 — Remove `read_subscription_batch` + `advance_subscription`.**
      These two go together (the read/ack pair). Biggest conformance
      churn (the `subscription-persistent.test.ts` delivery + advance
      describe blocks). Re-annotate INV-SUB-P-030/031/032/034 onto the
      surviving `route_batch`/takeover tests (see §5 gotcha).
- [ ] **A4 — Remove `shard`.** ~158 SQL occurrences + SDK + conformance
      + docs. Scripted transform; mind the terminator-semicolon and
      `claimSubscription` opts gotchas (§5). Largest single slice —
      keep it to *just* `shard`.
- [ ] **A5 — (optional) SQL helper extraction.** `_require_subscription`
      existence check first (cleanly uniform), then consider the
      recorded-event SELECT as a view and a shared snapshot-upsert.
      Pure refactor; conformance is SQLSTATE-checked so messages may
      change. Defer if appetite is low.
- [ ] **A6 — (optional) SDK row-mapper consolidation.** Pull
      `toBigInt`/`toDate`/`mapRecordedEvent`/`RawEventRow` and the
      read-event column list into one place. Naturally folds into B3.

A1–A3 could also be done as a single "remove the 4 legacy procs" slice
if a session has the appetite; they are separated here to stay small.

### Workstream B — SDK directory restructure (iteration 2)

Each Bn = mechanical move + a barrel `index.ts` + internal import
fixups; public barrels (`index.ts`, `core.ts`) keep the same exported
surface so nothing downstream changes. Validate with `npm run
type-check` + `npm test`.

- [ ] **B1 — `types.ts` → `types/`.**
- [ ] **B2 — `errors.ts` → `errors/`.**
- [ ] **B3 — `client.ts` → `client/`** (+ `row-mappers.ts`, `pack-event.ts`; folds in A6).
- [ ] **B4 — `aggregate.ts` (+ `aggregate-snapshots.ts`, `snapshot-version.ts`) → `aggregate/`.**
- [ ] **B5 — workers → `workers/{routing,processing,projection,pm}/`** (+ `pm-substrate`, `error-policies`).
- [ ] **B6 — facade + consistency → `facade/`, `consistency/`** (+ `command-router`, `partition-by`, `routing-helpers`, `logger`).

---

## 4. Per-slice recipe (apply to every A-slice that removes a proc)

A removed procedure has consumers in five places. The schema is
reinstalled fresh by both test harnesses (drop schema cascade +
reload `sql/instructed.sql`), so a SQL change is visible to tests
immediately — which is why a single commit must update **all**
consumers, or you split provider/consumer (see ordering note).

Touch-list for a legacy-proc removal:
1. **`sql/instructed.sql`** — delete the function block(s); fix the
   header procedure inventory (~line 47), the lock-set inventory
   comment (~line 440), and any prose cross-references to the proc.
2. **`sdks/typescript/src/client.ts`** — delete the wrapper method(s).
3. **`sdks/typescript/src/types.ts`** — fix the `RecordedEvent` doc
   comment that lists `read_subscription_batch`.
4. **SDK tests** — `test/client.test.ts` (delete/rewrite the cases),
   `test/consistency.test.ts` (the two `readSubscriptionPosition`
   assertions → `isSubscriptionCaughtUp`).
5. **Conformance** — `test/smoke.test.ts` (expected proc-name list),
   `test/cross-cutting.test.ts` (the stream_id proc-name `IN (...)`
   list), `test/subscription-persistent.test.ts` (remove the legacy
   wrappers + their describe blocks; rewrite cursor-dependent tests to
   move the cursor via `route_batch` and read it via a direct
   `subscriptions` SELECT). Re-annotate invariants (§5).
6. **Soak** — `tests/soak/checks.ts` + `tests/soak/soak.ts` (§5).
7. **Docs** — grep `docs/` for the proc name; update
   `sql-contract.md`, `architecture.md`, `invariants.md`,
   `decisions.md`, `maybe-later.md` as hit.

**Ordering note (how to keep each commit green):** two clean options.
- *Atomic*: do all of 1–7 in one commit.
- *Consumer-first / provider-second* (two green commits): first remove
  all **usage** (tests, soak, docs) while the proc + Client method
  still exist (green — nothing calls them); then remove the proc + the
  Client method (green — nothing references them). Prefer this when a
  slice feels too big to hold in one session.

Validation gate for every A-slice:
```sh
# 1. SQL parses + installs into a scratch DB
docker compose exec -T postgres psql -U postgres -d postgres \
  -c "drop database if exists sqlcheck;" -c "create database sqlcheck;"
docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U postgres -d sqlcheck \
  -f - < sql/instructed.sql 2>&1 | grep -i error   # expect: no output

# 2. SDK
cd sdks/typescript && npm run type-check && npm test        # 162 tests

# 3. Conformance
cd tests/conformance && npm test && npm run coverage         # 148 tests, MISSING 0
```
(DB is `docker compose up -d`; it was already up during the spike.)

---

## 5. Gotchas discovered during the spike (read before A4 / A2 / A3)

1. **Terminator semicolons (A4, `shard`).** Several procedures end a
   SELECT/DELETE with `... and shard = v_shard;` — the `;` is the
   statement terminator. Blindly deleting those lines leaves the
   preceding `and subscription_name = p_subscription_name` clause
   unterminated → `syntax error at or near "if"`. After the scripted
   `shard` strip, re-add `;` to the now-last clause. In the spike the
   broken statements were in `claim_subscription` (×3),
   `delete_subscription` (×1), and one work-item proc (×1) — 6 total
   (the other 2 `;`-terminated drops were inside legacy procs already
   removed). Find them with: lines ending `= p_subscription_name`
   whose next non-blank line is NOT a SQL continuation (`and`/`for`/
   `returning`/`)`/…). The `) then` cases are fine (subquery close).
2. **`claimSubscription` opts (A4).** Every other subscription Client
   method builds `opts` only for `shard`; `claimSubscription` also
   carries `start_from`. A blanket `JSON.stringify(opts) →
   JSON.stringify({})` replace silently drops `start_from`. Leave
   `claimSubscription`'s `opts` (built from `startFrom`) intact.
3. **Errors module (A4).** `shard` appears on `SubscriptionError`,
   `WorkItemLeaseLost`, and `MapPgErrorContext` (`errors.ts`), plus the
   `case "IS020/021/022/030"` branches in `mapPgError`. Remove all.
4. **Coverage catalogue (A3).** `tests/conformance/coverage-report.ts`
   hardcodes an INV-* catalogue mirroring `docs/invariants.md`.
   INV-SUB-P-030/031/032/034 are **still live invariants** — they were
   *rewritten* for the SUB-A routing cursor; only their conformance
   *annotations* lived on the deleted legacy tests. Do **not** delete
   the invariants. Instead re-annotate the surviving tests in
   `subscription-work-items-procedures.test.ts`:
   - "inserts decisions and advances the cursor atomically" → add
     `INV-SUB-P-030` + `INV-SUB-P-032`;
   - "cursor advance is monotone (lower target is a no-op)" → add
     `INV-SUB-P-034`;
   - "lease takeover: expired 'claimed' row is re-claimable" → add
     `INV-SUB-P-031`.
   INV-SUB-P-012 and -033 are already covered elsewhere. Target:
   `npm run coverage` reports `MISSING 0`.
5. **Soak harness (A2).** `tests/soak/checks.ts` (3 sites) and
   `tests/soak/soak.ts` (1 site) call `client.readSubscriptionPosition`.
   Replace with a local helper that reads the cursor directly (both
   files already hold a `pg.Pool`):
   ```ts
   async function readCursor(pool: pg.Pool, stream: string, name: string):
     Promise<bigint | null> {
     const r = await pool.query<{ last_seen: string }>(
       `SELECT s.last_seen::text AS last_seen
          FROM instructed.subscriptions s
          JOIN instructed.streams str ON str.stream_id = s.stream_id
         WHERE str.stream_uuid = $1 AND s.subscription_name = $2`,
       [stream, name]);
     return r.rows.length === 0 ? null : BigInt(r.rows[0].last_seen);
   }
   ```
6. **`dist/` is gitignored** — no build step needed for a commit.
7. **`maybe-later.md` ML-0013** legitimately discusses sharded routing
   as a future feature. Don't delete it; reword the forward-compat note
   to say the speculative `shard` column was removed and ML-0013 would
   re-introduce a partition dimension (additive via `p_options` + a
   defaulted migration column).

---

## 6. Spike scope reference (what a full pass touched)

For sizing only — the reverted spike modified: `sql/instructed.sql`;
`sdks/typescript/src/{client,errors,types,routing-worker}.ts`;
`sdks/typescript/test/{client,consistency}.test.ts`;
`tests/conformance/test/{cross-cutting,smoke,subscription-persistent,
subscription-work-items-procedures,subscription-work-items-schema}.test.ts`;
`tests/soak/checks.ts`; `docs/{architecture,decisions,invariants,
maybe-later,sql-contract}.md`. All suites passed at the end — the
revert was for risk/context management, not because it was broken.

---

## 7. Session log

- (write entries here: date, slice, commit hash, surprises)
- 2026-05-29 — **A1** (remove `extend_subscription_claim`). Done
  atomically (one commit, all 7 consumer classes). Touched:
  `sql/instructed.sql` (proc block + header inventory + lock-set
  inventory + 2 prose cross-refs), `client.ts` (method + a
  `readSubscriptionBatch` doc-comment ref), `routing-worker.ts`
  doc comment, SDK `client.test.ts` (2 cases), conformance
  `smoke.test.ts` (proc list) + `subscription-persistent.test.ts`
  (header, `extend` wrapper, 3 heartbeat tests, plus the `extend`
  call inside the INV-SUB-P-011 takeover test and its SP:357
  cross-ref comment), and docs (`sql-contract`, `architecture`,
  `invariants`, `decisions`, `TODO`).
  Surprises / notes:
  * `extend` was used in a *4th* place in subscription-persistent
    (the INV-SUB-P-011 composed-takeover test, line ~770) beyond
    the 3 obvious heartbeat tests — dropped just that one
    assertion; surviving `readBatch`/`release` still cover IS022.
  * INV-SUB-P-012 is also annotated on a non-heartbeat test
    (subscription disconnect), so deleting the 3 heartbeat tests
    left coverage at MISSING 0 as predicted by §5 gotcha 4.
  * cross-cutting.test.ts did **not** reference the proc (the
    recipe lists it for A2/A3, not A1).
  * Gates: SQL installs clean; SDK 168/168 + type-check; conformance
    166/166; coverage MISSING 0.
- 2026-05-29 — **A2** (remove `read_subscription_position`). Atomic.
  Touched: `sql/instructed.sql` (proc block + header + lock-set
  inventories), `client.ts` (method), SDK `consistency.test.ts` (2
  assertions → `isSubscriptionCaughtUp`) + `client.test.ts` (2 cases
  removed), conformance `smoke`/`cross-cutting` (proc-name lists) +
  `subscription-persistent` (header, `position` wrapper rewritten to
  a direct `subscriptions` SELECT, delete-test IS020 check →
  `subscriptionGone` row-absence assert, 2 proc-specific tests
  removed), soak `checks.ts` (added `readCursor` helper, 3 sites) +
  `soak.ts` (inlined direct cursor query; dropped now-unused `client`
  param from `snapshotDrainState`), docs (`sql-contract`,
  `decisions` D-0010 implication → `is_subscription_caught_up`).
  Surprises / notes:
  * The conformance `position()` helper was load-bearing across ~6
    cursor-state assertions, not just the 2 proc-specific tests —
    rewrote it to read the cursor directly (D-0021 allows it) rather
    than delete it.
  * The delete-subscription test used `position()` raising IS020 as
    its "row is gone" proof; replaced with a direct row-absence
    check (`subscriptionGone`).
  * The 2 removed proc-specific tests carried no INV-* annotation
    (CON-010 prose only) — coverage stayed MISSING 0.
  * Soak: `pmLastSeen` can now be null (no row yet); guarded the
    PM-024 comparison accordingly.
  * Gates: SQL clean; SDK 166/166 + type-check; soak type-check;
    conformance 164/164; coverage MISSING 0.
- 2026-05-29 — **A3** (remove `read_subscription_batch` +
  `advance_subscription`). Atomic. Touched: `sql/instructed.sql`
  (both proc blocks + header + lock-set inventories + the
  `record_snapshot` and `delete_subscription` prose cross-refs),
  `client.ts` (both wrapper methods + their doc comments), `types.ts`
  (`RecordedEvent` doc-comment proc list), SDK `client.test.ts`
  ("release clears holder" rewritten to advance via `routeBatch`; 4
  readBatch/advance-specific cases removed), conformance
  `smoke`/`cross-cutting`/`subscription-transient` (proc-name lists +
  prose), `subscription-persistent.test.ts` (header; `readBatch`
  helper deleted; `advance` helper rewritten to wrap `route_batch`
  with empty decisions; the "delivery" + "advance and monotonicity"
  describe blocks removed; selector prose + composed-takeover
  IS022 assertion rewired off `readBatch`),
  `subscription-work-items-procedures.test.ts` (re-annotated
  INV-SUB-P-030/032 on "inserts decisions…", -034 on "cursor advance
  is monotone", -031 on "lease takeover…"), soak `README.md` prose.
  Surprises / notes:
  * §5 gotcha 4 confirmed: the four INV-SUB-P invariants stay live;
    only their conformance annotations moved to the surviving
    route_batch/takeover tests.
  * Coverage reporter (`COMMENT_REGEX`) only scrapes INV-* from
    comment lines that *start* with `// INV-`; a second INV-* placed
    mid-prose on a continuation line is invisible. INV-SUB-P-032 read
    MISSING until split onto its own leading-`// INV-` line.
  * Rewriting the `advance` helper to `route_batch(..., '[]')` kept
    every cursor-moving lifecycle/re-subscribe/start_from test intact
    (route_batch enforces the same IS022/IS020 lease contract).
  * No `docs/*.md` referenced the proc names (already proc-name-free).
  * Gates: SQL clean; SDK 162/162 + type-check; conformance 148/148
    + type-check; soak type-check; coverage MISSING 0.
