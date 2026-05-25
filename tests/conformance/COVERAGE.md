# Conformance coverage checklist

Cross-reference from Commanded's adapter conformance suite to the
INV-* identifiers in `docs/invariants.md` and the test files in
this directory. Per **D-0022**, cases here are organised by INV-*
rather than by Elixir source-file order; this checklist exists to
make sure no Elixir `test "…"` block is missed during the
hand-rewrite.

Source files in Commanded:

- `commanded/test/event_store/support/append_events_test_case.ex`
- `commanded/test/event_store/support/subscription_test_case.ex`
- `commanded/test/event_store/support/snapshot_test_case.ex`

The reference adapter conformance suite (`eventstore`) is a
secondary input — the abstract contract (the `_test_case.ex` files
above) is the authoritative source.

---

## Step-by-step status

| Step | Scope                                  | Status         |
|------|----------------------------------------|----------------|
| 1/8  | Harness skeleton, fixtures, smoke      | done           |
| 2/8  | Part B — Append (INV-APPEND-*)         | done           |
| 3/8  | Part C — Read (INV-READ-*)             | done           |
| 4/8  | Part D — Snapshots (INV-SNAP-*)        | done           |
| 5/8  | Part E persistent (INV-SUB-P-*)        | done           |
| 6/8  | Part E partitioned                     | n/a — sharded routing unbuilt, see ML-0013 |
| 7/8  | Part E transient + Part F cross-cutting| done           |
| 8/8  | Coverage reporter + non-goals reconcile| done           |
| —    | SUB-A re-fit (TODO #11)                | done (2026-05-25) |

### TODO #11 / SUB-A re-fit notes

The 2026-05-23 Commanded re-review listed eight §4 conformance
gaps against the pre-SUB-A single-cursor model. Re-walked against
the landed SUB-A subscription model:

- **Already covered** (drop from gap list): `:current` start_from
  skip; `$all` original-identity echo; release preserves cursor;
  monotone cumulative ack.
- **Landed in this revisit**:
  - INV-SUB-P-001/030 scope isolation (per-stream A doesn't
    deliver B's events).
  - INV-SUB-P-011 composed lease-expiry → takeover → IS022 on
    original's next op.
  - INV-SUB-P-061 composed delete-with-queued-items → re-claim
    from `:origin` → re-route reproduces work items.
  - INV-SUB-P-033 mixed-decisions route_batch advances past
    ignored event_numbers without writing a work-item row.
- **Re-labelled**: SP "redelivery: crash-before-advance is
  recovered by re-claim" now annotated as routing-layer
  no-auto-ack, with a pointer to the work-item-layer lease
  takeover test for the application-facing redelivery case.
- **SDK-side, landed alongside**: CON-B cross-stream guard
  (`ConsistencyTargetError`) in
  `sdks/typescript/test/consistency.test.ts`.

The `[mechanism-only]` marker now applies to:
INV-APPEND-022, INV-SUB-P-011, INV-SUB-W-003, INV-STREAM-002.
These identify invariants whose realisation is internal
mechanism (CHECK constraints, lease columns) rather than a
separately-testable behavioural contract; the behaviour they
realise is tested through the surface that depends on it.
Non-mechanism-only INV-SUB-* identifiers are application-facing
contract and must hold for any conformant store.

---

## Elixir `test "…"` → INV-* mapping

The table below is filled in as steps 2–7 land. Each row is one
Elixir `test "…"` block from the source files above. New rows are
appended (never reordered) so a diff shows what each step added.

(Empty until step 2/8.)

---

## INV-* identifiers expected to render in the coverage matrix

Listed here so step 1/8 sets the expectations explicitly. The list
must match the INV-* catalogue in `docs/invariants.md`; any drift
between the two is itself a bug.

### Part B — Append
INV-APPEND-001, 002, 003, 004, 005, 006, 007,
INV-APPEND-010, 011, 012, 013, 014,
INV-APPEND-020, 021, 022 *[reference-only mechanism]*,
INV-APPEND-030, 040, 041.

### Part C — Read
INV-READ-001, 002, 003, 004, 005, 006, 007, 008, 020.

### Part D — Snapshots
INV-SNAP-001, 002, 003, 004, 005, 006.

### Part E — Subscriptions (transient)
INV-SUB-T-001..005. *Dropped wholesale per NG-0006 / D-0007.*

### Part E — Subscriptions (persistent)
INV-SUB-P-001, 002,
INV-SUB-P-010, 011 *[mechanism-only; composed lease-takeover →
IS022 case in subscription-persistent.test.ts]*, 012,
INV-SUB-P-020, 021,
INV-SUB-P-030, 031, 032, 033, 034,
INV-SUB-P-050. *Above adapter line per D-0023 / ML-0003.*
INV-SUB-P-060, 061, 062.

*(INV-SUB-P-040/041/042 omitted: sharded routing is unbuilt; see
`docs/maybe-later.md` ML-0013. The previously stubbed
`subscription-partitioned.test.ts` has been removed.)*

### Part E — Subscriptions (work queue, SUB-A)
INV-SUB-W-001, 002, 003 *[mechanism-only]*,
INV-SUB-W-010, 011, 012, 013,
INV-SUB-W-020, 021, 022, 030.
All covered by `subscription-work-items-procedures.test.ts` and
`subscription-work-items-schema.test.ts`.

### Part E — Catch-up predicate (SUB-A)
INV-SUB-CATCHUP-001. SQL surface covered by
`subscription-work-items-procedures.test.ts ::
is_subscription_caught_up`; SDK end-to-end coverage (both
conjuncts under live routing + processing) lives in
`sdks/typescript/test/consistency.test.ts` :: "SUB-A work-item
conjunct" and "cross-stream guard (CON-B)".

### Part F — Cross-cutting
INV-META-001, 010, 011,
INV-STREAM-001, 002 *[reference-only mechanism]*, 003,
INV-LINK-001. *Dropped per mapping.md Pass 1.*
INV-DELETE-001. *Dropped per mapping.md Pass 1 / NG-* in non-goals.md.*
