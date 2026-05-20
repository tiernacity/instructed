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
| 6/8  | Part E partitioned (skipped, D-0024)   | **this commit**|
| 7/8  | Part E transient + Part F cross-cutting| pending        |
| 8/8  | Coverage reporter + non-goals reconcile| pending        |

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
INV-SUB-P-010, 011 *[reference-only mechanism]*, 012,
INV-SUB-P-020, 021,
INV-SUB-P-030, 031, 032, 033, 034,
INV-SUB-P-040, 041, 042 *[deferred per D-0024 / ML-0001]*,
INV-SUB-P-050. *Above adapter line per D-0023 / ML-0003.*
INV-SUB-P-060, 061, 062.

### Part F — Cross-cutting
INV-META-001, 010, 011,
INV-STREAM-001, 002 *[reference-only mechanism]*, 003,
INV-LINK-001. *Dropped per mapping.md Pass 1.*
INV-DELETE-001. *Dropped per mapping.md Pass 1 / NG-* in non-goals.md.*
