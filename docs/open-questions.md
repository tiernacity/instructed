# Open questions

Questions surfaced during the `instructed` exploration that have not
yet been resolved. Newest entries at the top. When a question is
resolved it is removed from this list and a corresponding entry is
added to [`decisions.md`](decisions.md) (with the question's text
preserved as part of the *Context* section, so the trail is intact).

Entries link to the phase that surfaced them and to the artifact(s)
that will likely resolve them.

---

## OQ-0001 — Global ordering mechanism for concurrent appends

**Surfaced in:** Phase 2 (`invariants.md`, end of Part B and the
status block).

**Question:** INV-APPEND-003 requires every event to receive a
globally-unique, monotonically-increasing, **gapless** `event_number`.
INV-APPEND-021 permits concurrent `:any_version` appends to interleave,
but the resulting global sequence must remain contiguous. What is the
serialisation point in Postgres that gives us a gapless global order
under concurrent writers?

Three candidate mechanisms:

1. **`$all`-as-stream with row-level lock** — what the reference
   adapter does. There is a real `streams` row for `$all`; every
   append takes its row-level lock to assign the next global number
   and link the events. Strong, simple, but serialises every append
   in the store.
2. **A `bigserial`/sequence column** — cheap and concurrent, but
   sequences can skip values (rollbacks, cache settings). "Gapless"
   would have to be reinterpreted as "monotone but not contiguous",
   which violates INV-APPEND-003 as currently written. We could
   weaken the invariant if downstream consumers tolerate gaps — but
   that needs an audit of Commanded's readers (`stream_forward(:all,
   from_event_number, ...)` semantics under gaps).
3. **`SERIALIZABLE` isolation + `MAX(event_number) + 1`** — correct
   and gapless but pessimistic; concurrency is bounded by the
   serialisation failure / retry rate.

**Why it matters:** the choice constrains throughput of the entire
store. Option 1 caps global write throughput at the rate of single-row
lock contention on `$all`. Option 2 trades a semantic invariant for
real concurrency. Option 3 trades observable retries for the same
serial throughput as option 1.

**To resolve in:** Phase 7 (SQL contract). The decision will be
recorded in `decisions.md` and the chosen mechanism documented in
`mapping.md` under INV-APPEND-003.
