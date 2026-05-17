# Open questions

Questions surfaced during the `instructed` exploration that have not
yet been resolved. Newest entries at the top. When a question is
resolved it is removed from this list and a corresponding entry is
added to [`decisions.md`](decisions.md) (with the question's text
preserved as part of the *Context* section, so the trail is intact).

Entries link to the phase that surfaced them and to the artifact(s)
that will likely resolve them.

---

## OQ-0003 — Selector evaluation: SDK-side or server-side?

**Surfaced in:** Phase 4 Pass 2 (`mapping.md`, INV-SUB-P-050).

**Question:** Persistent subscriptions accept an optional `selector`
that filters events before delivery; the cursor must advance past
filtered-out events (otherwise an unmatched event would block the
subscription forever). Where does the selector run?

1. **SDK-side after fetch.** The SDK reads a batch of events, runs
   the application's predicate locally, calls handlers only on
   matches, and advances the cursor to the last *fetched*
   event_number. Simple and matches the abstract contract directly.
   Cost: bandwidth wasted on events the application doesn't care
   about. For sparse selectors (1% match rate) this is real.
2. **Server-side via JSONB predicate.** The SDK passes a JSONB path
   expression (or a SQL fragment over the `data`/`metadata` columns)
   into a `read_subscription_batch(name, qty, selector_jsonb)`
   procedure. The server filters before returning. Bandwidth optimal.
   Cost: the selector is no longer arbitrary application code; it's
   restricted to whatever the server's predicate vocabulary supports.
3. **Server-side via stored helper functions.** Applications
   register their selectors as `CREATE FUNCTION ... LANGUAGE plpgsql`
   and pass the function name. Strong, but couples the application
   deployment to the database schema in a way we have generally
   avoided.

**Why it matters:** the choice is partly about the SDK API and
partly about who owns the predicate vocabulary. A v1 that only
supports (1) is the simplest contract and the easiest to migrate
from; adding (2) later is a pure additive option on
`read_subscription_batch`.

**To resolve in:** Phase 8 (SDK), informed by whatever the first
worked example (likely a bank account projector with a few selector
shapes) actually needs.

---

## OQ-0002 — SDK-owned transaction boundaries for co-transactional advance

**Surfaced in:** Phase 4 Pass 2 (`mapping.md`, HND-031; D-0008).

**Question:** D-0008 commits us to making `advance_subscription`
safely callable inside an SDK-opened transaction so the cursor
advance commits with the handler's projection writes. The question
is what shape the SDK API takes:

1. **The SDK owns the transaction.** `claim_subscription` and
   `advance_subscription` are both individual procedures; the SDK
   passes a connection that it has already wrapped in a `BEGIN`. The
   handler runs inside that transaction. Maximum flexibility, but
   the handler now has implicit access to a live transaction and
   must not commit or roll back itself.
2. **The SDK exposes a `process_batch(handler_fn)` helper** that
   internally opens the transaction, fetches the batch, runs the
   handler, calls `advance_subscription`, and commits. The handler
   never sees the transaction explicitly. Easier to use; harder to
   share a transaction with non-instructed writes the application
   wants to make.
3. **Two-phase commit.** The handler does its writes in its own
   transaction; the SDK calls `advance_subscription` after the
   handler commits but holds back redelivery via a separate
   `pending_ack` column. Defeats the simplicity D-0008 was buying.

**Why it matters:** the choice affects the language-level SDK API
shape, not the SQL contract. The SQL contract just needs
`advance_subscription` to be callable inside any well-formed
transaction; that is decided. The question is purely about ergonomics
and the locus of `BEGIN`/`COMMIT`.

**To resolve in:** Phase 8 (SDK). Likely the answer is "both": a
low-level individual-procedure API plus a high-level
`process_batch` helper that wraps it.

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
