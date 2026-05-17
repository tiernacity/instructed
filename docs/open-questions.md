# Open questions

Questions surfaced during the `instructed` exploration that have not
yet been resolved. Newest entries at the top. When a question is
resolved it is removed from this list and a corresponding entry is
added to [`decisions.md`](decisions.md) (with the question's text
preserved as part of the *Context* section, so the trail is intact).

Entries link to the phase that surfaced them and to the artifact(s)
that will likely resolve them.

---

*No open questions at present.*

OQ-0001 was resolved by D-0012 in Phase 7. OQ-0002 was resolved by
D-0014 in Phase 8 (transaction-boundary ownership: SDK exposes both
the low-level `withTransaction` form and the high-level worker
helper that wraps it). OQ-0003 was resolved in Phase 8 by adopting
**SDK-side selectors** for v1 (option 1 from its original entry);
server-side JSONB-predicate selectors remain a deliberate future
option \u2014 see `maybe-later.md` ML-0003.
