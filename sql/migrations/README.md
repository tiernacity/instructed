# Migrations

Following absurd's pattern: `sql/instructed.sql` is the canonical
schema (the spec), and `sql/migrations/` carries the deltas between
released versions.

Naming convention: `<from>-<to>.sql`, where `<from>` and `<to>` are
either semver tags (`0.1.0`) or the literal `main` for the
in-development tip. Migrations are linear and apply in order.

A migration MUST be idempotent enough to re-run safely on a partially
applied database (use `if not exists` / `if exists` guards, `alter
table ... add column if not exists`, etc.). The `get_schema_version()`
function — currently always returning `'main'` — is the canonical
"what version is installed" marker; release automation rewrites it to
the actual tag on a tagged build.

No migrations are recorded here yet: v1 has not shipped a release. The
first migration will land when the first tag is cut.
</content>
</invoke>