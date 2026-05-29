// The instructed schema, read from sql/instructed.sql.
//
// `import.meta.dirname` resolves to this module's directory both when running
// from source and inside a `deno compile` binary. The schema is read from
// `src/instructed.sql` — a symlink to the repo-root sql/instructed.sql — kept
// co-located with this module so the `compile.include` entry in deno.json
// embeds it into the binary's virtual filesystem. (deno compile only
// materialises included files reachable without traversing above the reading
// module's directory, so the symlink must live here in src/, next to this
// file, rather than at the tool root.) After compilation this read resolves
// from the embedded copy, so it works with no schema file on disk.
export const SCHEMA_SQL: string = Deno.readTextFileSync(
  import.meta.dirname + "/instructed.sql",
);
