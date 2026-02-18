# SQLite Event Store Adapter

## Checklist
- [x] Create instructed-sqlite package structure
- [x] Implement instructed_sqlite.gleam
- [x] Build and verify instructed-sqlite compiles
- [x] Update example-todo to use SQLite adapter
- [x] Build and verify example-todo compiles
- [x] Test end-to-end

## Verification
- `cd /workspace/instructed-sqlite && gleam build` → Compiled successfully
- `cd /workspace/example-todo && gleam build` → Compiled successfully
- `gleam run -- add "Buy groceries" high 2026-03-01` → ✓ Todo created
- `gleam run -- add "Write tests" critical` → ✓ Todo created
- `gleam run -- list` → Shows both todos
- `gleam run -- complete <id>` → ✓ Todo completed
- `gleam run -- list by-priority` → Correct grouping
- `gleam run -- reset` → ✓ Event store reset
- `gleam run -- list` → Shows 0 todos (persistence + reset works)
