# Port Commanded (Elixir CQRS/ES) to Gleam as "Instructed"

Port the Commanded CQRS/ES framework from Elixir to Gleam, creating an idiomatic, strongly-typed library.

## Goals
- Create `instructed` - a CQRS/ES framework in idiomatic Gleam
- Create `instructed_postgres` - PostgreSQL event store adapter
- Create an example todo CLI app exercising all features
- Full documentation, guides, examples equivalent to Commanded
- All tests passing, no warnings, no deprecations

## Checklist

### Phase 1: Research
- [x] 1. Research Gleam ecosystem: idiomatic patterns, available packages, OTP support, project structure
- [x] 2. Research Commanded: all modules, features, architecture, tests, docs, guides

### Phase 2: Core Library (instructed)
- [x] 3. Initialize instructed project with proper Gleam package structure
- [x] 4. Define core types: Command, Event, Aggregate, Error types (strongly-typed)
- [x] 5. Implement Aggregate behaviour/protocol with type-safe command handling and event application
- [x] 6. Implement Command Router with type-safe command dispatch
- [x] 7. Implement Command Validation middleware
- [x] 8. Implement Event Store behaviour (trait) and InMemoryEventStore
- [x] 9. Implement Aggregate Server (OTP GenServer/process for aggregate lifecycle)
- [x] 10. Implement Event Handler behaviour and subscription system
- [x] 11. Implement Process Manager behaviour (sagas/long-running processes)
- [x] 12. Implement Projections (read model builders from event streams)
- [x] 13. Implement Application module (top-level supervisor, wiring)
- [x] 14. Implement Middleware pipeline for command processing
- [x] 15. Write comprehensive tests for all core modules (39 tests)
- [x] 16. Ensure zero warnings, all checks pass

### Phase 3: PostgreSQL Adapter (instructed_postgres)
- [x] 17. Initialize instructed_postgres project
- [x] 18. Implement PostgreSQL EventStore (schema, read/write streams)
- [x] 19. Implement PostgreSQL subscription support
- [x] 20. Implement snapshot storage in PostgreSQL
- [x] 21. Write tests for postgres adapter (10 tests)
- [x] 22. Ensure zero warnings, all checks pass

### Phase 4: Documentation & Guides
- [x] 23. Write library documentation (module docs, README)
- [x] 24. Write guides equivalent to Commanded (11 guides)
- [x] 25. Write examples in documentation

### Phase 5: Example Todo App
- [x] 26. Initialize todo app project, choose CLI packages
- [x] 27. Define todo domain: commands, events, aggregate
- [x] 28. Implement todo server with instructed + postgres store
- [x] 29. Implement projections: active todos, overdue, completed, by priority, by due date
- [x] 30. Implement CLI client with all commands (add, remove, edit, complete, list views)
- [x] 31. Implement command validation (e.g., complete already-completed todo)
- [x] 32. Write CLI test cases (23 tests)
- [x] 33. Verify CLI is fully functional end-to-end

## Verification
- Instructed: 39 unit tests passing, 0 warnings, 0 errors
- Instructed Postgres: 10 integration tests passing, 0 warnings, 0 errors
- Example Todo CLI: 23 end-to-end tests passing
- All 3 projects build with 0 warnings, 0 errors, no deprecations
- CLI fully functional: add, complete, reopen, delete, edit description/priority/due-date
- 6 projections working: all_todos, active_todos, completed_todos, overdue_todos, by_priority, by_due_date
- Command validation: can't complete completed, can't reopen active, can't delete deleted, can't complete deleted
- PostgreSQL persistence verified end-to-end
- 11 guides equivalent to Commanded documentation

## Notes
- Gleam 1.14.0, Erlang/OTP 27
- Key packages: gleam_otp (actors), pog (postgres), argv (CLI args), youid (UUID), gleam_json
- rebar3 at ~/rebar3
- `todo` is a reserved word in Gleam - domain modules live under `app/` namespace
- Subscription restarts handled by deleting existing subscriptions before recreating
