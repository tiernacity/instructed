# Instructed Feature Parity with Commanded

Iterative module-by-module implementation to bring Instructed (Gleam CQRS/ES) to feature parity with Commanded (Elixir).

## Goals
- Achieve feature parity with Commanded across all 21 modules
- Each module must pass self-review (no ⚠️ or ❌ findings)
- Architecture reviews after each module completion
- All 20 key invariants maintained

## Checklist
- [x] 1. Event Store Interface & In-Memory Adapter
- [x] 2. Event Types & Recorded Events
- [x] 3. Error Types
- [x] 4. Aggregate Core (types + state rebuilding)
- [x] 5. Aggregate Server (GenServer process per instance)
- [x] 6. Snapshot Types & Integration
- [x] 7. Command Context & Execution
- [x] 8. Middleware Pipeline
- [x] 9. Command Router (wiring aggregate server, identity, retry)
- [x] 10. Event Handler (lifecycle, subscriptions, error handling, idempotency)
- [x] 11. Projection (builds on event handler patterns)
- [x] 12. Process Manager (routing, state persistence, error handling, command dispatch)
- [ ] 13. Application & Supervision Tree
- [ ] 14. Causation & Correlation Chain
- [ ] 15. Strong vs Eventual Consistency
- [ ] 16. PostgreSQL Adapter
- [ ] 17. SQLite Adapter
- [ ] 18. Multi Module
- [ ] 19. Aggregate Lifespan
- [ ] 20. Event Upcasting
- [ ] 21. Telemetry & Observability

## Verification
- Build passes: gleam build (clean, no errors)
- Tests pass: 73 passed, no failures

## Notes

### Module 12 Completed
Key additions to process_manager.gleam:
- Snapshot loading via get_or_load_instance (fixes Invariant 9)
- event_number as source_version (enables last_seen_event restore)
- Per-instance PMInstance(state, last_seen_event)
- StartStrict/ContinueStrict, StartMany/ContinueMany/StopMany
- after_command callback (AfterStop/AfterContinue)
- on_event_error + on_command_error with full action types
- dispatch_commands loop with error recovery
- 14 new tests

Architecture review after Module 12: all integration points OK.

### Module 13 Plan: Application & Supervision Tree

Next to compare:
- `/tmp/commanded/lib/application.ex`
- `/tmp/commanded/lib/commanded/application/*.ex`
- Current: `/workspace/instructed/src/instructed/application.gleam`

Key expected features:
- Named application with isolated event store
- Supervision tree starting event store, aggregate supervisor, event handlers, PMs, projections
- `start_link` / `child_spec` pattern
- Application-level dispatch function
- Configuration: event_store adapter, pubsub, registry
