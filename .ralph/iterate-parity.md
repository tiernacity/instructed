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
- [x] 13. Application & Supervision Tree
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
- Tests pass: 83 passed, no failures

## Notes

### Module 13 Completed
Key changes to application.gleam:
- Application is now a plain struct (no unnecessary actor)
- start_event_handler, start_process_manager added
- read_stream_from (paginated reads) added
- event_store accessor added
- Dispatch error when no router configured
- 10 new tests

Architecture review after Module 13: all integration points OK.

### Module 14 Plan: Causation & Correlation Chain

Causation and correlation IDs flow through the entire system. They connect:
- Commands → Events (via aggregate execution)
- Events → Process Manager Commands (PM sets causation_id = event_id)
- Commands → Commands (correlation_id preserved)

Key questions:
1. Is causation_id correctly set on events from aggregate execution?
2. Is correlation_id correctly preserved from command to events?
3. Does the router's dispatch_with_context correctly pass these IDs?
4. Does the aggregate server correctly propagate them to EventData?
5. Does the in-memory event store correctly store and return them?
6. Is the chain end-to-end verifiable (command → events → PM commands → more events)?

Compare:
- `/tmp/commanded/lib/commanded/commands/dispatcher.ex` 
- `/tmp/commanded/lib/commanded/aggregates/aggregate.ex`
- How Commanded propagates causation/correlation from command dispatch options → EventData → RecordedEvent
