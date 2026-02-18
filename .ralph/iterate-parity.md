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
- [ ] 6. Snapshot Types & Integration
- [ ] 7. Command Context & Execution
- [ ] 8. Middleware Pipeline
- [ ] 9. Command Router (wiring aggregate server, identity, retry)
- [ ] 10. Event Handler (lifecycle, subscriptions, error handling, idempotency)
- [ ] 11. Projection (builds on event handler patterns)
- [ ] 12. Process Manager (routing, state persistence, error handling, command dispatch)
- [ ] 13. Application & Supervision Tree
- [ ] 14. Causation & Correlation Chain
- [ ] 15. Strong vs Eventual Consistency
- [ ] 16. PostgreSQL Adapter
- [ ] 17. SQLite Adapter
- [ ] 18. Multi Module
- [ ] 19. Aggregate Lifespan
- [ ] 20. Event Upcasting
- [ ] 21. Telemetry & Observability