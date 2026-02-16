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
- [ ] 4. Define core types: Command, Event, Aggregate, Error types (strongly-typed)
- [ ] 5. Implement Aggregate behaviour/protocol with type-safe command handling and event application
- [ ] 6. Implement Command Router with type-safe command dispatch
- [ ] 7. Implement Command Validation middleware
- [ ] 8. Implement Event Store behaviour (trait) and InMemoryEventStore
- [ ] 9. Implement Aggregate Server (OTP GenServer/process for aggregate lifecycle)
- [ ] 10. Implement Event Handler behaviour and subscription system
- [ ] 11. Implement Process Manager behaviour (sagas/long-running processes)
- [ ] 12. Implement Projections (read model builders from event streams)
- [ ] 13. Implement Application module (top-level supervisor, wiring)
- [ ] 14. Implement Middleware pipeline for command processing
- [ ] 15. Write comprehensive tests for all core modules
- [ ] 16. Ensure zero warnings, all checks pass

### Phase 3: PostgreSQL Adapter (instructed_postgres)
- [ ] 17. Initialize instructed_postgres project
- [ ] 18. Implement PostgreSQL EventStore (schema, read/write streams)
- [ ] 19. Implement PostgreSQL subscription support
- [ ] 20. Implement snapshot storage in PostgreSQL
- [ ] 21. Write tests for postgres adapter
- [ ] 22. Ensure zero warnings, all checks pass

### Phase 4: Documentation & Guides
- [ ] 23. Write library documentation (module docs, README)
- [ ] 24. Write guides equivalent to Commanded (Getting Started, Aggregates, Commands, Events, Process Managers, Projections)
- [ ] 25. Write examples in documentation

### Phase 5: Example Todo App
- [ ] 26. Initialize todo app project, choose HTTP/CLI packages
- [ ] 27. Define todo domain: commands, events, aggregate
- [ ] 28. Implement todo server with instructed + postgres store
- [ ] 29. Implement projections: active todos, overdue, completed, by priority, by due date
- [ ] 30. Implement CLI client with all commands (add, remove, edit, complete, list views)
- [ ] 31. Implement command validation (e.g., complete already-completed todo)
- [ ] 32. Write CLI test cases
- [ ] 33. Verify CLI is fully functional end-to-end

## Verification
(Updated as tasks complete)

## Notes
### Gleam Ecosystem Research (Task 1)
- Gleam 1.14.0, Erlang/OTP 27
- Key packages: gleam_otp (actors, supervisors), gleam_erlang (processes), pog (postgres), glint (CLI), youid (UUID), gleam_json, wisp (web), argv
- gleam_pgo is deprecated/broken with current stdlib, use `pog` instead
- Actor pattern: `actor.new(state) |> actor.on_message(handler) |> actor.start`
- Supervisors: `static_supervisor.new(OneForOne) |> supervisor.add(child) |> supervisor.start`
- Gleam uses: opaque types, Result for errors, pattern matching, pipe operator, no classes/traits (use functions + records)
- rebar3 installed at ~/rebar3, must add to PATH

### Commanded Architecture Research (Task 2)
Key Commanded concepts to port:
- **Aggregate**: GenServer holding event-sourced state, execute/2 for commands, apply/2 for events
- **Router**: Routes commands to aggregates, identifies aggregate by field, supports middleware
- **Event Store Adapter**: append_to_stream, stream_forward, subscribe, subscribe_to, ack_event, snapshots
- **In-Memory Event Store**: GenServer storing events in memory
- **Event Handler**: Subscribes to events, processes them (projections, side effects)
- **Process Manager**: Coordinates multiple aggregates, handles events and dispatches commands
- **Middleware**: Pipeline with before_dispatch/after_dispatch/after_failure
- **Multi**: Generate multiple events from single command with intermediate state
- **Aggregate Lifespan**: Controls when aggregate process shuts down
- **Execution Context**: Command metadata (causation_id, correlation_id, metadata)
- **RecordedEvent**: event_id, event_number, stream_id, stream_version, causation_id, correlation_id, data, metadata, created_at
- **EventData**: Pre-persistence event structure
- **SnapshotData**: Aggregate state snapshots
- **Subscription**: Transient and persistent subscriptions
- **Application**: Top-level supervisor wiring everything together
- Guides: Getting Started, Aggregates, Commands, Events, Process Managers, Projections, Serialization, Supervision, Testing, Deployment