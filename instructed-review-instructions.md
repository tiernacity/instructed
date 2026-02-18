# Instructed Review Instructions

This document describes how to produce a comprehensive CQRS/ES feature and guarantee review comparing **Instructed** (this Gleam repo) against **Commanded** (the Elixir library it ports: https://github.com/commanded/commanded).

Read this document fully before starting. Follow the phases in order.

---

## Purpose

Produce a `REVIEW.md` at the repo root that systematically compares every feature, constraint, and guarantee between the two frameworks. The review must cover not just "does the feature exist" but "does it work correctly, with the right guarantees."

---

## Phase 1: Research Commanded

Commanded is the reference implementation. We need deep, exhaustive research — not surface-level. Clone it and use **three parallel sub-agents** (via tmux) to research different areas simultaneously.

### Setup

```bash
# Clone commanded if not already present
if [ ! -d /tmp/commanded ]; then
  git clone --depth 1 https://github.com/commanded/commanded.git /tmp/commanded
fi
```

### Sub-Agent 1: Aggregates, Commands, Routing

Launch via tmux with this prompt:

> You are a research agent. Deeply analyze the Commanded Elixir library. Focus on: **AGGREGATES, COMMANDS, ROUTING, and the MULTI module**. Read all relevant source files and guides. Output your findings to `/tmp/research_aggregates_commands.md`.
>
> Cover: aggregate lifecycle (GenServer process, init, state population, event subscription), state rebuilding (batching, snapshot integration), command dispatch pipeline (Dispatcher, Payload, Task-based execution), command handlers (Handler behaviour, `execute/2` vs `handle/2`, `before_execute`), routing (`identify` macro, `dispatch` macro, identity extraction, prefix, duplicate detection), CompositeRouter, the Multi module (API, named steps, reduce, nested multi, atomic semantics), lifespan management (`AggregateLifespan` behaviour, `DefaultLifespan`, timeout/hibernate/stop), execution context (retry mechanism, `returning` option, `format_reply`), execution result struct, snapshotting (configuration, lifecycle, versioning, interaction with lifespan), aggregate state builder (populate, rebuild_from_events, batch size), aggregate supervisor (DynamicSupervisor, `open_aggregate`), concurrency guarantees (GenServer serialization, task isolation), and all constraints/invariants.
>
> Read these files thoroughly:
> - `/tmp/commanded/guides/Aggregates.md`
> - `/tmp/commanded/guides/Commands.md`
> - `/tmp/commanded/lib/commanded/aggregates/*.ex`
> - `/tmp/commanded/lib/commanded/commands/*.ex`
>
> Be exhaustive — include code patterns, configuration options, error handling, return types, and edge cases. This document will be used to write a specification.

### Sub-Agent 2: Events, Projections, Subscriptions, Consistency

Launch via tmux with this prompt:

> You are a research agent. Deeply analyze the Commanded Elixir library. Focus on: **EVENT HANDLERS, PROJECTIONS, SUBSCRIPTIONS, CONSISTENCY, and PUBSUB**. Read all relevant source files and guides. Output your findings to `/tmp/research_events_projections.md`.
>
> Cover: event handler lifecycle (GenServer startup sequence, subscription, event processing loop, termination), handler configuration (name, consistency, start_from, subscribe_to, concurrency, batch_size, state), singleton guarantee, `handle/2` callback and return values, error handling chain (`error/3` callback, FailureContext struct, retry/skip/stop strategies, exponential backoff, application-level config), idempotency tracking (last_seen_event, durable subscription position), handler state management, subscription management (Subscription module, backoff, reset mechanism), `start_from` semantics, subscription protocol (subscribe_to, events delivery, ack_event), Subscriptions GenServer (ETS tracking, wait_for, purging), strong vs eventual consistency (ConsistencyGuarantee middleware, `wait_for` flow, timeout, selective waiting, constraints with concurrency), PubSub system (adapter behaviour, LocalPubSub, PhoenixPubSub), read model projections (Ecto projections pattern, strong consistency for POST/Redirect/GET, rebuild/reset), event upcasting (Upcaster protocol, runtime transformation, chaining), event mapping (map_to_event_data, RecordedEvent enrichment, TypeProvider), telemetry events, concurrency and partitioning (multi-process handlers, partition_by, constraints), batching (batch_size, handle_batch, acknowledgment, error handling).
>
> Read these files thoroughly:
> - `/tmp/commanded/guides/Events.md`
> - `/tmp/commanded/guides/Read Model Projections.md`
> - `/tmp/commanded/lib/commanded/event/*.ex`
> - `/tmp/commanded/lib/commanded/event_store/*.ex`
> - `/tmp/commanded/lib/commanded/pubsub/*.ex`
> - `/tmp/commanded/lib/commanded/middleware/consistency_guarantee.ex`
>
> Be exhaustive — include code patterns, configuration options, error handling, return types, and edge cases.

### Sub-Agent 3: Process Managers, Middleware, Infrastructure

Launch via tmux with this prompt:

> You are a research agent. Deeply analyze the Commanded Elixir library. Focus on: **PROCESS MANAGERS, MIDDLEWARE, SNAPSHOTS, APPLICATION SUPERVISION, REGISTRATION, SERIALIZATION, and the EVENT STORE ADAPTER interface**. Read all relevant source files and guides. Output your findings to `/tmp/research_process_managers_infra.md`.
>
> Cover: process manager architecture (three-layer hierarchy: ProcessRouter, DynamicSupervisor, ProcessManagerInstance), callback behaviour (interested?/1,2 with start/start!/continue/continue!/stop/false, multi-instance routing, handle/2,3, apply/2,3, after_command/2,3, error/3), event routing and strict validation, event handling execution order (handle → dispatch commands → apply → persist snapshot → ack → after_command), error handling and failure strategies (FailureContext, event errors vs command dispatch errors, retry/skip/stop/continue strategies, pending commands), state persistence via snapshots (write after each event, read on startup, delete on stop), idempotency (last_seen_event from snapshot), event timeout and idle timeout, ProcessRouter internals (subscription, event pipeline, acknowledgment tracking, event timeout, instance lifecycle), middleware pipeline (behaviour callbacks, Pipeline struct with all fields, chain execution and halting semantics, built-in middleware: ExtractAggregateIdentity, ConsistencyGuarantee, Logger), snapshot operations (aggregate vs PM snapshots, timing, versioning), application supervision tree (full tree structure, adapter initialization, dynamic named applications, config sources and priority), registration adapters (behaviour, LocalRegistry via Elixir Registry, GlobalRegistry via :global, usage patterns), serialization (JsonSerializer, JsonDecoder protocol, custom serializers, configuration), EventStore adapter behaviour (full callback list with types, expected_version semantics, subscription types, snapshot operations), in-memory EventStore implementation (state structure, append with OCC, stream reading, transient/persistent subscriptions, serialization), cross-cutting concerns (event_number vs stream_version, enriched metadata, causation/correlation chain propagation).
>
> Read these files thoroughly:
> - `/tmp/commanded/guides/Process Managers.md`
> - `/tmp/commanded/guides/Supervision.md`
> - `/tmp/commanded/guides/Serialization.md`
> - `/tmp/commanded/lib/commanded/process_managers/*.ex`
> - `/tmp/commanded/lib/commanded/middleware/*.ex`
> - `/tmp/commanded/lib/commanded/event_store/adapter.ex`
> - `/tmp/commanded/lib/commanded/event_store/adapters/in_memory.ex`
> - `/tmp/commanded/lib/commanded/registration/*.ex`
> - `/tmp/commanded/lib/application.ex`
>
> Be exhaustive — include code patterns, configuration options, error handling, return types, and edge cases.

### Launching Sub-Agents

Use tmux to run all three in parallel:

```bash
tmux new-window -n agent1 "pi -p '<prompt1>' 2>&1; sleep 5"
tmux new-window -n agent2 "pi -p '<prompt2>' 2>&1; sleep 5"
tmux new-window -n agent3 "pi -p '<prompt3>' 2>&1; sleep 5"
```

Wait for all three research files to be written (each should be 500+ lines):

```bash
while true; do
  count=0
  for f in /tmp/research_aggregates_commands.md /tmp/research_events_projections.md /tmp/research_process_managers_infra.md; do
    if [ -f "$f" ] && [ $(wc -c < "$f") -gt 500 ]; then count=$((count+1)); fi
  done
  echo "$(date +%H:%M:%S) - $count/3 research files ready"
  if [ $count -eq 3 ]; then echo "All research complete!"; break; fi
  sleep 15
done
```

---

## Phase 2: Read the Instructed Codebase

Read ALL source files in the instructed repo:

```
/workspace/instructed/src/instructed.gleam
/workspace/instructed/src/instructed/*.gleam        (all modules)
/workspace/instructed-postgres/src/*.gleam
/workspace/instructed-sqlite/src/*.gleam
/workspace/instructed/test/*.gleam                  (all tests)
/workspace/example-todo/src/**/*.gleam              (example app)
```

Also read:
- `/workspace/README.md`
- `/workspace/NOTES.md` (if it exists)
- Any existing `/workspace/REVIEW.md` (for context on previous reviews)

---

## Phase 3: Produce the Review

Use a Ralph loop to systematically compare each area. **One area per iteration.**

### Review Areas (in this order)

#### Core Features
1. **Aggregates**: definition, execute/apply pattern, state rebuilding, process model
2. **Command Routing & Dispatch**: identity extraction, prefix, dispatch pipeline, timeout, returning
3. **Middleware Pipeline**: before/after dispatch, after failure, halting, assigns, built-in middleware
4. **Event Handlers**: lifecycle, subscription, event processing, error handling, state management
5. **Projections**: read model building, persistence, querying, consistency
6. **Process Managers**: interested routing, handle/apply, command dispatch, state persistence, error handling
7. **Snapshots**: aggregate snapshotting, PM snapshotting, state rebuilding integration
8. **Event Store Interface**: adapter contract, expected version, subscriptions, snapshot operations

#### CQRS/ES Guarantees & Constraints
9. **Optimistic Concurrency Control**: version checking, retry on conflict, atomicity
10. **Command Serialization per Aggregate**: process-level serialization, concurrent command safety
11. **Event Ordering**: global ordering, per-stream ordering, handler delivery order, subscription callbacks
12. **Strong vs Eventual Consistency**: consistency model, waiting mechanism, timeout
13. **Error Handling & Retry Strategies**: handler errors, PM errors, dispatch errors, backoff, FailureContext
14. **Causation & Correlation ID Tracking**: automatic propagation, chain preservation through PMs
15. **Idempotency**: duplicate event detection, subscription position tracking, restart behaviour

#### Advanced Features
16. **Multi Module**: multi-step command execution with intermediate state
17. **Aggregate Lifespan Management**: timeout, hibernate, stop, DefaultLifespan
18. **Composite Router**: combining multiple routers, duplicate detection
19. **Application Supervision Tree**: supervision, process registration, dynamic applications
20. **Event Upcasting**: schema evolution, runtime transformation
21. **Telemetry & Observability**: instrumentation events, logging
22. **Batch Processing & Concurrency in Handlers**: batch_size, concurrency, partitioning

### Review Format

For each area, write a section in `REVIEW.md` with this structure:

```markdown
## N. Feature Name [STATUS]

### What Commanded Provides
- Bullet points describing Commanded's implementation in detail
- Include specifics: defaults, types, constraints, error cases

### What Instructed Provides
- Bullet points with status indicators (✅ ⚠️ ❌ 📝)
- Map each Commanded feature to what exists in Instructed

### Gaps & Issues
- Specific problems, missing features, incorrect behaviour
- Note severity: CRITICAL, HIGH, MEDIUM, LOW
- Note whether the gap is a design choice vs an omission
```

Status indicators:
- ✅ Feature present and equivalent
- ⚠️ Feature present but with gaps or differences
- ❌ Feature missing entirely
- 📝 Intentional design difference (not a problem)

### Summary Section

After all 22 areas, include:

1. **Feature Parity Scorecard**: Table with feature, status, severity
2. **Critical Issues**: List of must-fix problems for production use
3. **Recommended Priority Order**: Ordered list of fixes by importance

---

## Phase 4: Commit

```bash
cd /workspace && git add REVIEW.md && git commit -m "Update CQRS/ES feature review: Instructed vs Commanded"
```

---

## Key Principles

1. **Be specific**: Don't just say "missing error handling" — say "handler errors in `handle_actor_message` at `Error(_reason) -> actor.continue(state)` silently swallow failures, losing the event"
2. **Reference source locations**: Point to specific files and functions in both codebases
3. **Distinguish design choices from bugs**: Gleam doesn't have macros — no compile-time router is a design choice, not a bug. Silent error swallowing IS a bug
4. **Check the wiring**: A feature isn't "present" if the types exist but nothing calls them (e.g., snapshots in Instructed have types but are never read/written)
5. **Consider production impact**: A missing feature that prevents production use is CRITICAL; a missing convenience feature is LOW
6. **Check tests**: Do tests exist for the feature? Do they cover the important cases?

---

## Estimated Time

- Phase 1 (research): ~5-10 minutes (parallel sub-agents)
- Phase 2 (read codebase): ~2 minutes
- Phase 3 (review): ~20-30 minutes (22 iterations)
- Phase 4 (commit): ~1 minute

Total: ~30-45 minutes
