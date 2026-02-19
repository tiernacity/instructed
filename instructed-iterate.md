# Instructed: Iterative Feature Parity Build

This document drives an iterative process to bring **Instructed** (Gleam CQRS/ES framework) to feature parity with **Commanded** (Elixir, https://github.com/commanded/commanded).

## How To Use This Document

Start a new session and say:

> Read `./instructed-iterate.md` and follow its instructions. Use ralph loop.

---

## Background

Instructed is a Gleam port of Commanded. Comprehensive reviews (`REVIEW-feature-comparison.md` and `REVIEW-robustness.md`) have identified significant gaps. Rather than starting over, we iterate module-by-module toward parity, with periodic architecture reviews that grow as more modules are completed.

Previous research documents may exist at `/tmp/research_*.md` — if not, Phase 1 regenerates them.

---

## The Process

### Ralph Loop Structure

Use a Ralph loop. The task file at `.ralph/iterate-parity.md` tracks module status. Each iteration does ONE of:

**Option A — Module Work** (pick the first `[ ]` or `[~]` module):
1. Deep comparison against Commanded
2. Implement for parity
3. Re-compare until parity achieved
4. Mark module `[x]`

**Option B — Architecture Review** (triggered after completing a module):
1. Review ALL `[x]` modules together for integration correctness
2. If any module needs changes, mark it `[~]` (incomplete — needs rework)
3. Fix issues found, then re-mark `[x]`

**The rule**: Always work on the FIRST `[ ]` or `[~]` module in the list. Architecture reviews happen after each module completion. A module is only truly done when it survives the architecture review.

---

## Phase 0: Setup (Do This First)

### Clone Commanded
```bash
if [ ! -d /tmp/commanded ]; then
  git clone --depth 1 https://github.com/commanded/commanded.git /tmp/commanded
fi
```

### Research Commanded (if research docs don't exist)

Check if `/tmp/research_aggregates_commands.md`, `/tmp/research_events_projections.md`, and `/tmp/research_process_managers_infra.md` exist and have content. If not, regenerate them using three parallel sub-agents via tmux. See `instructed-review-instructions.md` for the exact prompts.

### Read Existing Review

Read `/workspace/REVIEW-feature-comparison.md` and `/workspace/REVIEW-robustness.md` for context on known gaps.

### Create/Update Task File

Create `.ralph/iterate-parity.md` with the module checklist (see Module Order below), or read the existing one to find where we left off.

---

## Module Order

Ordered by dependency. Later modules depend on earlier ones being correct.

```
[ ] 1. Event Store Interface & In-Memory Adapter
[ ] 2. Event Types & Recorded Events
[ ] 3. Error Types
[ ] 4. Aggregate Core (types + state rebuilding)
[ ] 5. Aggregate Server (GenServer process per instance)
[ ] 6. Snapshot Types & Integration
[ ] 7. Command Context & Execution
[ ] 8. Middleware Pipeline
[ ] 9. Command Router (wiring aggregate server, identity, retry)
[ ] 10. Event Handler (lifecycle, subscriptions, error handling, idempotency)
[ ] 11. Projection (builds on event handler patterns)
[ ] 12. Process Manager (routing, state persistence, error handling, command dispatch)
[ ] 13. Application & Supervision Tree
[ ] 14. Causation & Correlation Chain
[ ] 15. Strong vs Eventual Consistency
[ ] 16. PostgreSQL Adapter
[ ] 17. SQLite Adapter
[ ] 18. Multi Module
[ ] 19. Aggregate Lifespan
[ ] 20. Event Upcasting
[ ] 21. Telemetry & Observability
```

Status markers:
- `[ ]` — not started
- `[~]` — was completed but needs rework (architecture review found issues)
- `[x]` — completed and survived architecture review

---

## Phase 1: Module Deep Comparison

For the current module, perform an exhaustive comparison. This is the most important step — rushed comparisons lead to incomplete implementations.

### What To Read in Commanded

For each module, read the ACTUAL SOURCE CODE, not just the research docs. The research docs are a starting point. Always verify against source.

**Key Commanded source paths:**
- Aggregates: `lib/commanded/aggregates/*.ex`
- Commands/Router: `lib/commanded/commands/*.ex`
- Event Handler: `lib/commanded/event/handler.ex`, `lib/commanded/event/error_handler.ex`, `lib/commanded/event/failure_context.ex`
- Event Store: `lib/commanded/event_store/*.ex`, `lib/commanded/event_store/adapters/in_memory.ex`
- Middleware: `lib/commanded/middleware/*.ex`
- Process Managers: `lib/commanded/process_managers/*.ex`
- PubSub: `lib/commanded/pubsub/*.ex`
- Registration: `lib/commanded/registration/*.ex`
- Subscriptions: `lib/commanded/event_store/subscription.ex`
- Application: `lib/application.ex`, `lib/commanded/application/*.ex`
- Guides: `guides/*.md`

### Comparison Checklist (apply to every module)

For each module, extract and compare:

1. **Public API**: Every public function/callback, its signature, return types, and all valid return values
2. **Configuration options**: Every option, its type, default value, and validation rules
3. **Process model**: Is it a GenServer? Supervisor? Plain module? What's its lifecycle?
4. **State shape**: What data does it hold? What's mutable vs immutable?
5. **Invariants**: What MUST always be true? (e.g., "apply/2 must never fail", "subscription name must never change")
6. **Error handling**: What errors can occur? How are they handled? What's the default?
7. **Concurrency**: What's serialized? What's parallel? What are the race conditions?
8. **Integration points**: How does this module interact with others? What does it call? What calls it?
9. **Edge cases**: What happens on empty input? On restart? On timeout? On crash?
10. **Tests**: What test cases exist in Commanded? What should exist in Instructed?

### Output

Write the comparison into the task file notes for reference during implementation.

---

## Phase 2: Implement for Parity

Implement changes to bring the Instructed module to parity.

### Your Standard of Quality

Your work will be reviewed using the process defined in `instructed-review-instructions.md`. That review compares every feature, constraint, and guarantee against Commanded. Your goal is:

- **Every feature for this module scores ✅** (present and equivalent)
- **Zero ⚠️ or ❌ findings** for this module when reviewed
- **No silent gaps** — if something is intentionally different (Gleam idiom), it must be clearly documented in code comments explaining why it differs and that the equivalent guarantee is preserved
- **Tests prove correctness** — the review checks for test coverage; missing tests are a finding

Think of it this way: after you finish a module, a separate agent will run the review process from `instructed-review-instructions.md`. That agent will read Commanded source, read your code, and compare them exhaustively. Anything it finds is a failure. Your job is to leave nothing for it to find.

### Key principles:

### Gleam Idioms (Intentional Differences)

These are NOT gaps — they're correct Gleam translations:
- **Records of functions** instead of behaviours/macros — this is idiomatic Gleam
- **Result types** instead of multiple return value patterns — Gleam's type system handles this
- **No compile-time macros** — Gleam doesn't have them; runtime configuration is fine
- **Pattern matching on custom types** instead of protocol dispatch

### Things That MUST Match Commanded

These are non-negotiable for a correct CQRS/ES system:
- **Aggregate commands serialized per instance** via GenServer/actor process
- **Optimistic concurrency** with expected version on every append
- **Retry on version conflict** with state rebuild from new events (not full replay)
- **Event handlers acknowledge events** to track subscription position
- **Idempotency guards** — last_seen_event check before processing
- **Errors never silently swallowed** — always surface to error callback or caller
- **Subscription callbacks execute outside the event store process**
- **Process manager state persisted** via snapshots — survives restart
- **Causation/correlation IDs propagated** through PM command dispatch
- **Event ordering guaranteed** — handlers receive events in global order

### Testing

Every module must have tests. At minimum:
- Happy path for each public function
- Error cases for each error type
- Concurrency/ordering tests where relevant (e.g., version conflict retry)
- Restart/recovery tests for stateful components (handlers, PMs)

### Commit

Commit after each module is implemented (before architecture review):
```bash
git add -A && git commit -m "Module N: <name> — implement for Commanded parity"
```

---

## Phase 3: Re-Compare (Self-Review)

After implementation, run a self-review using the criteria from `instructed-review-instructions.md`. Specifically:

1. Re-read the Commanded source for this module
2. Apply the review format: "What Commanded Provides" vs "What Instructed Provides" with ✅/⚠️/❌ markers
3. Check every item from the 10-point comparison checklist (Phase 1)
4. Verify tests exist for every public function, error case, and edge case
5. Check that the 20 Key Invariants (listed at the bottom of this document) are not violated

**The bar**: If a review agent running `instructed-review-instructions.md` would find ANY issue with this module — a missing feature, a silent error, a missing test, a broken invariant — you are not done. Go back to Phase 2.

Only proceed to Phase 4 when you are confident this module would score ✅ across every comparison point.

---

## Phase 4: Architecture Review

**This is triggered after EACH module completion.** It reviews ALL completed modules together.

### What To Check

For every pair of completed modules that interact:

1. **Data flow**: Does data flow correctly between them? Are types compatible?
2. **Process boundaries**: Which module owns which process? Are there unexpected blockings?
3. **Error propagation**: When module A fails, does module B handle it correctly?
4. **Lifecycle coordination**: When module A starts/stops, does module B react correctly?
5. **Concurrency**: Can modules A and B create race conditions together?
6. **Causation chain**: Does the causation/correlation chain flow correctly across module boundaries?

### Specific Integration Points to Verify

(Check these as the relevant modules become complete)

- [ ] **Event Store → Event Handler**: Subscription callbacks run in handler's process, not event store's process
- [ ] **Event Store → Aggregate Server**: append_to_stream returns correct version; stream_forward supports incremental reads
- [ ] **Aggregate Server → Router**: Router dispatches through aggregate server (not directly); server caches state
- [ ] **Router → Middleware**: Pipeline response actually influences dispatch result (not discarded)
- [ ] **Event Handler → Subscription tracking**: ack_event called after successful processing; position persisted
- [ ] **Process Manager → Router**: PM dispatches commands through router with causation_id from triggering event
- [ ] **Process Manager → Event Store**: PM state saved as snapshot after each event; restored on restart
- [ ] **Application → All**: Supervision tree starts all components; named application isolation works
- [ ] **Consistency → Subscriptions**: Strong consistency handlers register; dispatch waits for acks
- [ ] **Aggregate Server → Snapshots**: Snapshot read during state population; snapshot written after N events

### If Issues Found

1. Identify which completed module(s) need changes
2. Mark those modules as `[~]` in the task file
3. The next iteration will pick up the first `[~]` module and rework it
4. After rework, another architecture review runs

### Commit

```bash
git add -A && git commit -m "Architecture review after module N: <findings/fixes>"
```

---

## Reference: Commanded Module-to-Instructed Module Mapping

| Commanded Module | Instructed File | Notes |
|---|---|---|
| `Commanded.Aggregates.Aggregate` | `aggregate.gleam` + `aggregate_server.gleam` | Split into types + process |
| `Commanded.Aggregates.AggregateStateBuilder` | Part of `aggregate_server.gleam` | State rebuild logic |
| `Commanded.Aggregates.Multi` | (not yet created) | Needs new module |
| `Commanded.Aggregates.Supervisor` | (not yet created) | DynamicSupervisor for aggregates |
| `Commanded.Aggregates.AggregateLifespan` | (not yet created) | Lifespan behaviour |
| `Commanded.Aggregates.ExecutionContext` | `command_context.gleam` | Rename/expand |
| `Commanded.Commands.Router` | `router.gleam` | Needs aggregate server integration |
| `Commanded.Commands.CompositeRouter` | (not yet created) | Optional |
| `Commanded.Commands.Dispatcher` | Part of `router.gleam` | Dispatch logic |
| `Commanded.Commands.Handler` | (not needed) | Gleam uses function records |
| `Commanded.Commands.ExecutionResult` | Part of `router.gleam` | `DispatchResult` type |
| `Commanded.Event.Handler` | `event_handler.gleam` | Needs major rework |
| `Commanded.Event.ErrorHandler` | (not yet created) | Error strategies |
| `Commanded.Event.FailureContext` | (not yet created) | Failure context type |
| `Commanded.Event.Upcast` | (not yet created) | Event upcasting |
| `Commanded.EventStore.Adapter` | `event_store.gleam` | Record of functions |
| `Commanded.EventStore.Adapters.InMemory` | `in_memory_event_store.gleam` | Needs fixes |
| `Commanded.EventStore.RecordedEvent` | `event.gleam` | Mostly complete |
| `Commanded.EventStore.Subscription` | (not yet created) | Subscription lifecycle |
| `Commanded.Middleware` | `middleware.gleam` | Needs expansion |
| `Commanded.Middleware.Pipeline` | Part of `middleware.gleam` | |
| `Commanded.Middleware.ExtractAggregateIdentity` | (not yet created) | Built-in middleware |
| `Commanded.Middleware.ConsistencyGuarantee` | (not yet created) | Built-in middleware |
| `Commanded.ProcessManagers.ProcessManager` | `process_manager.gleam` | Needs major rework |
| `Commanded.ProcessManagers.ProcessRouter` | Part of `process_manager.gleam` | |
| `Commanded.ProcessManagers.ProcessManagerInstance` | (not yet created) | Per-instance process |
| `Commanded.Subscriptions` | (not yet created) | Consistency tracking |
| `Commanded.PubSub` | (not yet created) | Internal pub/sub |
| `Commanded.Registration` | (not yet created) | Process registration |
| `Commanded.Application` | `application.gleam` | Needs major rework |
| `Commanded.Serialization` | (handled by adapters) | Per-adapter serialization |

---

## Reference: Key Commanded Invariants

These must hold true in Instructed. Verify each during architecture review.

1. Commands to the same aggregate instance are serialized (GenServer)
2. `apply/2` (apply_event) must never fail — it's used during replay
3. Events are appended atomically with expected version check
4. On version conflict, rebuild state from NEW events only (not full replay), then retry
5. Event handlers are singletons — one instance per handler name across the cluster
6. Handler subscription name must never change between releases
7. `start_from` only applies on FIRST subscription creation — restarts resume from last ack
8. Event handler errors invoke error/3 callback — never silently swallowed
9. Process manager state is persisted (snapshot) after each successfully handled event
10. PM dispatches commands with causation_id = source event_id, correlation_id preserved
11. Strong consistency blocks dispatch until all strong handlers have ack'd
12. Subscription callbacks deliver events to subscriber's process (not event store's process)
13. Aggregate stream prefix must never change after events are persisted
14. Snapshot version must be incremented when aggregate struct changes
15. Multi errors discard all events — atomic all-or-nothing
16. Default retry attempts: 10 for version conflicts
17. Default dispatch timeout: 5 seconds
18. Event batch read size: 1,000 when rebuilding state
19. Aggregate processes are :temporary — started on demand, not restarted by supervisor
20. Handler `last_seen_event` provides in-process idempotency guard
