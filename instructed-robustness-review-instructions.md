# Instructed Robustness Review Instructions

This document describes how to produce a deep correctness and robustness review of **Instructed** (this Gleam CQRS/ES repo), using **Commanded** (https://github.com/commanded/commanded) and **Commanded EventStore** (https://github.com/commanded/eventstore) as reference implementations.

The correctness guarantees of a CQRS/ES framework depend critically on a well-behaved event store. Commanded and its EventStore library work in concert — both must be studied. Instructed has three event store adapters (in-memory, SQLite, PostgreSQL) which must all be reviewed.

Read this document fully before starting. Follow the phases in order.

**Output file: `REVIEW-robustness.md`**

---

## Design Principles

1. **Correctness over feature counting.** "Does the OCC retry loop actually prevent lost writes?" matters more than "is there a CompositeRouter?"
2. **Evidence-based.** Every claim must cite a source file and function/line. Test coverage (or lack thereof) is primary evidence.
3. **Tiered severity.** Not all gaps are equal. A missing convenience feature is not the same as a data-loss bug.
4. **Read the tests.** A feature without tests is a feature that might not work.

---

## Phase 1: Research (Parallel Sub-Agents)

### 1A: Research Commanded

Clone Commanded and launch **two** research sub-agents. We want focused, high-quality research — not three agents racing.

```bash
if [ ! -d /tmp/commanded ]; then
  git clone --depth 1 https://github.com/commanded/commanded.git /tmp/commanded
fi

if [ ! -d /tmp/commanded-eventstore ]; then
  git clone --depth 1 https://github.com/commanded/eventstore.git /tmp/commanded-eventstore
fi
```

**Sub-Agent A — Core Guarantees & Invariants:**

> You are a research agent. Analyze the Commanded Elixir library to extract the **fundamental CQRS/ES guarantees and invariants** it provides. Output to `/tmp/research_commanded_guarantees.md`.
>
> For each guarantee, document: (1) what the guarantee is, (2) the mechanism that enforces it, (3) what happens when the mechanism fails, (4) relevant source code locations.
>
> Also research the **Commanded EventStore** library (`/tmp/commanded-eventstore/`), which provides the production PostgreSQL-backed event store. The framework's correctness guarantees depend on the event store's behavior — both must be studied together.
>
> Guarantees to investigate:
> - **Event append atomicity**: How does `append_to_stream` ensure events are persisted atomically? What happens on partial failure? How does the Commanded EventStore enforce this at the database level?
> - **Optimistic concurrency**: How is `expected_version` checked? What triggers retry? How many retries? What if all retries fail? How does the EventStore's schema enforce this?
> - **Command serialization per aggregate**: How does the GenServer model ensure no concurrent command execution? What about Task isolation?
> - **Event ordering**: Global ordering (event_number) vs per-stream (stream_version). Are there gaps? How are subscriptions ordered?
> - **At-least-once delivery**: How do persistent subscriptions ensure events are not lost? What is the ack protocol? What happens on handler crash before ack?
> - **Idempotency**: How is duplicate processing prevented? At handler level? At PM level? After restart?
> - **Causation/correlation chain**: How are IDs propagated through command → event → PM → command → event?
> - **Snapshot consistency**: Can a snapshot be read that is inconsistent with the event stream? How is version validated?
> - **Process manager state persistence**: When is state saved? What happens if the process crashes between command dispatch and snapshot write?
> - **Strong consistency**: How does the wait mechanism work? What are the failure modes (timeout, deadlock, single-node limitation)?
> - **Error recovery**: What happens when a handler returns an error? When a PM command dispatch fails? Default behaviors.
> - **Supervision and restart**: What restarts what? What state is lost on restart? What is preserved?
>
> Read these files:
> - `/tmp/commanded/lib/commanded/aggregates/aggregate.ex`
> - `/tmp/commanded/lib/commanded/aggregates/execution_context.ex`
> - `/tmp/commanded/lib/commanded/commands/dispatcher.ex`
> - `/tmp/commanded/lib/commanded/event/handler.ex`
> - `/tmp/commanded/lib/commanded/process_managers/process_manager_instance.ex`
> - `/tmp/commanded/lib/commanded/process_managers/process_router.ex`
> - `/tmp/commanded/lib/commanded/subscriptions.ex`
> - `/tmp/commanded/lib/commanded/event_store/adapters/in_memory.ex`
> - `/tmp/commanded/lib/commanded/middleware/consistency_guarantee.ex`
> - `/tmp/commanded-eventstore/lib/event_store.ex`
> - `/tmp/commanded-eventstore/lib/event_store/storage/appender.ex`
> - `/tmp/commanded-eventstore/lib/event_store/storage/subscription.ex`
> - `/tmp/commanded-eventstore/lib/event_store/subscriptions/*.ex`
> - `/tmp/commanded-eventstore/priv/event_store/migrations/*.sql`
>
> Be precise. Quote code where it clarifies behavior.

**Sub-Agent B — Feature Surface & API:**

> You are a research agent. Analyze the Commanded Elixir library to document its **complete public API surface and feature set**. Output to `/tmp/research_commanded_features.md`.
>
> For each feature area, document: (1) the public API (callbacks, functions, configuration), (2) default values, (3) edge cases, (4) what's optional vs required.
>
> Feature areas:
> - Aggregate definition (callbacks, return types, state struct)
> - Command routing (identify, dispatch macros, options, CompositeRouter)
> - Middleware pipeline (callbacks, Pipeline struct fields, halting, built-in middleware)
> - Event handlers (configuration options, handle/2, handle_batch/1, error/3, state, concurrency, partitioning)
> - Process managers (interested?/1, handle/2, apply/2, after_command/2, error/3 for events vs commands)
> - Multi module (API, named steps, reduce, nested, atomicity)
> - Lifespan (callbacks, return values, DefaultLifespan, interaction with snapshots)
> - Event upcasting (protocol, chaining)
> - Snapshots (config, versioning, aggregate vs PM)
> - Event store adapter interface (full callback list, types)
> - Application supervision (tree structure, dynamic apps, config priority)
> - Telemetry events (full catalogue)
> - Serialization (JsonSerializer, JsonDecoder, TypeProvider)
>
> Read these files:
> - `/tmp/commanded/guides/*.md` (all guide files)
> - `/tmp/commanded/lib/commanded/aggregates/multi.ex`
> - `/tmp/commanded/lib/commanded/commands/router.ex`
> - `/tmp/commanded/lib/commanded/commands/composite_router.ex`
> - `/tmp/commanded/lib/commanded/event/handler.ex`
> - `/tmp/commanded/lib/commanded/process_managers/process_manager.ex`
> - `/tmp/commanded/lib/commanded/aggregates/aggregate_lifespan.ex`
> - `/tmp/commanded/lib/commanded/event_store/adapter.ex`
> - `/tmp/commanded-eventstore/lib/event_store.ex`
> - `/tmp/commanded-eventstore/lib/event_store/subscriptions/subscription.ex`
>
> Be exhaustive on API surface. Include every configuration option, callback, and return type.

### 1B: Research CQRS/ES Invariants (Independent of Commanded)

Launch a **third** sub-agent for framework-independent CQRS/ES theory:

> You are a research agent. Document the **fundamental invariants and guarantees that any correct CQRS/ES system must provide**, independent of any specific framework. Output to `/tmp/research_cqrs_invariants.md`.
>
> For each invariant, explain: (1) what it is, (2) why it matters, (3) what goes wrong if violated, (4) how it's typically enforced.
>
> Cover at minimum:
> - **Event store append atomicity**: All events from a single command are persisted atomically (all or none)
> - **Optimistic concurrency on write**: Concurrent writes to the same stream are detected and one is rejected
> - **Command serialization per aggregate**: Only one command executes against an aggregate at a time
> - **Event immutability**: Events, once written, are never modified or deleted
> - **Deterministic state rebuild**: Replaying the same events always produces the same state
> - **At-least-once event delivery**: Every persisted event is eventually delivered to every subscriber
> - **Ordered delivery per stream**: Events within a stream are delivered in stream_version order
> - **Idempotent event handling**: Handlers must tolerate receiving the same event more than once
> - **Causation chain integrity**: The causal relationship between commands and events is preserved
> - **Saga/PM atomicity considerations**: Commands dispatched by a PM may partially succeed — what are the implications?
> - **Read model eventual consistency**: Read models may lag behind the write model
> - **Snapshot consistency**: A snapshot must represent a valid state reachable by replaying events up to its version
>
> Also cover common failure modes:
> - Handler crash between processing and ack
> - Event store unavailable during command dispatch
> - Snapshot from a different schema version
> - Process manager crash between command dispatch and state persistence
> - Concurrent aggregate access bypassing the serialization mechanism
>
> Use references to academic papers, Martin Fowler's patterns, Greg Young's writings, or the EventStore documentation where appropriate.

### Launching and Waiting

```bash
tmux new-window -n agent_a "pi -p '<prompt_a>' 2>&1; sleep 5"
tmux new-window -n agent_b "pi -p '<prompt_b>' 2>&1; sleep 5"
tmux new-window -n agent_c "pi -p '<prompt_c>' 2>&1; sleep 5"
```

**Wait for ALL THREE to complete before proceeding:**

```bash
while true; do
  count=0
  for f in /tmp/research_commanded_guarantees.md /tmp/research_commanded_features.md /tmp/research_cqrs_invariants.md; do
    if [ -f "$f" ] && [ $(wc -l < "$f" 2>/dev/null || echo 0) -gt 100 ]; then count=$((count+1)); fi
  done
  echo "$(date +%H:%M:%S) - $count/3 research files ready"
  if [ $count -eq 3 ]; then echo "All research complete!"; break; fi
  sleep 15
done
```

**After all three complete, read ALL three research files fully before proceeding to Phase 2.**

---

## Phase 2: Read the Instructed Codebase

Read **every** file listed below. Do not skip any.

### Source Files (read all)

```
/workspace/instructed/src/instructed.gleam
/workspace/instructed/src/instructed/*.gleam        (every module)
/workspace/instructed-postgres/src/*.gleam
/workspace/instructed-sqlite/src/*.gleam
```

### Test Files (read all — this is critical)

```
/workspace/instructed/test/*.gleam                  (every test file)
```

For each test file, note:
- What features/invariants does it test?
- What edge cases are covered?
- What is NOT tested?

### Supporting Files

```
/workspace/instructed/src/instructed/conformance/*.gleam   (conformance test helpers)
/workspace/example-todo/server/src/todo_server/*.gleam     (example app — not build packages)
/workspace/README.md
/workspace/NOTES.md (if exists)
```

---

## Phase 3: Produce the Review

Use **parallel sub-agents** to review different tiers simultaneously. The main agent reads all research, reads all source code, then launches review sub-agents and assembles the final document.

### Tier Structure

The review is organized in three tiers by importance:

**Tier 1 — Correctness Invariants** (must be right for production use):
1. Event append atomicity and OCC
2. Command serialization per aggregate
3. At-least-once delivery and subscription protocol
4. Idempotency (handler, PM, after restart)
5. Error handling and failure recovery (handler crash, PM crash, dispatch failure)
6. Process lifecycle and supervision (what restarts, what's lost)

**Tier 2 — Core Feature Parity** (should match Commanded for a useful framework):
7. Aggregates (definition, state rebuild, snapshot integration)
8. Command dispatch pipeline (routing, middleware, identity, timeout, retry)
9. Event handlers (lifecycle, configuration, error callbacks, state)
10. Process managers (routing, handle/apply order, state persistence, causation chain)
11. Strong vs eventual consistency (mechanism, timeout, deadlock prevention)
12. Snapshots (aggregate + PM, versioning, correctness)

**Tier 3 — Advanced & Convenience Features** (nice to have):
13. Multi module
14. Aggregate lifespan management
15. Event upcasting
16. Telemetry & observability
17. Projections (as distinct from event handlers)
18. Missing features (composite router, batch processing, handler concurrency, PubSub, reset)

### Launching Review Sub-Agents

Launch **three** review sub-agents, one per tier:

**Review Sub-Agent 1 — Tier 1 (Correctness):**

> You are a review agent. You will produce a deep correctness analysis of the Instructed CQRS/ES framework.
>
> Read these research files first:
> - `/tmp/research_commanded_guarantees.md`
> - `/tmp/research_cqrs_invariants.md`
>
> Then read ALL of these Instructed source files:
> - `/workspace/instructed/src/instructed/aggregate.gleam`
> - `/workspace/instructed/src/instructed/aggregate_server.gleam`
> - `/workspace/instructed/src/instructed/event_store.gleam`
> - `/workspace/instructed/src/instructed/in_memory_event_store.gleam`
> - `/workspace/instructed/src/instructed/event_handler.gleam`
> - `/workspace/instructed/src/instructed/process_manager.gleam`
> - `/workspace/instructed/src/instructed/subscriptions.gleam`
> - `/workspace/instructed/src/instructed/error.gleam`
> - `/workspace/instructed/src/instructed/application.gleam`
> - `/workspace/instructed/src/instructed/router.gleam`
>
> And ALL test files:
> - `/workspace/instructed/test/*.gleam` (every test file)
>
> And ALL event store adapters:
> - `/workspace/instructed/src/instructed/in_memory_event_store.gleam`
> - `/workspace/instructed-postgres/src/instructed_postgres.gleam`
> - `/workspace/instructed-sqlite/src/instructed_sqlite.gleam`
>
> For each of these 7 correctness areas, write a detailed analysis:
>
> 1. **Event append atomicity and OCC** — review all three adapters (in-memory, SQLite, PostgreSQL) against guarantees from Commanded EventStore
> 2. **Command serialization per aggregate**
> 3. **At-least-once delivery and subscription protocol** — review persistent subscription implementations in all three adapters
> 4. **Idempotency**
> 5. **Error handling and failure recovery**
> 6. **Process lifecycle and supervision**
> 7. **Event store adapter correctness** — systematic comparison of all three Instructed adapters against the Commanded EventStore adapter behaviour and the production EventStore library. Cover: append semantics, subscription state machine, checkpoint durability, gap handling, schema constraints
>
> For each area use this format:
>
> ```
> ## T1.N. Area Name [VERDICT]
>
> ### Required Invariant
> What must be true for correctness. Reference /tmp/research_cqrs_invariants.md.
>
> ### How Commanded Enforces It
> Specific mechanism. Reference /tmp/research_commanded_guarantees.md.
>
> ### How Instructed Implements It
> Trace the actual code path. Cite specific files, functions, and line patterns.
> Show the relevant code flow step by step.
>
> ### Test Coverage
> What tests exist for this invariant? Cite specific test files and test function names.
> What is NOT tested? What edge cases are missing?
>
> ### Verdict & Issues
> Does Instructed correctly enforce this invariant?
> List specific issues with severity (CRITICAL/HIGH/MEDIUM/LOW).
> For each issue, cite the exact code location and explain the failure mode.
> ```
>
> Verdicts: ✅ CORRECT | ⚠️ MOSTLY CORRECT | ❌ BROKEN | 🔍 UNTESTED
>
> Output your analysis to `/tmp/review_tier1_correctness.md`.
>
> **Key principle: trace the actual code paths, don't just check if types exist. A feature isn't correct if the types are defined but the wiring is wrong.**

**Review Sub-Agent 2 — Tier 2 (Core Features):**

> You are a review agent. You will produce a detailed feature comparison of Instructed vs Commanded for core CQRS/ES features.
>
> Read these research files first:
> - `/tmp/research_commanded_features.md`
> - `/tmp/research_commanded_guarantees.md`
>
> Then read ALL of these Instructed source files:
> - `/workspace/instructed/src/instructed/*.gleam` (every module)
> - `/workspace/instructed-postgres/src/instructed_postgres.gleam`
> - `/workspace/instructed-sqlite/src/instructed_sqlite.gleam`
>
> And ALL test files:
> - `/workspace/instructed/test/*.gleam` (every test file)
>
> For each of these 6 feature areas, write a detailed comparison:
>
> 7. **Aggregates** (definition, state rebuild, snapshot integration)
> 8. **Command dispatch pipeline** (routing, middleware, identity, timeout, retry)
> 9. **Event handlers** (lifecycle, configuration, error callbacks, state)
> 10. **Process managers** (routing, handle/apply order, state persistence, causation chain)
> 11. **Strong vs eventual consistency** (mechanism, timeout, deadlock prevention)
> 12. **Snapshots** (aggregate + PM, versioning, correctness)
>
> For each area use this format:
>
> ```
> ## T2.N. Feature Name [STATUS]
>
> ### Commanded Reference
> What Commanded provides. Be specific about API, defaults, edge cases.
>
> ### Instructed Implementation
> What Instructed provides. Map each Commanded feature point to Instructed.
> Use: ✅ equivalent | ⚠️ partial | ❌ missing | 📝 intentional difference
>
> ### Test Coverage
> What tests exist? What's untested?
>
> ### Gaps & Issues
> Specific issues with severity. Cite code locations.
> Distinguish design choices from bugs/omissions.
> ```
>
> Output your analysis to `/tmp/review_tier2_features.md`.

**Review Sub-Agent 3 — Tier 3 (Advanced & Missing):**

> You are a review agent. You will document advanced features, convenience features, and notable missing capabilities in Instructed compared to Commanded.
>
> Read these research files first:
> - `/tmp/research_commanded_features.md`
>
> Then read ALL of these Instructed source files:
> - `/workspace/instructed/src/instructed/*.gleam` (every module)
>
> And these test files:
> - `/workspace/instructed/test/multi_test.gleam`
> - `/workspace/instructed/test/lifespan_test.gleam`
> - `/workspace/instructed/test/upcast_test.gleam`
> - `/workspace/instructed/test/telemetry_test.gleam`
> - `/workspace/instructed/test/projection_test.gleam`
>
> For each of these 6 areas, write a concise comparison:
>
> 13. **Multi module**
> 14. **Aggregate lifespan management**
> 15. **Event upcasting**
> 16. **Telemetry & observability**
> 17. **Projections** (as distinct from event handlers)
> 18. **Missing features** (composite router, batch processing, handler concurrency, PubSub, reset, serialization/TypeProvider)
>
> For each area use this format:
>
> ```
> ## T3.N. Feature Name [STATUS]
>
> ### Commanded Reference
> Brief summary of what Commanded provides.
>
> ### Instructed Implementation
> What exists, what's missing. Use status indicators.
>
> ### Assessment
> Severity of any gaps. Is this a blocker or a nice-to-have?
> ```
>
> Output your analysis to `/tmp/review_tier3_advanced.md`.

### Launching and Waiting

```bash
tmux new-window -n review1 "pi -p '<review_prompt_1>' 2>&1; sleep 5"
tmux new-window -n review2 "pi -p '<review_prompt_2>' 2>&1; sleep 5"
tmux new-window -n review3 "pi -p '<review_prompt_3>' 2>&1; sleep 5"
```

```bash
while true; do
  count=0
  for f in /tmp/review_tier1_correctness.md /tmp/review_tier2_features.md /tmp/review_tier3_advanced.md; do
    if [ -f "$f" ] && [ $(wc -l < "$f" 2>/dev/null || echo 0) -gt 50 ]; then count=$((count+1)); fi
  done
  echo "$(date +%H:%M:%S) - $count/3 review files ready"
  if [ $count -eq 3 ]; then echo "All reviews complete!"; break; fi
  sleep 15
done
```

---

## Phase 4: Assemble the Final Review

After all three review sub-agents complete, **read all three review files**, then assemble `REVIEW-robustness.md`.

### Document Structure

```markdown
# CQRS/ES Robustness Review: Instructed vs Commanded

> [metadata: date, methodology description]

## Executive Summary
- One paragraph: overall assessment
- Verdicts by tier: Tier 1 (X/6 correct), Tier 2 (X/6 at parity), Tier 3 (X/6 present)
- Top 3 critical issues

---

## Tier 1: Correctness Invariants

[Paste/edit from review_tier1_correctness.md]
[Ensure all 6 areas are present with full format]

---

## Tier 2: Core Feature Parity

[Paste/edit from review_tier2_features.md]
[Ensure all 6 areas are present with full format]

---

## Tier 3: Advanced & Convenience Features

[Paste/edit from review_tier3_advanced.md]
[Ensure all 6 areas are present with full format]

---

## Summary

### Correctness Scorecard
| # | Invariant | Verdict | Issues |
|---|-----------|---------|--------|

### Feature Parity Scorecard
| # | Feature | Status | Severity |
|---|---------|--------|----------|

### Critical Issues (must-fix for production)
[Ordered list with specific code citations]

### Recommended Improvements (by priority)
[Ordered list — correctness fixes first, then features, then convenience]

### Test Coverage Gaps
[Areas with no tests or insufficient tests]
```

### Assembly Principles

1. **Preserve specificity.** Don't summarize away code citations or test coverage analysis from the sub-agents. The value is in the detail.
2. **Resolve conflicts.** If two sub-agents assessed the same area differently, investigate and pick the more accurate assessment.
3. **Add cross-cutting observations.** Things that span multiple areas (e.g., "subscription recreation on restart affects both handlers and PMs").
4. **Ensure every issue has a code citation.** If a sub-agent flagged an issue without citing code, find the code location and add it.

---

## Phase 5: Commit

```bash
cd /workspace && git add REVIEW-robustness.md && git commit -m "Add CQRS/ES robustness review: Instructed vs Commanded"
```

---

## Key Principles (For All Agents)

1. **Trace code paths, not types.** A type existing doesn't mean it's wired in. Follow the actual execution from dispatch to event persistence to handler delivery.
2. **Read the tests.** No test = unverified = suspect. A well-tested feature with a minor gap is better than an untested feature that looks complete.
3. **Cite everything.** File names, function names, code patterns. "The handler silently continues on error" → "In `event_handler.gleam`, `handle_error` with `on_error: None` calls `actor.stop()` which is correct, but in `process_manager.gleam`, `handle_event_error` with `on_event_error: None` returns `state` unchanged, silently skipping the error."
4. **Severity matters.** CRITICAL = data loss or incorrect behavior in production. HIGH = significant missing guarantee. MEDIUM = missing feature that has workarounds. LOW = convenience or completeness.
5. **Design choices are fine.** Gleam has no macros (runtime router is fine). Gleam has no lazy streams (batched reading is fine). Single-actor PM is a tradeoff (document it, don't condemn it). Flag these as 📝 not ❌.

---

## Estimated Time

- Phase 1 (research sub-agents): ~5-10 minutes
- Phase 2 (read codebase): ~5 minutes (must be thorough — read tests!)
- Phase 3 (review sub-agents): ~10-15 minutes
- Phase 4 (assemble): ~10-15 minutes
- Phase 5 (commit): ~1 minute

Total: ~30-45 minutes
