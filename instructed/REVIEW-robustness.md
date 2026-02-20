# Instructed CQRS/ES Framework — Robustness Review

> **Scope**: End-to-end robustness review of the Instructed framework (Gleam CQRS/ES)
> compared against Commanded (Elixir), covering correctness invariants, feature parity,
> and advanced capabilities.

---

## Executive Summary

Instructed is a well-structured CQRS/ES framework for Gleam that achieves **strong feature parity** with Commanded across core CQRS/ES concepts. The framework correctly implements the fundamental invariants — event append atomicity via OCC, command serialization per aggregate, at-least-once delivery, and causation/correlation chains. Several areas (upcasting, projections, explicit Multi chaining) are **arguably superior** to Commanded's equivalents.

However, the review identified **one critical issue** (registry never cleans up dead aggregate servers) and several **moderate concerns** around process lifecycle, handler restart behavior, and missing production convenience features.

### Verdict Summary

| Tier | Area | Verdict |
|------|------|---------|
| **1** | Event Append Atomicity & OCC | ⚠️ Mostly Correct |
| **1** | Command Serialization | ✅ Correct |
| **1** | At-Least-Once Delivery | ⚠️ Mostly Correct |
| **1** | Idempotency | ⚠️ Mostly Correct |
| **1** | Error Handling & Recovery | ⚠️ Mostly Correct |
| **1** | Process Lifecycle & Supervision | ❌ Broken |
| **2** | Aggregates | ✅ Strong |
| **2** | Command Dispatch Pipeline | ⚠️ Mostly Complete |
| **2** | Event Handlers | ✅ Strong |
| **2** | Process Managers | ✅ Strong |
| **2** | Strong vs Eventual Consistency | ✅ Strong |
| **2** | Snapshots | ✅ Strong |
| **3** | Multi Module | ✅ Complete |
| **3** | Aggregate Lifespan | ✅ Complete |
| **3** | Event Upcasting | ✅ Complete (superior) |
| **3** | Telemetry | ✅ Complete |
| **3** | Projections | ✅ Bonus Feature |

---

## Critical Issues

### 1. Registry Never Cleans Up Dead Aggregate Servers (CRITICAL)

**Location**: `router.gleam` — `RegistryState.servers` dict

The router's registry stores aggregate server `Subject` references in a dict. When an aggregate server stops (via lifespan timeout, error, or `actor.stop()`), the registry is **never notified**. The stale `Subject` remains indefinitely.

On the next command to that aggregate, `get_or_start_server` returns the stale Subject. The `process.call` to the dead process times out after 5000ms, returning `Error(error.Timeout)`. The registry never starts a replacement.

**Impact**: After any aggregate server stops (which is normal — lifespan timeouts are a supported feature), that aggregate becomes **permanently unreachable** until the entire application is restarted.

**Recommended fix**: Monitor aggregate server processes from the registry. On process exit, remove the entry from the dict. Alternatively, check `process.is_alive` before returning cached subjects. (Note: the Tier 2 review found stale server detection exists in some paths — verify it covers all cases.)

### 2. Deadlock Prevention PID Registration Bug (HIGH)

**Location**: `event_handler.gleam` — `subscriptions.register_pid` call

The `register_pid` call uses `process.self()` from the **spawning process**, not from within the handler actor. When the handler actor later dispatches a command with strong consistency, the `wait_for` call captures the handler actor's PID, but the registered PID is the spawner's PID. The auto-exclusion won't match, and the handler could **deadlock waiting for itself**.

**Recommended fix**: Move the `register_pid` call inside the actor's init or first message handler where `process.self()` returns the actor's PID.

---

## High-Priority Issues

### 3. Event Handler Restart Replays All Events from Origin

**Location**: `event_handler.gleam` — `start` function; `process_manager.gleam` — `start` function

On restart, both event handlers and process managers handle `SubscriptionAlreadyExists` by **deleting and recreating** the subscription. This destroys the checkpoint, causing replay from `Origin`. Combined with the handler's in-memory-only `last_seen_event` (not persisted), all historical events are reprocessed.

**Impact**: Correctness is preserved (no events lost), but performance degrades linearly with event history size on every handler restart. The process manager is partially protected by snapshot-based idempotency.

### 4. `returning` Option is Dead Code

**Location**: `command_context.gleam` lines 13-25

`CommandContext.returning` defines `ReturnNothing`, `ReturnAggregateState`, `ReturnEvents`, etc., but is **never referenced** in `router.gleam` or `aggregate_server.gleam`. The router always returns the full `DispatchResult`.

**Recommended fix**: Either wire the `returning` option into the dispatch pipeline or remove it to avoid confusion.

### 5. PM Snapshot Versioning Missing

**Location**: `process_manager.gleam` — `save_pm_snapshot`

Aggregate snapshots use `encode_snapshot_type` with version checking on load. PM snapshots use a hardcoded `"process_manager"` source_type with **no version encoding**. Schema changes to PM state will silently load incompatible snapshots, causing runtime errors.

**Recommended fix**: Apply the same `encode_snapshot_type("process_manager", version)` pattern used for aggregates.

### 6. Unbounded Recursive Retry in Error Handlers

**Location**: `event_handler.gleam` — `handle_error`; `process_manager.gleam` — `handle_event_error`

If the `on_error` callback always returns `Retry(...)`, the framework creates infinite recursion that will blow the stack. No built-in safety valve exists.

**Recommended fix**: Add a max retry counter or detect unbounded recursion.

---

## Moderate Issues

### 7. Event Type Always Empty String via Aggregate Server

**Location**: `aggregate_server.gleam` — `execute_once`

Every `EventData` created through the aggregate server path has `event_type: ""`. The `Aggregate.event_type` callback exists but is unused in this code path. Events lack type metadata for querying and debugging.

### 8. No Monitoring of Event Handler or Process Manager Processes

**Location**: Application-level concern

Neither event handlers nor process managers are monitored or supervised. If they crash, nothing restarts them. The API returns `Subject` values rather than supervision-compatible child specs.

### 9. Aggregate Server Self-Subscription Leak

**Location**: `aggregate_server.gleam` — `start`

Transient subscriptions created for self-notification are never cleaned up when the aggregate server dies. This grows linearly over aggregate server lifecycle events.

### 10. `SetSelf` / `SetSubscriptionInfo` Race Condition

**Location**: `aggregate_server.gleam` — `start`; `event_handler.gleam` — `start`

Setup messages sent asynchronously after actor creation may not be processed before the first real message arrives, causing silent failures in lifespan scheduling or event acknowledgment.

### 11. PM Default Event Error Silently Skips

**Location**: `process_manager.gleam` — `handle_event_error`

Without `on_event_error` configured, errors are silently swallowed and events are acked. Commanded's default is to **stop** the PM instance on event handling error.

---

## Feature Gaps

### Production Readiness Gaps (Medium-High Severity)

| Gap | Description |
|-----|-------------|
| **Application Module** | No supervision tree orchestration, no multi-tenancy, no centralized config |
| **Composite Router** | No way to compose routers for multi-aggregate applications |
| **Serialization** | No serialization strategy for persistent backends (needed for PostgreSQL/SQLite adapters) |

### Feature Parity Gaps (Low-Medium Severity)

| Gap | Description |
|-----|-------------|
| Handler concurrency | No `concurrency: N` with `partition_by` |
| Batch processing | No `handle_batch` for bulk event processing |
| `before_execute` hook | Missing pre-validation hook (workaround: middleware `before_dispatch`) |
| PM idle/event timeout | No per-instance timeout management |
| Subscription reset | No handler-level reset workflow |

### Minor / Cosmetic Gaps

| Gap | Description |
|-----|-------------|
| Named Multi steps | No `step_name` parameter for intermediate state access |
| `FailureContext` unused | Defined but never constructed or passed to callbacks |
| `assigns` restricted to strings | Commanded allows any type; Instructed uses `Dict(String, String)` |
| `identity_prefix` function variant | Only static string prefixes, no 0-arity function support |

---

## Strengths Over Commanded

| Area | Advantage |
|------|-----------|
| **Event Upcasting** | Explicit `chain`/`chain_all` composition is more powerful than Commanded's single-protocol-dispatch |
| **Projections** | First-class in-memory projections with queryable state — requires external library in Commanded |
| **Multi Module** | Explicit `apply` step between stages is more transparent than Commanded's implicit application |
| **Lifespan Callbacks** | Richer signatures `(state, command)` / `(state, event)` provide more context for decisions |
| **Type Safety** | Gleam's type system catches errors at compile time that would be runtime errors in Elixir |
| **Telemetry** | Dual emission path (pure-Gleam + Erlang `:telemetry`) supports both testing and production |

---

## Test Coverage Assessment

### Well-Tested Areas
- Event store operations (append, read, version conflicts, subscriptions)
- Aggregate state rebuild (pure + event store integration)
- Event handler error strategies (stop, skip, retry, retry-with-delay)
- Process manager lifecycle (start, continue, stop, fan-out)
- Strong consistency (subscriptions ack/wait)
- Middleware pipeline (halting, after_failure)
- Causation/correlation chains
- Snapshot version match/mismatch

### Untested Areas
- OCC retry under concurrent writes
- Handler/PM restart and subscription resumption
- Registry stale-entry handling
- Deadlock prevention (PID exclusion)
- PM command retry/delay paths
- Ack TTL purge mechanism
- Cross-aggregate interactions via PM
- PostgreSQL/SQLite adapter integration (no integration tests visible)

---

## Recommended Priority Order

1. **Fix registry cleanup** — Critical for any application using lifespan management
2. **Fix deadlock prevention PID** — Affects strong-consistency handlers
3. **Wire or remove `returning`** — Dead code cleanup
4. **Add PM snapshot versioning** — Prevents silent data corruption on schema changes
5. **Add retry depth limit** — Prevents stack overflow in error handlers
6. **Move `register_pid` inside actor** — Correct process identity registration
7. **Consider supervision story** — Provide child-spec-compatible start functions
8. **Test untested paths** — Especially OCC retry, handler restart, registry lifecycle
