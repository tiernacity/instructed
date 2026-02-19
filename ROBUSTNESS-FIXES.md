# Robustness Fixes: Implementation Plan

Based on findings from `REVIEW-robustness.md`. These are correctness and liveness fixes — not feature additions. They should be implemented in order.

**Use a ralph loop to implement these fixes.** Each fix is a discrete unit of work with clear verification criteria.

---

## Fix 1: Registry Must Clean Up Dead Aggregate Servers [CRITICAL]

### Problem

In `router.gleam`, `RegistryState` holds a `Dict(String, Subject(...))`. When an aggregate server stops (lifespan timeout, crash, or explicit `actor.stop()`), the registry is never notified. The stale `Subject` remains in the dict. Subsequent commands to that aggregate call `process.call` on a dead process, resulting in a 5000ms timeout → `Error(Timeout)`. The aggregate is permanently unreachable until application restart.

This completely breaks lifespan management — an aggregate that stops after idle timeout can never be used again.

### Root Cause

The registry does a simple `dict.get` → found? return subject : start new server. There is no process monitoring. When `get_or_start_server` returns a stale subject, `process.call` hangs until timeout.

### Fix

**Option A (recommended): Monitor aggregate servers from the registry.**

The registry actor should monitor each aggregate server process. When a monitored process exits, send a cleanup message to the registry to remove the entry.

In Gleam OTP, `process.monitor_process` returns a `ProcessMonitor`. The registry can use `process.selecting_process_down` to receive `ProcessDown` messages.

However, the current `actor.on_message` API only handles one message type. The registry needs to handle both `RegistryMessage` and `ProcessDown`. This requires either:

1. Making `RegistryMessage` include a `ServerDown(stream_id: String)` variant and using a selector
2. Using `process.selecting_process_down` with a mapping function that converts `ProcessDown` to a `RegistryMessage`

Implementation steps:

1. Add a `ServerDown(stream_id: String)` variant to `RegistryMessage`
2. When starting a new aggregate server in `handle_registry_message`, extract the server's `Pid` from the `Subject` using `process.subject_owner`
3. Monitor the PID with `process.monitor_process`
4. Store a mapping from `Pid` → `stream_id` in `RegistryState` so we can look up which stream_id to remove when a `ProcessDown` arrives
5. In the actor's init/selector setup, use `process.selecting_process_down` to map `ProcessDown` messages to `ServerDown(stream_id)` messages
6. Handle `ServerDown` by removing the entry from `servers` dict and the pid mapping

**Note on selectors:** The `actor` module in `gleam_otp` supports custom selectors. Check if `actor.new` supports `.with_selector()` or similar. If not, the registry may need to be implemented as a raw `process.start` with a custom receive loop, or use the approach of trying `process.call` first and falling back to starting a new server on timeout.

**Option B (simpler fallback): Detect dead servers on lookup.**

If monitoring is complex to wire in:

1. In `handle_registry_message` `GetOrStart`, when we find an existing server, try a lightweight ping (e.g., `process.try_call` with a very short timeout, or check if the process is alive via `process.is_alive`)
2. If the process is dead, remove it from the dict and start a new server
3. Gleam has `erlang.process.is_alive` — use the PID from `process.subject_owner` to check

This is simpler but has a race condition (process could die between check and call). Still much better than current behavior.

### Verification

1. Write a test: start aggregate server with `stop_after_command()` lifespan → dispatch command → server stops → dispatch another command → should succeed (new server started)
2. Write a test: start aggregate server → kill it externally → dispatch command → should succeed
3. Existing lifespan tests should still pass

### Files to Modify

- `/workspace/instructed/src/instructed/router.gleam` — `RegistryState`, `RegistryMessage`, `handle_registry_message`, `start_registry`
- `/workspace/instructed/test/router_test.gleam` — add tests

---

## Fix 2: `register_pid` Uses Spawner PID, Not Handler Actor PID [HIGH]

### Problem

In `event_handler.gleam:start` and `process_manager.gleam:start`:

```gleam
subscriptions.register_pid(subs, process.self(), config.name)
```

`process.self()` is called in the *spawning* function, not inside the actor. It captures the PID of the process that called `event_handler.start(...)`, not the PID of the handler actor. 

When the handler actor later dispatches a strong-consistency command, `wait_for` captures the handler actor's PID via `process.self()`. The subscriptions actor looks up that PID in `pid_to_name` — but it won't find it, because a different PID (the spawner's) was registered. Deadlock prevention fails: the handler waits for itself to ack.

### Fix

Move `register_pid` inside the actor, so it captures the actor's own PID. The cleanest approach:

1. Add a new message variant to `HandlerMessage`: `Init` (or reuse `SetSubscriptionInfo` to also trigger PID registration)
2. When the actor receives this init message, it calls `process.self()` (now inside the actor process) and sends `register_pid` to the subscriptions actor
3. Do the same for `PMMessage` in `process_manager.gleam`

Alternatively, since we already send `SetSubscriptionInfo` to the actor after start, we can piggyback PID registration on that message handler. When the actor handles `SetSubscriptionInfo`, it also does:

```gleam
case state.config.subscriptions {
  Some(subs) -> 
    subscriptions.register_pid(subs, process.self(), state.config.name)
  None -> Nil
}
```

This is the simplest change — just move the `register_pid` call from the `start` function into the `SetSubscriptionInfo` handler inside the actor.

### Verification

1. Write a test: create a strong-consistency handler that dispatches a command with strong consistency inside its handler callback → should NOT deadlock
2. The test should verify that the command returns within timeout (not `ConsistencyTimeout`)
3. Existing strong consistency tests should still pass

### Files to Modify

- `/workspace/instructed/src/instructed/event_handler.gleam` — move `register_pid` from `start` to `handle_actor_message` `SetSubscriptionInfo` branch
- `/workspace/instructed/src/instructed/process_manager.gleam` — same change in `handle_pm_message` `SetSubscriptionInfo` branch
- `/workspace/instructed/test/subscriptions_test.gleam` — add deadlock prevention test

---

## Fix 3: Subscription Delete-Recreate Loses Checkpoint Position [HIGH]

### Problem

In `event_handler.gleam:start` and `process_manager.gleam:start`, when `subscribe_persistent` returns `SubscriptionAlreadyExists`:

```gleam
Error(error.SubscriptionAlreadyExists) -> {
  let _ = event_store.delete_subscription(stream, config.name)
  case event_store.subscribe_persistent(...) { ... }
}
```

This deletes the subscription (including its checkpoint), then recreates with `config.start_from` (default `Origin`). All events replay from the beginning.

This happens on every handler/PM restart when the subscription already exists in the event store. Combined with the fact that `last_seen_event` is not persisted for event handlers (only PMs have snapshots), this means:
- **Event handlers**: Full event replay on every restart
- **Process managers**: Events replay but per-instance idempotency via snapshots mitigates duplicate processing (partially)

### Root Cause

The handler needs to re-register its callback function with an existing subscription, but the event store API only supports `subscribe_persistent` (create new) and `delete_subscription` (destroy). There's no "re-attach to existing subscription" operation.

### Fix

**Approach: Add `reconnect_subscription` to the EventStore interface.**

1. Add a new function to the `EventStore` record: `reconnect_subscription: fn(String, String, handler) -> Result(Subscription, EventStoreError)` — takes stream_id, subscription_name, and new handler callback; re-attaches to an existing subscription preserving its checkpoint position
2. Implement in all three adapters:
   - **In-memory**: Look up existing `PersistentSub` by name, update its `handler` callback, deliver any pending events
   - **SQLite**: Look up subscription row, create new `Subscription` reference with existing checkpoint
   - **PostgreSQL**: Look up subscription row, restart poller from existing checkpoint
3. Update `event_handler.gleam:start` and `process_manager.gleam:start`:
   ```
   Error(error.SubscriptionAlreadyExists) -> {
     case event_store.reconnect_subscription(stream, config.name, handler) {
       Ok(subscription) -> Ok(subscription)
       Error(_) -> {
         // Fallback: delete and recreate (losing position)
         let _ = event_store.delete_subscription(stream, config.name)
         event_store.subscribe_persistent(stream, config.name, config.start_from, handler)
       }
     }
   }
   ```

**Simpler alternative: Update handler callback on existing subscription.**

If adding a new EventStore function is too invasive:

1. In the in-memory adapter's `subscribe_persistent`, instead of returning `SubscriptionAlreadyExists`, accept the new handler and update the existing subscription's callback. This makes `subscribe_persistent` idempotent.
2. Do the same for SQLite and PostgreSQL adapters.
3. Remove the delete-recreate logic from handlers/PMs entirely.

This is simpler but changes the semantics of `subscribe_persistent` (which currently errors on duplicate). Check if anything depends on the error behavior.

### Verification

1. Write a test: start handler → process some events → stop handler → start handler again → verify it resumes from last acked position (not from Origin)
2. Write a test for PM: same pattern, verify PM instances don't reprocess events
3. Test with in-memory, SQLite, and PostgreSQL adapters

### Files to Modify

- `/workspace/instructed/src/instructed/event_store.gleam` — add `reconnect_subscription` (or make `subscribe_persistent` idempotent)
- `/workspace/instructed/src/instructed/in_memory_event_store.gleam` — implement reconnect
- `/workspace/instructed-sqlite/src/instructed_sqlite.gleam` — implement reconnect
- `/workspace/instructed-postgres/src/instructed_postgres.gleam` — implement reconnect
- `/workspace/instructed/src/instructed/event_handler.gleam` — use reconnect instead of delete-recreate
- `/workspace/instructed/src/instructed/process_manager.gleam` — same
- Add tests in relevant test files

---

## Fix 4: PM Default Event Error Silently Skips Instead of Stopping [HIGH]

### Problem

In `process_manager.gleam`, `handle_event_error`:

```gleam
case state.config.on_event_error {
  None ->
    // Default: skip the event (log and continue)
    state
  ...
}
```

When no `on_event_error` is configured, event handling errors are silently swallowed — the event is skipped and the PM continues. Commanded defaults to stopping the PM instance on error. This can lead to silently lost events and inconsistent PM state.

### Fix

Change the default behavior from "skip and continue" to "stop the PM actor":

```gleam
case state.config.on_event_error {
  None ->
    // Default: stop on error (matching Commanded's default).
    // This ensures event handling errors are not silently swallowed.
    actor.stop()
  ...
}
```

However, since `handle_event_error` currently returns `PMRouterState` (not `actor.Next`), and is called from `process_instance_event` which also returns `PMRouterState`, we need to change the return type or use a different signaling mechanism.

**Options:**

1. **Change return types to `Result`**: Make `handle_event_error`, `process_instance_event`, `start_or_continue_instance`, and `route_event` return `Result(PMRouterState, String)` where `Error` means "stop the actor". Then in `handle_pm_message`, check the result and call `actor.stop()` on error.

2. **Use a flag in state**: Add a `should_stop: Bool` field to `PMRouterState`. Set it in `handle_event_error` when default stop is needed. Check it in `handle_pm_message` after `route_event` returns.

3. **Simpler: just use the existing error action pattern**: Since the `on_event_error` callback returns `error.Stop(reason)`, we could change the `None` case to behave like `error.Stop("unhandled event error: " <> reason)` — which in the current code deletes the PM snapshot and removes the instance from the dict. This doesn't stop the whole actor (which would kill all PM instances) but does stop the individual instance. This is closer to Commanded's behavior (which stops the individual instance, not the entire router).

**Recommended: Option 3** — change default to stop the *instance*, not the entire actor:

```gleam
case state.config.on_event_error {
  None -> {
    // Default: stop the instance on error (matching Commanded's default).
    delete_pm_snapshot(state, uuid)
    PMRouterState(
      ..state,
      instances: dict.delete(state.instances, uuid),
    )
  }
  ...
}
```

But wait — we should also NOT ack the event in this case, so it gets redelivered. Currently `route_event` always calls `ack_event` after `start_or_continue_instance`. We need to signal to `route_event` that the event should NOT be acked.

**Full fix**: Make `start_or_continue_instance` (and `process_instance_event`) return a `Result` indicating success/failure. On failure, `route_event` should NOT ack the event. This ensures the event is redelivered on PM restart.

### Verification

1. Write a test: PM with no `on_event_error` → event causes handle error → verify the PM instance is stopped (removed from instances dict) and the event is NOT acked
2. Write a test: PM with explicit `on_event_error: Skip` → verify the event IS acked and processing continues (existing behavior for explicit skip)
3. Existing PM error tests should still pass

### Files to Modify

- `/workspace/instructed/src/instructed/process_manager.gleam` — change `handle_event_error` default, change `route_event`/`start_or_continue_instance`/`process_instance_event` return types to signal ack/no-ack
- `/workspace/instructed/test/process_manager_test.gleam` — add/update tests

---

## Fix 5: Event Type Always Empty String [MEDIUM]

### Problem

In `aggregate_server.gleam:execute_once`:

```gleam
let event_data =
  list.map(events, fn(evt) {
    EventData(
      data: evt,
      event_type: "",
      ...
    )
  })
```

Every event persisted through the aggregate server has `event_type: ""`. This means:
- Events in the store have no type metadata
- Can't query events by type
- Can't implement type-based upcasting (the upcaster receives `RecordedEvent` with empty `event_type`)
- Debugging and observability are hampered

### Fix

The aggregate needs a way to derive the event type string from an event value. Add an optional `event_type` function to the `Aggregate` record:

1. Add `event_type: fn(event) -> String` to the `Aggregate` record in `aggregate.gleam`
2. Update `aggregate.new` to require this parameter
3. In `aggregate_server.gleam:execute_once`, use `state.config.aggregate.event_type(evt)` instead of `""`
4. Update all existing aggregate definitions in tests and examples

**Alternative (simpler):** Use `string.inspect(evt)` or a Gleam-idiomatic type name extraction. But this produces ugly strings and isn't stable across refactors.

**Alternative (simplest, pragmatic):** Use the Erlang `erlang:element(1, Term)` trick to extract the constructor tag name from a custom type. This is what Commanded does with Elixir's `__struct__` key. In Gleam, custom type variants are Erlang tuples where the first element is an atom of the constructor name.

```gleam
@external(erlang, "erlang", "element")
fn element(index: Int, tuple: anything) -> Dynamic
```

Then: `event_type = dynamic.classify(element(1, evt))` or similar. This is fragile but automatic.

**Recommended: explicit `event_type` function.** It's the most type-safe and Gleam-idiomatic approach.

### Verification

1. After fix, verify events in the store have non-empty `event_type`
2. Verify upcasters receive events with correct `event_type`
3. All existing tests pass (update assertions for event_type)

### Files to Modify

- `/workspace/instructed/src/instructed/aggregate.gleam` — add `event_type` field
- `/workspace/instructed/src/instructed/aggregate_server.gleam` — use `event_type` function
- `/workspace/instructed/test/*.gleam` — update aggregate definitions
- `/workspace/example-todo/` — update aggregate definition

---

## Fix 6: Aggregate Server Self-Subscription Leak [MEDIUM]

### Problem

In `aggregate_server.gleam:start`:

```gleam
let handler = fn(evt: RecordedEvent(event)) {
  process.send(subject, ExternalEvent(evt))
}
let _ = config.event_store.subscribe_to_stream(config.stream_id, handler)
```

This creates a transient subscription with a closure capturing the server's `Subject`. If the server dies (lifespan, crash), the subscription remains in the event store. The closure calls `process.send` on a dead `Subject` — which is a no-op in Erlang (sends to dead PID are silently dropped). But the subscription entry itself is never cleaned up.

For the in-memory event store, this means the `transient_subscribers` list grows linearly with aggregate server churn. Each entry holds a closure referencing a dead `Subject`.

### Fix

1. Add an `unsubscribe` or `remove_transient_subscription` function to the EventStore interface
2. Store the subscription reference in `ServerState`
3. Clean up the subscription when the server stops (in the `LifespanTimeout` handler, or via a cleanup mechanism)

**Alternatively**, since transient subscriptions are lightweight (just a callback function), and sending to dead PIDs is harmless in Erlang, this could be addressed by:

1. In the in-memory event store, periodically cleaning up transient subscribers that reference dead PIDs
2. Or, wrapping the handler callback to check if the target process is alive before sending

The simplest fix: In `in_memory_event_store.gleam`, when delivering to transient subscribers, remove any that fail to deliver (i.e., the target process is dead). This is a lazy cleanup approach.

### Verification

1. Write a test: start many aggregate servers → stop them all → verify transient subscriber count doesn't grow unboundedly
2. Or verify that new events don't trigger sends to dead processes

### Files to Modify

- `/workspace/instructed/src/instructed/in_memory_event_store.gleam` — lazy cleanup of dead transient subscribers
- `/workspace/instructed/src/instructed/event_store.gleam` — optionally add `unsubscribe` function
- `/workspace/instructed/src/instructed/aggregate_server.gleam` — optionally track and clean up subscription

---

## Fix 7: SetSelf / SetSubscriptionInfo Race Condition [LOW-MEDIUM]

### Problem

In `aggregate_server.gleam:start`, `event_handler.gleam:start`, and `process_manager.gleam:start`, setup messages (`SetSelf`, `SetSubscriptionInfo`) are sent asynchronously after the actor starts. If a command or event arrives before these setup messages are processed:

- **Aggregate server**: `self_subject` is `None` → lifespan timer can't be scheduled (silent failure)
- **Event handler**: `event_store` and `subscription` are `None` → `ack_event` is a no-op (event not acked, will be redelivered, but handler continues processing — could cause duplicate processing without ack)
- **Process manager**: same as event handler

### Fix

For the aggregate server, the race is benign — the first command triggers `load_state`, and lifespan decisions just fall through to `KeepRunning` when `self_subject` is `None`. This is a minor issue.

For event handlers and PMs, the race is more concerning because events could arrive before `SetSubscriptionInfo`. However, the persistent subscription's handler callback sends `HandleEvent` messages to the actor, and `SetSubscriptionInfo` is sent from the same thread immediately after `subscribe_persistent` succeeds. Since Erlang mailboxes are ordered per sender, and the subscription callback is set up AFTER the actor starts, the first event delivery will happen after `subscribe_persistent` returns, which is after `SetSubscriptionInfo` is sent. So in practice the race is very unlikely.

**Minimal fix**: In `handle_actor_message` for `HandleEvent`, if `subscription` is `None`, buffer the event and reprocess when `SetSubscriptionInfo` arrives. Or simply requeue the message:

```gleam
HandleEvent(recorded_event) -> {
  case state.subscription {
    None -> {
      // Subscription info not yet set — requeue
      // This is safe because SetSubscriptionInfo will arrive soon
      process.send(self, HandleEvent(recorded_event))
      actor.continue(state)
    }
    Some(_) -> { ... normal processing ... }
  }
}
```

But we don't have `self` in the handler. We'd need to store it via `SetSelf` first — same chicken-and-egg problem.

**Pragmatic fix**: Accept the race as extremely unlikely and add a comment documenting it. The worst case is a single event gets processed without acking, causing one redelivery on next startup.

### Verification

This is hard to test deterministically. Add a comment documenting the race condition and its impact.

### Files to Modify

- `/workspace/instructed/src/instructed/event_handler.gleam` — add comment
- `/workspace/instructed/src/instructed/process_manager.gleam` — add comment
- `/workspace/instructed/src/instructed/aggregate_server.gleam` — add comment

---

## Implementation Order

1. **Fix 1** (Registry cleanup) — CRITICAL, blocks lifespan from working at all
2. **Fix 2** (register_pid) — HIGH, simple change, prevents deadlocks
3. **Fix 4** (PM default error) — HIGH, simple behavioral change
4. **Fix 3** (Subscription reconnect) — HIGH, most complex change, affects all adapters
5. **Fix 5** (Event type) — MEDIUM, API change affecting all aggregates
6. **Fix 6** (Subscription leak) — MEDIUM, defensive cleanup
7. **Fix 7** (Race condition) — LOW, document and optionally mitigate

## Verification Strategy

After all fixes, run:
```bash
cd /workspace && make check
```

All existing tests must pass. New tests must be added for each fix. The test count should increase significantly.

## Key Files Reference

| File | Role |
|------|------|
| `instructed/src/instructed/router.gleam` | Registry, dispatch pipeline |
| `instructed/src/instructed/aggregate_server.gleam` | Aggregate lifecycle, command execution |
| `instructed/src/instructed/event_handler.gleam` | Event handler actor |
| `instructed/src/instructed/process_manager.gleam` | Process manager actor |
| `instructed/src/instructed/subscriptions.gleam` | Strong consistency tracking |
| `instructed/src/instructed/event_store.gleam` | Event store interface (record-of-functions) |
| `instructed/src/instructed/in_memory_event_store.gleam` | In-memory adapter |
| `instructed-sqlite/src/instructed_sqlite.gleam` | SQLite adapter |
| `instructed-postgres/src/instructed_postgres.gleam` | PostgreSQL adapter |
| `instructed/src/instructed/aggregate.gleam` | Aggregate definition record |
