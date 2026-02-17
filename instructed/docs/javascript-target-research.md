# JavaScript Target Research for Instructed

## Status: Investigation

This document captures research into whether `instructed` (a CQRS/ES framework for Gleam, ported from Elixir's Commanded) can compile to Gleam's JavaScript target. The core challenge is that `instructed` depends on `gleam_otp` and `gleam_erlang`, which are 100% Erlang-only with zero JS support.

---

## Current OTP Dependencies

`instructed` uses the following from `gleam_otp` and `gleam_erlang`:

| OTP Feature | Where Used | What It Does |
|---|---|---|
| **Actor (gen_server)** | `aggregate_server`, `in_memory_event_store`, `event_handler`, `projection`, `process_manager`, `application` | Stateful processes that receive typed messages |
| **Subject (typed mailbox)** | Everywhere | Type-safe process references for sending messages |
| **process.call** | `aggregate_server`, `in_memory_event_store`, `projection` | Synchronous request/response (like gen_server call) |
| **process.send** | Everywhere | Async fire-and-forget messaging |
| **Supervision** | `application.gleam` mentions it | Restart-on-crash |

Notably, it does **not** use: distributed Erlang, ETS, monitors/links directly, selective receive, or named processes.

---

## The Fundamental Tension: Function Coloring

On the BEAM, `process.call` is written as a normal synchronous expression because the runtime preemptively schedules processes. The code *looks* blocking but *isn't*.

In JS there's no preemption. The only way to express "suspend me, let others run, resume when the reply arrives" is `await`. This means every function that transitively calls `process.call` must be async. It cascades upward:

- `aggregate_server.execute` → calls `process.call`
- `router.dispatch` → calls event store operations (which call `process.call`)
- `application.dispatch` → calls `router.dispatch`
- `projection.get_state` → calls `process.call`

On Erlang, these are all `fn() -> Result(...)`. On JS, they'd all need to be `fn() -> Promise(Result(...))`.

**Key consideration**: We don't want `instructed` to impose non-idiomatic patterns on client Gleam code. If using `instructed` on JS forces users into callback chains or unfamiliar effect types, that's a problem.

---

## Actors in JavaScript: Proven to Work

The actor model maps cleanly to JavaScript. An actor is:

1. **State** — a mutable value
2. **A handler** — `(state, message) → state`
3. **A mailbox** — a message queue processed one-at-a-time

The actor's lifecycle is an async loop:

```
loop:
  msg = await mailbox.take()    ← suspends until message arrives
  try:
    state = handler(state, msg)
  catch:
    state = initial_state       ← supervision/restart
  goto loop
```

- `await` yields to the event loop — the actor sleeps between messages
- Between `await` and next `await`, the handler runs uninterrupted (same "one message at a time" guarantee as BEAM)
- `send` (fire-and-forget) = push to the actor's queue, return immediately
- `call` (request/response) = push a message carrying a Promise resolver to the queue, caller awaits the Promise

### Supervision

A supervisor starts actors, monitors for errors, and decides whether/how to restart:

- **Permanent**: always restart (reset state, continue loop)
- **Transient**: restart only on abnormal exit
- **Temporary**: never restart
- **Strategies**: one_for_one, one_for_all, rest_for_one

In JS, "process crash" = "exception thrown in handler." A supervisor wraps the handler in try/catch and applies the restart policy. This maps directly to OTP supervision semantics.

---

## Existing npm Implementations

### @hamicek/noex — Full OTP Model for TypeScript

Published January 2026. Implements GenServer, Supervisor, Registry, process linking, monitors — the full OTP surface area.

**Approach**: Embraces `async/await`. Every `GenServer.call` returns a Promise.

```typescript
// Caller — always async
const value = await GenServer.call(counter, 'get');

// Handler — can be sync or async
handleCall: (msg, state) => [state, state],                // sync
handleCall: async (msg, state) => [await db.get(), state],  // async
```

Core mechanism (~50 lines of meaningful logic):

```javascript
// Sequential message processing
processQueue() {
    if (this.processing || this.status === 'stopped') return;
    const message = this.queue.shift();
    if (!message) return;
    this.processing = true;
    void this.processMessage(message).finally(() => {
        this.processing = false;
        this.processQueue();
    });
}

// Call = enqueue with Promise resolver
enqueueCall(msg, timeoutMs) {
    return new Promise((resolve, reject) => {
        const timeoutId = setTimeout(() => reject(...), timeoutMs);
        this.queue.push({ kind: 'call', msg, resolve, reject });
        this.processQueue();
    });
}

// Cast = enqueue, fire and forget
enqueueCast(msg) {
    this.queue.push({ kind: 'cast', msg });
    this.processQueue();
}
```

**Features**: GenServer (call/cast), Supervisor (one_for_one/one_for_all/rest_for_one), Registry, process linking, monitors, lifecycle events, persistence, distributed clustering. Zero dependencies for core.

**Tradeoff**: `async` spreads to all callers.

### actor (by Gozala) — Generator-Based, No Color Split

Published 2022, multiple iterations. Uses generators instead of async/await to avoid function coloring.

```javascript
function* work(url) {
  const response = yield* Task.wait(fetch(url))  // suspends here
  const data = yield* Task.wait(response.json())  // and here
  return data
}
```

`yield*` suspends the generator. A cooperative scheduler resumes it when the promise resolves. Code reads like synchronous code — no `async` keyword anywhere.

**Key difference from noex**:

| | noex | actor (Gozala) |
|---|---|---|
| Suspend mechanism | `await` (Promise) | `yield*` (generator) |
| Function coloring | Yes — `async` spreads | No — all functions are generators |
| Has GenServer/Supervisor | Yes, full OTP model | No — task primitives only |
| Has supervision | Yes (all strategies) | No — fork/join/abort |
| Complexity | ~600 lines core | ~400 lines core |

Gozala's approach avoids function coloring but lacks OTP abstractions. noex has the full OTP surface but accepts the async tax.

---

## The Gleam Ecosystem

### What Exists

| Package | Target | What it provides |
|---|---|---|
| `gleam_otp` | Erlang only | Actor, Supervisor — zero JS FFI |
| `gleam_erlang` | Erlang only | process, Subject, Selector — zero JS FFI |
| `gleam_javascript` | JS only | `Promise(a)` type with `promise.await`, `promise.map`, etc. |
| `lustre` | Both targets | Uses `@target` splits — server components return `Error("Not Erlang")` on JS |
| `chip` | Erlang only | Actor registry — depends on `gleam_otp` |

### What Doesn't Exist

There is **no cross-target actor/process library** for Gleam. Nobody has built a `gleam_actor` that works on both Erlang and JavaScript.

### How Lustre Handles It

Lustre is the main cross-target Gleam framework. Its approach:

- **Client-side (browser)**: Elm Architecture with `Effect` as data. No actors.
- **Server-side**: Uses `gleam_otp` actors. On JS target, returns `Error("Not Erlang")`.
- **Cross-target code**: Uses `@target(erlang)` / `@target(javascript)` conditional compilation and the `Effect` type to abstract over platform differences.

The `Effect` type (effects as data) is Lustre's main cross-target abstraction:

```gleam
pub fn from(effect: fn(fn(msg) -> Nil) -> Nil) -> Effect(msg) {
  // Stores callback. Runtime interprets per-platform.
}
```

This works for UI but is specific to Lustre's architecture.

### Gleam Platform Mechanisms

Gleam provides three mechanisms for handling target differences:

1. **`@target` conditional compilation**: Write two implementations, compiler picks one
2. **Dual `@external` FFI**: `@external(erlang, ...)` and `@external(javascript, ...)` on same function
3. **Abstraction**: Define a common interface, implement differently per target

---

## Optimistic Locking

Already implemented correctly in `router.gleam`, independent of the process model:

```gleam
// Append with exact version check
router.event_store.append_to_stream(stream_id, ExactVersion(version), event_data)

// Retry on version conflict
case event_store_err, remaining_attempts > 0 {
  error.VersionConflict, True ->
    dispatch_to_aggregate(router, pipeline, remaining_attempts - 1)
```

The event store types support this:

```gleam
pub type ExpectedVersion {
  AnyVersion        // no check
  NoStream          // must not exist
  StreamExists      // must exist
  ExactVersion(Int) // must be at this version
}
```

This works regardless of runtime — it's a property of the event store append, not the process model.

---

## Missing Feature: Fire-and-Forget Dispatch

Commanded (Elixir) has two dispatch modes:

- **Synchronous**: caller waits for result
- **Fire-and-forget** (`consistency: :eventual`): returns `:ok` immediately, command processed async

`instructed` currently only has synchronous dispatch. Fire-and-forget maps naturally to the JS actor model:

- Synchronous → `GenServer.call` → caller awaits Promise
- Fire-and-forget → `GenServer.cast` → push to queue, return immediately

---

## Potential Approaches for instructed

### Approach 1: Target-Specific Return Types

```gleam
@target(erlang)
pub fn dispatch(router, command) -> Result(DispatchResult, DispatchError) { ... }

@target(javascript)
pub fn dispatch(router, command) -> Promise(Result(DispatchResult, DispatchError)) { ... }
```

**Problem**: Different return types cascade — every caller needs `@target` branches too. Highly non-idiomatic for client code.

### Approach 2: Callback/Continuation Style

```gleam
pub fn dispatch(router, command, then: fn(Result(...)) -> Nil) -> Nil
```

On Erlang, `then` called synchronously. On JS, called when async work completes. This is the Lustre pattern.

**Problem**: Forces CPS on users. Not idiomatic Gleam.

### Approach 3: Effect/Task Abstraction

```gleam
pub opaque type Effect(a)
pub fn dispatch(router, command) -> Effect(Result(DispatchResult, DispatchError))
pub fn run(effect: Effect(a), callback: fn(a) -> Nil) -> Nil
```

On Erlang, `Effect` wraps a thunk. On JS, wraps a Promise. Cleanest API but requires users to work with `Effect` values.

**Problem**: Non-standard pattern for Gleam. Effect type would need to be ergonomic.

### Approach 4: Pure Core + Platform Runtimes

Extract pure business logic from OTP wiring. Provide two thin runtimes.

**Pure core (both targets)**:
```gleam
pub fn execute(state, command, handler) -> Result(List(event), String)
pub fn apply_events(state, events, handler) -> state
```

**Platform-specific runtime**:
```gleam
@target(erlang)
pub fn start(config) -> App { ... }  // actors, OTP supervision

@target(javascript)
pub fn start(config) -> App { ... }  // noex-style async actors
```

**Same public API on both**:
```gleam
pub fn dispatch(app, command) -> Result(List(event), DispatchError)
```

**This approach pushes platform differences to the edges.** Client code is identical. But `dispatch` still has the async problem on JS.

### Approach 5: Generators (Gozala-style)

Use JS generators via FFI to avoid function coloring entirely. `yield*` replaces `await`. Code reads synchronously on both targets.

**Problem**: Gleam doesn't have native generator support. Would require significant FFI and may feel foreign. Unexplored territory in the Gleam ecosystem.

---

## What Gets Easier vs. Harder on JS

| Concern | Erlang | JS | Notes |
|---|---|---|---|
| Command dispatch (call) | Actor mailbox, sync from caller | Must be async (Promise) | The main pain point |
| Event store mutations | Actor serializes access | Single-threaded, natural serialization | Actually simpler on JS |
| Event notification (send) | Actor mailbox delivery | `queueMicrotask` or async channel | Straightforward |
| Projections (I/O) | Actor with blocking calls | Async loop with `await` | Natural fit |
| Supervision | OTP supervisor tree | try/catch + state reset | Simpler on JS |
| Optimistic locking | Works | Works identically | Not affected by runtime |
| Preemptive scheduling | Yes | No — run-to-completion | Fine for short handlers |
| Process isolation | Yes | Shared memory | Gleam's type system helps |

---

## Open Questions

1. **Can we make the API identical on both targets without imposing non-idiomatic patterns?** The `dispatch` return type is the crux — `Result(...)` vs `Promise(Result(...))`.

2. **Would Gleam's compiler/tooling ever support "implicit async" for JS targets?** If `process.call` on JS compiled to `await genserver.call(...)` automatically, the source code could be identical. This would be a compiler-level change.

3. **Is the generator approach viable in Gleam?** Generators avoid function coloring but are unexplored in the Gleam ecosystem. Would need investigation into FFI feasibility.

4. **Is there appetite in the Gleam community for a cross-target actor library?** This is an open niche. A `gleam_actor` package that works on both targets would benefit the whole ecosystem, not just `instructed`.

5. **Should `instructed` target JS at all?** The BEAM is the natural home for actor-based CQRS/ES. JS might be better served by a different architecture (e.g., pure functions + external event store, no actors).

6. **What does "idiomatic" look like for cross-target Gleam?** The community is young. Lustre's `Effect` pattern is one answer. The right pattern for backend services may be different.

---

## Key References

- **noex** (`@hamicek/noex`): Full OTP (GenServer + Supervisor) for TypeScript. Published Jan 2026. Zero dependencies.
- **actor** (`actor` on npm, by Gozala): Generator-based cooperative concurrency. No function coloring. Published 2022.
- **gleam_javascript**: Official Gleam package for JS interop. Provides `Promise(a)` type.
- **lustre**: Main cross-target Gleam framework. Uses `@target` splits and `Effect` as data.
- **gleam_otp**: Official Gleam OTP bindings. Erlang-only, no JS support planned by core team.
