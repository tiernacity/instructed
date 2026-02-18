> [!CAUTION]
> **DO NOT USE THIS LIBRARY IN PRODUCTION.** This code is entirely agent-generated and has not been independently reviewed, battle-tested, or exercised beyond unit tests. Critical CQRS/ES guarantees — including distributed consistency, error recovery, idempotency under failure, and supervision correctness — have not been validated in real-world conditions. Serious bugs, data loss, or silent failures are likely. See `REVIEW.md` for a detailed list of known gaps.

> [!NOTE]
> This repository is **entirely agent-generated** using [ralph-wiggum](https://ghuntley.com/ralph/). Its primary purpose is a medium-complexity test case for exercising iterative AI coding agent workflows. It exists to explore what agents can produce — not to be used as software.

# Instructed

A **CQRS/ES framework for Gleam**, ported from the Elixir [Commanded](https://github.com/commanded/commanded) library.

## Packages

| Package | Description |
|---------|-------------|
| [`instructed/`](instructed/) | Core framework |
| [`instructed-sqlite/`](instructed-sqlite/) | SQLite event store adapter |
| [`instructed-postgres/`](instructed-postgres/) | PostgreSQL event store adapter |

## Status

- ✅ 151 core tests passing
- ✅ 18 SQLite adapter tests passing
- ✅ PostgreSQL adapter builds clean (requires live DB for tests)
- ✅ All 21 Commanded feature areas implemented

## Quick Start

```gleam
import instructed/aggregate.{Aggregate}
import instructed/router
import instructed/in_memory_event_store

// 1. Define your domain types
type BankAccount { BankAccount(balance: Int) }
type Command { Deposit(amount: Int) | Withdraw(amount: Int) }
type Event { Deposited(amount: Int) | Withdrawn(amount: Int) }

// 2. Define your aggregate
fn bank_account() -> Aggregate(BankAccount, Command, Event) {
  Aggregate(
    empty_state: BankAccount(balance: 0),
    execute: fn(state, cmd) {
      case cmd {
        Deposit(amount) -> Ok([Deposited(amount)])
        Withdraw(amount) if amount > state.balance -> Error("insufficient funds")
        Withdraw(amount) -> Ok([Withdrawn(amount)])
      }
    },
    apply_event: fn(state, event) {
      case event {
        Deposited(amount) -> BankAccount(balance: state.balance + amount)
        Withdrawn(amount) -> BankAccount(balance: state.balance - amount)
      }
    },
  )
}

// 3. Start an event store and create a router
pub fn main() {
  let assert Ok(store_subject) = in_memory_event_store.start()
  let store = in_memory_event_store.to_event_store(store_subject)

  let my_router = router.new(
    aggregate: bank_account(),
    event_store: store,
    identity: fn(cmd) {
      case cmd {
        Deposit(_) | Withdraw(_) -> "account-1"
      }
    },
  )

  let assert Ok(_result) = router.dispatch(my_router, Deposit(100))
  let assert Ok(_result) = router.dispatch(my_router, Withdraw(30))
}
```

## Features

### Core
- **Aggregates** — record-of-functions with `execute` and `apply_event` callbacks
- **Aggregate server** — OTP actor per instance; serializes commands, caches state, handles snapshots and lifespan
- **Command router** — runtime configuration with identity extraction, middleware, retry, and consistency control
- **Middleware pipeline** — `before_dispatch`, `after_dispatch`, `after_failure` hooks
- **Multi** — composable multi-step command execution with intermediate state application

### Event Handling
- **Event handlers** — actor-based, idempotent, with error callbacks and resumable position tracking
- **Projections** — in-memory read models built on the event handler pattern
- **Process managers** — saga-style coordination; dispatches commands from event sequences; state persisted as snapshots

### Infrastructure
- **Strong vs eventual consistency** — dispatch can block until all strong-consistency handlers have acked
- **Causation & correlation** — full causal chain: command_id → causation_id on events; propagated through process managers
- **Event upcasting** — transform historical events to current schema at read time
- **Aggregate lifespan** — stop or time out idle aggregate processes
- **Telemetry** — structured instrumentation events with Gleam callback and optional Erlang `:telemetry` emission

### Event Store Adapters
- **In-memory** — for tests and development
- **SQLite** — via `sqlight`; single-file DB; actor-serialized; no external server
- **PostgreSQL** — via `pog`; `LISTEN/NOTIFY` for live subscriptions

## Documents

- **[`DESIGN.md`](DESIGN.md)** — Architecture, design decisions, and detailed Commanded comparison
- **[`REVIEW.md`](REVIEW.md)** — Original feature-by-feature review (pre-iteration baseline)
