# Instructed

A strongly-typed CQRS/ES (Command Query Responsibility Segregation / Event Sourcing) framework for Gleam.

Instructed is a port of the Elixir [Commanded](https://github.com/commanded/commanded) library, redesigned to fully leverage Gleam's type system for compile-time safety.

## Features

- **Aggregates**: Event-sourced domain entities with type-safe commands and events
- **Command Routing**: Type-safe command dispatch to aggregates
- **Event Handlers**: Subscribe to and process domain events
- **Process Managers**: Coordinate multiple aggregates (sagas)
- **Projections**: Build read models from event streams
- **Middleware**: Extensible command processing pipeline
- **Event Store**: Pluggable event storage (in-memory included)
- **Supervision**: OTP supervisor support via `gleam_otp`
- **Snapshotting**: Aggregate state snapshots for performance
- **Strong Types**: Commands, events, and state are all fully typed

## Installation

Add `instructed` to your `gleam.toml`:

```toml
[dependencies]
instructed = { path = "../instructed" }
```

For PostgreSQL persistence, also add:

```toml
[dependencies]
instructed_postgres = { path = "../instructed_postgres" }
```

## Quick Start

### 1. Define your domain types

```gleam
type BankAccount {
  BankAccount(account_number: String, balance: Int, open: Bool)
}

type BankCommand {
  OpenAccount(account_number: String, initial_balance: Int)
  DepositMoney(amount: Int)
  WithdrawMoney(amount: Int)
}

type BankEvent {
  AccountOpened(account_number: String, initial_balance: Int)
  MoneyDeposited(amount: Int, new_balance: Int)
  MoneyWithdrawn(amount: Int, new_balance: Int)
}
```

### 2. Create your aggregate

```gleam
import instructed/aggregate

let bank_aggregate = aggregate.new(
  empty_state: fn() { BankAccount("", 0, False) },
  execute: fn(state, cmd) {
    case cmd {
      OpenAccount(num, balance) if balance > 0 ->
        Ok([AccountOpened(num, balance)])
      DepositMoney(amount) if amount > 0 ->
        Ok([MoneyDeposited(amount, state.balance + amount)])
      WithdrawMoney(amount) if amount > 0 && amount <= state.balance ->
        Ok([MoneyWithdrawn(amount, state.balance - amount)])
      _ -> Error("Invalid command")
    }
  },
  apply_event: fn(state, event) {
    case event {
      AccountOpened(num, balance) ->
        BankAccount(num, balance, True)
      MoneyDeposited(_, new_balance) ->
        BankAccount(..state, balance: new_balance)
      MoneyWithdrawn(_, new_balance) ->
        BankAccount(..state, balance: new_balance)
    }
  },
)
```

### 3. Set up the event store and router

```gleam
import instructed/in_memory_event_store
import instructed/router

let assert Ok(store_subject) = in_memory_event_store.start()
let store = in_memory_event_store.to_event_store(store_subject)

let bank_router = router.new(
  aggregate: bank_aggregate,
  event_store: store,
  identity: fn(cmd) {
    case cmd {
      OpenAccount(num, _) -> num
      DepositMoney(_) -> "ACC1"
      WithdrawMoney(_) -> "ACC1"
    }
  },
)
```

### 4. Dispatch commands

```gleam
let assert Ok(result) = router.dispatch(bank_router, OpenAccount("ACC1", 1000))
// result.aggregate_state == BankAccount("ACC1", 1000, True)
// result.events == [AccountOpened("ACC1", 1000)]
```

## Guides

- [Getting Started](guides/Getting_Started.md)
- [Aggregates](guides/Aggregates.md)
- [Commands](guides/Commands.md)
- [Events](guides/Events.md)
- [Process Managers](guides/Process_Managers.md)
- [Read Model Projections](guides/Projections.md)
- [Middleware](guides/Middleware.md)
- [Supervision](guides/Supervision.md)
- [Testing](guides/Testing.md)
- [Serialization](guides/Serialization.md)
- [In-Memory Event Store](guides/InMemoryEventStore.md)

## PostgreSQL Event Store

See [instructed_postgres](../instructed_postgres/) for PostgreSQL-backed event persistence.

## Example

See the [example todo app](../example-todo/) for a complete working example.

## Architecture

Instructed follows the CQRS/ES pattern:

```
Command → Router → Middleware → Aggregate → Events → Event Store
                                                  ↓
                                          Event Handlers
                                          Process Managers
                                          Projections (Read Models)
```

## Key Differences from Commanded

| Feature | Commanded (Elixir) | Instructed (Gleam) |
|---------|-------------------|-------------------|
| Type Safety | Runtime (structs) | Compile-time (generics) |
| Behaviours | Elixir behaviours + macros | Records of functions |
| Error Handling | `{:error, reason}` tuples | `Result` type |
| Event Store | Adapter behaviour | Function record |
| Configuration | Application config | Explicit parameters |
| Concurrency | GenServer | OTP Actor |
| Clustering | Supported | Not supported |

## License

MIT
