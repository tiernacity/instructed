# Aggregates

An aggregate is the core building block in CQRS/ES. It receives commands, validates them against its current state, and produces domain events. State is rebuilt by replaying events.

## Aggregate Definition

In Instructed, an aggregate is defined using `aggregate.new/3` with three functions:

```gleam
import instructed/aggregate

let my_aggregate = aggregate.new(
  empty_state: fn() -> state,
  execute: fn(state, command) -> Result(List(event), String),
  apply_event: fn(state, event) -> state,
)
```

### empty_state

Returns the initial state of a new aggregate instance. This is used before any events have been applied.

### execute

The command handler. Receives the current aggregate state and a command. Returns either:
- `Ok(events)` - A list of domain events to persist
- `Ok([])` - Command accepted but no events produced
- `Error(reason)` - Command rejected with an error message

### apply_event

Applies a domain event to the aggregate state, returning the new state. This function **must never fail** - events are facts that have already occurred and must always be applicable.

## Example: Bank Account

```gleam
type BankAccount {
  BankAccount(account_number: String, balance: Int, open: Bool)
}

type BankCommand {
  OpenAccount(account_number: String, initial_balance: Int)
  DepositMoney(amount: Int)
  WithdrawMoney(amount: Int)
  CloseAccount
}

type BankEvent {
  AccountOpened(account_number: String, initial_balance: Int)
  MoneyDeposited(amount: Int, new_balance: Int)
  MoneyWithdrawn(amount: Int, new_balance: Int)
  AccountClosed
}

let bank = aggregate.new(
  empty_state: fn() { BankAccount("", 0, False) },
  execute: fn(state, cmd) {
    case cmd {
      OpenAccount(num, balance) ->
        case state.open {
          True -> Error("Account already open")
          False if balance > 0 -> Ok([AccountOpened(num, balance)])
          False -> Error("Balance must be positive")
        }
      DepositMoney(amount) if state.open && amount > 0 ->
        Ok([MoneyDeposited(amount, state.balance + amount)])
      WithdrawMoney(amount) if state.open && amount > 0 && amount <= state.balance ->
        Ok([MoneyWithdrawn(amount, state.balance - amount)])
      CloseAccount if state.open ->
        Ok([AccountClosed])
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
      AccountClosed ->
        BankAccount(..state, open: False)
    }
  },
)
```

## Rebuilding State

You can manually rebuild aggregate state from a list of events:

```gleam
let events = [AccountOpened("ACC1", 100), MoneyDeposited(50, 150)]
let state = aggregate.rebuild_state(bank, events)
// state == BankAccount("ACC1", 150, True)
```

## Aggregate Server

For production use, aggregates run as OTP Actor processes via `aggregate_server`:

```gleam
import instructed/aggregate_server

let config = aggregate_server.Config(
  aggregate: bank,
  event_store: store,
  stream_id: "bank-ACC1",
)

let assert Ok(server) = aggregate_server.start(config)
let assert Ok(result) = aggregate_server.execute(server, OpenAccount("ACC1", 100), 5000)
```

The aggregate server:
- Loads state from the event store on first command
- Caches state in memory
- Serializes command execution
- Persists events atomically
- Handles version conflicts

## Type Safety

Unlike Commanded (Elixir), Instructed enforces type safety at compile time:

- Commands and events are custom types, not generic structs
- The aggregate definition ties state, command, and event types together
- The compiler ensures you handle all command and event variants
- No runtime type errors from mismatched commands or events
