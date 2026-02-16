# Read Model Projections

Projections build queryable read models from domain events. This is the "Query" side of CQRS.

## Overview

A projection:
1. Subscribes to domain events
2. Processes each event to update its state
3. Provides a queryable API for the current state

## Defining a Projection

```gleam
import gleam/dict.{type Dict}
import instructed/projection

type AccountSummary {
  AccountSummary(account_number: String, balance: Int, open: Bool)
}

let account_projection = projection.new(
  name: "account_summaries",
  initial_state: dict.new(),
  handle_event: fn(event, _recorded, state: Dict(String, AccountSummary)) {
    case event {
      AccountOpened(num, balance) ->
        Ok(dict.insert(state, num, AccountSummary(num, balance, True)))
      MoneyDeposited(num, _, new_balance) -> {
        case dict.get(state, num) {
          Ok(summary) ->
            Ok(dict.insert(state, num, AccountSummary(..summary, balance: new_balance)))
          Error(_) -> Ok(state)
        }
      }
      AccountClosed(num) -> {
        case dict.get(state, num) {
          Ok(summary) ->
            Ok(dict.insert(state, num, AccountSummary(..summary, open: False)))
          Error(_) -> Ok(state)
        }
      }
      _ -> Ok(state)
    }
  },
)
```

## Starting a Projection

```gleam
let assert Ok(proj) = projection.start(account_projection, event_store)
```

## Querying Projection State

```gleam
let state = projection.get_state(proj, 5000)
// state is Dict(String, AccountSummary)

case dict.get(state, "ACC1") {
  Ok(summary) -> io.println("Balance: " <> int.to_string(summary.balance))
  Error(_) -> io.println("Account not found")
}
```

## Multiple Projections

You can create multiple projections from the same events:

```gleam
// Projection for account balances
let balance_proj = projection.new(
  name: "balances",
  initial_state: dict.new(),
  handle_event: fn(event, _, state) { ... },
)

// Projection for transaction history
let history_proj = projection.new(
  name: "history",
  initial_state: [],
  handle_event: fn(event, _, state) { ... },
)

// Projection for statistics
let stats_proj = projection.new(
  name: "stats",
  initial_state: Stats(0, 0, 0),
  handle_event: fn(event, _, state) { ... },
)

let assert Ok(_) = projection.start(balance_proj, store)
let assert Ok(_) = projection.start(history_proj, store)
let assert Ok(_) = projection.start(stats_proj, store)
```

## Projection with Application

Use the application module for convenience:

```gleam
import instructed/application as app

let assert Ok(application) = app.start(app.new(store))
let assert Ok(proj) = app.start_projection(application, account_projection)
```

## Best Practices

1. **Idempotency**: Projections should handle duplicate events gracefully
2. **Performance**: Keep projection handlers fast
3. **Separation**: Create separate projections for different query needs
4. **Recovery**: Projections can be rebuilt from scratch by replaying events
