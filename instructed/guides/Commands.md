# Commands

Commands represent intentions to change the system state. In Instructed, commands are defined as Gleam custom types and dispatched through a router.

## Defining Commands

```gleam
type BankCommand {
  OpenAccount(account_number: String, initial_balance: Int)
  DepositMoney(account_number: String, amount: Int)
  WithdrawMoney(account_number: String, amount: Int)
  CloseAccount(account_number: String)
}
```

Commands must contain a field to identify the aggregate instance. This is extracted via the `identity` function on the router.

## Command Dispatch and Routing

A router dispatches commands to their aggregate:

```gleam
import instructed/router

let bank_router = router.new(
  aggregate: bank_aggregate,
  event_store: store,
  identity: fn(cmd) {
    case cmd {
      OpenAccount(num, _) -> num
      DepositMoney(num, _) -> num
      WithdrawMoney(num, _) -> num
      CloseAccount(num) -> num
    }
  },
)
```

### Dispatching

```gleam
// Simple dispatch
let assert Ok(result) = router.dispatch(bank_router, OpenAccount("ACC1", 100))

// Dispatch with context
let assert Ok(result) = router.dispatch_with_context(
  bank_router,
  OpenAccount("ACC1", 100),
  "cmd-uuid-1",
  option.Some("cause-uuid"),
  option.Some("correlation-uuid"),
  dict.from_list([#("user", "admin")]),
)
```

### Dispatch Result

A successful dispatch returns a `DispatchResult`:

```gleam
type DispatchResult(state, event) {
  DispatchResult(
    aggregate_state: state,    // State after command execution
    aggregate_version: Int,    // New aggregate version
    events: List(event),       // Events produced
  )
}
```

### Identity Prefix

You can add a prefix to aggregate stream IDs:

```gleam
let router = router.new(...)
  |> router.with_prefix("bank-account-")
// Stream ID will be "bank-account-ACC1" instead of "ACC1"
```

### Retry Attempts

Configure retry attempts for version conflicts:

```gleam
let router = router.new(...)
  |> router.with_retry_attempts(5)
```

## Middleware

Middleware can intercept commands before and after dispatch. See the [Middleware guide](Middleware.md).

```gleam
let router = router.new(...)
  |> router.with_middleware(my_middleware)
```

## Command Validation

Validate commands in the aggregate's `execute` function:

```gleam
execute: fn(state, cmd) {
  case cmd {
    OpenAccount(_, balance) if balance <= 0 ->
      Error("Balance must be positive")
    OpenAccount(num, balance) ->
      Ok([AccountOpened(num, balance)])
    // ...
  }
}
```

Or use middleware for cross-cutting validation:

```gleam
let validation_middleware = middleware.new(
  before_dispatch: fn(pipeline) {
    // Validate command here
    case validate(pipeline.command) {
      Ok(_) -> pipeline
      Error(_) -> middleware.halt(pipeline)
    }
  },
  after_dispatch: fn(p) { p },
  after_failure: fn(p) { p },
)
```

## Dispatch Errors

Dispatch can fail with a `DispatchError`:

- `AggregateError(reason)` - Business rule violation
- `Halted` - Middleware halted the pipeline
- `Timeout` - Command execution timed out
- `WrongExpectedVersion` - Concurrency conflict
- `EventStoreError(reason)` - Storage failure
- `TooManyAttempts` - Too many retry attempts
