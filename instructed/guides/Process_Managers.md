# Process Managers

Process managers coordinate multiple aggregates in response to domain events. They implement the Saga pattern for long-running transactions.

## Overview

A process manager:
1. Subscribes to domain events
2. Determines if it's interested in each event
3. Handles the event by producing commands
4. Dispatches those commands to aggregates
5. Maintains its own state

## Defining a Process Manager

```gleam
import instructed/process_manager

type TransferState {
  TransferState(transfer_id: String, from: String, to: String, amount: Int, status: String)
}

type TransferEvent {
  TransferRequested(id: String, from: String, to: String, amount: Int)
  MoneyDebited(account: String, amount: Int)
  MoneyCredited(account: String, amount: Int)
  TransferCompleted(id: String)
}

type TransferCommand {
  DebitAccount(account: String, amount: Int)
  CreditAccount(account: String, amount: Int)
}

let transfer_pm = process_manager.new(
  name: "transfer_process",
  interested: fn(event) {
    case event {
      TransferRequested(id, ..) -> process_manager.Start(id)
      MoneyDebited(..) -> process_manager.Continue("current")
      TransferCompleted(id) -> process_manager.Stop(id)
      _ -> process_manager.Skip
    }
  },
  handle: fn(event, _recorded, _state) {
    case event {
      TransferRequested(_, from, to, amount) ->
        Ok([DebitAccount(from, amount)])
      MoneyDebited(_, _) ->
        Ok([CreditAccount(state.to, state.amount)])
      _ -> Ok([])
    }
  },
  apply_event: fn(state, event) {
    case event {
      TransferRequested(id, from, to, amount) ->
        TransferState(id, from, to, amount, "requested")
      MoneyDebited(..) ->
        TransferState(..state, status: "debited")
      MoneyCredited(..) ->
        TransferState(..state, status: "completed")
      _ -> state
    }
  },
  initial_state: TransferState("", "", "", 0, ""),
  dispatch_command: fn(cmd) {
    // Dispatch to your router
    case router.dispatch(bank_router, cmd) {
      Ok(_) -> Ok(Nil)
      Error(_) -> Error("Dispatch failed")
    }
  },
)
```

## Interest Levels

The `interested` function returns one of:

- `Start(uuid)` - Start a new process manager instance
- `Continue(uuid)` - Continue an existing instance
- `Stop(uuid)` - Complete and remove the instance
- `Skip` - Ignore this event

## Starting a Process Manager

```gleam
let assert Ok(pm) = process_manager.start(transfer_pm, event_store)
```

The process manager will subscribe to all events and automatically manage instances.

## Error Handling

If a command dispatch fails, the process manager continues running. You can implement compensation logic in your event handler.
