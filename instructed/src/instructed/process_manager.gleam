//// Process Manager - coordinates multiple aggregates via sagas.
////
//// A process manager handles domain events and dispatches commands
//// in response, coordinating complex business processes that span
//// multiple aggregates.
////
//// ## Example
////
//// ```gleam
//// import instructed/process_manager
////
//// type TransferState {
////   TransferState(transfer_id: String, status: String)
//// }
////
//// let pm = process_manager.new(
////   name: "transfer_process",
////   interested: fn(event) {
////     case event {
////       TransferStarted(id, ..) -> process_manager.Start(id)
////       TransferCompleted(id, ..) -> process_manager.Stop(id)
////       _ -> process_manager.Skip
////     }
////   },
////   handle: fn(event, _metadata, state) {
////     case event {
////       TransferStarted(_, from, to, amount) ->
////         Ok([WithdrawMoney(from, amount), DepositMoney(to, amount)])
////       _ -> Ok([])
////     }
////   },
////   apply_event: fn(state, event) {
////     case event {
////       TransferStarted(id, ..) -> TransferState(id, "started")
////       _ -> state
////     }
////   },
////   initial_state: TransferState("", ""),
//// )
//// ```

import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import gleam/list
import gleam/otp/actor
import instructed/event.{type RecordedEvent}
import instructed/event_store.{type EventStore, Origin}

/// Indicates whether a process manager is interested in an event.
pub type Interest {
  /// Start a new process manager instance with this ID
  Start(process_uuid: String)
  /// Continue an existing process manager instance
  Continue(process_uuid: String)
  /// Stop (complete) a process manager instance
  Stop(process_uuid: String)
  /// Skip this event - process manager is not interested
  Skip
}

/// Configuration for a process manager.
pub type ProcessManagerConfig(event, command, pm_state) {
  ProcessManagerConfig(
    /// Unique name for this process manager
    name: String,
    /// Determine interest in an event - returns Start/Continue/Stop/Skip
    interested: fn(event) -> Interest,
    /// Handle an event and return commands to dispatch
    handle: fn(event, RecordedEvent(event), pm_state) ->
      Result(List(command), String),
    /// Apply an event to update process manager state
    apply_event: fn(pm_state, event) -> pm_state,
    /// Initial state for new process manager instances
    initial_state: pm_state,
    /// Function to dispatch commands produced by the process manager
    dispatch_command: fn(command) -> Result(Nil, String),
  )
}

/// Create a new process manager configuration.
pub fn new(
  name name: String,
  interested interested: fn(event) -> Interest,
  handle handle: fn(event, RecordedEvent(event), pm_state) ->
    Result(List(command), String),
  apply_event apply_event: fn(pm_state, event) -> pm_state,
  initial_state initial_state: pm_state,
  dispatch_command dispatch_command: fn(command) -> Result(Nil, String),
) -> ProcessManagerConfig(event, command, pm_state) {
  ProcessManagerConfig(
    name: name,
    interested: interested,
    handle: handle,
    apply_event: apply_event,
    initial_state: initial_state,
    dispatch_command: dispatch_command,
  )
}

/// Internal state of the process manager router.
type PMRouterState(event, command, pm_state) {
  PMRouterState(
    config: ProcessManagerConfig(event, command, pm_state),
    instances: Dict(String, pm_state),
  )
}

/// Messages for the process manager.
pub opaque type PMMessage(event) {
  PMHandleEvent(RecordedEvent(event))
}

/// Start a process manager, subscribing to the event store.
pub fn start(
  config: ProcessManagerConfig(event, command, pm_state),
  event_store: EventStore(event),
) -> Result(Subject(PMMessage(event)), String) {
  let router_state =
    PMRouterState(config: config, instances: dict.new())

  case
    actor.new(router_state)
    |> actor.on_message(handle_pm_message)
    |> actor.start
  {
    Ok(started) -> {
      let subject = started.data

      // Subscribe to all events
      let handler = fn(event: RecordedEvent(event)) {
        process.send(subject, PMHandleEvent(event))
      }

      // Delete existing subscription (for restarts)
      let _ = event_store.delete_subscription("$all", config.name)

      let _ =
        event_store.subscribe_persistent(
          "$all",
          config.name,
          Origin,
          handler,
        )

      Ok(subject)
    }
    Error(_) -> Error("Failed to start process manager")
  }
}

fn handle_pm_message(
  state: PMRouterState(event, command, pm_state),
  msg: PMMessage(event),
) -> actor.Next(PMRouterState(event, command, pm_state), PMMessage(event)) {
  case msg {
    PMHandleEvent(recorded_event) -> {
      let event = recorded_event.data
      let interest = state.config.interested(event)

      case interest {
        Skip -> actor.continue(state)

        Start(uuid) -> {
          let pm_state = state.config.initial_state
          let pm_state = state.config.apply_event(pm_state, event)

          // Handle the event to get commands
          case state.config.handle(event, recorded_event, pm_state) {
            Ok(commands) -> {
              // Dispatch each command
              list.each(commands, fn(cmd) {
                let _ = state.config.dispatch_command(cmd)
                Nil
              })

              let new_instances =
                dict.insert(state.instances, uuid, pm_state)
              actor.continue(
                PMRouterState(..state, instances: new_instances),
              )
            }
            Error(_) -> actor.continue(state)
          }
        }

        Continue(uuid) -> {
          let pm_state = case dict.get(state.instances, uuid) {
            Ok(s) -> s
            Error(_) -> state.config.initial_state
          }
          let pm_state = state.config.apply_event(pm_state, event)

          case state.config.handle(event, recorded_event, pm_state) {
            Ok(commands) -> {
              list.each(commands, fn(cmd) {
                let _ = state.config.dispatch_command(cmd)
                Nil
              })

              let new_instances =
                dict.insert(state.instances, uuid, pm_state)
              actor.continue(
                PMRouterState(..state, instances: new_instances),
              )
            }
            Error(_) -> actor.continue(state)
          }
        }

        Stop(uuid) -> {
          // Process the final event, then remove the instance
          let pm_state = case dict.get(state.instances, uuid) {
            Ok(s) -> s
            Error(_) -> state.config.initial_state
          }

          case state.config.handle(event, recorded_event, pm_state) {
            Ok(commands) -> {
              list.each(commands, fn(cmd) {
                let _ = state.config.dispatch_command(cmd)
                Nil
              })
              Nil
            }
            Error(_) -> Nil
          }

          let new_instances = dict.delete(state.instances, uuid)
          actor.continue(
            PMRouterState(..state, instances: new_instances),
          )
        }
      }
    }
  }
}
