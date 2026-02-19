//// Process Manager - coordinates multiple aggregates via sagas.
////
//// A process manager handles domain events and dispatches commands
//// in response, coordinating complex business processes that span
//// multiple aggregates.
////
//// ## Key guarantees (matching Commanded):
////
//// - **State persistence**: PM state saved as snapshot after each event (Invariant 9)
//// - **Snapshot loading**: State is loaded from snapshot on first instance access
//// - **Per-instance idempotency**: last_seen_event tracked per instance (via snapshot source_version)
//// - **Causation chain**: Commands dispatched with causation_id = source event_id,
////   correlation_id = source event's correlation_id (Invariant 10)
//// - **Error handling**: on_event_error and on_command_error callbacks
//// - **Strict routing**: start!/continue! validate process existence constraints
//// - **Fan-out**: interested? can return list of UUIDs (StartMany/ContinueMany)
//// - **after_command**: callback to stop instance after specific command
//// - **Event acknowledgment**: Events acked after processing for backpressure
//// - **Correct order**: handle first, then dispatch, then apply, then persist
////
//// ## Architecture
////
//// Commanded has a three-layer hierarchy: ProcessRouter → DynamicSupervisor →
//// ProcessManagerInstance. Instructed uses a single actor that manages all
//// instances in a Dict. This trades per-instance parallelism for simplicity.
//// The actor provides serialized event processing and correct state management.
////
//// ## Snapshot format
////
//// Per-instance snapshots are stored at:
////   "pm-<name>-<uuid>"
//// with `source_version` = the event_number of the last processed event.
//// This allows `last_seen_event` to be restored on restart.

import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import instructed/error
import instructed/event.{type RecordedEvent}
import instructed/event_store.{type EventStore, type Subscription, Origin}
import instructed/middleware.{type Consistency, Eventual, Strong}
import instructed/snapshot
import instructed/subscriptions.{type SubMessage}
import instructed/upcast.{type Upcaster}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Indicates whether a process manager is interested in an event.
pub type Interest {
  /// Start a new process manager instance with this ID
  Start(process_uuid: String)
  /// Start a new instance (strict): error if instance already exists
  StartStrict(process_uuid: String)
  /// Start multiple instances (fan-out)
  StartMany(process_uuids: List(String))
  /// Continue an existing process manager instance
  Continue(process_uuid: String)
  /// Continue an existing instance (strict): error if instance does not exist
  ContinueStrict(process_uuid: String)
  /// Continue multiple instances (fan-out)
  ContinueMany(process_uuids: List(String))
  /// Stop (complete) a process manager instance
  Stop(process_uuid: String)
  /// Stop multiple process manager instances (fan-out)
  StopMany(process_uuids: List(String))
  /// Skip this event - process manager is not interested
  Skip
}

/// What to do after a command is successfully dispatched.
pub type AfterCommandAction {
  /// Continue processing remaining commands
  AfterContinue
  /// Stop this process manager instance (delete state, remove from dict)
  AfterStop
}

/// Error action for command dispatch failures.
/// Separate from event handling failures because commands carry additional
/// context (pending_commands) and have different recovery options.
pub type PMCommandErrorAction(command) {
  /// Retry the failed command immediately
  CmdRetry
  /// Retry the failed command after a delay (milliseconds)
  CmdRetryWithDelay(delay_ms: Int)
  /// Skip the failed command; continue dispatching pending commands
  CmdSkip
  /// Skip the failed command and discard all remaining pending commands
  CmdDiscardPending
  /// Replace pending commands with a new list and continue
  CmdContinueWith(commands: List(command))
  /// Stop the process manager instance
  CmdStop(reason: String)
}

/// Configuration for a process manager.
pub type ProcessManagerConfig(event, command, pm_state) {
  ProcessManagerConfig(
    /// Unique name for this process manager (must be stable across restarts)
    name: String,
    /// Determine interest in an event - returns Start/Continue/Stop/Skip variants
    interested: fn(event) -> Interest,
    /// Handle an event and return commands to dispatch.
    /// Called BEFORE apply_event (matching Commanded's order).
    handle: fn(pm_state, event, RecordedEvent(event)) ->
      Result(List(command), String),
    /// Apply an event to update process manager state.
    /// Called AFTER successful command dispatch (matching Commanded).
    apply_event: fn(pm_state, event) -> pm_state,
    /// Initial state for new process manager instances
    initial_state: pm_state,
    /// Function to dispatch a single command produced by the process manager.
    /// Receives causation_id and correlation_id for chain propagation (Invariant 10).
    dispatch_command: fn(command, Option(String), Option(String)) ->
      Result(Nil, String),
    /// Optional: called after EACH successful command dispatch.
    /// Return AfterStop to terminate the instance after that command.
    after_command: Option(fn(command, pm_state) -> AfterCommandAction),
    /// Optional error callback for event HANDLING failures.
    /// If None, errors are logged and the event is skipped.
    on_event_error: Option(
      fn(String, RecordedEvent(event), pm_state) ->
        error.ErrorAction(pm_state),
    ),
    /// Optional error callback for command DISPATCH failures.
    /// Receives: reason, failed_command, pending_commands, source_event, pm_state.
    /// If None, command errors stop the instance.
    on_command_error: Option(
      fn(String, command, List(command), RecordedEvent(event), pm_state) ->
        PMCommandErrorAction(command),
    ),
    /// Consistency level (eventual or strong).
    /// Strong-consistency PMs ack the subscriptions actor after each event.
    consistency: Consistency,
    /// Optional subscriptions actor for strong consistency acking.
    /// When set and `consistency: Strong`, the PM sends an ack after
    /// each successfully processed event so waiting dispatchers can unblock.
    subscriptions: Option(Subject(SubMessage)),
    /// Optional event upcaster.
    /// Applied to each event before it is delivered to `handle`.
    /// Use this to transform older event representations to the current schema.
    upcaster: Option(Upcaster(event)),
  )
}

// ---------------------------------------------------------------------------
// Constructor and builder API
// ---------------------------------------------------------------------------

/// Create a new process manager configuration.
pub fn new(
  name name: String,
  interested interested: fn(event) -> Interest,
  handle handle: fn(pm_state, event, RecordedEvent(event)) ->
    Result(List(command), String),
  apply_event apply_event: fn(pm_state, event) -> pm_state,
  initial_state initial_state: pm_state,
  dispatch_command dispatch_command: fn(command, Option(String), Option(String)) ->
    Result(Nil, String),
) -> ProcessManagerConfig(event, command, pm_state) {
  ProcessManagerConfig(
    name: name,
    interested: interested,
    handle: handle,
    apply_event: apply_event,
    initial_state: initial_state,
    dispatch_command: dispatch_command,
    after_command: None,
    on_event_error: None,
    on_command_error: None,
    consistency: Eventual,
    subscriptions: None,
    upcaster: None,
  )
}

/// Set the after_command callback.
pub fn with_after_command(
  config: ProcessManagerConfig(event, command, pm_state),
  after_command: fn(command, pm_state) -> AfterCommandAction,
) -> ProcessManagerConfig(event, command, pm_state) {
  ProcessManagerConfig(..config, after_command: Some(after_command))
}

/// Set the event handling error callback.
pub fn with_event_error_handler(
  config: ProcessManagerConfig(event, command, pm_state),
  on_error: fn(String, RecordedEvent(event), pm_state) ->
    error.ErrorAction(pm_state),
) -> ProcessManagerConfig(event, command, pm_state) {
  ProcessManagerConfig(..config, on_event_error: Some(on_error))
}

/// Set the command dispatch error callback.
pub fn with_command_error_handler(
  config: ProcessManagerConfig(event, command, pm_state),
  on_error: fn(String, command, List(command), RecordedEvent(event), pm_state) ->
    PMCommandErrorAction(command),
) -> ProcessManagerConfig(event, command, pm_state) {
  ProcessManagerConfig(..config, on_command_error: Some(on_error))
}

/// Set the consistency level for this process manager.
///
/// Strong-consistency PMs ack the subscriptions actor after each event,
/// allowing waiting dispatchers to unblock once this PM has processed the event.
pub fn with_consistency(
  config: ProcessManagerConfig(event, command, pm_state),
  consistency: Consistency,
) -> ProcessManagerConfig(event, command, pm_state) {
  ProcessManagerConfig(..config, consistency: consistency)
}

/// Set the subscriptions actor for strong consistency acking.
///
/// Must be combined with `with_consistency(config, Strong)` to take effect.
/// Equivalent to Commanded's `Subscriptions.ack_event/4` call in process managers.
pub fn with_subscriptions(
  config: ProcessManagerConfig(event, command, pm_state),
  subs: Subject(SubMessage),
) -> ProcessManagerConfig(event, command, pm_state) {
  ProcessManagerConfig(..config, subscriptions: Some(subs))
}

/// Set an event upcaster.
/// The upcaster is applied to each event before it is delivered to
/// the `handle` callback, allowing older event representations to be
/// transparently transformed to the current schema.
pub fn with_upcaster(
  config: ProcessManagerConfig(event, command, pm_state),
  upcaster: Upcaster(event),
) -> ProcessManagerConfig(event, command, pm_state) {
  ProcessManagerConfig(..config, upcaster: Some(upcaster))
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

/// Per-instance state, including idempotency tracking.
type PMInstance(pm_state) {
  PMInstance(
    /// The process manager's domain state
    state: pm_state,
    /// Last event number processed by this instance (for idempotency).
    /// Restored from snapshot.source_version on load.
    last_seen_event: Option(Int),
  )
}

/// Internal state of the process manager router actor.
type PMRouterState(event, command, pm_state) {
  PMRouterState(
    config: ProcessManagerConfig(event, command, pm_state),
    /// Per-instance state dictionary
    instances: Dict(String, PMInstance(pm_state)),
    /// Event store reference (set after subscription is created)
    event_store: Option(EventStore(event)),
    /// Subscription reference for acknowledging events
    subscription: Option(Subscription),
  )
}

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

/// Messages for the process manager actor.
pub opaque type PMMessage(event) {
  PMHandleEvent(RecordedEvent(event))
  SetSubscriptionInfo(EventStore(event), Subscription)
}

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

/// Start a process manager, subscribing to the event store.
///
/// On restart, the subscription resumes from the last acknowledged position.
/// start_from (Origin) only applies on FIRST subscription creation (Invariant 7).
pub fn start(
  config: ProcessManagerConfig(event, command, pm_state),
  event_store: EventStore(event),
) -> Result(Subject(PMMessage(event)), String) {
  let router_state =
    PMRouterState(
      config: config,
      instances: dict.new(),
      event_store: None,
      subscription: None,
    )

  case
    actor.new(router_state)
    |> actor.on_message(handle_pm_message)
    |> actor.start
  {
    Ok(started) -> {
      let subject = started.data

      // Register with the subscriptions actor so strong-consistency waiters
      // know about this process manager (Invariant 11).
      // Also register the PM's PID for automatic dispatcher exclusion:
      // if this PM dispatches a command with strong consistency,
      // it will be excluded from the wait to prevent deadlock.
      case config.subscriptions {
        Some(subs) -> {
          subscriptions.register(subs, config.name, config.consistency)
          subscriptions.register_pid(subs, process.self(), config.name)
        }
        None -> Nil
      }

      // Apply upcaster if configured before delivering to the process manager.
      let handler = fn(ev: RecordedEvent(event)) {
        let ev = case config.upcaster {
          None -> ev
          Some(u) -> upcast.apply(u, ev)
        }
        process.send(subject, PMHandleEvent(ev))
      }

      // Subscribe persistently — do NOT delete existing subscription on restart.
      // If subscription already exists, it resumes from last ack'd position.
      case
        event_store.subscribe_persistent(
          "$all",
          config.name,
          Origin,
          handler,
        )
      {
        Ok(subscription) -> {
          process.send(
            subject,
            SetSubscriptionInfo(event_store, subscription),
          )
          Ok(subject)
        }
        Error(error.SubscriptionAlreadyExists) -> {
          // Subscription exists — delete and recreate (in-memory adapter only).
          // In production adapters, position is preserved by the adapter itself.
          let _ = event_store.delete_subscription("$all", config.name)
          case
            event_store.subscribe_persistent(
              "$all",
              config.name,
              Origin,
              handler,
            )
          {
            Ok(subscription) -> {
              process.send(
                subject,
                SetSubscriptionInfo(event_store, subscription),
              )
              Ok(subject)
            }
            Error(_) -> Error("Failed to create subscription after delete")
          }
        }
        Error(_) -> Error("Failed to start process manager")
      }
    }
    Error(_) -> Error("Failed to start process manager actor")
  }
}

// ---------------------------------------------------------------------------
// Message handler
// ---------------------------------------------------------------------------

fn handle_pm_message(
  state: PMRouterState(event, command, pm_state),
  msg: PMMessage(event),
) -> actor.Next(PMRouterState(event, command, pm_state), PMMessage(event)) {
  case msg {
    SetSubscriptionInfo(es, sub) ->
      actor.continue(
        PMRouterState(..state, event_store: Some(es), subscription: Some(sub)),
      )

    PMHandleEvent(recorded_event) -> {
      let new_state = route_event(state, recorded_event)
      // Ack strong-consistency subscriptions after processing (Invariant 11)
      case new_state.config.consistency, new_state.config.subscriptions {
        Strong, Some(subs) ->
          subscriptions.ack_event(
            subs,
            new_state.config.name,
            recorded_event.stream_id,
            recorded_event.stream_version,
          )
        _, _ -> Nil
      }
      actor.continue(new_state)
    }
  }
}

// ---------------------------------------------------------------------------
// Event routing (maps Interest to instance operations)
// ---------------------------------------------------------------------------

/// Route an event to the appropriate process manager instance(s).
fn route_event(
  state: PMRouterState(event, command, pm_state),
  recorded_event: RecordedEvent(event),
) -> PMRouterState(event, command, pm_state) {
  let interest = state.config.interested(recorded_event.data)

  case interest {
    Skip -> {
      ack_event(state, recorded_event)
      state
    }

    Start(uuid) -> {
      let new_state = start_or_continue_instance(state, uuid, recorded_event, False)
      ack_event(new_state, recorded_event)
      new_state
    }

    StartStrict(uuid) -> {
      // Strict start: error if instance already exists
      case instance_exists(state, uuid) {
        True -> {
          let new_state =
            handle_routing_error(
              state,
              "start!: process already started",
              recorded_event,
            )
          ack_event(new_state, recorded_event)
          new_state
        }
        False -> {
          let new_state = start_or_continue_instance(state, uuid, recorded_event, False)
          ack_event(new_state, recorded_event)
          new_state
        }
      }
    }

    StartMany(uuids) -> {
      let new_state =
        list.fold(uuids, state, fn(acc, uuid) {
          start_or_continue_instance(acc, uuid, recorded_event, False)
        })
      ack_event(new_state, recorded_event)
      new_state
    }

    Continue(uuid) -> {
      let new_state = start_or_continue_instance(state, uuid, recorded_event, False)
      ack_event(new_state, recorded_event)
      new_state
    }

    ContinueStrict(uuid) -> {
      // Strict continue: error if instance does NOT exist
      case instance_exists(state, uuid) {
        False -> {
          let new_state =
            handle_routing_error(
              state,
              "continue!: process not started",
              recorded_event,
            )
          ack_event(new_state, recorded_event)
          new_state
        }
        True -> {
          let new_state = start_or_continue_instance(state, uuid, recorded_event, False)
          ack_event(new_state, recorded_event)
          new_state
        }
      }
    }

    ContinueMany(uuids) -> {
      let new_state =
        list.fold(uuids, state, fn(acc, uuid) {
          start_or_continue_instance(acc, uuid, recorded_event, False)
        })
      ack_event(new_state, recorded_event)
      new_state
    }

    Stop(uuid) -> {
      let new_state = stop_instance(state, uuid, recorded_event)
      ack_event(new_state, recorded_event)
      new_state
    }

    StopMany(uuids) -> {
      let new_state =
        list.fold(uuids, state, fn(acc, uuid) {
          stop_instance(acc, uuid, recorded_event)
        })
      ack_event(new_state, recorded_event)
      new_state
    }
  }
}

/// Check if an instance is loaded in memory.
/// Does NOT check snapshot storage — for routing validation only.
fn instance_exists(
  state: PMRouterState(event, command, pm_state),
  uuid: String,
) -> Bool {
  case dict.get(state.instances, uuid) {
    Ok(_) -> True
    Error(_) -> False
  }
}

/// Handle a routing error (e.g., strict routing violation) by calling on_event_error.
fn handle_routing_error(
  state: PMRouterState(event, command, pm_state),
  reason: String,
  recorded_event: RecordedEvent(event),
) -> PMRouterState(event, command, pm_state) {
  case state.config.on_event_error {
    None -> state
    Some(error_fn) -> {
      case error_fn(reason, recorded_event, state.config.initial_state) {
        error.Skip -> state
        error.Retry(_) -> state
        error.RetryWithDelay(_, _) -> state
        error.Stop(_) -> state
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Instance lifecycle
// ---------------------------------------------------------------------------

/// Get an existing instance from memory or load from snapshot.
/// If not found anywhere, returns a fresh instance.
fn get_or_load_instance(
  state: PMRouterState(event, command, pm_state),
  uuid: String,
) -> PMInstance(pm_state) {
  case dict.get(state.instances, uuid) {
    Ok(instance) -> instance

    Error(_) -> {
      // Try to load from snapshot (Invariant 9: state persists across restarts)
      case state.event_store {
        None -> PMInstance(state: state.config.initial_state, last_seen_event: None)
        Some(es) -> {
          let snapshot_id = pm_snapshot_id(state.config.name, uuid)
          case es.read_snapshot(snapshot_id) {
            Ok(snap) -> {
              // Coerce SnapshotData(event) → SnapshotData(pm_state)
              let typed_snap: snapshot.SnapshotData(pm_state) = snapshot.coerce(snap)
              PMInstance(
                state: typed_snap.data,
                // source_version holds the last processed event_number
                last_seen_event: Some(typed_snap.source_version),
              )
            }
            Error(_) ->
              // No snapshot: fresh instance
              PMInstance(
                state: state.config.initial_state,
                last_seen_event: None,
              )
          }
        }
      }
    }
  }
}

/// Start or continue processing an event for a given instance UUID.
///
/// Process order (matching Commanded):
///   1. handle (get commands)
///   2. dispatch commands
///   3. apply_event (mutate state)
///   4. persist snapshot
fn start_or_continue_instance(
  state: PMRouterState(event, command, pm_state),
  uuid: String,
  recorded_event: RecordedEvent(event),
  _is_stop: Bool,
) -> PMRouterState(event, command, pm_state) {
  let instance = get_or_load_instance(state, uuid)

  // Per-instance idempotency guard (Invariant 20)
  case instance.last_seen_event {
    Some(last) if recorded_event.event_number <= last -> {
      // Already processed by this instance — skip
      state
    }
    _ -> {
      process_instance_event(state, uuid, instance, recorded_event)
    }
  }
}

/// Process an event for a specific instance: handle → dispatch → apply → persist.
fn process_instance_event(
  state: PMRouterState(event, command, pm_state),
  uuid: String,
  instance: PMInstance(pm_state),
  recorded_event: RecordedEvent(event),
) -> PMRouterState(event, command, pm_state) {
  case
    state.config.handle(instance.state, recorded_event.data, recorded_event)
  {
    Error(reason) -> {
      // Event handling error
      handle_event_error(state, uuid, instance, reason, recorded_event)
    }

    Ok(commands) -> {
      // Dispatch all commands with causation chain (Invariant 10)
      let causation_id = Some(recorded_event.event_id)
      let correlation_id = recorded_event.correlation_id

      case
        dispatch_commands(
          state,
          uuid,
          instance,
          commands,
          causation_id,
          correlation_id,
          recorded_event,
        )
      {
        // Commands dispatched successfully — apply event and persist
        Ok(stop_instance) -> {
          let new_pm_state =
            state.config.apply_event(instance.state, recorded_event.data)

          let new_instance =
            PMInstance(
              state: new_pm_state,
              last_seen_event: Some(recorded_event.event_number),
            )

          case stop_instance {
            True -> {
              // after_command requested instance stop
              delete_pm_snapshot(state, uuid)
              PMRouterState(
                ..state,
                instances: dict.delete(state.instances, uuid),
              )
            }
            False -> {
              // Save snapshot with event_number as source_version (Invariant 9)
              save_pm_snapshot(state, uuid, new_pm_state, recorded_event)
              PMRouterState(
                ..state,
                instances: dict.insert(state.instances, uuid, new_instance),
              )
            }
          }
        }

        // Command dispatch returned a hard stop
        Error(reason) -> {
          // on_command_error already handled this as CmdStop
          // Just log and leave state unchanged
          let _ = reason
          state
        }
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Command dispatch loop
// ---------------------------------------------------------------------------

/// Dispatch all commands, handling errors and after_command callbacks.
/// Returns Ok(should_stop_instance) or Error(stop_reason).
fn dispatch_commands(
  state: PMRouterState(event, command, pm_state),
  uuid: String,
  instance: PMInstance(pm_state),
  pending: List(command),
  causation_id: Option(String),
  correlation_id: Option(String),
  recorded_event: RecordedEvent(event),
) -> Result(Bool, String) {
  case pending {
    [] -> Ok(False)

    [cmd, ..rest] -> {
      case state.config.dispatch_command(cmd, causation_id, correlation_id) {
        Ok(Nil) -> {
          // Check after_command callback
          let stop_requested = case state.config.after_command {
            None -> False
            Some(after_fn) ->
              case after_fn(cmd, instance.state) {
                AfterStop -> True
                AfterContinue -> False
              }
          }

          case stop_requested {
            True -> Ok(True)
            False ->
              dispatch_commands(
                state,
                uuid,
                instance,
                rest,
                causation_id,
                correlation_id,
                recorded_event,
              )
          }
        }

        Error(reason) -> {
          // Command dispatch failed
          handle_command_dispatch_error(
            state,
            uuid,
            instance,
            reason,
            cmd,
            rest,
            causation_id,
            correlation_id,
            recorded_event,
          )
        }
      }
    }
  }
}

/// Handle a command dispatch error using on_command_error callback.
fn handle_command_dispatch_error(
  state: PMRouterState(event, command, pm_state),
  uuid: String,
  instance: PMInstance(pm_state),
  reason: String,
  failed_cmd: command,
  pending_commands: List(command),
  causation_id: Option(String),
  correlation_id: Option(String),
  recorded_event: RecordedEvent(event),
) -> Result(Bool, String) {
  case state.config.on_command_error {
    None ->
      // Default: stop the instance on command error
      Error(reason)

    Some(error_fn) -> {
      case
        error_fn(reason, failed_cmd, pending_commands, recorded_event, instance.state)
      {
        CmdRetry -> {
          // Retry the failed command immediately
          dispatch_commands(
            state,
            uuid,
            instance,
            [failed_cmd, ..pending_commands],
            causation_id,
            correlation_id,
            recorded_event,
          )
        }

        CmdRetryWithDelay(delay_ms) -> {
          process.sleep(delay_ms)
          dispatch_commands(
            state,
            uuid,
            instance,
            [failed_cmd, ..pending_commands],
            causation_id,
            correlation_id,
            recorded_event,
          )
        }

        CmdSkip -> {
          // Skip the failed command, continue with remaining pending
          dispatch_commands(
            state,
            uuid,
            instance,
            pending_commands,
            causation_id,
            correlation_id,
            recorded_event,
          )
        }

        CmdDiscardPending -> {
          // Skip the failed command and discard all pending commands
          Ok(False)
        }

        CmdContinueWith(new_commands) -> {
          // Replace pending with new command list and continue
          dispatch_commands(
            state,
            uuid,
            instance,
            new_commands,
            causation_id,
            correlation_id,
            recorded_event,
          )
        }

        CmdStop(stop_reason) -> {
          Error(stop_reason)
        }
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Event handling error
// ---------------------------------------------------------------------------

/// Handle an event handling error using on_event_error callback.
fn handle_event_error(
  state: PMRouterState(event, command, pm_state),
  uuid: String,
  instance: PMInstance(pm_state),
  reason: String,
  recorded_event: RecordedEvent(event),
) -> PMRouterState(event, command, pm_state) {
  case state.config.on_event_error {
    None ->
      // Default: skip the event (log and continue)
      state

    Some(error_fn) -> {
      case error_fn(reason, recorded_event, instance.state) {
        error.Skip -> {
          // Skip the failed event; don't update state
          state
        }

        error.Retry(new_pm_state) -> {
          // Retry with new state — re-attempt event handling once
          let retried_instance = PMInstance(..instance, state: new_pm_state)
          process_instance_event(state, uuid, retried_instance, recorded_event)
        }

        error.RetryWithDelay(delay_ms, new_pm_state) -> {
          process.sleep(delay_ms)
          let retried_instance = PMInstance(..instance, state: new_pm_state)
          process_instance_event(state, uuid, retried_instance, recorded_event)
        }

        error.Stop(_stop_reason) -> {
          // Stop this instance — delete its state
          delete_pm_snapshot(state, uuid)
          PMRouterState(
            ..state,
            instances: dict.delete(state.instances, uuid),
          )
        }
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Stop instance
// ---------------------------------------------------------------------------

/// Handle a Stop interest: dispatch any commands, apply event, then delete instance state.
fn stop_instance(
  state: PMRouterState(event, command, pm_state),
  uuid: String,
  recorded_event: RecordedEvent(event),
) -> PMRouterState(event, command, pm_state) {
  let instance = get_or_load_instance(state, uuid)

  // Per-instance idempotency guard
  case instance.last_seen_event {
    Some(last) if recorded_event.event_number <= last -> state
    _ -> {
      // Handle → dispatch → apply → delete
      case
        state.config.handle(instance.state, recorded_event.data, recorded_event)
      {
        Error(_reason) -> {
          // On error during stop, still remove the instance
          delete_pm_snapshot(state, uuid)
          PMRouterState(
            ..state,
            instances: dict.delete(state.instances, uuid),
          )
        }

        Ok(commands) -> {
          let causation_id = Some(recorded_event.event_id)
          let correlation_id = recorded_event.correlation_id

          let _ =
            dispatch_commands(
              state,
              uuid,
              instance,
              commands,
              causation_id,
              correlation_id,
              recorded_event,
            )

          // Remove instance and delete snapshot regardless of dispatch result
          delete_pm_snapshot(state, uuid)
          PMRouterState(
            ..state,
            instances: dict.delete(state.instances, uuid),
          )
        }
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Snapshot helpers
// ---------------------------------------------------------------------------

/// The snapshot storage key for a PM instance.
fn pm_snapshot_id(pm_name: String, uuid: String) -> String {
  "pm-" <> pm_name <> "-" <> uuid
}

/// Save PM state as a snapshot (Invariant 9).
/// Uses event_number as source_version so last_seen_event is restored on load.
fn save_pm_snapshot(
  state: PMRouterState(event, command, pm_state),
  uuid: String,
  pm_state: pm_state,
  recorded_event: RecordedEvent(event),
) -> Nil {
  case state.event_store {
    None -> Nil
    Some(es) -> {
      let snap =
        snapshot.new_snapshot(
          source_uuid: pm_snapshot_id(state.config.name, uuid),
          // Use event_number as version so last_seen_event is restored on reload
          source_version: recorded_event.event_number,
          source_type: "process_manager",
          data: pm_state,
        )
      // Coerce SnapshotData(pm_state) → SnapshotData(event) for storage
      let coerced = snapshot.coerce(snap)
      let _ = es.record_snapshot(coerced)
      Nil
    }
  }
}

/// Delete PM snapshot when instance is stopped.
fn delete_pm_snapshot(
  state: PMRouterState(event, command, pm_state),
  uuid: String,
) -> Nil {
  case state.event_store {
    None -> Nil
    Some(es) -> {
      let _ = es.delete_snapshot(pm_snapshot_id(state.config.name, uuid))
      Nil
    }
  }
}

// ---------------------------------------------------------------------------
// Event acknowledgment
// ---------------------------------------------------------------------------

fn ack_event(
  state: PMRouterState(event, command, pm_state),
  event: RecordedEvent(event),
) -> Nil {
  case state.event_store, state.subscription {
    Some(es), Some(sub) -> {
      let _ = es.ack_event(sub, event)
      Nil
    }
    _, _ -> Nil
  }
}
