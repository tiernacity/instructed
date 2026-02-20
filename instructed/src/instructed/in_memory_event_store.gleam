//// In-memory event store implementation using an OTP Actor.
////
//// This event store stores all events in memory and is designed
//// primarily for testing purposes. Events are lost when the process stops.
////
//// ## Subscription Model
////
//// Persistent subscriptions deliver events one at a time to the subscriber
//// via message passing. The subscriber must acknowledge each event via
//// `ack_event` before the next event is delivered. This provides backpressure
//// and matches Commanded's in-memory adapter behaviour.
////
//// The subscription checkpoint (last acknowledged event number) is tracked
//// in memory. On restart, subscriptions resume from the last checkpoint.
////
//// ## Example
////
//// ```gleam
//// import instructed/in_memory_event_store
//// import instructed/event_store
////
//// let assert Ok(store) = in_memory_event_store.start()
//// // Use store as an EventStore(event) value
//// ```

import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import instructed/error.{
  type EventStoreError, SnapshotNotFound, StreamAlreadyExists,
  StreamNotFound, SubscriptionNotFound,
  VersionConflict,
}
import instructed/event.{type EventData, type RecordedEvent, RecordedEvent}
import instructed/event_store.{
  type EventStore, type ExpectedVersion, type StartFrom, type Subscription,
  AnyVersion, Current, EventStore, ExactVersion, FromEventNumber, NoStream,
  Origin, StreamExists, Subscription,
}
import instructed/snapshot
import youid/uuid

// --- Actor Message Types ---

type StoreState(event) {
  StoreState(
    /// All events stored globally, newest first (actually append order)
    all_events: List(RecordedEvent(event)),
    /// Events indexed by stream ID (in append order)
    streams: Dict(String, List(RecordedEvent(event))),
    /// Transient subscribers (all streams)
    all_subscribers: List(TransientSub(event)),
    /// Transient subscribers (specific streams)
    stream_subscribers: Dict(String, List(TransientSub(event))),
    /// Persistent subscriptions keyed by "stream:name"
    persistent_subs: Dict(String, PersistentSub(event)),
    /// Snapshots by source UUID
    snapshots: Dict(String, snapshot.SnapshotData(event)),
    /// Next global event number
    next_event_number: Int,
    /// Next subscriber ID counter
    next_sub_id: Int,
  )
}

type TransientSub(event) {
  TransientSub(
    id: String,
    handler: fn(RecordedEvent(event)) -> Nil,
    /// Optional PID of the owning process. Used for lazy cleanup
    /// of dead transient subscribers (Fix 6).
    owner_pid: Option(process.Pid),
  )
}

/// A persistent subscription tracks:
/// - checkpoint: last acknowledged event number (durable position)
/// - in_flight: event currently being processed by subscriber (if any)
/// - pending: events queued waiting to be sent
/// - handler: non-blocking callback to deliver events to subscriber
type PersistentSub(event) {
  PersistentSub(
    name: String,
    stream: String,
    handler: fn(RecordedEvent(event)) -> Nil,
    checkpoint: Int,
    in_flight: Option(RecordedEvent(event)),
    pending: List(RecordedEvent(event)),
  )
}

/// Opaque message type for the in-memory event store actor.
pub opaque type Message(event) {
  Append(
    stream_id: String,
    expected_version: ExpectedVersion,
    events: List(EventData(event)),
    reply: Subject(Result(Int, EventStoreError)),
  )
  ReadStream(
    stream_id: String,
    start_version: Int,
    batch_size: Int,
    reply: Subject(Result(List(RecordedEvent(event)), EventStoreError)),
  )
  ReadAll(
    start_number: Int,
    reply: Subject(Result(List(RecordedEvent(event)), EventStoreError)),
  )
  SubscribeAll(
    handler: fn(RecordedEvent(event)) -> Nil,
    owner_pid: Option(process.Pid),
    reply: Subject(Result(Subscription, EventStoreError)),
  )
  SubscribeStream(
    stream_id: String,
    handler: fn(RecordedEvent(event)) -> Nil,
    owner_pid: Option(process.Pid),
    reply: Subject(Result(Subscription, EventStoreError)),
  )
  SubscribePersistent(
    stream: String,
    name: String,
    start_from: StartFrom,
    handler: fn(RecordedEvent(event)) -> Nil,
    reply: Subject(Result(Subscription, EventStoreError)),
  )
  AckEvent(
    sub: Subscription,
    event: RecordedEvent(event),
    reply: Subject(Result(Nil, EventStoreError)),
  )
  Unsubscribe(
    sub: Subscription,
    reply: Subject(Result(Nil, EventStoreError)),
  )
  DeleteSubscription(
    stream: String,
    name: String,
    reply: Subject(Result(Nil, EventStoreError)),
  )
  ReadSnapshot(
    source_uuid: String,
    reply: Subject(Result(snapshot.SnapshotData(event), EventStoreError)),
  )
  RecordSnapshot(
    snapshot: snapshot.SnapshotData(event),
    reply: Subject(Result(Nil, EventStoreError)),
  )
  DeleteSnapshot(
    source_uuid: String,
    reply: Subject(Result(Nil, EventStoreError)),
  )
  Reset(reply: Subject(Result(Nil, EventStoreError)))
  GetLatestEventNumber(reply: Subject(Result(Option(Int), EventStoreError)))
}

fn initial_state() -> StoreState(event) {
  StoreState(
    all_events: [],
    streams: dict.new(),
    all_subscribers: [],
    stream_subscribers: dict.new(),
    persistent_subs: dict.new(),
    snapshots: dict.new(),
    next_event_number: 1,
    next_sub_id: 1,
  )
}

fn handle_message(
  state: StoreState(event),
  msg: Message(event),
) -> actor.Next(StoreState(event), Message(event)) {
  case msg {
    Append(stream_id, expected_version, events, reply) ->
      handle_append(state, stream_id, expected_version, events, reply)

    ReadStream(stream_id, start_version, batch_size, reply) ->
      handle_read_stream(state, stream_id, start_version, batch_size, reply)

    ReadAll(start_number, reply) ->
      handle_read_all(state, start_number, reply)

    SubscribeAll(handler, owner_pid, reply) ->
      handle_subscribe_all(state, handler, owner_pid, reply)

    SubscribeStream(stream_id, handler, owner_pid, reply) ->
      handle_subscribe_stream(state, stream_id, handler, owner_pid, reply)

    SubscribePersistent(stream, name, start_from, handler, reply) ->
      handle_subscribe_persistent(
        state,
        stream,
        name,
        start_from,
        handler,
        reply,
      )

    AckEvent(sub, event, reply) ->
      handle_ack_event(state, sub, event, reply)

    Unsubscribe(sub, reply) ->
      handle_unsubscribe(state, sub, reply)

    DeleteSubscription(stream, name, reply) ->
      handle_delete_subscription(state, stream, name, reply)

    ReadSnapshot(source_uuid, reply) ->
      handle_read_snapshot(state, source_uuid, reply)

    RecordSnapshot(snap, reply) ->
      handle_record_snapshot(state, snap, reply)

    DeleteSnapshot(source_uuid, reply) ->
      handle_delete_snapshot(state, source_uuid, reply)

    Reset(reply) -> {
      process.send(reply, Ok(Nil))
      actor.continue(initial_state())
    }

    GetLatestEventNumber(reply) ->
      handle_get_latest_event_number(state, reply)
  }
}

// --- Append ---

fn handle_append(
  state: StoreState(event),
  stream_id: String,
  expected_version: ExpectedVersion,
  events: List(EventData(event)),
  reply: Subject(Result(Int, EventStoreError)),
) -> actor.Next(StoreState(event), Message(event)) {
  let stream_events = dict.get(state.streams, stream_id)
  let current_version = case stream_events {
    Ok(evts) -> list.length(evts)
    Error(_) -> 0
  }
  let stream_exists = case stream_events {
    Ok(_) -> True
    Error(_) -> False
  }

  // Check expected version - with specific error types matching Commanded
  let version_check = case expected_version {
    AnyVersion -> Ok(Nil)
    NoStream ->
      case stream_exists {
        True -> Error(StreamAlreadyExists)
        False -> Ok(Nil)
      }
    StreamExists ->
      case stream_exists {
        True -> Ok(Nil)
        False -> Error(StreamNotFound)
      }
    ExactVersion(v) ->
      case current_version == v {
        True -> Ok(Nil)
        False -> Error(VersionConflict)
      }
  }

  case version_check {
    Error(err) -> {
      process.send(reply, Error(err))
      actor.continue(state)
    }
    Ok(_) -> {
      let existing = case stream_events {
        Ok(evts) -> evts
        Error(_) -> []
      }

      let #(recorded, next_num, _next_ver) =
        create_recorded_events(
          events,
          stream_id,
          state.next_event_number,
          current_version,
        )

      let new_stream_events = list.append(existing, recorded)
      let new_streams =
        dict.insert(state.streams, stream_id, new_stream_events)
      let new_all = list.append(state.all_events, recorded)

      // Notify transient subscribers (non-blocking callbacks).
      // Lazy cleanup: filter out subscribers whose owner process is dead (Fix 6).
      let live_all_subs = filter_live_subscribers(state.all_subscribers)
      list.each(live_all_subs, fn(sub) {
        list.each(recorded, fn(evt) { sub.handler(evt) })
      })
      let live_stream_subs = case dict.get(state.stream_subscribers, stream_id) {
        Ok(subs) -> {
          let live = filter_live_subscribers(subs)
          list.each(live, fn(sub) {
            list.each(recorded, fn(evt) { sub.handler(evt) })
          })
          live
        }
        Error(_) -> []
      }

      // Queue events for persistent subscribers and deliver if available
      let new_persistent_subs =
        dict.map_values(state.persistent_subs, fn(_key, psub) {
          case psub.stream == "$all" || psub.stream == stream_id {
            True -> {
              // Filter to only new events (after checkpoint)
              let new_events =
                list.filter(recorded, fn(evt) {
                  evt.event_number > psub.checkpoint
                })
              // Add to pending queue
              let updated =
                PersistentSub(
                  ..psub,
                  pending: list.append(psub.pending, new_events),
                )
              // Try to deliver next event if nothing in-flight
              maybe_deliver_next(updated)
            }
            False -> psub
          }
        })

      // Update stream_subscribers with cleaned list (Fix 6 lazy cleanup)
      let new_stream_subscribers = case live_stream_subs {
        [] -> state.stream_subscribers
        _ -> dict.insert(state.stream_subscribers, stream_id, live_stream_subs)
      }

      let new_state =
        StoreState(
          ..state,
          all_events: new_all,
          streams: new_streams,
          next_event_number: next_num,
          persistent_subs: new_persistent_subs,
          all_subscribers: live_all_subs,
          stream_subscribers: new_stream_subscribers,
        )
      process.send(reply, Ok(current_version + list.length(recorded)))
      actor.continue(new_state)
    }
  }
}

/// Try to deliver the next pending event to the subscriber.
/// Only delivers if there's no event currently in-flight.
fn maybe_deliver_next(psub: PersistentSub(event)) -> PersistentSub(event) {
  case psub.in_flight {
    Some(_) ->
      // Already processing an event, wait for ack
      psub
    None ->
      case psub.pending {
        [] ->
          // No pending events
          psub
        [next, ..rest] -> {
          // Deliver event to subscriber via handler callback.
          // The handler MUST be non-blocking (e.g., process.send to an actor).
          psub.handler(next)
          PersistentSub(..psub, in_flight: Some(next), pending: rest)
        }
      }
  }
}

// --- Read Stream ---

fn handle_read_stream(
  state: StoreState(event),
  stream_id: String,
  start_version: Int,
  batch_size: Int,
  reply: Subject(Result(List(RecordedEvent(event)), EventStoreError)),
) -> actor.Next(StoreState(event), Message(event)) {
  case dict.get(state.streams, stream_id) {
    Ok(events) -> {
      let filtered =
        events
        |> list.filter(fn(e) { e.stream_version >= start_version })
        |> list.take(batch_size)
      process.send(reply, Ok(filtered))
    }
    Error(_) -> process.send(reply, Error(StreamNotFound))
  }
  actor.continue(state)
}

// --- Read All ---

fn handle_read_all(
  state: StoreState(event),
  start_number: Int,
  reply: Subject(Result(List(RecordedEvent(event)), EventStoreError)),
) -> actor.Next(StoreState(event), Message(event)) {
  let filtered =
    list.filter(state.all_events, fn(e) { e.event_number >= start_number })
  process.send(reply, Ok(filtered))
  actor.continue(state)
}

// --- Transient Subscriptions ---

/// Lazy cleanup of dead transient subscribers (Fix 6).
/// Filters out subscribers whose owner process is no longer alive.
/// Subscribers without an owner_pid are kept (no cleanup possible).
fn filter_live_subscribers(
  subs: List(TransientSub(event)),
) -> List(TransientSub(event)) {
  list.filter(subs, fn(sub) {
    case sub.owner_pid {
      None -> True
      Some(pid) -> process.is_alive(pid)
    }
  })
}

fn handle_subscribe_all(
  state: StoreState(event),
  handler: fn(RecordedEvent(event)) -> Nil,
  owner_pid: Option(process.Pid),
  reply: Subject(Result(Subscription, EventStoreError)),
) -> actor.Next(StoreState(event), Message(event)) {
  let sub_id = "sub-" <> int.to_string(state.next_sub_id)
  let sub = TransientSub(id: sub_id, handler: handler, owner_pid: owner_pid)
  let new_state =
    StoreState(
      ..state,
      all_subscribers: [sub, ..state.all_subscribers],
      next_sub_id: state.next_sub_id + 1,
    )
  process.send(reply, Ok(Subscription(id: sub_id)))
  actor.continue(new_state)
}

fn handle_subscribe_stream(
  state: StoreState(event),
  stream_id: String,
  handler: fn(RecordedEvent(event)) -> Nil,
  owner_pid: Option(process.Pid),
  reply: Subject(Result(Subscription, EventStoreError)),
) -> actor.Next(StoreState(event), Message(event)) {
  let sub_id = "sub-" <> int.to_string(state.next_sub_id)
  let sub = TransientSub(id: sub_id, handler: handler, owner_pid: owner_pid)
  let existing = case dict.get(state.stream_subscribers, stream_id) {
    Ok(s) -> s
    Error(_) -> []
  }
  let subs = [sub, ..existing]
  let new_state =
    StoreState(
      ..state,
      stream_subscribers: dict.insert(
        state.stream_subscribers,
        stream_id,
        subs,
      ),
      next_sub_id: state.next_sub_id + 1,
    )
  process.send(reply, Ok(Subscription(id: sub_id)))
  actor.continue(new_state)
}

// --- Persistent Subscriptions ---

fn handle_subscribe_persistent(
  state: StoreState(event),
  stream: String,
  name: String,
  start_from: StartFrom,
  handler: fn(RecordedEvent(event)) -> Nil,
  reply: Subject(Result(Subscription, EventStoreError)),
) -> actor.Next(StoreState(event), Message(event)) {
  let key = stream <> ":" <> name
  case dict.get(state.persistent_subs, key) {
    Ok(existing) -> {
      // Idempotent reconnect: update the handler callback but preserve
      // the checkpoint position. This allows handlers/PMs to restart
      // and re-attach without losing their position (Fix 3).
      let reconnected =
        PersistentSub(
          ..existing,
          handler: handler,
        )
      // Deliver any pending events with the new handler
      let reconnected = maybe_deliver_next(reconnected)
      let new_state =
        StoreState(
          ..state,
          persistent_subs: dict.insert(state.persistent_subs, key, reconnected),
        )
      process.send(reply, Ok(Subscription(id: key)))
      actor.continue(new_state)
    }
    Error(_) -> {
      let checkpoint = case start_from {
        Origin -> 0
        Current ->
          case state.all_events {
            [] -> 0
            _ ->
              list.fold(state.all_events, 0, fn(acc, e) {
                case e.event_number > acc {
                  True -> e.event_number
                  False -> acc
                }
              })
          }
        FromEventNumber(n) -> n - 1
      }

      // Gather historical events to replay
      let historical_events = case start_from {
        Current -> []
        _ -> {
          let events = case stream == "$all" {
            True -> state.all_events
            False ->
              case dict.get(state.streams, stream) {
                Ok(evts) -> evts
                Error(_) -> []
              }
          }
          list.filter(events, fn(evt) { evt.event_number > checkpoint })
        }
      }

      let psub =
        PersistentSub(
          name: name,
          stream: stream,
          handler: handler,
          checkpoint: checkpoint,
          in_flight: None,
          pending: historical_events,
        )

      // Try to deliver the first event
      let psub = maybe_deliver_next(psub)

      let new_state =
        StoreState(
          ..state,
          persistent_subs: dict.insert(state.persistent_subs, key, psub),
        )
      process.send(reply, Ok(Subscription(id: key)))
      actor.continue(new_state)
    }
  }
}

// --- Ack Event ---

fn handle_ack_event(
  state: StoreState(event),
  sub: Subscription,
  event: RecordedEvent(event),
  reply: Subject(Result(Nil, EventStoreError)),
) -> actor.Next(StoreState(event), Message(event)) {
  case dict.get(state.persistent_subs, sub.id) {
    Ok(psub) -> {
      // Update checkpoint to the acknowledged event's event_number
      let new_checkpoint = event.event_number
      let updated =
        PersistentSub(
          ..psub,
          checkpoint: new_checkpoint,
          in_flight: None,
        )
      // Try to deliver next pending event
      let updated = maybe_deliver_next(updated)
      let new_state =
        StoreState(
          ..state,
          persistent_subs: dict.insert(
            state.persistent_subs,
            sub.id,
            updated,
          ),
        )
      process.send(reply, Ok(Nil))
      actor.continue(new_state)
    }
    Error(_) -> {
      process.send(reply, Error(SubscriptionNotFound))
      actor.continue(state)
    }
  }
}

// --- Unsubscribe ---

fn handle_unsubscribe(
  state: StoreState(event),
  sub: Subscription,
  reply: Subject(Result(Nil, EventStoreError)),
) -> actor.Next(StoreState(event), Message(event)) {
  // Remove from transient subscribers
  let new_all_subs =
    list.filter(state.all_subscribers, fn(s) { s.id != sub.id })
  let new_stream_subs =
    dict.map_values(state.stream_subscribers, fn(_k, subs) {
      list.filter(subs, fn(s) { s.id != sub.id })
    })
  // For persistent subscriptions: pause delivery but keep position
  // We remove the subscriber reference but keep the subscription with its checkpoint
  let new_persistent =
    dict.map_values(state.persistent_subs, fn(k, psub) {
      case k == sub.id {
        True ->
          // Reset in-flight back to pending if there was one
          case psub.in_flight {
            Some(evt) ->
              PersistentSub(
                ..psub,
                in_flight: None,
                pending: [evt, ..psub.pending],
              )
            None -> psub
          }
        False -> psub
      }
    })
  let new_state =
    StoreState(
      ..state,
      all_subscribers: new_all_subs,
      stream_subscribers: new_stream_subs,
      persistent_subs: new_persistent,
    )
  process.send(reply, Ok(Nil))
  actor.continue(new_state)
}

// --- Delete Subscription ---

fn handle_delete_subscription(
  state: StoreState(event),
  stream: String,
  name: String,
  reply: Subject(Result(Nil, EventStoreError)),
) -> actor.Next(StoreState(event), Message(event)) {
  let key = stream <> ":" <> name
  case dict.get(state.persistent_subs, key) {
    Ok(_) -> {
      let new_state =
        StoreState(
          ..state,
          persistent_subs: dict.delete(state.persistent_subs, key),
        )
      process.send(reply, Ok(Nil))
      actor.continue(new_state)
    }
    Error(_) -> {
      process.send(reply, Error(SubscriptionNotFound))
      actor.continue(state)
    }
  }
}

// --- Snapshots ---

fn handle_read_snapshot(
  state: StoreState(event),
  source_uuid: String,
  reply: Subject(Result(snapshot.SnapshotData(event), EventStoreError)),
) -> actor.Next(StoreState(event), Message(event)) {
  case dict.get(state.snapshots, source_uuid) {
    Ok(snap) -> process.send(reply, Ok(snap))
    Error(_) -> process.send(reply, Error(SnapshotNotFound))
  }
  actor.continue(state)
}

fn handle_record_snapshot(
  state: StoreState(event),
  snap: snapshot.SnapshotData(event),
  reply: Subject(Result(Nil, EventStoreError)),
) -> actor.Next(StoreState(event), Message(event)) {
  let new_state =
    StoreState(
      ..state,
      snapshots: dict.insert(state.snapshots, snap.source_uuid, snap),
    )
  process.send(reply, Ok(Nil))
  actor.continue(new_state)
}

fn handle_delete_snapshot(
  state: StoreState(event),
  source_uuid: String,
  reply: Subject(Result(Nil, EventStoreError)),
) -> actor.Next(StoreState(event), Message(event)) {
  let new_state =
    StoreState(
      ..state,
      snapshots: dict.delete(state.snapshots, source_uuid),
    )
  process.send(reply, Ok(Nil))
  actor.continue(new_state)
}

// --- Get Latest Event Number ---

fn handle_get_latest_event_number(
  state: StoreState(event),
  reply: Subject(Result(Option(Int), EventStoreError)),
) -> actor.Next(StoreState(event), Message(event)) {
  case state.all_events {
    [] -> process.send(reply, Ok(None))
    _ -> {
      let max =
        list.fold(state.all_events, 0, fn(acc, e) {
          case e.event_number > acc {
            True -> e.event_number
            False -> acc
          }
        })
      process.send(reply, Ok(Some(max)))
    }
  }
  actor.continue(state)
}

// --- Helpers ---

fn create_recorded_events(
  events: List(EventData(event)),
  stream_id: String,
  start_number: Int,
  start_version: Int,
) -> #(List(RecordedEvent(event)), Int, Int) {
  let result =
    list.fold(events, #([], start_number, start_version), fn(acc, evt) {
      let #(recorded_list, num, ver) = acc
      let new_ver = ver + 1
      let recorded =
        RecordedEvent(
          event_id: uuid.v4_string(),
          event_number: num,
          stream_id: stream_id,
          stream_version: new_ver,
          event_type: evt.event_type,
          causation_id: evt.causation_id,
          correlation_id: evt.correlation_id,
          data: evt.data,
          metadata: evt.metadata,
          created_at: now_ms(),
        )
      #(list.append(recorded_list, [recorded]), num + 1, new_ver)
    })
  result
}

@external(erlang, "os", "system_time")
fn system_time_native() -> Int

fn now_ms() -> Int {
  system_time_native() / 1_000_000
}

/// Start the in-memory event store actor.
pub fn start() -> Result(Subject(Message(event)), actor.StartError) {
  actor.new(initial_state())
  |> actor.on_message(handle_message)
  |> actor.start
  |> result_map_started
}

fn result_map_started(
  result: Result(actor.Started(Subject(Message(event))), actor.StartError),
) -> Result(Subject(Message(event)), actor.StartError) {
  case result {
    Ok(started) -> Ok(started.data)
    Error(e) -> Error(e)
  }
}

/// Create an EventStore interface from a running in-memory event store actor.
pub fn to_event_store(
  subject: Subject(Message(event)),
) -> EventStore(event) {
  let call_timeout = 5000

  EventStore(
    append_to_stream: fn(stream_id, expected_version, events) {
      process.call(subject, call_timeout, fn(reply) {
        Append(stream_id, expected_version, events, reply)
      })
    },
    read_stream_forward: fn(stream_id, start_version, batch_size) {
      process.call(subject, call_timeout, fn(reply) {
        ReadStream(stream_id, start_version, batch_size, reply)
      })
    },
    subscribe: fn(handler) {
      let caller_pid = process.self()
      process.call(subject, call_timeout, fn(reply) {
        SubscribeAll(handler, Some(caller_pid), reply)
      })
    },
    subscribe_to_stream: fn(stream_id, handler) {
      let caller_pid = process.self()
      process.call(subject, call_timeout, fn(reply) {
        SubscribeStream(stream_id, handler, Some(caller_pid), reply)
      })
    },
    subscribe_persistent: fn(stream, name, start_from, handler) {
      process.call(subject, call_timeout, fn(reply) {
        SubscribePersistent(stream, name, start_from, handler, reply)
      })
    },
    ack_event: fn(sub, event) {
      process.call(subject, call_timeout, fn(reply) {
        AckEvent(sub, event, reply)
      })
    },
    unsubscribe: fn(sub) {
      process.call(subject, call_timeout, fn(reply) {
        Unsubscribe(sub, reply)
      })
    },
    delete_subscription: fn(stream, name) {
      process.call(subject, call_timeout, fn(reply) {
        DeleteSubscription(stream, name, reply)
      })
    },
    read_snapshot: fn(source_uuid) {
      process.call(subject, call_timeout, fn(reply) {
        ReadSnapshot(source_uuid, reply)
      })
    },
    record_snapshot: fn(snap) {
      process.call(subject, call_timeout, fn(reply) {
        RecordSnapshot(snap, reply)
      })
    },
    delete_snapshot: fn(source_uuid) {
      process.call(subject, call_timeout, fn(reply) {
        DeleteSnapshot(source_uuid, reply)
      })
    },
    reset: fn() {
      process.call(subject, call_timeout, fn(reply) { Reset(reply) })
    },
    read_all_forward: fn(start_number) {
      process.call(subject, call_timeout, fn(reply) {
        ReadAll(start_number, reply)
      })
    },
    get_latest_event_number: fn() {
      process.call(subject, call_timeout, fn(reply) {
        GetLatestEventNumber(reply)
      })
    },
  )
}
