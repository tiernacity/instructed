//// In-memory event store implementation using an OTP Actor.
////
//// This event store stores all events in memory and is designed
//// primarily for testing purposes. Events are lost when the process stops.
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
  type EventStoreError, SnapshotNotFound, StreamNotFound,
  SubscriptionAlreadyExists, SubscriptionNotFound, VersionConflict,
}
import instructed/event.{type EventData, type RecordedEvent,
  RecordedEvent}
import instructed/event_store.{
  type EventStore, type ExpectedVersion, type StartFrom, type Subscription,
  AnyVersion, Current, EventStore, ExactVersion, FromEventNumber, NoStream,
  Origin, StreamExists, Subscription,
}
import instructed/snapshot.{type SnapshotData}
import youid/uuid

// --- Actor Message Types ---

type StoreState(event) {
  StoreState(
    /// All events stored globally, newest first
    all_events: List(RecordedEvent(event)),
    /// Events indexed by stream ID
    streams: Dict(String, List(RecordedEvent(event))),
    /// Transient subscribers (all streams)
    all_subscribers: List(TransientSub(event)),
    /// Transient subscribers (specific streams)
    stream_subscribers: Dict(String, List(TransientSub(event))),
    /// Persistent subscriptions
    persistent_subs: Dict(String, PersistentSub(event)),
    /// Snapshots by source UUID
    snapshots: Dict(String, SnapshotData(event)),
    /// Next global event number
    next_event_number: Int,
    /// Next subscriber ID counter
    next_sub_id: Int,
  )
}

type TransientSub(event) {
  TransientSub(id: String, handler: fn(RecordedEvent(event)) -> Nil)
}

type PersistentSub(event) {
  PersistentSub(
    name: String,
    stream: String,
    handler: fn(RecordedEvent(event)) -> Nil,
    last_seen: Int,
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
    reply: Subject(Result(List(RecordedEvent(event)), EventStoreError)),
  )
  ReadAll(
    start_number: Int,
    reply: Subject(Result(List(RecordedEvent(event)), EventStoreError)),
  )
  SubscribeAll(
    handler: fn(RecordedEvent(event)) -> Nil,
    reply: Subject(Result(Subscription, EventStoreError)),
  )
  SubscribeStream(
    stream_id: String,
    handler: fn(RecordedEvent(event)) -> Nil,
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
    reply: Subject(Result(SnapshotData(event), EventStoreError)),
  )
  RecordSnapshot(
    snapshot: SnapshotData(event),
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
    Append(stream_id, expected_version, events, reply) -> {
      let stream_events =
        dict.get(state.streams, stream_id) |> option_from_result
      let current_version = case stream_events {
        Some(evts) -> list.length(evts)
        None -> 0
      }

      let version_ok = case expected_version {
        AnyVersion -> True
        NoStream -> current_version == 0
        StreamExists -> current_version > 0
        ExactVersion(v) -> current_version == v
      }

      case version_ok {
        False -> {
          process.send(reply, Error(VersionConflict))
          actor.continue(state)
        }
        True -> {
          let existing = case stream_events {
            Some(evts) -> evts
            None -> []
          }

          let #(recorded, next_num, next_ver) =
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

          // Notify transient subscribers
          list.each(state.all_subscribers, fn(sub) {
            list.each(recorded, fn(evt) { sub.handler(evt) })
          })
          let stream_subs =
            dict.get(state.stream_subscribers, stream_id)
            |> option_from_result
          case stream_subs {
            Some(subs) ->
              list.each(subs, fn(sub) {
                list.each(recorded, fn(evt) { sub.handler(evt) })
              })
            None -> Nil
          }

          // Notify persistent subscribers
          let _ = dict.each(state.persistent_subs, fn(_key, psub) {
            case psub.stream == "$all" || psub.stream == stream_id {
              True ->
                list.each(recorded, fn(evt) {
                  case evt.event_number > psub.last_seen {
                    True -> psub.handler(evt)
                    False -> Nil
                  }
                })
              False -> Nil
            }
          })

          let _ = next_ver
          let new_state =
            StoreState(
              ..state,
              all_events: new_all,
              streams: new_streams,
              next_event_number: next_num,
            )
          process.send(reply, Ok(current_version + list.length(recorded)))
          actor.continue(new_state)
        }
      }
    }

    ReadStream(stream_id, start_version, reply) -> {
      case dict.get(state.streams, stream_id) {
        Ok(events) -> {
          let filtered =
            list.filter(events, fn(e) { e.stream_version >= start_version })
          process.send(reply, Ok(filtered))
        }
        Error(_) -> process.send(reply, Error(StreamNotFound))
      }
      actor.continue(state)
    }

    ReadAll(start_number, reply) -> {
      let filtered =
        list.filter(state.all_events, fn(e) {
          e.event_number >= start_number
        })
      process.send(reply, Ok(filtered))
      actor.continue(state)
    }

    SubscribeAll(handler, reply) -> {
      let sub_id = "sub-" <> int.to_string(state.next_sub_id)
      let sub = TransientSub(id: sub_id, handler: handler)
      let new_state =
        StoreState(
          ..state,
          all_subscribers: [sub, ..state.all_subscribers],
          next_sub_id: state.next_sub_id + 1,
        )
      process.send(reply, Ok(Subscription(id: sub_id)))
      actor.continue(new_state)
    }

    SubscribeStream(stream_id, handler, reply) -> {
      let sub_id = "sub-" <> int.to_string(state.next_sub_id)
      let sub = TransientSub(id: sub_id, handler: handler)
      let existing =
        dict.get(state.stream_subscribers, stream_id)
        |> option_from_result
      let subs = case existing {
        Some(s) -> [sub, ..s]
        None -> [sub]
      }
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

    SubscribePersistent(stream, name, start_from, handler, reply) -> {
      let key = stream <> ":" <> name
      case dict.get(state.persistent_subs, key) {
        Ok(_) -> {
          process.send(reply, Error(SubscriptionAlreadyExists))
          actor.continue(state)
        }
        Error(_) -> {
          let last_seen = case start_from {
            Origin -> 0
            Current -> case state.all_events {
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
          let psub =
            PersistentSub(
              name: name,
              stream: stream,
              handler: handler,
              last_seen: last_seen,
            )
          let new_state =
            StoreState(
              ..state,
              persistent_subs: dict.insert(state.persistent_subs, key, psub),
            )
          // Send historical events if starting from origin or specific number
          case start_from {
            Current -> Nil
            _ -> {
              let events = case stream == "$all" {
                True -> state.all_events
                False ->
                  dict.get(state.streams, stream)
                  |> option_from_result
                  |> option.unwrap([])
              }
              list.each(events, fn(evt) {
                case evt.event_number > last_seen {
                  True -> handler(evt)
                  False -> Nil
                }
              })
            }
          }
          process.send(reply, Ok(Subscription(id: key)))
          actor.continue(new_state)
        }
      }
    }

    AckEvent(sub, _event, reply) -> {
      // For in-memory, ack is essentially a no-op but we track position
      let _ = sub
      process.send(reply, Ok(Nil))
      actor.continue(state)
    }

    Unsubscribe(sub, reply) -> {
      let new_all_subs =
        list.filter(state.all_subscribers, fn(s) { s.id != sub.id })
      let new_stream_subs =
        dict.map_values(state.stream_subscribers, fn(_k, subs) {
          list.filter(subs, fn(s) { s.id != sub.id })
        })
      let new_persistent =
        dict.filter(state.persistent_subs, fn(k, _v) { k != sub.id })
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

    DeleteSubscription(stream, name, reply) -> {
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

    ReadSnapshot(source_uuid, reply) -> {
      case dict.get(state.snapshots, source_uuid) {
        Ok(snap) -> process.send(reply, Ok(snap))
        Error(_) -> process.send(reply, Error(SnapshotNotFound))
      }
      actor.continue(state)
    }

    RecordSnapshot(snap, reply) -> {
      let new_state =
        StoreState(
          ..state,
          snapshots: dict.insert(state.snapshots, snap.source_uuid, snap),
        )
      process.send(reply, Ok(Nil))
      actor.continue(new_state)
    }

    DeleteSnapshot(source_uuid, reply) -> {
      let new_state =
        StoreState(
          ..state,
          snapshots: dict.delete(state.snapshots, source_uuid),
        )
      process.send(reply, Ok(Nil))
      actor.continue(new_state)
    }

    Reset(reply) -> {
      process.send(reply, Ok(Nil))
      actor.continue(initial_state())
    }

    GetLatestEventNumber(reply) -> {
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
  }
}

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

fn option_from_result(result: Result(a, b)) -> Option(a) {
  case result {
    Ok(val) -> Some(val)
    Error(_) -> None
  }
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
    read_stream_forward: fn(stream_id, start_version) {
      process.call(subject, call_timeout, fn(reply) {
        ReadStream(stream_id, start_version, reply)
      })
    },
    subscribe: fn(handler) {
      process.call(subject, call_timeout, fn(reply) {
        SubscribeAll(handler, reply)
      })
    },
    subscribe_to_stream: fn(stream_id, handler) {
      process.call(subject, call_timeout, fn(reply) {
        SubscribeStream(stream_id, handler, reply)
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
