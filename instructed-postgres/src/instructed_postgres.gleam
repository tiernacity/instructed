//// Instructed PostgreSQL Event Store Adapter
////
//// Provides a PostgreSQL-backed event store for the Instructed CQRS/ES framework.
//// Events are stored in a PostgreSQL database with proper schemas for:
//// - Event streams and recorded events
//// - Persistent subscriptions with position tracking
//// - Aggregate state snapshots
////
//// ## Setup
////
//// 1. Create the required database tables using `create_schema/1`
//// 2. Create the event store with `new/2`, providing serialization functions
//// 3. Use the returned `EventStore` with your Instructed application
////
//// ## Example
////
//// ```gleam
//// import instructed_postgres
//// import pog
////
//// let db = pog.default_config() |> pog.connect()
//// let assert Ok(Nil) = instructed_postgres.create_schema(db)
//// let store = instructed_postgres.new(
////   db: db,
////   serialize: fn(event) { json.to_string(encode_event(event)) },
////   deserialize: fn(json_str) { decode_event(json_str) },
////   event_type: fn(event) { event_type_name(event) },
//// )
//// ```

import gleam/dict
import gleam/dynamic/decode
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor

import gleam/string
import instructed/error.{
  type EventStoreError, SnapshotNotFound, StorageError, StreamNotFound,
  SubscriptionAlreadyExists, SubscriptionNotFound, VersionConflict,
}
import instructed/event.{type EventData, type RecordedEvent,
  RecordedEvent}
import instructed/event_store.{
  type EventStore, type ExpectedVersion, type StartFrom, type Subscription,
  AnyVersion, Current, EventStore, ExactVersion, FromEventNumber, NoStream,
  Origin, StreamExists, Subscription,
}
import instructed/snapshot.{type SnapshotData, SnapshotData}
import pog
import youid/uuid

/// Configuration for the PostgreSQL event store.
pub type PgConfig(event) {
  PgConfig(
    /// Database connection
    db: pog.Connection,
    /// Serialize a domain event to JSON string
    serialize: fn(event) -> String,
    /// Deserialize a JSON string back to a domain event
    deserialize: fn(String) -> Result(event, String),
    /// Get the event type name for a domain event
    event_type: fn(event) -> String,
  )
}

/// Create the required database schema for the event store.
pub fn create_schema(db: pog.Connection) -> Result(Nil, String) {
  let statements = [
    "CREATE TABLE IF NOT EXISTS event_store_events (
      event_id TEXT PRIMARY KEY,
      event_number BIGSERIAL UNIQUE NOT NULL,
      stream_id TEXT NOT NULL,
      stream_version INTEGER NOT NULL,
      event_type TEXT NOT NULL,
      data TEXT NOT NULL,
      metadata TEXT NOT NULL DEFAULT '{}',
      causation_id TEXT,
      correlation_id TEXT,
      created_at BIGINT NOT NULL,
      UNIQUE(stream_id, stream_version)
    )",
    "CREATE INDEX IF NOT EXISTS idx_events_stream_id ON event_store_events(stream_id)",
    "CREATE INDEX IF NOT EXISTS idx_events_event_number ON event_store_events(event_number)",
    "CREATE INDEX IF NOT EXISTS idx_events_stream_version ON event_store_events(stream_id, stream_version)",
    "CREATE TABLE IF NOT EXISTS event_store_snapshots (
      source_uuid TEXT PRIMARY KEY,
      source_version INTEGER NOT NULL,
      source_type TEXT NOT NULL,
      data TEXT NOT NULL,
      created_at BIGINT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS event_store_subscriptions (
      subscription_id TEXT PRIMARY KEY,
      stream_id TEXT NOT NULL,
      subscription_name TEXT NOT NULL,
      last_seen_event_number BIGINT NOT NULL DEFAULT 0,
      created_at BIGINT NOT NULL,
      UNIQUE(stream_id, subscription_name)
    )",
  ]

  execute_statements(db, statements)
}

/// Drop all event store tables (for testing).
pub fn drop_schema(db: pog.Connection) -> Result(Nil, String) {
  let statements = [
    "DROP TABLE IF EXISTS event_store_events CASCADE",
    "DROP TABLE IF EXISTS event_store_snapshots CASCADE",
    "DROP TABLE IF EXISTS event_store_subscriptions CASCADE",
  ]

  execute_statements(db, statements)
}

fn execute_statements(
  db: pog.Connection,
  statements: List(String),
) -> Result(Nil, String) {
  list.fold(statements, Ok(Nil), fn(acc, sql) {
    case acc {
      Error(e) -> Error(e)
      Ok(Nil) ->
        case pog.query(sql) |> pog.execute(db) {
          Ok(_) -> Ok(Nil)
          Error(e) ->
            Error("Failed to execute: " <> string.inspect(e))
        }
    }
  })
}

/// Create a new PostgreSQL-backed EventStore.
pub fn new(config: PgConfig(event)) -> EventStore(event) {
  // Start a subscription notifier actor
  let assert Ok(notifier) = start_notifier()

  EventStore(
    append_to_stream: fn(stream_id, expected_version, events) {
      append_to_stream(config, stream_id, expected_version, events, notifier)
    },
    read_stream_forward: fn(stream_id, start_version, count) {
      read_stream_forward(config, stream_id, start_version, count)
    },
    subscribe: fn(handler) {
      subscribe_transient(notifier, "$all", handler)
    },
    subscribe_to_stream: fn(stream_id, handler) {
      subscribe_transient(notifier, stream_id, handler)
    },
    subscribe_persistent: fn(stream, name, start_from, handler) {
      subscribe_persistent(config, notifier, stream, name, start_from, handler)
    },
    ack_event: fn(sub, event) {
      ack_event(config, sub, event)
    },
    unsubscribe: fn(sub) {
      unsubscribe(notifier, sub)
    },
    delete_subscription: fn(stream, name) {
      delete_subscription(config, stream, name)
    },
    read_snapshot: fn(source_uuid) {
      read_snapshot(config, source_uuid)
    },
    record_snapshot: fn(snap) {
      record_snapshot(config, snap)
    },
    delete_snapshot: fn(source_uuid) {
      delete_snapshot(config, source_uuid)
    },
    reset: fn() { reset(config) },
    read_all_forward: fn(start_number) {
      read_all_forward(config, start_number)
    },
    get_latest_event_number: fn() {
      get_latest_event_number(config)
    },
  )
}

// --- Append ---

fn append_to_stream(
  config: PgConfig(event),
  stream_id: String,
  expected_version: ExpectedVersion,
  events: List(EventData(event)),
  notifier: Subject(NotifierMessage(event)),
) -> Result(Int, EventStoreError) {
  // Check expected version
  let current_version = get_stream_version(config, stream_id)

  let version_ok = case expected_version {
    AnyVersion -> True
    NoStream -> current_version == 0
    StreamExists -> current_version > 0
    ExactVersion(v) -> current_version == v
  }

  case version_ok {
    False -> Error(VersionConflict)
    True -> {
      let result =
        list.fold(events, Ok(current_version), fn(acc, evt: EventData(event)) {
          case acc {
            Error(e) -> Error(e)
            Ok(ver) -> {
              let new_ver = ver + 1
              let event_id = uuid.v4_string()
              let now = now_ms()
              let data_json = config.serialize(evt.data)
              let metadata_json = serialize_metadata(evt.metadata)
              let causation = option.unwrap(evt.causation_id, "")
              let correlation = option.unwrap(evt.correlation_id, "")
              let event_type = config.event_type(evt.data)

              let sql =
                "INSERT INTO event_store_events (event_id, stream_id, stream_version, event_type, data, metadata, causation_id, correlation_id, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"

              case
                pog.query(sql)
                |> pog.parameter(pog.text(event_id))
                |> pog.parameter(pog.text(stream_id))
                |> pog.parameter(pog.int(new_ver))
                |> pog.parameter(pog.text(event_type))
                |> pog.parameter(pog.text(data_json))
                |> pog.parameter(pog.text(metadata_json))
                |> pog.parameter(pog.text(causation))
                |> pog.parameter(pog.text(correlation))
                |> pog.parameter(pog.int(now))
                |> pog.execute(config.db)
              {
                Ok(_) -> Ok(new_ver)
                Error(e) -> Error(StorageError(string.inspect(e)))
              }
            }
          }
        })

      case result {
        Ok(final_version) -> {
          // Notify subscribers about new events
          let _ = case read_stream_forward(config, stream_id, current_version + 1, 100_000) {
            Ok(new_events) -> {
              list.each(new_events, fn(evt) {
                process.send(notifier, NotifyEvent(stream_id, evt))
              })
              Nil
            }
            Error(_) -> Nil
          }
          Ok(final_version)
        }
        Error(e) -> Error(e)
      }
    }
  }
}

fn get_stream_version(config: PgConfig(event), stream_id: String) -> Int {
  let sql =
    "SELECT COALESCE(MAX(stream_version), 0) FROM event_store_events WHERE stream_id = $1"

  let row_decoder = decode.at([0], decode.int)

  case
    pog.query(sql)
    |> pog.parameter(pog.text(stream_id))
    |> pog.returning(row_decoder)
    |> pog.execute(config.db)
  {
    Ok(response) ->
      case response.rows {
        [version] -> version
        _ -> 0
      }
    Error(_) -> 0
  }
}

// --- Read ---

fn read_stream_forward(
  config: PgConfig(event),
  stream_id: String,
  start_version: Int,
  count: Int,
) -> Result(List(RecordedEvent(event)), EventStoreError) {
  let sql =
    "SELECT event_id, event_number, stream_id, stream_version, event_type, causation_id, correlation_id, data, metadata, created_at FROM event_store_events WHERE stream_id = $1 AND stream_version >= $2 ORDER BY stream_version ASC LIMIT $3"

  let row_decoder =
    decode.then(decode.at([0], decode.string), fn(event_id) {
      decode.then(decode.at([1], decode.int), fn(event_number) {
        decode.then(decode.at([2], decode.string), fn(sid) {
          decode.then(decode.at([3], decode.int), fn(stream_version) {
            decode.then(decode.at([4], decode.string), fn(event_type) {
              decode.then(decode.at([5], decode.string), fn(causation_id) {
                decode.then(decode.at([6], decode.string), fn(correlation_id) {
                  decode.then(decode.at([7], decode.string), fn(data_json) {
                    decode.then(decode.at([8], decode.string), fn(metadata_json) {
                      decode.then(decode.at([9], decode.int), fn(created_at) {
                        decode.success(#(
                          event_id,
                          event_number,
                          sid,
                          stream_version,
                          event_type,
                          causation_id,
                          correlation_id,
                          data_json,
                          metadata_json,
                          created_at,
                        ))
                      })
                    })
                  })
                })
              })
            })
          })
        })
      })
    })

  case
    pog.query(sql)
    |> pog.parameter(pog.text(stream_id))
    |> pog.parameter(pog.int(start_version))
    |> pog.parameter(pog.int(count))
    |> pog.returning(row_decoder)
    |> pog.execute(config.db)
  {
    Ok(response) -> {
      case response.rows {
        [] -> Error(StreamNotFound)
        rows -> {
          let events =
            list.filter_map(rows, fn(row) {
              let #(
                event_id,
                event_number,
                sid,
                stream_version,
                event_type,
                causation_id_str,
                correlation_id_str,
                data_json,
                metadata_json,
                created_at,
              ) = row
              case config.deserialize(data_json) {
                Ok(event_data) ->
                  Ok(RecordedEvent(
                    event_id: event_id,
                    event_number: event_number,
                    stream_id: sid,
                    stream_version: stream_version,
                    event_type: event_type,
                    causation_id: nullable_to_option(causation_id_str),
                    correlation_id: nullable_to_option(correlation_id_str),
                    data: event_data,
                    metadata: json_to_metadata(metadata_json),
                    created_at: created_at,
                  ))
                Error(_) -> Error(Nil)
              }
            })
          case events {
            [] -> Error(StreamNotFound)
            _ -> Ok(events)
          }
        }
      }
    }
    Error(e) -> Error(StorageError(string.inspect(e)))
  }
}

fn read_all_forward(
  config: PgConfig(event),
  start_number: Int,
) -> Result(List(RecordedEvent(event)), EventStoreError) {
  let sql =
    "SELECT event_id, event_number, stream_id, stream_version, event_type, causation_id, correlation_id, data, metadata, created_at FROM event_store_events WHERE event_number >= $1 ORDER BY event_number ASC"

  let row_decoder =
    decode.then(decode.at([0], decode.string), fn(event_id) {
      decode.then(decode.at([1], decode.int), fn(event_number) {
        decode.then(decode.at([2], decode.string), fn(sid) {
          decode.then(decode.at([3], decode.int), fn(stream_version) {
            decode.then(decode.at([4], decode.string), fn(event_type) {
              decode.then(decode.at([5], decode.string), fn(causation_id) {
                decode.then(decode.at([6], decode.string), fn(correlation_id) {
                  decode.then(decode.at([7], decode.string), fn(data_json) {
                    decode.then(decode.at([8], decode.string), fn(metadata_json) {
                      decode.then(decode.at([9], decode.int), fn(created_at) {
                        decode.success(#(
                          event_id,
                          event_number,
                          sid,
                          stream_version,
                          event_type,
                          causation_id,
                          correlation_id,
                          data_json,
                          metadata_json,
                          created_at,
                        ))
                      })
                    })
                  })
                })
              })
            })
          })
        })
      })
    })

  case
    pog.query(sql)
    |> pog.parameter(pog.int(start_number))
    |> pog.returning(row_decoder)
    |> pog.execute(config.db)
  {
    Ok(response) -> {
      let events =
        list.filter_map(response.rows, fn(row) {
          let #(
            event_id,
            event_number,
            sid,
            stream_version,
            event_type,
            causation_id_str,
            correlation_id_str,
            data_json,
            metadata_json,
            created_at,
          ) = row
          case config.deserialize(data_json) {
            Ok(event_data) ->
              Ok(RecordedEvent(
                event_id: event_id,
                event_number: event_number,
                stream_id: sid,
                stream_version: stream_version,
                event_type: event_type,
                causation_id: nullable_to_option(causation_id_str),
                correlation_id: nullable_to_option(correlation_id_str),
                data: event_data,
                metadata: json_to_metadata(metadata_json),
                created_at: created_at,
              ))
            Error(_) -> Error(Nil)
          }
        })
      Ok(events)
    }
    Error(e) -> Error(StorageError(string.inspect(e)))
  }
}

// --- Subscriptions ---

/// Notifier actor for managing subscription callbacks
type NotifierState(event) {
  NotifierState(
    transient_subs: List(TransientSub(event)),
    next_id: Int,
  )
}

type TransientSub(event) {
  TransientSub(
    id: String,
    stream: String,
    handler: fn(RecordedEvent(event)) -> Nil,
  )
}

pub opaque type NotifierMessage(event) {
  NotifyEvent(stream_id: String, event: RecordedEvent(event))
  AddTransientSub(
    stream: String,
    handler: fn(RecordedEvent(event)) -> Nil,
    reply: Subject(Subscription),
  )
  RemoveSub(sub_id: String, reply: Subject(Nil))
}

fn start_notifier() -> Result(Subject(NotifierMessage(event)), actor.StartError) {
  let state = NotifierState(transient_subs: [], next_id: 1)
  case
    actor.new(state)
    |> actor.on_message(handle_notifier)
    |> actor.start
  {
    Ok(started) -> Ok(started.data)
    Error(e) -> Error(e)
  }
}

fn handle_notifier(
  state: NotifierState(event),
  msg: NotifierMessage(event),
) -> actor.Next(NotifierState(event), NotifierMessage(event)) {
  case msg {
    NotifyEvent(stream_id, event) -> {
      list.each(state.transient_subs, fn(sub) {
        case sub.stream == "$all" || sub.stream == stream_id {
          True -> sub.handler(event)
          False -> Nil
        }
      })
      actor.continue(state)
    }

    AddTransientSub(stream, handler, reply) -> {
      let sub_id = "tsub-" <> int.to_string(state.next_id)
      let sub = TransientSub(id: sub_id, stream: stream, handler: handler)
      process.send(reply, Subscription(id: sub_id))
      actor.continue(NotifierState(
        transient_subs: [sub, ..state.transient_subs],
        next_id: state.next_id + 1,
      ))
    }

    RemoveSub(sub_id, reply) -> {
      let new_subs =
        list.filter(state.transient_subs, fn(s) { s.id != sub_id })
      process.send(reply, Nil)
      actor.continue(NotifierState(..state, transient_subs: new_subs))
    }
  }
}

fn subscribe_transient(
  notifier: Subject(NotifierMessage(event)),
  stream: String,
  handler: fn(RecordedEvent(event)) -> Nil,
) -> Result(Subscription, EventStoreError) {
  let sub =
    process.call(notifier, 5000, fn(reply) {
      AddTransientSub(stream, handler, reply)
    })
  Ok(sub)
}

fn subscribe_persistent(
  config: PgConfig(event),
  notifier: Subject(NotifierMessage(event)),
  stream: String,
  name: String,
  start_from: StartFrom,
  handler: fn(RecordedEvent(event)) -> Nil,
) -> Result(Subscription, EventStoreError) {
  let sub_id = stream <> ":" <> name
  let now = now_ms()

  // Check if subscription already exists
  let check_sql =
    "SELECT subscription_id FROM event_store_subscriptions WHERE stream_id = $1 AND subscription_name = $2"

  let check_decoder = decode.at([0], decode.string)

  case
    pog.query(check_sql)
    |> pog.parameter(pog.text(stream))
    |> pog.parameter(pog.text(name))
    |> pog.returning(check_decoder)
    |> pog.execute(config.db)
  {
    Ok(response) -> {
      case response.rows {
        [_] -> Error(SubscriptionAlreadyExists)
        _ -> {
          let last_seen = case start_from {
            Origin -> 0
            Current -> {
              case get_latest_event_number(config) {
                Ok(Some(n)) -> n
                _ -> 0
              }
            }
            FromEventNumber(n) -> n - 1
          }

          let insert_sql =
            "INSERT INTO event_store_subscriptions (subscription_id, stream_id, subscription_name, last_seen_event_number, created_at) VALUES ($1, $2, $3, $4, $5)"

          case
            pog.query(insert_sql)
            |> pog.parameter(pog.text(sub_id))
            |> pog.parameter(pog.text(stream))
            |> pog.parameter(pog.text(name))
            |> pog.parameter(pog.int(last_seen))
            |> pog.parameter(pog.int(now))
            |> pog.execute(config.db)
          {
            Ok(_) -> {
              // Send historical events if starting from origin
              case start_from {
                Current -> Nil
                _ -> {
                  let events = case stream == "$all" {
                    True -> read_all_forward(config, last_seen + 1)
                    False -> read_stream_forward(config, stream, 1, 100_000)
                  }
                  case events {
                    Ok(evts) ->
                      list.each(evts, fn(evt) {
                        case evt.event_number > last_seen {
                          True -> handler(evt)
                          False -> Nil
                        }
                      })
                    Error(_) -> Nil
                  }
                }
              }

              // Register for new events
              let _ = subscribe_transient(notifier, stream, handler)

              Ok(Subscription(id: sub_id))
            }
            Error(e) -> Error(StorageError(string.inspect(e)))
          }
        }
      }
    }
    Error(e) -> Error(StorageError(string.inspect(e)))
  }
}

fn ack_event(
  config: PgConfig(event),
  sub: Subscription,
  event: RecordedEvent(event),
) -> Result(Nil, EventStoreError) {
  let sql =
    "UPDATE event_store_subscriptions SET last_seen_event_number = $1 WHERE subscription_id = $2"

  case
    pog.query(sql)
    |> pog.parameter(pog.int(event.event_number))
    |> pog.parameter(pog.text(sub.id))
    |> pog.execute(config.db)
  {
    Ok(_) -> Ok(Nil)
    Error(e) -> Error(StorageError(string.inspect(e)))
  }
}

fn unsubscribe(
  notifier: Subject(NotifierMessage(event)),
  sub: Subscription,
) -> Result(Nil, EventStoreError) {
  process.call(notifier, 5000, fn(reply) { RemoveSub(sub.id, reply) })
  Ok(Nil)
}

fn delete_subscription(
  config: PgConfig(event),
  stream: String,
  name: String,
) -> Result(Nil, EventStoreError) {
  let sql =
    "DELETE FROM event_store_subscriptions WHERE stream_id = $1 AND subscription_name = $2"

  case
    pog.query(sql)
    |> pog.parameter(pog.text(stream))
    |> pog.parameter(pog.text(name))
    |> pog.execute(config.db)
  {
    Ok(response) -> {
      case response.count > 0 {
        True -> Ok(Nil)
        False -> Error(SubscriptionNotFound)
      }
    }
    Error(e) -> Error(StorageError(string.inspect(e)))
  }
}

// --- Snapshots ---

fn read_snapshot(
  config: PgConfig(event),
  source_uuid: String,
) -> Result(SnapshotData(event), EventStoreError) {
  let sql =
    "SELECT source_uuid, source_version, source_type, data, created_at FROM event_store_snapshots WHERE source_uuid = $1"

  let row_decoder =
    decode.then(decode.at([0], decode.string), fn(suuid) {
      decode.then(decode.at([1], decode.int), fn(sver) {
        decode.then(decode.at([2], decode.string), fn(stype) {
          decode.then(decode.at([3], decode.string), fn(data_json) {
            decode.then(decode.at([4], decode.int), fn(created_at) {
              decode.success(#(suuid, sver, stype, data_json, created_at))
            })
          })
        })
      })
    })

  case
    pog.query(sql)
    |> pog.parameter(pog.text(source_uuid))
    |> pog.returning(row_decoder)
    |> pog.execute(config.db)
  {
    Ok(response) -> {
      case response.rows {
        [#(suuid, sver, stype, data_json, created_at)] -> {
          case config.deserialize(data_json) {
            Ok(data) ->
              Ok(SnapshotData(
                source_uuid: suuid,
                source_version: sver,
                source_type: stype,
                data: data,
                created_at: created_at,
              ))
            Error(_) -> Error(SnapshotNotFound)
          }
        }
        _ -> Error(SnapshotNotFound)
      }
    }
    Error(e) -> Error(StorageError(string.inspect(e)))
  }
}

fn record_snapshot(
  config: PgConfig(event),
  snap: SnapshotData(event),
) -> Result(Nil, EventStoreError) {
  let sql =
    "INSERT INTO event_store_snapshots (source_uuid, source_version, source_type, data, created_at) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (source_uuid) DO UPDATE SET source_version = $2, source_type = $3, data = $4, created_at = $5"

  let data_json = config.serialize(snap.data)

  case
    pog.query(sql)
    |> pog.parameter(pog.text(snap.source_uuid))
    |> pog.parameter(pog.int(snap.source_version))
    |> pog.parameter(pog.text(snap.source_type))
    |> pog.parameter(pog.text(data_json))
    |> pog.parameter(pog.int(snap.created_at))
    |> pog.execute(config.db)
  {
    Ok(_) -> Ok(Nil)
    Error(e) -> Error(StorageError(string.inspect(e)))
  }
}

fn delete_snapshot(
  config: PgConfig(event),
  source_uuid: String,
) -> Result(Nil, EventStoreError) {
  let sql =
    "DELETE FROM event_store_snapshots WHERE source_uuid = $1"

  case
    pog.query(sql)
    |> pog.parameter(pog.text(source_uuid))
    |> pog.execute(config.db)
  {
    Ok(_) -> Ok(Nil)
    Error(e) -> Error(StorageError(string.inspect(e)))
  }
}

// --- Reset ---

fn reset(config: PgConfig(event)) -> Result(Nil, EventStoreError) {
  let statements = [
    "DELETE FROM event_store_subscriptions",
    "DELETE FROM event_store_snapshots",
    "DELETE FROM event_store_events",
  ]

  list.fold(statements, Ok(Nil), fn(acc, sql) {
    case acc {
      Error(e) -> Error(e)
      Ok(Nil) ->
        case pog.query(sql) |> pog.execute(config.db) {
          Ok(_) -> Ok(Nil)
          Error(e) -> Error(StorageError(string.inspect(e)))
        }
    }
  })
}

// --- Helpers ---

fn get_latest_event_number(
  config: PgConfig(event),
) -> Result(Option(Int), EventStoreError) {
  let sql = "SELECT MAX(event_number) FROM event_store_events"

  let row_decoder = decode.at([0], decode.optional(decode.int))

  case
    pog.query(sql)
    |> pog.returning(row_decoder)
    |> pog.execute(config.db)
  {
    Ok(response) -> {
      case response.rows {
        [Some(n)] -> Ok(Some(n))
        _ -> Ok(None)
      }
    }
    Error(e) -> Error(StorageError(string.inspect(e)))
  }
}

fn serialize_metadata(metadata: dict.Dict(String, String)) -> String {
  metadata
  |> dict.to_list
  |> list.map(fn(pair) {
    let #(k, v) = pair
    #(k, json.string(v))
  })
  |> json.object
  |> json.to_string
}

fn json_to_metadata(json_str: String) -> dict.Dict(String, String) {
  case json.parse(json_str, decode.dict(decode.string, decode.string)) {
    Ok(d) -> d
    Error(_) -> dict.new()
  }
}

/// Convert a nullable DB string (empty or SQL NULL decoded as "") to Option.
fn nullable_to_option(s: String) -> Option(String) {
  case s {
    "" -> None
    v -> Some(v)
  }
}

@external(erlang, "os", "system_time")
fn system_time_native() -> Int

fn now_ms() -> Int {
  system_time_native() / 1_000_000
}
