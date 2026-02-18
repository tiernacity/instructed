//// Instructed SQLite Event Store Adapter
////
//// Provides a SQLite-backed event store for the Instructed CQRS/ES framework.
//// Uses the `sqlight` package which bundles SQLite — no separate installation needed.
////
//// All operations are serialized through an OTP actor, ensuring sequential
//// event handling. Optimistic locking is enforced via a UNIQUE constraint
//// on (stream_id, stream_version).
////
//// ## Example
////
//// ```gleam
//// import instructed_sqlite
////
//// let config = instructed_sqlite.SqliteConfig(
////   db_path: "events.db",
////   serialize: fn(event) { json.to_string(encode_event(event)) },
////   deserialize: fn(json_str) { decode_event(json_str) },
////   event_type: fn(event) { event_type_name(event) },
//// )
//// let assert Ok(store) = instructed_sqlite.start(config)
//// let event_store = instructed_sqlite.to_event_store(store)
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
import instructed/event.{type EventData, type RecordedEvent, RecordedEvent}
import instructed/event_store.{
  type EventStore, type ExpectedVersion, type StartFrom, type Subscription,
  AnyVersion, Current, EventStore, ExactVersion, FromEventNumber, NoStream,
  Origin, StreamExists, Subscription,
}
import instructed/snapshot.{type SnapshotData, SnapshotData}
import sqlight
import youid/uuid

// --- Public Types ---

/// Configuration for the SQLite event store.
pub type SqliteConfig(event) {
  SqliteConfig(
    /// Path to the SQLite database file (e.g. "events.db")
    db_path: String,
    /// Serialize a domain event to JSON string
    serialize: fn(event) -> String,
    /// Deserialize a JSON string back to a domain event
    deserialize: fn(String) -> Result(event, String),
    /// Get the event type name for a domain event
    event_type: fn(event) -> String,
  )
}

// --- Actor State ---

type StoreState(event) {
  StoreState(
    conn: sqlight.Connection,
    config: SqliteConfig(event),
    /// Transient all-stream subscribers
    all_subscribers: List(TransientSub(event)),
    /// Transient per-stream subscribers
    stream_subscribers: List(TransientSub(event)),
    /// Next subscriber ID counter
    next_sub_id: Int,
  )
}

type TransientSub(event) {
  TransientSub(
    id: String,
    stream: String,
    handler: fn(RecordedEvent(event)) -> Nil,
  )
}

// --- Actor Messages ---

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
    count: Int,
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

// --- Schema Creation ---

fn create_schema(conn: sqlight.Connection) -> Result(Nil, String) {
  let statements = [
    "PRAGMA journal_mode=WAL",
    "PRAGMA foreign_keys=ON",
    "CREATE TABLE IF NOT EXISTS event_store_events (
      event_number INTEGER PRIMARY KEY AUTOINCREMENT,
      event_id TEXT NOT NULL UNIQUE,
      stream_id TEXT NOT NULL,
      stream_version INTEGER NOT NULL,
      event_type TEXT NOT NULL,
      data TEXT NOT NULL,
      metadata TEXT NOT NULL DEFAULT '{}',
      causation_id TEXT,
      correlation_id TEXT,
      created_at INTEGER NOT NULL,
      UNIQUE(stream_id, stream_version)
    )",
    "CREATE INDEX IF NOT EXISTS idx_events_stream_id ON event_store_events(stream_id)",
    "CREATE INDEX IF NOT EXISTS idx_events_stream_version ON event_store_events(stream_id, stream_version)",
    "CREATE TABLE IF NOT EXISTS event_store_snapshots (
      source_uuid TEXT PRIMARY KEY,
      source_version INTEGER NOT NULL,
      source_type TEXT NOT NULL,
      data TEXT NOT NULL,
      created_at INTEGER NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS event_store_subscriptions (
      subscription_id TEXT PRIMARY KEY,
      stream_id TEXT NOT NULL,
      subscription_name TEXT NOT NULL,
      last_seen_event_number INTEGER NOT NULL DEFAULT 0,
      created_at INTEGER NOT NULL,
      UNIQUE(stream_id, subscription_name)
    )",
  ]

  list.fold(statements, Ok(Nil), fn(acc, sql) {
    case acc {
      Error(e) -> Error(e)
      Ok(Nil) ->
        case sqlight.exec(sql, on: conn) {
          Ok(Nil) -> Ok(Nil)
          Error(e) -> Error("Failed to execute: " <> string.inspect(e))
        }
    }
  })
}

// --- Actor Start ---

/// Start the SQLite event store actor.
/// Opens the database, creates the schema, and starts the actor.
pub fn start(
  config: SqliteConfig(event),
) -> Result(Subject(Message(event)), String) {
  case sqlight.open(config.db_path) {
    Error(e) -> Error("Failed to open SQLite database: " <> string.inspect(e))
    Ok(conn) -> {
      case create_schema(conn) {
        Error(e) -> Error(e)
        Ok(Nil) -> {
          let state =
            StoreState(
              conn: conn,
              config: config,
              all_subscribers: [],
              stream_subscribers: [],
              next_sub_id: 1,
            )
          case
            actor.new(state)
            |> actor.on_message(handle_message)
            |> actor.start
          {
            Ok(started) -> Ok(started.data)
            Error(_) -> Error("Failed to start SQLite event store actor")
          }
        }
      }
    }
  }
}

/// Create an EventStore interface from a running SQLite event store actor.
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
    read_stream_forward: fn(stream_id, start_version, count) {
      process.call(subject, call_timeout, fn(reply) {
        ReadStream(stream_id, start_version, count, reply)
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

// --- Message Handler ---

fn handle_message(
  state: StoreState(event),
  msg: Message(event),
) -> actor.Next(StoreState(event), Message(event)) {
  case msg {
    Append(stream_id, expected_version, events, reply) -> {
      let result = handle_append(state, stream_id, expected_version, events)
      case result {
        Ok(#(version, recorded)) -> {
          // Notify subscribers
          notify_subscribers(state, stream_id, recorded)
          process.send(reply, Ok(version))
        }
        Error(e) -> process.send(reply, Error(e))
      }
      actor.continue(state)
    }

    ReadStream(stream_id, start_version, count, reply) -> {
      process.send(
        reply,
        handle_read_stream(state, stream_id, start_version, count),
      )
      actor.continue(state)
    }

    ReadAll(start_number, reply) -> {
      process.send(reply, handle_read_all(state, start_number))
      actor.continue(state)
    }

    SubscribeAll(handler, reply) -> {
      let sub_id = "sub-" <> int.to_string(state.next_sub_id)
      let sub = TransientSub(id: sub_id, stream: "$all", handler: handler)
      process.send(reply, Ok(Subscription(id: sub_id)))
      actor.continue(StoreState(
        ..state,
        all_subscribers: [sub, ..state.all_subscribers],
        next_sub_id: state.next_sub_id + 1,
      ))
    }

    SubscribeStream(stream_id, handler, reply) -> {
      let sub_id = "sub-" <> int.to_string(state.next_sub_id)
      let sub =
        TransientSub(id: sub_id, stream: stream_id, handler: handler)
      process.send(reply, Ok(Subscription(id: sub_id)))
      actor.continue(StoreState(
        ..state,
        stream_subscribers: [sub, ..state.stream_subscribers],
        next_sub_id: state.next_sub_id + 1,
      ))
    }

    SubscribePersistent(stream, name, start_from, handler, reply) -> {
      let new_state =
        handle_subscribe_persistent(
          state,
          stream,
          name,
          start_from,
          handler,
          reply,
        )
      actor.continue(new_state)
    }

    AckEvent(sub, event, reply) -> {
      process.send(reply, handle_ack_event(state, sub, event))
      actor.continue(state)
    }

    Unsubscribe(sub, reply) -> {
      let new_all =
        list.filter(state.all_subscribers, fn(s) { s.id != sub.id })
      let new_stream =
        list.filter(state.stream_subscribers, fn(s) { s.id != sub.id })
      process.send(reply, Ok(Nil))
      actor.continue(StoreState(
        ..state,
        all_subscribers: new_all,
        stream_subscribers: new_stream,
      ))
    }

    DeleteSubscription(stream, name, reply) -> {
      process.send(reply, handle_delete_subscription(state, stream, name))
      actor.continue(state)
    }

    ReadSnapshot(source_uuid, reply) -> {
      process.send(reply, handle_read_snapshot(state, source_uuid))
      actor.continue(state)
    }

    RecordSnapshot(snap, reply) -> {
      process.send(reply, handle_record_snapshot(state, snap))
      actor.continue(state)
    }

    DeleteSnapshot(source_uuid, reply) -> {
      process.send(reply, handle_delete_snapshot(state, source_uuid))
      actor.continue(state)
    }

    Reset(reply) -> {
      process.send(reply, handle_reset(state))
      actor.continue(StoreState(
        ..state,
        all_subscribers: [],
        stream_subscribers: [],
        next_sub_id: 1,
      ))
    }

    GetLatestEventNumber(reply) -> {
      process.send(reply, handle_get_latest_event_number(state))
      actor.continue(state)
    }
  }
}

// --- Append ---

fn handle_append(
  state: StoreState(event),
  stream_id: String,
  expected_version: ExpectedVersion,
  events: List(EventData(event)),
) -> Result(#(Int, List(RecordedEvent(event))), EventStoreError) {
  let current_version = get_stream_version(state.conn, stream_id)

  let version_ok = case expected_version {
    AnyVersion -> True
    NoStream -> current_version == 0
    StreamExists -> current_version > 0
    ExactVersion(v) -> current_version == v
  }

  case version_ok {
    False -> Error(VersionConflict)
    True -> {
      // Insert events one by one, building up recorded events
      insert_events(
        state.conn,
        state.config,
        stream_id,
        current_version,
        events,
        [],
      )
    }
  }
}

fn insert_events(
  conn: sqlight.Connection,
  config: SqliteConfig(event),
  stream_id: String,
  current_version: Int,
  events: List(EventData(event)),
  acc: List(RecordedEvent(event)),
) -> Result(#(Int, List(RecordedEvent(event))), EventStoreError) {
  case events {
    [] -> Ok(#(current_version, list.reverse(acc)))
    [evt, ..rest] -> {
      let new_ver = current_version + 1
      let event_id = uuid.v4_string()
      let now = now_ms()
      let data_json = config.serialize(evt.data)
      let metadata_json = serialize_metadata(evt.metadata)
      let causation = option.unwrap(evt.causation_id, "")
      let correlation = option.unwrap(evt.correlation_id, "")
      let event_type = config.event_type(evt.data)

      let sql =
        "INSERT INTO event_store_events (event_id, stream_id, stream_version, event_type, data, metadata, causation_id, correlation_id, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"

      case
        sqlight.query(
          sql,
          on: conn,
          with: [
            sqlight.text(event_id),
            sqlight.text(stream_id),
            sqlight.int(new_ver),
            sqlight.text(event_type),
            sqlight.text(data_json),
            sqlight.text(metadata_json),
            sqlight.text(causation),
            sqlight.text(correlation),
            sqlight.int(now),
          ],
          expecting: decode.success(Nil),
        )
      {
        Ok(_) -> {
          // Get the auto-generated event_number
          case
            sqlight.query(
              "SELECT last_insert_rowid()",
              on: conn,
              with: [],
              expecting: decode.at([0], decode.int),
            )
          {
            Ok([event_number]) -> {
              let recorded =
                RecordedEvent(
                  event_id: event_id,
                  event_number: event_number,
                  stream_id: stream_id,
                  stream_version: new_ver,
                  event_type: event_type,
                  causation_id: evt.causation_id,
                  correlation_id: evt.correlation_id,
                  data: evt.data,
                  metadata: evt.metadata,
                  created_at: now,
                )
              insert_events(
                conn,
                config,
                stream_id,
                new_ver,
                rest,
                [recorded, ..acc],
              )
            }
            _ -> Error(StorageError("Failed to get last insert rowid"))
          }
        }
        Error(sqlight.SqlightError(sqlight.ConstraintUnique, _, _)) ->
          Error(VersionConflict)
        Error(e) -> Error(StorageError(string.inspect(e)))
      }
    }
  }
}

fn get_stream_version(conn: sqlight.Connection, stream_id: String) -> Int {
  case
    sqlight.query(
      "SELECT COALESCE(MAX(stream_version), 0) FROM event_store_events WHERE stream_id = ?",
      on: conn,
      with: [sqlight.text(stream_id)],
      expecting: decode.at([0], decode.int),
    )
  {
    Ok([version]) -> version
    _ -> 0
  }
}

// --- Read ---

fn handle_read_stream(
  state: StoreState(event),
  stream_id: String,
  start_version: Int,
  count: Int,
) -> Result(List(RecordedEvent(event)), EventStoreError) {
  let sql =
    "SELECT event_id, event_number, stream_id, stream_version, event_type, causation_id, correlation_id, data, metadata, created_at FROM event_store_events WHERE stream_id = ? AND stream_version >= ? ORDER BY stream_version ASC LIMIT ?"

  case
    sqlight.query(
      sql,
      on: state.conn,
      with: [
        sqlight.text(stream_id),
        sqlight.int(start_version),
        sqlight.int(count),
      ],
      expecting: event_row_decoder(),
    )
  {
    Ok([]) -> Error(StreamNotFound)
    Ok(rows) -> {
      let events = decode_event_rows(state.config, rows)
      case events {
        [] -> Error(StreamNotFound)
        _ -> Ok(events)
      }
    }
    Error(e) -> Error(StorageError(string.inspect(e)))
  }
}

fn handle_read_all(
  state: StoreState(event),
  start_number: Int,
) -> Result(List(RecordedEvent(event)), EventStoreError) {
  let sql =
    "SELECT event_id, event_number, stream_id, stream_version, event_type, causation_id, correlation_id, data, metadata, created_at FROM event_store_events WHERE event_number >= ? ORDER BY event_number ASC"

  case
    sqlight.query(
      sql,
      on: state.conn,
      with: [sqlight.int(start_number)],
      expecting: event_row_decoder(),
    )
  {
    Ok(rows) -> Ok(decode_event_rows(state.config, rows))
    Error(e) -> Error(StorageError(string.inspect(e)))
  }
}

type EventRow {
  EventRow(
    event_id: String,
    event_number: Int,
    stream_id: String,
    stream_version: Int,
    event_type: String,
    causation_id: String,
    correlation_id: String,
    data_json: String,
    metadata_json: String,
    created_at: Int,
  )
}

fn event_row_decoder() -> decode.Decoder(EventRow) {
  use event_id <- decode.field(0, decode.string)
  use event_number <- decode.field(1, decode.int)
  use stream_id <- decode.field(2, decode.string)
  use stream_version <- decode.field(3, decode.int)
  use event_type <- decode.field(4, decode.string)
  use causation_id <- decode.field(5, decode.string)
  use correlation_id <- decode.field(6, decode.string)
  use data_json <- decode.field(7, decode.string)
  use metadata_json <- decode.field(8, decode.string)
  use created_at <- decode.field(9, decode.int)
  decode.success(EventRow(
    event_id: event_id,
    event_number: event_number,
    stream_id: stream_id,
    stream_version: stream_version,
    event_type: event_type,
    causation_id: causation_id,
    correlation_id: correlation_id,
    data_json: data_json,
    metadata_json: metadata_json,
    created_at: created_at,
  ))
}

fn decode_event_rows(
  config: SqliteConfig(event),
  rows: List(EventRow),
) -> List(RecordedEvent(event)) {
  list.filter_map(rows, fn(row) {
    case config.deserialize(row.data_json) {
      Ok(event_data) ->
        Ok(RecordedEvent(
          event_id: row.event_id,
          event_number: row.event_number,
          stream_id: row.stream_id,
          stream_version: row.stream_version,
          event_type: row.event_type,
          causation_id: case row.causation_id {
            "" -> None
            s -> Some(s)
          },
          correlation_id: case row.correlation_id {
            "" -> None
            s -> Some(s)
          },
          data: event_data,
          metadata: case
            json.parse(row.metadata_json, decode.dict(decode.string, decode.string))
          {
            Ok(d) -> d
            Error(_) -> dict.new()
          },
          created_at: row.created_at,
        ))
      Error(_) -> Error(Nil)
    }
  })
}

// --- Subscriptions ---

fn notify_subscribers(
  state: StoreState(event),
  stream_id: String,
  events: List(RecordedEvent(event)),
) -> Nil {
  // Notify all-stream subscribers
  list.each(state.all_subscribers, fn(sub) {
    list.each(events, fn(evt) { sub.handler(evt) })
  })
  // Notify stream-specific subscribers
  list.each(state.stream_subscribers, fn(sub) {
    case sub.stream == stream_id {
      True -> list.each(events, fn(evt) { sub.handler(evt) })
      False -> Nil
    }
  })
}

fn handle_subscribe_persistent(
  state: StoreState(event),
  stream: String,
  name: String,
  start_from: StartFrom,
  handler: fn(RecordedEvent(event)) -> Nil,
  reply: Subject(Result(Subscription, EventStoreError)),
) -> StoreState(event) {
  let sub_id = stream <> ":" <> name

  // Check if subscription already exists
  case
    sqlight.query(
      "SELECT subscription_id FROM event_store_subscriptions WHERE stream_id = ? AND subscription_name = ?",
      on: state.conn,
      with: [sqlight.text(stream), sqlight.text(name)],
      expecting: decode.at([0], decode.string),
    )
  {
    Ok([_]) -> {
      process.send(reply, Error(SubscriptionAlreadyExists))
      state
    }
    _ -> {
      let last_seen = case start_from {
        Origin -> 0
        Current -> {
          case handle_get_latest_event_number(state) {
            Ok(Some(n)) -> n
            _ -> 0
          }
        }
        FromEventNumber(n) -> n - 1
      }

      case
        sqlight.query(
          "INSERT INTO event_store_subscriptions (subscription_id, stream_id, subscription_name, last_seen_event_number, created_at) VALUES (?, ?, ?, ?, ?)",
          on: state.conn,
          with: [
            sqlight.text(sub_id),
            sqlight.text(stream),
            sqlight.text(name),
            sqlight.int(last_seen),
            sqlight.int(now_ms()),
          ],
          expecting: decode.success(Nil),
        )
      {
        Ok(_) -> {
          // Send historical events
          case start_from {
            Current -> Nil
            _ -> {
              let events = case stream == "$all" {
                True -> handle_read_all(state, last_seen + 1)
                False -> handle_read_stream(state, stream, 1, 1_000_000)
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

          // Register as a transient subscriber for new events
          let tsub =
            TransientSub(id: sub_id, stream: stream, handler: handler)
          process.send(reply, Ok(Subscription(id: sub_id)))

          case stream {
            "$all" ->
              StoreState(
                ..state,
                all_subscribers: [tsub, ..state.all_subscribers],
                next_sub_id: state.next_sub_id + 1,
              )
            _ ->
              StoreState(
                ..state,
                stream_subscribers: [tsub, ..state.stream_subscribers],
                next_sub_id: state.next_sub_id + 1,
              )
          }
        }
        Error(e) -> {
          process.send(reply, Error(StorageError(string.inspect(e))))
          state
        }
      }
    }
  }
}

fn handle_ack_event(
  state: StoreState(event),
  sub: Subscription,
  event: RecordedEvent(event),
) -> Result(Nil, EventStoreError) {
  case
    sqlight.query(
      "UPDATE event_store_subscriptions SET last_seen_event_number = ? WHERE subscription_id = ?",
      on: state.conn,
      with: [sqlight.int(event.event_number), sqlight.text(sub.id)],
      expecting: decode.success(Nil),
    )
  {
    Ok(_) -> Ok(Nil)
    Error(e) -> Error(StorageError(string.inspect(e)))
  }
}

fn handle_delete_subscription(
  state: StoreState(event),
  stream: String,
  name: String,
) -> Result(Nil, EventStoreError) {
  // Check it exists first
  case
    sqlight.query(
      "SELECT subscription_id FROM event_store_subscriptions WHERE stream_id = ? AND subscription_name = ?",
      on: state.conn,
      with: [sqlight.text(stream), sqlight.text(name)],
      expecting: decode.at([0], decode.string),
    )
  {
    Ok([]) -> Error(SubscriptionNotFound)
    Ok(_) -> {
      case
        sqlight.query(
          "DELETE FROM event_store_subscriptions WHERE stream_id = ? AND subscription_name = ?",
          on: state.conn,
          with: [sqlight.text(stream), sqlight.text(name)],
          expecting: decode.success(Nil),
        )
      {
        Ok(_) -> Ok(Nil)
        Error(e) -> Error(StorageError(string.inspect(e)))
      }
    }
    Error(e) -> Error(StorageError(string.inspect(e)))
  }
}

// --- Snapshots ---

fn handle_read_snapshot(
  state: StoreState(event),
  source_uuid: String,
) -> Result(SnapshotData(event), EventStoreError) {
  case
    sqlight.query(
      "SELECT source_uuid, source_version, source_type, data, created_at FROM event_store_snapshots WHERE source_uuid = ?",
      on: state.conn,
      with: [sqlight.text(source_uuid)],
      expecting: {
        use suuid <- decode.field(0, decode.string)
        use sver <- decode.field(1, decode.int)
        use stype <- decode.field(2, decode.string)
        use data_json <- decode.field(3, decode.string)
        use created_at <- decode.field(4, decode.int)
        decode.success(#(suuid, sver, stype, data_json, created_at))
      },
    )
  {
    Ok([#(suuid, sver, stype, data_json, created_at)]) -> {
      case state.config.deserialize(data_json) {
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
    Ok(_) -> Error(SnapshotNotFound)
    Error(e) -> Error(StorageError(string.inspect(e)))
  }
}

fn handle_record_snapshot(
  state: StoreState(event),
  snap: SnapshotData(event),
) -> Result(Nil, EventStoreError) {
  let data_json = state.config.serialize(snap.data)

  case
    sqlight.query(
      "INSERT INTO event_store_snapshots (source_uuid, source_version, source_type, data, created_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT (source_uuid) DO UPDATE SET source_version = excluded.source_version, source_type = excluded.source_type, data = excluded.data, created_at = excluded.created_at",
      on: state.conn,
      with: [
        sqlight.text(snap.source_uuid),
        sqlight.int(snap.source_version),
        sqlight.text(snap.source_type),
        sqlight.text(data_json),
        sqlight.int(snap.created_at),
      ],
      expecting: decode.success(Nil),
    )
  {
    Ok(_) -> Ok(Nil)
    Error(e) -> Error(StorageError(string.inspect(e)))
  }
}

fn handle_delete_snapshot(
  state: StoreState(event),
  source_uuid: String,
) -> Result(Nil, EventStoreError) {
  case
    sqlight.query(
      "DELETE FROM event_store_snapshots WHERE source_uuid = ?",
      on: state.conn,
      with: [sqlight.text(source_uuid)],
      expecting: decode.success(Nil),
    )
  {
    Ok(_) -> Ok(Nil)
    Error(e) -> Error(StorageError(string.inspect(e)))
  }
}

// --- Reset ---

fn handle_reset(state: StoreState(event)) -> Result(Nil, EventStoreError) {
  let statements = [
    "DELETE FROM event_store_subscriptions",
    "DELETE FROM event_store_snapshots",
    "DELETE FROM event_store_events",
  ]

  list.fold(statements, Ok(Nil), fn(acc, sql) {
    case acc {
      Error(e) -> Error(e)
      Ok(Nil) ->
        case sqlight.exec(sql, on: state.conn) {
          Ok(Nil) -> Ok(Nil)
          Error(e) -> Error(StorageError(string.inspect(e)))
        }
    }
  })
}

// --- Latest Event Number ---

fn handle_get_latest_event_number(
  state: StoreState(event),
) -> Result(Option(Int), EventStoreError) {
  case
    sqlight.query(
      "SELECT MAX(event_number) FROM event_store_events",
      on: state.conn,
      with: [],
      expecting: decode.at([0], decode.optional(decode.int)),
    )
  {
    Ok([Some(n)]) -> Ok(Some(n))
    Ok(_) -> Ok(None)
    Error(e) -> Error(StorageError(string.inspect(e)))
  }
}

// --- Helpers ---

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

@external(erlang, "os", "system_time")
fn system_time_native() -> Int

fn now_ms() -> Int {
  system_time_native() / 1_000_000
}
