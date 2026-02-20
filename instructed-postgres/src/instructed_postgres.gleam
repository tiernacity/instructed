//// Instructed PostgreSQL Event Store Adapter
////
//// Provides a PostgreSQL-backed event store for the Instructed CQRS/ES framework.
//// Events are stored in a PostgreSQL database with proper schemas for:
//// - Event streams and recorded events
//// - Persistent subscriptions with position tracking
//// - Aggregate state snapshots
////
//// ## Architecture
////
//// Unlike the in-memory and SQLite adapters which serialize all operations
//// through a single OTP actor, this adapter supports concurrent writes.
//// Correctness is achieved via:
////
//// 1. **Transactional append with OCC** — version check + insert run in a
////    single database transaction. UNIQUE constraint violations map to
////    VersionConflict, enabling the aggregate server's OCC retry loop.
////
//// 2. **Poll-based persistent subscriptions** — each persistent subscription
////    runs a SubscriptionPoller actor that reads events from postgres in
////    event_number order. Notifications are wake-up signals, not the delivery
////    mechanism. This guarantees ordered, gap-free delivery even under
////    concurrent writes with out-of-order BIGSERIAL commits.
////
//// 3. **Transient subscriptions** — still use direct push via the notifier
////    actor (unchanged, used by aggregate server self-subscription).

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
  SubscriptionNotFound, VersionConflict,
}
import instructed/event.{type EventData, type RecordedEvent, RecordedEvent}
import instructed/event_store.{
  type EventStore, type ExpectedVersion, type StartFrom, type Subscription,
  AnyVersion, Current, EventStore, ExactVersion, FromEventNumber, NoStream,
  Origin, StreamExists, Subscription,
}
import instructed/snapshot.{type SnapshotData, SnapshotData}
import pog
import youid/uuid

// ============================================================================
// Public API
// ============================================================================

/// Configuration for the PostgreSQL event store.
pub type PgConfig(event) {
  PgConfig(
    /// Database connection pool
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
          Error(e) -> Error("Failed to execute: " <> string.inspect(e))
        }
    }
  })
}

/// Create a new PostgreSQL-backed EventStore.
pub fn new(config: PgConfig(event)) -> EventStore(event) {
  // Start the notifier actor (manages transient subs + wakes pollers)
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
      ack_event(config, notifier, sub, event)
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

// ============================================================================
// Component 2: Transactional Append with OCC
// ============================================================================

fn append_to_stream(
  config: PgConfig(event),
  stream_id: String,
  expected_version: ExpectedVersion,
  events: List(EventData(event)),
  notifier: Subject(NotifierMessage(event)),
) -> Result(Int, EventStoreError) {
  // Run the version check + insert inside a single transaction
  let tx_result =
    pog.transaction(config.db, fn(conn) {
      // Read current version inside the transaction
      let current_version = get_stream_version_with_conn(conn, stream_id)

      let version_ok = case expected_version {
        AnyVersion -> True
        NoStream -> current_version == 0
        StreamExists -> current_version > 0
        ExactVersion(v) -> current_version == v
      }

      case version_ok {
        False -> Error(VersionConflict)
        True -> {
          insert_events_in_tx(
            conn,
            config,
            stream_id,
            current_version,
            events,
          )
        }
      }
    })

  case tx_result {
    Ok(final_version) -> {
      // After successful commit, notify:
      // 1. Read back committed events for transient subscribers
      let _ = case
        read_stream_forward(
          config,
          stream_id,
          final_version - list.length(events) + 1,
          list.length(events),
        )
      {
        Ok(new_events) -> {
          list.each(new_events, fn(evt) {
            process.send(notifier, NotifyEvent(stream_id, evt))
          })
          Nil
        }
        Error(_) -> Nil
      }
      // 2. Wake all pollers
      process.send(notifier, Wake)
      Ok(final_version)
    }
    Error(pog.TransactionQueryError(pog.ConstraintViolated(_, constraint, _))) -> {
      // UNIQUE(stream_id, stream_version) violation → VersionConflict
      case string.contains(constraint, "stream_id") {
        True -> Error(VersionConflict)
        False -> Error(VersionConflict)
      }
    }
    Error(pog.TransactionQueryError(e)) ->
      Error(StorageError(string.inspect(e)))
    Error(pog.TransactionRolledBack(VersionConflict)) ->
      Error(VersionConflict)
    Error(pog.TransactionRolledBack(e)) -> Error(e)
  }
}

fn insert_events_in_tx(
  conn: pog.Connection,
  config: PgConfig(event),
  stream_id: String,
  current_version: Int,
  events: List(EventData(event)),
) -> Result(Int, EventStoreError) {
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
          |> pog.execute(conn)
        {
          Ok(_) -> Ok(new_ver)
          Error(pog.ConstraintViolated(_, _, _)) ->
            Error(VersionConflict)
          Error(e) -> Error(StorageError(string.inspect(e)))
        }
      }
    }
  })
}

fn get_stream_version_with_conn(
  conn: pog.Connection,
  stream_id: String,
) -> Int {
  let sql =
    "SELECT COALESCE(MAX(stream_version), 0) FROM event_store_events WHERE stream_id = $1"

  let row_decoder = decode.at([0], decode.int)

  case
    pog.query(sql)
    |> pog.parameter(pog.text(stream_id))
    |> pog.returning(row_decoder)
    |> pog.execute(conn)
  {
    Ok(response) ->
      case response.rows {
        [version] -> version
        _ -> 0
      }
    Error(_) -> 0
  }
}

// ============================================================================
// Component 1: SubscriptionPoller Actor
// ============================================================================

/// Messages for the subscription poller actor.
type PollerMessage {
  /// Wake up and poll for new events
  PollNow
  /// Acknowledge an event, advancing the cursor and delivering next
  PollerAck(event_number: Int)
  /// Timer-based fallback poll
  TimerPoll
  /// Set self reference (sent once after actor start)
  SetSelf(Subject(PollerMessage))
}

/// State of the subscription poller actor.
type PollerState(event) {
  PollerState(
    /// Last event_number successfully delivered to the handler
    last_seen: Int,
    /// The stream to subscribe to ("$all" or specific stream_id)
    stream: String,
    /// The handler callback
    handler: fn(RecordedEvent(event)) -> Nil,
    /// Database config for reading events
    config: PgConfig(event),
    /// Batch size for polling
    batch_size: Int,
    /// Fallback poll interval in milliseconds
    poll_interval: Int,
    /// Whether we have an event in-flight (waiting for ack)
    in_flight: Bool,
    /// Events fetched but not yet delivered (pending queue)
    pending: List(RecordedEvent(event)),
    /// Self-reference for scheduling timer
    self: Subject(PollerMessage),
    /// Number of consecutive polls that found a gap at last_seen+1.
    /// BIGSERIAL gaps happen when concurrent transactions commit out of order
    /// or roll back (e.g. VersionConflict). Temporary gaps resolve when the
    /// in-flight transaction commits; permanent gaps (from rollbacks) never do.
    gap_retries: Int,
    /// After this many gap retries, skip past the gap (permanent gap from rollback)
    max_gap_retries: Int,
  )
}

fn start_poller(
  config: PgConfig(event),
  sub_id: String,
  stream: String,
  last_seen: Int,
  handler: fn(RecordedEvent(event)) -> Nil,
  notifier: Subject(NotifierMessage(event)),
) -> Result(Subject(PollerMessage), actor.StartError) {
  // We need to create the subject first, then start the actor
  // Use a temporary subject to receive the poller subject back
  let poller_ready = process.new_subject()

  let init_state =
    PollerState(
      last_seen: last_seen,
      stream: stream,
      handler: handler,
      config: config,
      batch_size: 1000,
      poll_interval: 1000,
      in_flight: False,
      pending: [],
      // Will be set after start
      self: poller_ready,
      gap_retries: 0,
      // 10 retries × 1s poll interval = 10s max wait for in-flight transactions.
      // After that, assume the gap is permanent (rolled-back transaction).
      max_gap_retries: 10,
    )

  case
    actor.new(init_state)
    |> actor.on_message(handle_poller_message)
    |> actor.start
  {
    Ok(started) -> {
      let poller_subject = started.data

      // Set self reference so the poller can schedule timers
      process.send(poller_subject, SetSelf(poller_subject))

      // Register with notifier for wake signals and ack routing
      process.send(notifier, AddPoller(sub_id, poller_subject))

      // Trigger initial poll to deliver historical events
      process.send(poller_subject, PollNow)

      Ok(poller_subject)
    }
    Error(e) -> Error(e)
  }
}

fn handle_poller_message(
  state: PollerState(event),
  msg: PollerMessage,
) -> actor.Next(PollerState(event), PollerMessage) {
  case msg {
    PollNow | TimerPoll -> {
      case state.in_flight {
        True -> {
          // Already processing an event, don't poll yet
          actor.continue(state)
        }
        False -> {
          case state.pending {
            [next, ..rest] -> {
              // Deliver next pending event
              state.handler(next)
              actor.continue(
                PollerState(
                  ..state,
                  in_flight: True,
                  pending: rest,
                ),
              )
            }
            [] -> {
              // No pending events, poll from database
              let new_state = do_poll(state)
              // Schedule fallback timer
              schedule_timer(new_state)
              actor.continue(new_state)
            }
          }
        }
      }
    }

    SetSelf(self) -> {
      actor.continue(PollerState(..state, self: self))
    }

    PollerAck(event_number) -> {
      // Advance cursor
      let new_state =
        PollerState(
          ..state,
          last_seen: event_number,
          in_flight: False,
        )
      // Try to deliver next pending or poll for more
      case new_state.pending {
        [next, ..rest] -> {
          new_state.handler(next)
          actor.continue(
            PollerState(
              ..new_state,
              in_flight: True,
              pending: rest,
            ),
          )
        }
        [] -> {
          // Poll for more events
          let polled = do_poll(new_state)
          schedule_timer(polled)
          actor.continue(polled)
        }
      }
    }
  }
}

fn do_poll(state: PollerState(event)) -> PollerState(event) {
  let events = case state.stream {
    "$all" -> read_all_forward_limited(state.config, state.last_seen + 1, state.batch_size)
    stream_id -> read_stream_by_event_number(state.config, stream_id, state.last_seen + 1, state.batch_size)
  }

  case events {
    Ok([]) -> state
    Ok([first, ..rest]) -> {
      // Gap detection: with concurrent BIGSERIAL inserts, we may see
      // event N+2 before N+1 if the transaction for N+1 hasn't committed yet.
      // We must not advance past the gap or we'll permanently skip events.
      case first.event_number == state.last_seen + 1 {
        True -> {
          // Contiguous — deliver first event, queue contiguous tail
          let #(contiguous_tail, _non_contiguous) =
            take_contiguous_prefix(rest, first.event_number + 1)
          state.handler(first)
          PollerState(
            ..state,
            in_flight: True,
            pending: contiguous_tail,
            gap_retries: 0,
          )
        }
        False -> {
          // Gap detected at last_seen+1. Either:
          // (a) in-flight transaction — will fill on next poll
          // (b) permanent gap from rolled-back transaction
          let new_retries = state.gap_retries + 1
          case new_retries >= state.max_gap_retries {
            True -> {
              // Gap persisted too long — skip it (permanent gap from rollback).
              // Deliver from the first available event.
              let #(contiguous_tail, _non_contiguous) =
                take_contiguous_prefix(rest, first.event_number + 1)
              state.handler(first)
              PollerState(
                ..state,
                in_flight: True,
                pending: contiguous_tail,
                gap_retries: 0,
              )
            }
            False -> {
              // Wait for gap to fill — don't deliver anything yet
              PollerState(..state, gap_retries: new_retries)
            }
          }
        }
      }
    }
    Error(_) -> state
  }
}

/// Extract the contiguous prefix of events starting from expected_number.
/// Returns (contiguous_events, remaining_events).
fn take_contiguous_prefix(
  events: List(RecordedEvent(event)),
  expected_number: Int,
) -> #(List(RecordedEvent(event)), List(RecordedEvent(event))) {
  do_take_contiguous(events, expected_number, [])
}

fn do_take_contiguous(
  events: List(RecordedEvent(event)),
  expected_number: Int,
  acc: List(RecordedEvent(event)),
) -> #(List(RecordedEvent(event)), List(RecordedEvent(event))) {
  case events {
    [] -> #(list.reverse(acc), [])
    [first, ..rest] -> {
      case first.event_number == expected_number {
        True -> do_take_contiguous(rest, expected_number + 1, [first, ..acc])
        False -> #(list.reverse(acc), events)
      }
    }
  }
}

fn read_all_forward_limited(
  config: PgConfig(event),
  start_number: Int,
  limit: Int,
) -> Result(List(RecordedEvent(event)), EventStoreError) {
  let sql =
    "SELECT event_id, event_number, stream_id, stream_version, event_type, causation_id, correlation_id, data, metadata, created_at FROM event_store_events WHERE event_number >= $1 ORDER BY event_number ASC LIMIT $2"

  case
    pog.query(sql)
    |> pog.parameter(pog.int(start_number))
    |> pog.parameter(pog.int(limit))
    |> pog.returning(event_row_decoder())
    |> pog.execute(config.db)
  {
    Ok(response) -> Ok(decode_event_rows(config, response.rows))
    Error(e) -> Error(StorageError(string.inspect(e)))
  }
}

fn read_stream_by_event_number(
  config: PgConfig(event),
  stream_id: String,
  start_event_number: Int,
  limit: Int,
) -> Result(List(RecordedEvent(event)), EventStoreError) {
  let sql =
    "SELECT event_id, event_number, stream_id, stream_version, event_type, causation_id, correlation_id, data, metadata, created_at FROM event_store_events WHERE stream_id = $1 AND event_number >= $2 ORDER BY event_number ASC LIMIT $3"

  case
    pog.query(sql)
    |> pog.parameter(pog.text(stream_id))
    |> pog.parameter(pog.int(start_event_number))
    |> pog.parameter(pog.int(limit))
    |> pog.returning(event_row_decoder())
    |> pog.execute(config.db)
  {
    Ok(response) -> Ok(decode_event_rows(config, response.rows))
    Error(e) -> Error(StorageError(string.inspect(e)))
  }
}

fn schedule_timer(state: PollerState(event)) -> Nil {
  // Use process.send_after for fallback polling
  let self = state.self
  let _timer =
    process.send_after(self, state.poll_interval, TimerPoll)
  Nil
}

// ============================================================================
// Component 3: Notifier (simplified — Wake for pollers, NotifyEvent for transient)
// ============================================================================

type NotifierState(event) {
  NotifierState(
    transient_subs: List(TransientSub(event)),
    /// All registered pollers (for wake broadcasting)
    pollers: List(Subject(PollerMessage)),
    /// Pollers keyed by subscription_id (for ack routing)
    poller_map: dict.Dict(String, Subject(PollerMessage)),
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
  /// Push event data to transient subscribers (aggregate self-sub)
  NotifyEvent(stream_id: String, event: RecordedEvent(event))
  /// Wake all pollers — no event payload, just a signal
  Wake
  /// Register a transient subscriber
  AddTransientSub(
    stream: String,
    handler: fn(RecordedEvent(event)) -> Nil,
    reply: Subject(Subscription),
  )
  /// Register a poller for wake signals, with its subscription ID
  AddPoller(sub_id: String, poller: Subject(PollerMessage))
  /// Look up a poller by subscription ID
  GetPoller(sub_id: String, reply: Subject(Option(Subject(PollerMessage))))
  /// Remove a transient subscriber
  RemoveSub(sub_id: String, reply: Subject(Nil))
}

fn start_notifier() -> Result(
  Subject(NotifierMessage(event)),
  actor.StartError,
) {
  let state =
    NotifierState(
      transient_subs: [],
      pollers: [],
      poller_map: dict.new(),
      next_id: 1,
    )
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
      // Deliver to transient subscribers only
      list.each(state.transient_subs, fn(sub) {
        case sub.stream == "$all" || sub.stream == stream_id {
          True -> sub.handler(event)
          False -> Nil
        }
      })
      actor.continue(state)
    }

    Wake -> {
      // Forward PollNow to all registered pollers
      list.each(state.pollers, fn(poller) {
        process.send(poller, PollNow)
      })
      actor.continue(state)
    }

    AddTransientSub(stream, handler, reply) -> {
      let sub_id = "tsub-" <> int.to_string(state.next_id)
      let sub = TransientSub(id: sub_id, stream: stream, handler: handler)
      process.send(reply, Subscription(id: sub_id))
      actor.continue(NotifierState(
        ..state,
        transient_subs: [sub, ..state.transient_subs],
        next_id: state.next_id + 1,
      ))
    }

    AddPoller(sub_id, poller) -> {
      actor.continue(NotifierState(
        ..state,
        pollers: [poller, ..state.pollers],
        poller_map: dict.insert(state.poller_map, sub_id, poller),
      ))
    }

    GetPoller(sub_id, reply) -> {
      process.send(reply, dict.get(state.poller_map, sub_id) |> option.from_result)
      actor.continue(state)
    }

    RemoveSub(sub_id, reply) -> {
      let new_subs =
        list.filter(state.transient_subs, fn(s) { s.id != sub_id })
      process.send(reply, Nil)
      actor.continue(NotifierState(..state, transient_subs: new_subs))
    }
  }
}

// ============================================================================
// Subscription Management
// ============================================================================

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
        [_] -> {
          // Idempotent reconnect: read the existing checkpoint and start
          // a new poller with the updated handler (Fix 3).
          let last_seen_sql =
            "SELECT last_seen_event_number FROM event_store_subscriptions WHERE stream_id = $1 AND subscription_name = $2"
          let last_seen = case
            pog.query(last_seen_sql)
            |> pog.parameter(pog.text(stream))
            |> pog.parameter(pog.text(name))
            |> pog.returning(decode.at([0], decode.int))
            |> pog.execute(config.db)
          {
            Ok(response) -> case response.rows {
              [n] -> n
              _ -> 0
            }
            Error(_) -> 0
          }

          // Start a new poller with the existing checkpoint
          case start_poller(config, sub_id, stream, last_seen, handler, notifier) {
            Ok(_) -> Ok(Subscription(id: sub_id))
            Error(e) -> Error(StorageError(string.inspect(e)))
          }
        }
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
              // Start a poller actor for this persistent subscription.
              // The poller handles both historical replay and live delivery
              // through the same poll-from-cursor mechanism.
              let assert Ok(_poller) =
                start_poller(config, sub_id, stream, last_seen, handler, notifier)

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
  notifier: Subject(NotifierMessage(event)),
  sub: Subscription,
  event: RecordedEvent(event),
) -> Result(Nil, EventStoreError) {
  // Update the subscription checkpoint in the database
  let sql =
    "UPDATE event_store_subscriptions SET last_seen_event_number = $1 WHERE subscription_id = $2"

  case
    pog.query(sql)
    |> pog.parameter(pog.int(event.event_number))
    |> pog.parameter(pog.text(sub.id))
    |> pog.execute(config.db)
  {
    Ok(_) -> {
      // Notify the poller that it can deliver the next event
      let poller_opt =
        process.call(notifier, 5000, fn(reply) {
          GetPoller(sub.id, reply)
        })
      case poller_opt {
        Some(poller) -> process.send(poller, PollerAck(event.event_number))
        None -> Nil
      }
      Ok(Nil)
    }
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

// ============================================================================
// Read Operations
// ============================================================================

fn read_stream_forward(
  config: PgConfig(event),
  stream_id: String,
  start_version: Int,
  count: Int,
) -> Result(List(RecordedEvent(event)), EventStoreError) {
  let sql =
    "SELECT event_id, event_number, stream_id, stream_version, event_type, causation_id, correlation_id, data, metadata, created_at FROM event_store_events WHERE stream_id = $1 AND stream_version >= $2 ORDER BY stream_version ASC LIMIT $3"

  case
    pog.query(sql)
    |> pog.parameter(pog.text(stream_id))
    |> pog.parameter(pog.int(start_version))
    |> pog.parameter(pog.int(count))
    |> pog.returning(event_row_decoder())
    |> pog.execute(config.db)
  {
    Ok(response) -> {
      case response.rows {
        [] -> Error(StreamNotFound)
        rows -> {
          let events = decode_event_rows(config, rows)
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

  case
    pog.query(sql)
    |> pog.parameter(pog.int(start_number))
    |> pog.returning(event_row_decoder())
    |> pog.execute(config.db)
  {
    Ok(response) -> Ok(decode_event_rows(config, response.rows))
    Error(e) -> Error(StorageError(string.inspect(e)))
  }
}

// ============================================================================
// Snapshots
// ============================================================================

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
  let sql = "DELETE FROM event_store_snapshots WHERE source_uuid = $1"

  case
    pog.query(sql)
    |> pog.parameter(pog.text(source_uuid))
    |> pog.execute(config.db)
  {
    Ok(_) -> Ok(Nil)
    Error(e) -> Error(StorageError(string.inspect(e)))
  }
}

// ============================================================================
// Reset
// ============================================================================

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

// ============================================================================
// Shared Helpers
// ============================================================================

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
  decode.then(decode.at([0], decode.string), fn(event_id) {
    decode.then(decode.at([1], decode.int), fn(event_number) {
      decode.then(decode.at([2], decode.string), fn(sid) {
        decode.then(decode.at([3], decode.int), fn(stream_version) {
          decode.then(decode.at([4], decode.string), fn(event_type) {
            decode.then(decode.at([5], decode.string), fn(causation_id) {
              decode.then(decode.at([6], decode.string), fn(correlation_id) {
                decode.then(decode.at([7], decode.string), fn(data_json) {
                  decode.then(
                    decode.at([8], decode.string),
                    fn(metadata_json) {
                      decode.then(
                        decode.at([9], decode.int),
                        fn(created_at) {
                          decode.success(EventRow(
                            event_id:,
                            event_number:,
                            stream_id: sid,
                            stream_version:,
                            event_type:,
                            causation_id:,
                            correlation_id:,
                            data_json:,
                            metadata_json:,
                            created_at:,
                          ))
                        },
                      )
                    },
                  )
                })
              })
            })
          })
        })
      })
    })
  })
}

fn decode_event_rows(
  config: PgConfig(event),
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
          causation_id: nullable_to_option(row.causation_id),
          correlation_id: nullable_to_option(row.correlation_id),
          data: event_data,
          metadata: json_to_metadata(row.metadata_json),
          created_at: row.created_at,
        ))
      Error(_) -> Error(Nil)
    }
  })
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
