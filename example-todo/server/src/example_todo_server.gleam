//// Todo Server - starts the CQRS HTTP server with a configurable event store.
////
//// Usage:
////   todo-server [--store sqlite|postgres|memory] [--port PORT] [--reset]
////
//// Options:
////   --store   Event store backend (default: sqlite)
////             sqlite   - SQLite file-based (.todo/db)
////             postgres - PostgreSQL (uses DATABASE_URL or defaults)
////             memory   - In-memory (data lost on restart)
////   --port    HTTP port (default: 8400)
////   --reset   Reset the event store before starting (clears all data)

import argv
import gleam/dynamic
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/list
import instructed/event_store.{type EventStore}
import instructed/in_memory_event_store
import instructed_postgres
import instructed_sqlite
import pog
import todo_server/http
import todo_server/serialization
import todo_server/server
import todo_shared/domain.{type TodoEvent}

const default_port = 8400

const default_store = "sqlite"

const default_db_path = ".todo/db"

pub fn main() -> Nil {
  let args = argv.load().arguments
  let port = parse_flag_int(args, "--port", default_port)
  let store_name = parse_flag_string(args, "--store", default_store)
  let should_reset = list.contains(args, "--reset")

  io.println("Starting Todo Server (store: " <> store_name <> ")...")

  let store = create_store(store_name)

  case should_reset {
    True -> {
      io.println("Resetting event store...")
      let assert Ok(Nil) = store.reset()
      io.println("Event store reset complete.")
    }
    False -> Nil
  }

  let assert Ok(srv) = server.start(store)
  let assert Ok(_) = http.start(srv, port)

  io.println(
    "Todo Server started on port "
    <> int.to_string(port)
    <> ". Press Ctrl+C to stop.",
  )
  process.sleep_forever()
}

fn create_store(name: String) -> EventStore(TodoEvent) {
  case name {
    "memory" | "mem" | "in-memory" -> {
      io.println("Using in-memory event store")
      let assert Ok(actor) = in_memory_event_store.start()
      in_memory_event_store.to_event_store(actor)
    }

    "postgres" | "pg" | "postgresql" -> {
      io.println("Using PostgreSQL event store")
      let pool_name = process.new_name("todo_pg_pool")
      let config = case get_env("DATABASE_URL") {
        Ok(url) -> {
          io.println("Using DATABASE_URL")
          let assert Ok(c) = pog.url_config(pool_name, url)
          c
        }
        Error(_) -> {
          io.println("No DATABASE_URL set, using defaults (localhost/instructed_todo)")
          pog.default_config(pool_name)
          |> pog.database("instructed_todo")
        }
      }
      let assert Ok(started) = pog.start(config)
      let db = started.data
      let assert Ok(Nil) = instructed_postgres.create_schema(db)
      let pg_config =
        instructed_postgres.PgConfig(
          db: db,
          serialize: serialization.serialize_event,
          deserialize: serialization.deserialize_event,
          event_type: serialization.event_type_name,
        )
      instructed_postgres.new(pg_config)
    }

    _ -> {
      // Default: sqlite
      ensure_directory(".todo")
      io.println("Using SQLite event store (" <> default_db_path <> ")")
      let sqlite_config =
        instructed_sqlite.SqliteConfig(
          db_path: default_db_path,
          serialize: serialization.serialize_event,
          deserialize: serialization.deserialize_event,
          event_type: serialization.event_type_name,
        )
      let assert Ok(sqlite_actor) = instructed_sqlite.start(sqlite_config)
      instructed_sqlite.to_event_store(sqlite_actor)
    }
  }
}

// --- Argument parsing helpers ---

fn parse_flag_string(
  args: List(String),
  flag: String,
  default: String,
) -> String {
  case find_flag_value(args, flag) {
    Ok(value) -> value
    Error(_) -> default
  }
}

fn parse_flag_int(args: List(String), flag: String, default: Int) -> Int {
  case find_flag_value(args, flag) {
    Ok(value) ->
      case int.parse(value) {
        Ok(n) -> n
        Error(_) -> default
      }
    Error(_) -> default
  }
}

fn find_flag_value(
  args: List(String),
  flag: String,
) -> Result(String, Nil) {
  case args {
    [] -> Error(Nil)
    [f, value, ..] if f == flag -> Ok(value)
    [_, ..rest] -> find_flag_value(rest, flag)
  }
}

fn ensure_directory(path: String) -> Nil {
  let _ = make_directory(path)
  Nil
}

@external(erlang, "file", "make_dir")
fn make_directory(path: String) -> Result(Nil, dynamic.Dynamic)

@external(erlang, "example_todo_server_ffi", "get_env")
fn get_env(name: String) -> Result(String, Nil)

