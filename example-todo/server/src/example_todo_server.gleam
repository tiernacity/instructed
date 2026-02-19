//// Todo Server - starts the CQRS HTTP server on a configurable port.

import argv
import gleam/erlang/process
import gleam/int
import gleam/io
import todo_server/http
import todo_server/server

const db_path = "todo_events.db"

const default_port = 8400

pub fn main() -> Nil {
  let args = argv.load().arguments
  let port = case args {
    [port_str, ..] ->
      case int.parse(port_str) {
        Ok(p) -> p
        Error(_) -> {
          io.println("Invalid port: " <> port_str <> ", using default 8400")
          default_port
        }
      }
    _ -> default_port
  }

  io.println("Starting Todo Server...")
  let assert Ok(srv) = server.start(db_path)
  let assert Ok(_) = http.start(srv, port)
  io.println(
    "Todo Server started on port "
    <> int.to_string(port)
    <> ". Press Ctrl+C to stop.",
  )
  process.sleep_forever()
}
