//// HTTP server layer - wraps TodoServer with a mist HTTP API

import gleam/bit_array
import gleam/bytes_tree
import gleam/dict
import gleam/http
import gleam/http/request.{type Request}
import gleam/http/response.{type Response}
import gleam/io
import gleam/json
import gleam/list
import gleam/string
import mist.{type Connection}
import todo_server/server.{type TodoServer}
import todo_shared/codec

/// Start the HTTP server on the given port, backed by the TodoServer
pub fn start(
  srv: TodoServer,
  port: Int,
) -> Result(Nil, String) {
  let handler = fn(req: Request(Connection)) -> Response(mist.ResponseData) {
    handle_request(req, srv)
  }

  let assert Ok(_) =
    mist.new(handler)
    |> mist.port(port)
    |> mist.start()

  io.println("HTTP server listening on port " <> string.inspect(port))
  Ok(Nil)
}

fn handle_request(
  req: Request(Connection),
  srv: TodoServer,
) -> Response(mist.ResponseData) {
  let path = request.path_segments(req)
  case req.method, path {
    http.Post, ["dispatch"] -> handle_dispatch(req, srv)
    http.Get, ["todos"] -> handle_get_all(srv)
    http.Get, ["todos", "active"] -> handle_get_active(srv)
    http.Get, ["todos", "completed"] -> handle_get_completed(srv)
    http.Get, ["todos", "overdue"] -> handle_get_overdue(srv)
    http.Get, ["todos", "by-priority"] -> handle_get_by_priority(srv)
    http.Get, ["todos", "by-due-date"] -> handle_get_by_due_date(srv)
    _, _ -> json_response(404, json.object([#("error", json.string("Not found"))]))
  }
}

fn handle_dispatch(
  req: Request(Connection),
  srv: TodoServer,
) -> Response(mist.ResponseData) {
  case mist.read_body(req, 1024 * 1024) {
    Ok(req) -> {
      case bit_array.to_string(req.body) {
        Ok(body_str) -> {
          case codec.decode_command(body_str) {
            Ok(command) -> {
              case server.dispatch(srv, command) {
                Ok(_) -> json_response(200, json.object([#("ok", json.bool(True))]))
                Error(reason) ->
                  json_response(400, json.object([
                    #("ok", json.bool(False)),
                    #("error", json.string(reason)),
                  ]))
              }
            }
            Error(reason) ->
              json_response(400, json.object([
                #("ok", json.bool(False)),
                #("error", json.string(reason)),
              ]))
          }
        }
        Error(_) ->
          json_response(400, json.object([
            #("ok", json.bool(False)),
            #("error", json.string("Invalid UTF-8 in request body")),
          ]))
      }
    }
    Error(_) ->
      json_response(400, json.object([
        #("ok", json.bool(False)),
        #("error", json.string("Failed to read request body")),
      ]))
  }
}

fn handle_get_all(srv: TodoServer) -> Response(mist.ResponseData) {
  let todos = server.get_all_todos(srv)
  json_response(200, json.array(dict.values(todos), codec.encode_todo_view))
}

fn handle_get_active(srv: TodoServer) -> Response(mist.ResponseData) {
  let todos = server.get_active_todos(srv)
  json_response(200, json.array(dict.values(todos), codec.encode_todo_view))
}

fn handle_get_completed(srv: TodoServer) -> Response(mist.ResponseData) {
  let todos = server.get_completed_todos(srv)
  let filtered =
    dict.to_list(todos)
    |> list.filter(fn(pair) { !string.starts_with(pair.0, "pending:") })
    |> list.map(fn(pair) { pair.1 })
  json_response(200, json.array(filtered, codec.encode_todo_view))
}

fn handle_get_overdue(srv: TodoServer) -> Response(mist.ResponseData) {
  let todos = server.get_overdue_todos(srv)
  let filtered =
    dict.to_list(todos)
    |> list.filter(fn(pair) { !string.starts_with(pair.0, "maybe:") })
    |> list.map(fn(pair) { pair.1 })
  json_response(200, json.array(filtered, codec.encode_todo_view))
}

fn handle_get_by_priority(srv: TodoServer) -> Response(mist.ResponseData) {
  let by_pri = server.get_by_priority(srv)
  let body_str = codec.encode_by_priority(by_pri)
  response.new(200)
  |> response.set_header("content-type", "application/json")
  |> response.set_body(mist.Bytes(bytes_tree.from_string(body_str)))
}

fn handle_get_by_due_date(srv: TodoServer) -> Response(mist.ResponseData) {
  let todos = server.get_by_due_date(srv)
  json_response(200, json.array(todos, codec.encode_todo_view))
}

// --- JSON helpers ---

fn json_response(
  status: Int,
  body: json.Json,
) -> Response(mist.ResponseData) {
  let body_str = json.to_string(body)
  response.new(status)
  |> response.set_header("content-type", "application/json")
  |> response.set_body(mist.Bytes(bytes_tree.from_string(body_str)))
}
