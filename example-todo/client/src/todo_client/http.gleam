//// HTTP client - sends commands and queries to the todo HTTP server via fetch

import gleam/fetch
import gleam/http
import gleam/http/request
import gleam/int
import gleam/javascript/promise.{type Promise}
import todo_shared/codec
import todo_shared/domain
import todo_shared/views.{type ByPriorityState, type TodoView}

/// Result of a dispatch call
pub type DispatchResult {
  DispatchOk
  DispatchError(reason: String)
}

const connection_error = "Could not connect to server. Is it running? Start with: todo-server"

/// Dispatch a command to the server
pub fn dispatch(
  port: Int,
  command: domain.TodoCommand,
) -> Promise(Result(DispatchResult, String)) {
  let body = codec.encode_command(command)
  post(port, "/dispatch", body)
  |> promise.map(fn(result) {
    case result {
      Ok(resp_body) -> {
        case codec.decode_dispatch_result(resp_body) {
          Ok(True) -> Ok(DispatchOk)
          Ok(False) -> Ok(DispatchError(codec.decode_error_message(resp_body)))
          Error(_) -> Error("Invalid response from server")
        }
      }
      Error(_) -> Error(connection_error)
    }
  })
}

/// Get all todos
pub fn get_all_todos(port: Int) -> Promise(Result(List(TodoView), String)) {
  get_todo_list(port, "/todos")
}

/// Get active todos
pub fn get_active_todos(port: Int) -> Promise(Result(List(TodoView), String)) {
  get_todo_list(port, "/todos/active")
}

/// Get completed todos
pub fn get_completed_todos(
  port: Int,
) -> Promise(Result(List(TodoView), String)) {
  get_todo_list(port, "/todos/completed")
}

/// Get overdue todos
pub fn get_overdue_todos(port: Int) -> Promise(Result(List(TodoView), String)) {
  get_todo_list(port, "/todos/overdue")
}

/// Get todos grouped by priority
pub fn get_by_priority(
  port: Int,
) -> Promise(Result(ByPriorityState, String)) {
  get(port, "/todos/by-priority")
  |> promise.map(fn(result) {
    case result {
      Ok(body) -> codec.decode_by_priority(body)
      Error(_) -> Error(connection_error)
    }
  })
}

/// Get todos sorted by due date
pub fn get_by_due_date(port: Int) -> Promise(Result(List(TodoView), String)) {
  get_todo_list(port, "/todos/by-due-date")
}

// --- Internal helpers ---

fn get_todo_list(
  port: Int,
  path: String,
) -> Promise(Result(List(TodoView), String)) {
  get(port, path)
  |> promise.map(fn(result) {
    case result {
      Ok(body) -> codec.decode_todo_list(body)
      Error(_) -> Error(connection_error)
    }
  })
}

fn get(port: Int, path: String) -> Promise(Result(String, fetch.FetchError)) {
  let assert Ok(req) =
    request.to("http://localhost:" <> int.to_string(port) <> path)
  let req = request.set_method(req, http.Get)
  fetch.send(req)
  |> promise.try_await(fetch.read_text_body)
  |> promise.map(fn(result) {
    case result {
      Ok(resp) -> Ok(resp.body)
      Error(e) -> Error(e)
    }
  })
}

fn post(
  port: Int,
  path: String,
  body: String,
) -> Promise(Result(String, fetch.FetchError)) {
  let assert Ok(req) =
    request.to("http://localhost:" <> int.to_string(port) <> path)
  let req =
    req
    |> request.set_method(http.Post)
    |> request.set_header("content-type", "application/json")
    |> request.set_body(body)
  fetch.send(req)
  |> promise.try_await(fetch.read_text_body)
  |> promise.map(fn(result) {
    case result {
      Ok(resp) -> Ok(resp.body)
      Error(e) -> Error(e)
    }
  })
}
