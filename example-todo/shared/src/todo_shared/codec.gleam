//// JSON encode/decode for commands and views.
//// Shared by both the server (HTTP handler) and client (HTTP calls).

import gleam/dynamic/decode
import gleam/json
import gleam/result
import todo_shared/domain
import todo_shared/views.{type ByPriorityState, type TodoView, ByPriorityState, TodoView}

// --- Encoding ---

/// Encode a TodoCommand to a JSON string
pub fn encode_command(command: domain.TodoCommand) -> String {
  case command {
    domain.CreateTodo(id, description, priority, due_date) ->
      json.object([
        #("type", json.string("CreateTodo")),
        #("id", json.string(id)),
        #("description", json.string(description)),
        #("priority", json.string(domain.priority_to_string(priority))),
        #("due_date", json.string(due_date)),
      ])
      |> json.to_string
    domain.CompleteTodo(id) ->
      json.object([
        #("type", json.string("CompleteTodo")),
        #("id", json.string(id)),
      ])
      |> json.to_string
    domain.ReopenTodo(id) ->
      json.object([
        #("type", json.string("ReopenTodo")),
        #("id", json.string(id)),
      ])
      |> json.to_string
    domain.DeleteTodo(id) ->
      json.object([
        #("type", json.string("DeleteTodo")),
        #("id", json.string(id)),
      ])
      |> json.to_string
    domain.UpdateDescription(id, description) ->
      json.object([
        #("type", json.string("UpdateDescription")),
        #("id", json.string(id)),
        #("description", json.string(description)),
      ])
      |> json.to_string
    domain.UpdatePriority(id, priority) ->
      json.object([
        #("type", json.string("UpdatePriority")),
        #("id", json.string(id)),
        #("priority", json.string(domain.priority_to_string(priority))),
      ])
      |> json.to_string
    domain.UpdateDueDate(id, due_date) ->
      json.object([
        #("type", json.string("UpdateDueDate")),
        #("id", json.string(id)),
        #("due_date", json.string(due_date)),
      ])
      |> json.to_string
  }
}

/// Encode a TodoView as JSON
pub fn encode_todo_view(view: TodoView) -> json.Json {
  json.object([
    #("id", json.string(view.id)),
    #("description", json.string(view.description)),
    #("priority", json.string(domain.priority_to_string(view.priority))),
    #("due_date", json.string(view.due_date)),
    #("status", json.string(domain.status_to_string(view.status))),
    #("created_at", json.string(view.created_at)),
    #("completed_at", json.string(view.completed_at)),
  ])
}

/// Encode a list of TodoViews as a JSON string
pub fn encode_todo_list(todos: List(TodoView)) -> String {
  json.array(todos, encode_todo_view)
  |> json.to_string
}

/// Encode ByPriorityState as a JSON string
pub fn encode_by_priority(by_pri: ByPriorityState) -> String {
  json.object([
    #("critical", json.array(by_pri.critical, encode_todo_view)),
    #("high", json.array(by_pri.high, encode_todo_view)),
    #("medium", json.array(by_pri.medium, encode_todo_view)),
    #("low", json.array(by_pri.low, encode_todo_view)),
  ])
  |> json.to_string
}

// --- Decoding ---

/// Decode a command from a JSON string
pub fn decode_command(body: String) -> Result(domain.TodoCommand, String) {
  let type_decoder = decode.at(["type"], decode.string)
  case json.parse(body, type_decoder) {
    Ok("CreateTodo") -> {
      let decoder = {
        use id <- decode.field("id", decode.string)
        use description <- decode.field("description", decode.string)
        use priority_str <- decode.field("priority", decode.string)
        use due_date <- decode.field("due_date", decode.string)
        case domain.priority_from_string(priority_str) {
          Ok(priority) -> decode.success(domain.CreateTodo(id, description, priority, due_date))
          Error(_) -> decode.success(domain.CreateTodo(id, description, domain.Medium, due_date))
        }
      }
      json.parse(body, decoder)
      |> result.map_error(fn(_) { "Failed to decode CreateTodo" })
    }
    Ok("CompleteTodo") -> {
      let decoder = {
        use id <- decode.field("id", decode.string)
        decode.success(domain.CompleteTodo(id))
      }
      json.parse(body, decoder)
      |> result.map_error(fn(_) { "Failed to decode CompleteTodo" })
    }
    Ok("ReopenTodo") -> {
      let decoder = {
        use id <- decode.field("id", decode.string)
        decode.success(domain.ReopenTodo(id))
      }
      json.parse(body, decoder)
      |> result.map_error(fn(_) { "Failed to decode ReopenTodo" })
    }
    Ok("DeleteTodo") -> {
      let decoder = {
        use id <- decode.field("id", decode.string)
        decode.success(domain.DeleteTodo(id))
      }
      json.parse(body, decoder)
      |> result.map_error(fn(_) { "Failed to decode DeleteTodo" })
    }
    Ok("UpdateDescription") -> {
      let decoder = {
        use id <- decode.field("id", decode.string)
        use description <- decode.field("description", decode.string)
        decode.success(domain.UpdateDescription(id, description))
      }
      json.parse(body, decoder)
      |> result.map_error(fn(_) { "Failed to decode UpdateDescription" })
    }
    Ok("UpdatePriority") -> {
      let decoder = {
        use id <- decode.field("id", decode.string)
        use priority_str <- decode.field("priority", decode.string)
        case domain.priority_from_string(priority_str) {
          Ok(priority) -> decode.success(domain.UpdatePriority(id, priority))
          Error(_) -> decode.success(domain.UpdatePriority(id, domain.Medium))
        }
      }
      json.parse(body, decoder)
      |> result.map_error(fn(_) { "Failed to decode UpdatePriority" })
    }
    Ok("UpdateDueDate") -> {
      let decoder = {
        use id <- decode.field("id", decode.string)
        use due_date <- decode.field("due_date", decode.string)
        decode.success(domain.UpdateDueDate(id, due_date))
      }
      json.parse(body, decoder)
      |> result.map_error(fn(_) { "Failed to decode UpdateDueDate" })
    }
    Ok(other) -> Error("Unknown command type: " <> other)
    Error(_) -> Error("Failed to parse command JSON")
  }
}

/// Decoder for a single TodoView
pub fn todo_view_decoder() -> decode.Decoder(TodoView) {
  use id <- decode.field("id", decode.string)
  use description <- decode.field("description", decode.string)
  use priority_str <- decode.field("priority", decode.string)
  use due_date <- decode.field("due_date", decode.string)
  use status_str <- decode.field("status", decode.string)
  use created_at <- decode.field("created_at", decode.string)
  use completed_at <- decode.field("completed_at", decode.string)
  let priority = case domain.priority_from_string(priority_str) {
    Ok(p) -> p
    Error(_) -> domain.Medium
  }
  let status = case status_str {
    "active" -> domain.Active
    "completed" -> domain.Completed
    "deleted" -> domain.Deleted
    _ -> domain.Active
  }
  decode.success(TodoView(
    id, description, priority, due_date, status, created_at, completed_at,
  ))
}

/// Decode a JSON string into a list of TodoViews
pub fn decode_todo_list(body: String) -> Result(List(TodoView), String) {
  json.parse(body, decode.list(todo_view_decoder()))
  |> result.map_error(fn(_) { "Failed to decode todo list response" })
}

/// Decode a dispatch result's "ok" field
pub fn decode_dispatch_result(body: String) -> Result(Bool, String) {
  let ok_decoder = decode.at(["ok"], decode.bool)
  json.parse(body, ok_decoder)
  |> result.map_error(fn(_) { "Failed to decode dispatch result" })
}

/// Decode an error message from a dispatch response
pub fn decode_error_message(body: String) -> String {
  let error_decoder = decode.at(["error"], decode.string)
  case json.parse(body, error_decoder) {
    Ok(reason) -> reason
    Error(_) -> "Unknown error"
  }
}

/// Decode a JSON string into ByPriorityState
pub fn decode_by_priority(body: String) -> Result(ByPriorityState, String) {
  let decoder = {
    use critical <- decode.field("critical", decode.list(todo_view_decoder()))
    use high <- decode.field("high", decode.list(todo_view_decoder()))
    use medium <- decode.field("medium", decode.list(todo_view_decoder()))
    use low <- decode.field("low", decode.list(todo_view_decoder()))
    decode.success(ByPriorityState(critical, high, medium, low))
  }
  json.parse(body, decoder)
  |> result.map_error(fn(_) { "Failed to decode by-priority response" })
}
