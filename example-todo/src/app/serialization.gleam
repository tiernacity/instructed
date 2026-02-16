//// JSON serialization for todo events

import gleam/dynamic/decode
import gleam/json
import gleam/string
import app/domain.{
  type TodoEvent, Critical, DueDateUpdated, DescriptionUpdated, High, Low, Medium,
  PriorityUpdated, TodoCompleted, TodoCreated, TodoDeleted, TodoReopened,
}

/// Serialize a todo event to JSON string
pub fn serialize_event(event: TodoEvent) -> String {
  case event {
    TodoCreated(id, desc, priority, due_date, created_at) ->
      json.object([
        #("type", json.string("TodoCreated")),
        #("id", json.string(id)),
        #("description", json.string(desc)),
        #("priority", json.string(domain.priority_to_string(priority))),
        #("due_date", json.string(due_date)),
        #("created_at", json.string(created_at)),
      ])
      |> json.to_string

    DescriptionUpdated(id, desc) ->
      json.object([
        #("type", json.string("DescriptionUpdated")),
        #("id", json.string(id)),
        #("description", json.string(desc)),
      ])
      |> json.to_string

    PriorityUpdated(id, priority) ->
      json.object([
        #("type", json.string("PriorityUpdated")),
        #("id", json.string(id)),
        #("priority", json.string(domain.priority_to_string(priority))),
      ])
      |> json.to_string

    DueDateUpdated(id, due_date) ->
      json.object([
        #("type", json.string("DueDateUpdated")),
        #("id", json.string(id)),
        #("due_date", json.string(due_date)),
      ])
      |> json.to_string

    TodoCompleted(id, completed_at) ->
      json.object([
        #("type", json.string("TodoCompleted")),
        #("id", json.string(id)),
        #("completed_at", json.string(completed_at)),
      ])
      |> json.to_string

    TodoReopened(id) ->
      json.object([
        #("type", json.string("TodoReopened")),
        #("id", json.string(id)),
      ])
      |> json.to_string

    TodoDeleted(id) ->
      json.object([
        #("type", json.string("TodoDeleted")),
        #("id", json.string(id)),
      ])
      |> json.to_string
  }
}

/// Deserialize a JSON string to a todo event
pub fn deserialize_event(json_str: String) -> Result(TodoEvent, String) {
  let type_decoder = decode.at(["type"], decode.string)
  case json.parse(json_str, type_decoder) {
    Ok("TodoCreated") -> {
      let decoder = {
        use id <- decode.field("id", decode.string)
        use desc <- decode.field("description", decode.string)
        use pri_str <- decode.field("priority", decode.string)
        use due <- decode.field("due_date", decode.string)
        use created <- decode.field("created_at", decode.string)
        let priority = parse_priority(pri_str)
        decode.success(TodoCreated(id, desc, priority, due, created))
      }
      case json.parse(json_str, decoder) {
        Ok(event) -> Ok(event)
        Error(_) -> Error("Failed to decode TodoCreated")
      }
    }
    Ok("DescriptionUpdated") -> {
      let decoder = {
        use id <- decode.field("id", decode.string)
        use desc <- decode.field("description", decode.string)
        decode.success(DescriptionUpdated(id, desc))
      }
      case json.parse(json_str, decoder) {
        Ok(event) -> Ok(event)
        Error(_) -> Error("Failed to decode DescriptionUpdated")
      }
    }
    Ok("PriorityUpdated") -> {
      let decoder = {
        use id <- decode.field("id", decode.string)
        use pri_str <- decode.field("priority", decode.string)
        let priority = parse_priority(pri_str)
        decode.success(PriorityUpdated(id, priority))
      }
      case json.parse(json_str, decoder) {
        Ok(event) -> Ok(event)
        Error(_) -> Error("Failed to decode PriorityUpdated")
      }
    }
    Ok("DueDateUpdated") -> {
      let decoder = {
        use id <- decode.field("id", decode.string)
        use due <- decode.field("due_date", decode.string)
        decode.success(DueDateUpdated(id, due))
      }
      case json.parse(json_str, decoder) {
        Ok(event) -> Ok(event)
        Error(_) -> Error("Failed to decode DueDateUpdated")
      }
    }
    Ok("TodoCompleted") -> {
      let decoder = {
        use id <- decode.field("id", decode.string)
        use at <- decode.field("completed_at", decode.string)
        decode.success(TodoCompleted(id, at))
      }
      case json.parse(json_str, decoder) {
        Ok(event) -> Ok(event)
        Error(_) -> Error("Failed to decode TodoCompleted")
      }
    }
    Ok("TodoReopened") -> {
      let decoder = {
        use id <- decode.field("id", decode.string)
        decode.success(TodoReopened(id))
      }
      case json.parse(json_str, decoder) {
        Ok(event) -> Ok(event)
        Error(_) -> Error("Failed to decode TodoReopened")
      }
    }
    Ok("TodoDeleted") -> {
      let decoder = {
        use id <- decode.field("id", decode.string)
        decode.success(TodoDeleted(id))
      }
      case json.parse(json_str, decoder) {
        Ok(event) -> Ok(event)
        Error(_) -> Error("Failed to decode TodoDeleted")
      }
    }
    Ok(other) -> Error("Unknown event type: " <> other)
    Error(_) -> Error("Failed to parse event JSON")
  }
}

/// Get the event type name
pub fn event_type_name(event: TodoEvent) -> String {
  case event {
    TodoCreated(..) -> "TodoCreated"
    DescriptionUpdated(..) -> "DescriptionUpdated"
    PriorityUpdated(..) -> "PriorityUpdated"
    DueDateUpdated(..) -> "DueDateUpdated"
    TodoCompleted(..) -> "TodoCompleted"
    TodoReopened(..) -> "TodoReopened"
    TodoDeleted(..) -> "TodoDeleted"
  }
}

fn parse_priority(s: String) -> domain.Priority {
  case string.lowercase(s) {
    "low" -> Low
    "medium" -> Medium
    "high" -> High
    "critical" -> Critical
    _ -> Medium
  }
}
