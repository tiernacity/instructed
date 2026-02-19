//// Todo CLI - lightweight Deno-based client for the Todo CQRS/ES server.
////
//// All commands talk to the running server via HTTP.

import argv
import gleam/int
import gleam/io
import gleam/list
import gleam/javascript/promise.{type Promise}
import gleam/string
import todo_client/http
import todo_shared/domain
import todo_shared/views
import youid/uuid

const default_port = 8400

pub fn main() -> Nil {
  let args = argv.load().arguments
  let _ = run(args)
  Nil
}

fn run(args: List(String)) -> Promise(Nil) {
  case args {
    ["add", description, priority, due_date] ->
      add_todo(description, priority, due_date)
    ["add", description, priority] -> add_todo(description, priority, "")
    ["add", description] -> add_todo(description, "medium", "")
    ["complete", id] -> complete_todo(id)
    ["reopen", id] -> reopen_todo(id)
    ["delete", id] -> delete_todo(id)
    ["edit", id, "description", new_desc] -> edit_description(id, new_desc)
    ["edit", id, "priority", new_priority] -> edit_priority(id, new_priority)
    ["edit", id, "due", new_due] -> edit_due_date(id, new_due)
    ["list"] -> list_all()
    ["list", "active"] -> list_active()
    ["list", "completed"] -> list_completed()
    ["list", "overdue"] -> list_overdue()
    ["list", "by-priority"] -> list_by_priority()
    ["list", "by-due-date"] -> list_by_due_date()
    ["reset"] -> reset_store()
    _ -> {
      print_help()
      promise.resolve(Nil)
    }
  }
}

// --- Commands (via HTTP) ---

fn add_todo(
  description: String,
  priority_str: String,
  due_date: String,
) -> Promise(Nil) {
  let id = string.slice(uuid.v4_string(), 0, 8)
  let priority = case domain.priority_from_string(priority_str) {
    Ok(p) -> p
    Error(_) -> {
      io.println(
        "Invalid priority: "
        <> priority_str
        <> ". Use: low, medium, high, critical",
      )
      domain.Medium
    }
  }

  http.dispatch(
    default_port,
    domain.CreateTodo(id, description, priority, due_date),
  )
  |> promise.map(fn(result) {
    case result {
      Ok(http.DispatchOk) -> {
        io.println("✓ Todo created: " <> id)
        io.println("  Description: " <> description)
        io.println(
          "  Priority: " <> domain.priority_to_string(priority),
        )
        case due_date {
          "" -> Nil
          d -> io.println("  Due: " <> d)
        }
      }
      Ok(http.DispatchError(reason)) ->
        io.println("✗ Failed to create todo: " <> reason)
      Error(reason) -> io.println("✗ " <> reason)
    }
  })
}

fn complete_todo(id: String) -> Promise(Nil) {
  http.dispatch(default_port, domain.CompleteTodo(id))
  |> promise.map(fn(result) {
    case result {
      Ok(http.DispatchOk) -> io.println("✓ Todo completed: " <> id)
      Ok(http.DispatchError(reason)) -> io.println("✗ Failed: " <> reason)
      Error(reason) -> io.println("✗ " <> reason)
    }
  })
}

fn reopen_todo(id: String) -> Promise(Nil) {
  http.dispatch(default_port, domain.ReopenTodo(id))
  |> promise.map(fn(result) {
    case result {
      Ok(http.DispatchOk) -> io.println("✓ Todo reopened: " <> id)
      Ok(http.DispatchError(reason)) -> io.println("✗ Failed: " <> reason)
      Error(reason) -> io.println("✗ " <> reason)
    }
  })
}

fn delete_todo(id: String) -> Promise(Nil) {
  http.dispatch(default_port, domain.DeleteTodo(id))
  |> promise.map(fn(result) {
    case result {
      Ok(http.DispatchOk) -> io.println("✓ Todo deleted: " <> id)
      Ok(http.DispatchError(reason)) -> io.println("✗ Failed: " <> reason)
      Error(reason) -> io.println("✗ " <> reason)
    }
  })
}

fn edit_description(id: String, new_desc: String) -> Promise(Nil) {
  http.dispatch(default_port, domain.UpdateDescription(id, new_desc))
  |> promise.map(fn(result) {
    case result {
      Ok(http.DispatchOk) ->
        io.println("✓ Description updated for: " <> id)
      Ok(http.DispatchError(reason)) -> io.println("✗ Failed: " <> reason)
      Error(reason) -> io.println("✗ " <> reason)
    }
  })
}

fn edit_priority(id: String, new_priority: String) -> Promise(Nil) {
  case domain.priority_from_string(new_priority) {
    Ok(priority) ->
      http.dispatch(default_port, domain.UpdatePriority(id, priority))
      |> promise.map(fn(result) {
        case result {
          Ok(http.DispatchOk) ->
            io.println("✓ Priority updated for: " <> id)
          Ok(http.DispatchError(reason)) ->
            io.println("✗ Failed: " <> reason)
          Error(reason) -> io.println("✗ " <> reason)
        }
      })
    Error(reason) -> {
      io.println("✗ " <> reason)
      promise.resolve(Nil)
    }
  }
}

fn edit_due_date(id: String, new_due: String) -> Promise(Nil) {
  http.dispatch(default_port, domain.UpdateDueDate(id, new_due))
  |> promise.map(fn(result) {
    case result {
      Ok(http.DispatchOk) ->
        io.println("✓ Due date updated for: " <> id)
      Ok(http.DispatchError(reason)) -> io.println("✗ Failed: " <> reason)
      Error(reason) -> io.println("✗ " <> reason)
    }
  })
}

// --- Queries (via HTTP) ---

fn list_all() -> Promise(Nil) {
  http.get_all_todos(default_port)
  |> promise.map(fn(result) {
    case result {
      Ok(items) -> {
        io.println(
          "═══ All Todos (" <> int.to_string(list.length(items)) <> ") ═══",
        )
        list.each(items, print_todo_view)
      }
      Error(reason) -> io.println("✗ " <> reason)
    }
  })
}

fn list_active() -> Promise(Nil) {
  http.get_active_todos(default_port)
  |> promise.map(fn(result) {
    case result {
      Ok(items) -> {
        io.println(
          "═══ Active Todos ("
          <> int.to_string(list.length(items))
          <> ") ═══",
        )
        list.each(items, print_todo_view)
      }
      Error(reason) -> io.println("✗ " <> reason)
    }
  })
}

fn list_completed() -> Promise(Nil) {
  http.get_completed_todos(default_port)
  |> promise.map(fn(result) {
    case result {
      Ok(items) -> {
        io.println(
          "═══ Completed Todos ("
          <> int.to_string(list.length(items))
          <> ") ═══",
        )
        list.each(items, print_todo_view)
      }
      Error(reason) -> io.println("✗ " <> reason)
    }
  })
}

fn list_overdue() -> Promise(Nil) {
  http.get_overdue_todos(default_port)
  |> promise.map(fn(result) {
    case result {
      Ok(items) -> {
        io.println(
          "═══ Overdue Todos ("
          <> int.to_string(list.length(items))
          <> ") ═══",
        )
        list.each(items, print_todo_view)
      }
      Error(reason) -> io.println("✗ " <> reason)
    }
  })
}

fn list_by_priority() -> Promise(Nil) {
  http.get_by_priority(default_port)
  |> promise.map(fn(result) {
    case result {
      Ok(by_pri) -> {
        io.println("═══ Todos by Priority ═══")
        io.println(
          "\n🔴 CRITICAL ("
          <> int.to_string(list.length(by_pri.critical))
          <> ")",
        )
        list.each(by_pri.critical, print_todo_view)
        io.println(
          "\n🟠 HIGH (" <> int.to_string(list.length(by_pri.high)) <> ")",
        )
        list.each(by_pri.high, print_todo_view)
        io.println(
          "\n🟡 MEDIUM ("
          <> int.to_string(list.length(by_pri.medium))
          <> ")",
        )
        list.each(by_pri.medium, print_todo_view)
        io.println(
          "\n🟢 LOW (" <> int.to_string(list.length(by_pri.low)) <> ")",
        )
        list.each(by_pri.low, print_todo_view)
      }
      Error(reason) -> io.println("✗ " <> reason)
    }
  })
}

fn list_by_due_date() -> Promise(Nil) {
  http.get_by_due_date(default_port)
  |> promise.map(fn(result) {
    case result {
      Ok(todos) -> {
        io.println(
          "═══ Todos by Due Date ("
          <> int.to_string(list.length(todos))
          <> ") ═══",
        )
        list.each(todos, print_todo_view)
      }
      Error(reason) -> io.println("✗ " <> reason)
    }
  })
}

fn reset_store() -> Promise(Nil) {
  http.reset(default_port)
  |> promise.map(fn(result) {
    case result {
      Ok(http.DispatchOk) -> io.println("✓ Event store reset")
      Ok(http.DispatchError(reason)) -> io.println("✗ Failed: " <> reason)
      Error(reason) -> io.println("✗ " <> reason)
    }
  })
}

// --- Display ---

fn print_todo_view(item: views.TodoView) -> Nil {
  let priority_icon = case item.priority {
    domain.Critical -> "🔴"
    domain.High -> "🟠"
    domain.Medium -> "🟡"
    domain.Low -> "🟢"
  }

  let status_icon = case item.status {
    domain.Active -> "☐"
    domain.Completed -> "☑"
    domain.Deleted -> "✗"
  }

  let due_str = case item.due_date {
    "" -> ""
    d -> " | Due: " <> d
  }

  let completed_str = case item.completed_at {
    "" -> ""
    c -> " | Completed: " <> c
  }

  io.println(
    "  "
    <> status_icon
    <> " ["
    <> item.id
    <> "] "
    <> priority_icon
    <> " "
    <> item.description
    <> due_str
    <> completed_str,
  )
}

fn print_help() -> Nil {
  io.println("Todo CLI - CQRS/ES Todo Manager")
  io.println("")
  io.println("Usage:")
  io.println(
    "  todo add <description> [priority] [due]  Add a todo",
  )
  io.println("  todo complete <id>                    Complete a todo")
  io.println("  todo reopen <id>                      Reopen a todo")
  io.println("  todo delete <id>                      Delete a todo")
  io.println(
    "  todo edit <id> description <text>      Edit description",
  )
  io.println(
    "  todo edit <id> priority <priority>     Edit priority",
  )
  io.println(
    "  todo edit <id> due <date>              Edit due date",
  )
  io.println("  todo list                             List all todos")
  io.println(
    "  todo list active                      List active todos",
  )
  io.println(
    "  todo list completed                   List completed todos",
  )
  io.println(
    "  todo list overdue                     List overdue todos",
  )
  io.println(
    "  todo list by-priority                 Group by priority",
  )
  io.println(
    "  todo list by-due-date                 Sort by due date",
  )
  io.println("  todo reset                            Reset event store")
  io.println("")
  io.println("Priorities: low, medium, high, critical")
  io.println("Due date format: YYYY-MM-DD")
}
