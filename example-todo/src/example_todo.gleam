//// Example Todo App CLI
//// A command-line todo manager using the Instructed CQRS/ES framework.

import argv
import gleam/dict
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/list
import gleam/string
import app/domain
import app/projections
import app/server
import youid/uuid

const db_path = "todo_events.db"

pub fn main() -> Nil {
  let args = argv.load().arguments

  case args {
    ["server"] -> start_server()
    ["add", description, priority, due_date] ->
      run_command(fn(srv) { add_todo(srv, description, priority, due_date) })
    ["add", description, priority] ->
      run_command(fn(srv) { add_todo(srv, description, priority, "") })
    ["add", description] ->
      run_command(fn(srv) { add_todo(srv, description, "medium", "") })
    ["complete", id] -> run_command(fn(srv) { complete_todo(srv, id) })
    ["reopen", id] -> run_command(fn(srv) { reopen_todo(srv, id) })
    ["delete", id] -> run_command(fn(srv) { delete_todo(srv, id) })
    ["edit", id, "description", new_desc] ->
      run_command(fn(srv) { edit_description(srv, id, new_desc) })
    ["edit", id, "priority", new_priority] ->
      run_command(fn(srv) { edit_priority(srv, id, new_priority) })
    ["edit", id, "due", new_due] ->
      run_command(fn(srv) { edit_due_date(srv, id, new_due) })
    ["list"] -> run_query(fn(srv) { list_all(srv) })
    ["list", "active"] -> run_query(fn(srv) { list_active(srv) })
    ["list", "completed"] -> run_query(fn(srv) { list_completed(srv) })
    ["list", "overdue"] -> run_query(fn(srv) { list_overdue(srv) })
    ["list", "by-priority"] -> run_query(fn(srv) { list_by_priority(srv) })
    ["list", "by-due-date"] -> run_query(fn(srv) { list_by_due_date(srv) })
    ["reset"] -> run_command(fn(srv) { reset_store(srv) })
    ["help"] -> print_help()
    _ -> print_help()
  }
}

fn start_server() -> Nil {
  io.println("Starting Todo Server...")
  let assert Ok(_srv) = server.start(db_path)
  io.println("Todo Server started. Press Ctrl+C to stop.")
  // Keep the server running
  process.sleep_forever()
}

fn run_command(f: fn(server.TodoServer) -> Nil) -> Nil {
  let assert Ok(srv) = server.start(db_path)
  // Give projections time to catch up
  process.sleep(200)
  f(srv)
}

fn run_query(f: fn(server.TodoServer) -> Nil) -> Nil {
  let assert Ok(srv) = server.start(db_path)
  // Give projections time to catch up with historical events
  process.sleep(500)
  f(srv)
}

fn add_todo(
  srv: server.TodoServer,
  description: String,
  priority_str: String,
  due_date: String,
) -> Nil {
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

  case
    server.dispatch(srv, domain.CreateTodo(id, description, priority, due_date))
  {
    Ok(_) -> {
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
    Error(reason) -> io.println("✗ Failed to create todo: " <> reason)
  }
}

fn complete_todo(srv: server.TodoServer, id: String) -> Nil {
  case server.dispatch(srv, domain.CompleteTodo(id)) {
    Ok(_) -> io.println("✓ Todo completed: " <> id)
    Error(reason) -> io.println("✗ Failed: " <> reason)
  }
}

fn reopen_todo(srv: server.TodoServer, id: String) -> Nil {
  case server.dispatch(srv, domain.ReopenTodo(id)) {
    Ok(_) -> io.println("✓ Todo reopened: " <> id)
    Error(reason) -> io.println("✗ Failed: " <> reason)
  }
}

fn delete_todo(srv: server.TodoServer, id: String) -> Nil {
  case server.dispatch(srv, domain.DeleteTodo(id)) {
    Ok(_) -> io.println("✓ Todo deleted: " <> id)
    Error(reason) -> io.println("✗ Failed: " <> reason)
  }
}

fn edit_description(
  srv: server.TodoServer,
  id: String,
  new_desc: String,
) -> Nil {
  case server.dispatch(srv, domain.UpdateDescription(id, new_desc)) {
    Ok(_) -> io.println("✓ Description updated for: " <> id)
    Error(reason) -> io.println("✗ Failed: " <> reason)
  }
}

fn edit_priority(
  srv: server.TodoServer,
  id: String,
  new_priority: String,
) -> Nil {
  case domain.priority_from_string(new_priority) {
    Ok(priority) ->
      case server.dispatch(srv, domain.UpdatePriority(id, priority)) {
        Ok(_) -> io.println("✓ Priority updated for: " <> id)
        Error(reason) -> io.println("✗ Failed: " <> reason)
      }
    Error(reason) -> io.println("✗ " <> reason)
  }
}

fn edit_due_date(
  srv: server.TodoServer,
  id: String,
  new_due: String,
) -> Nil {
  case server.dispatch(srv, domain.UpdateDueDate(id, new_due)) {
    Ok(_) -> io.println("✓ Due date updated for: " <> id)
    Error(reason) -> io.println("✗ Failed: " <> reason)
  }
}

fn list_all(srv: server.TodoServer) -> Nil {
  let todos = server.get_all_todos(srv)
  let items = dict.values(todos)
  io.println("═══ All Todos (" <> int.to_string(list.length(items)) <> ") ═══")
  list.each(items, print_todo_view)
}

fn list_active(srv: server.TodoServer) -> Nil {
  let todos = server.get_active_todos(srv)
  let items = dict.values(todos)
  io.println(
    "═══ Active Todos (" <> int.to_string(list.length(items)) <> ") ═══",
  )
  list.each(items, print_todo_view)
}

fn list_completed(srv: server.TodoServer) -> Nil {
  let todos = server.get_completed_todos(srv)
  // Filter out pending: keys
  let items =
    dict.to_list(todos)
    |> list.filter(fn(pair) { !string.starts_with(pair.0, "pending:") })
    |> list.map(fn(pair) { pair.1 })
  io.println(
    "═══ Completed Todos (" <> int.to_string(list.length(items)) <> ") ═══",
  )
  list.each(items, print_todo_view)
}

fn list_overdue(srv: server.TodoServer) -> Nil {
  let todos = server.get_overdue_todos(srv)
  // Filter out maybe: keys
  let items =
    dict.to_list(todos)
    |> list.filter(fn(pair) { !string.starts_with(pair.0, "maybe:") })
    |> list.map(fn(pair) { pair.1 })
  io.println(
    "═══ Overdue Todos (" <> int.to_string(list.length(items)) <> ") ═══",
  )
  list.each(items, print_todo_view)
}

fn list_by_priority(srv: server.TodoServer) -> Nil {
  let by_pri = server.get_by_priority(srv)
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
    "\n🟡 MEDIUM (" <> int.to_string(list.length(by_pri.medium)) <> ")",
  )
  list.each(by_pri.medium, print_todo_view)

  io.println(
    "\n🟢 LOW (" <> int.to_string(list.length(by_pri.low)) <> ")",
  )
  list.each(by_pri.low, print_todo_view)
}

fn list_by_due_date(srv: server.TodoServer) -> Nil {
  let todos = server.get_by_due_date(srv)
  io.println(
    "═══ Todos by Due Date ("
    <> int.to_string(list.length(todos))
    <> ") ═══",
  )
  list.each(todos, print_todo_view)
}

fn reset_store(srv: server.TodoServer) -> Nil {
  case srv.event_store.reset() {
    Ok(Nil) -> io.println("✓ Event store reset")
    Error(_) -> io.println("✗ Failed to reset event store")
  }
}

fn print_todo_view(item: projections.TodoView) -> Nil {
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
  io.println("  todo server                           Start the server")
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

