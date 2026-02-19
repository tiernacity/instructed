//// Projections (read models) for the todo application.

import gleam/dict.{type Dict}
import gleam/list
import gleam/option.{None, Some}
import gleam/order
import gleam/string
import instructed/projection.{type ProjectionConfig}
import todo_shared/domain.{
  type TodoEvent, Active, Completed,
  Deleted, DescriptionUpdated, DueDateUpdated, Medium,
  PriorityUpdated, TodoCompleted, TodoCreated, TodoDeleted, TodoReopened,
}
import todo_shared/views.{type ByPriorityState, type TodoView, ByPriorityState, TodoView}

/// State for the all-todos projection
pub type AllTodosState =
  Dict(String, TodoView)

/// Active todos projection - only shows active (not completed/deleted) todos
pub fn active_todos_projection() -> ProjectionConfig(TodoEvent, AllTodosState) {
  projection.new(
    name: "active_todos",
    initial_state: dict.new(),
    handle_event: fn(event, _recorded, state: AllTodosState) {
      case event {
        TodoCreated(id, desc, priority, due_date, created_at) ->
          Ok(dict.insert(
            state,
            id,
            TodoView(id, desc, priority, due_date, Active, created_at, ""),
          ))
        DescriptionUpdated(id, desc) ->
          Ok(update_view(state, id, fn(v) { TodoView(..v, description: desc) }))
        PriorityUpdated(id, priority) ->
          Ok(update_view(state, id, fn(v) { TodoView(..v, priority: priority) }))
        DueDateUpdated(id, due_date) ->
          Ok(update_view(state, id, fn(v) { TodoView(..v, due_date: due_date) }))
        TodoCompleted(id, completed_at) -> {
          // Shadow-copy so we can restore on TodoReopened
          case dict.get(state, id) {
            Ok(v) -> {
              let state = dict.delete(state, id)
              Ok(dict.insert(state, "completed:" <> id, TodoView(..v, status: Completed, completed_at: completed_at)))
            }
            Error(_) -> Ok(dict.delete(state, id))
          }
        }
        TodoReopened(id) -> {
          case dict.get(state, "completed:" <> id) {
            Ok(v) -> {
              let state = dict.delete(state, "completed:" <> id)
              Ok(dict.insert(state, id, TodoView(..v, status: Active, completed_at: "")))
            }
            Error(_) -> Ok(state)
          }
        }
        TodoDeleted(id) -> {
          let state = dict.delete(state, id)
          Ok(dict.delete(state, "completed:" <> id))
        }
      }
    },
  )
}

/// Completed todos projection
pub fn completed_todos_projection() -> ProjectionConfig(
  TodoEvent,
  AllTodosState,
) {
  projection.new(
    name: "completed_todos",
    initial_state: dict.new(),
    handle_event: fn(event, _recorded, state: AllTodosState) {
      case event {
        TodoCreated(id, desc, priority, due_date, created_at) -> {
          let view =
            TodoView(id, desc, priority, due_date, Active, created_at, "")
          Ok(dict.insert(state, "pending:" <> id, view))
        }
        DescriptionUpdated(id, desc) -> {
          let state =
            update_view(state, "pending:" <> id, fn(v) {
              TodoView(..v, description: desc)
            })
          let state =
            update_view(state, id, fn(v) { TodoView(..v, description: desc) })
          Ok(state)
        }
        PriorityUpdated(id, priority) -> {
          let state =
            update_view(state, "pending:" <> id, fn(v) {
              TodoView(..v, priority: priority)
            })
          let state =
            update_view(state, id, fn(v) { TodoView(..v, priority: priority) })
          Ok(state)
        }
        DueDateUpdated(id, due_date) -> {
          let state =
            update_view(state, "pending:" <> id, fn(v) {
              TodoView(..v, due_date: due_date)
            })
          let state =
            update_view(state, id, fn(v) { TodoView(..v, due_date: due_date) })
          Ok(state)
        }
        TodoCompleted(id, completed_at) -> {
          let view = case dict.get(state, "pending:" <> id) {
            Ok(v) ->
              TodoView(..v, status: Completed, completed_at: completed_at)
            Error(_) ->
              TodoView(id, "", Medium, "", Completed, "", completed_at)
          }
          let state = dict.delete(state, "pending:" <> id)
          Ok(dict.insert(state, id, view))
        }
        TodoReopened(id) -> {
          case dict.get(state, id) {
            Ok(v) -> {
              let state = dict.delete(state, id)
              Ok(dict.insert(state, "pending:" <> id, TodoView(..v, status: Active, completed_at: "")))
            }
            Error(_) -> Ok(state)
          }
        }
        TodoDeleted(id) -> {
          let state = dict.delete(state, id)
          let state = dict.delete(state, "pending:" <> id)
          Ok(state)
        }
      }
    },
  )
}

/// All todos projection (full view of everything)
pub fn all_todos_projection() -> ProjectionConfig(TodoEvent, AllTodosState) {
  projection.new(
    name: "all_todos",
    initial_state: dict.new(),
    handle_event: fn(event, _recorded, state: AllTodosState) {
      case event {
        TodoCreated(id, desc, priority, due_date, created_at) ->
          Ok(dict.insert(
            state,
            id,
            TodoView(id, desc, priority, due_date, Active, created_at, ""),
          ))
        DescriptionUpdated(id, desc) ->
          Ok(update_view(state, id, fn(v) { TodoView(..v, description: desc) }))
        PriorityUpdated(id, priority) ->
          Ok(update_view(state, id, fn(v) { TodoView(..v, priority: priority) }))
        DueDateUpdated(id, due_date) ->
          Ok(update_view(state, id, fn(v) { TodoView(..v, due_date: due_date) }))
        TodoCompleted(id, completed_at) ->
          Ok(update_view(state, id, fn(v) {
            TodoView(..v, status: Completed, completed_at: completed_at)
          }))
        TodoReopened(id) ->
          Ok(update_view(state, id, fn(v) {
            TodoView(..v, status: Active, completed_at: "")
          }))
        TodoDeleted(id) ->
          Ok(update_view(state, id, fn(v) { TodoView(..v, status: Deleted) }))
      }
    },
  )
}

/// Overdue todos projection - tracks todos past their due date
pub fn overdue_todos_projection(
  today: String,
) -> ProjectionConfig(TodoEvent, AllTodosState) {
  projection.new(
    name: "overdue_todos",
    initial_state: dict.new(),
    handle_event: fn(event, _recorded, state: AllTodosState) {
      case event {
        TodoCreated(id, desc, priority, due_date, created_at) -> {
          case is_overdue(due_date, today) {
            True ->
              Ok(dict.insert(
                state,
                id,
                TodoView(id, desc, priority, due_date, Active, created_at, ""),
              ))
            False -> {
              Ok(dict.insert(
                state,
                "maybe:" <> id,
                TodoView(id, desc, priority, due_date, Active, created_at, ""),
              ))
            }
          }
        }
        DueDateUpdated(id, due_date) -> {
          let state = dict.delete(state, "maybe:" <> id)
          let state = dict.delete(state, id)
          let view = case dict.get(state, id) {
            Ok(v) -> TodoView(..v, due_date: due_date)
            Error(_) ->
              case dict.get(state, "maybe:" <> id) {
                Ok(v) -> TodoView(..v, due_date: due_date)
                Error(_) -> TodoView(id, "", Medium, due_date, Active, "", "")
              }
          }
          case is_overdue(due_date, today) && view.status == Active {
            True -> Ok(dict.insert(state, id, view))
            False -> Ok(dict.insert(state, "maybe:" <> id, view))
          }
        }
        TodoCompleted(id, _) -> {
          let state = dict.delete(state, id)
          let state = dict.delete(state, "maybe:" <> id)
          Ok(state)
        }
        TodoDeleted(id) -> {
          let state = dict.delete(state, id)
          let state = dict.delete(state, "maybe:" <> id)
          Ok(state)
        }
        _ -> Ok(state)
      }
    },
  )
}

/// By priority projection - groups todos by priority level
pub fn by_priority_projection() -> ProjectionConfig(
  TodoEvent,
  ByPriorityState,
) {
  projection.new(
    name: "by_priority",
    initial_state: ByPriorityState([], [], [], []),
    handle_event: fn(event, _recorded, state: ByPriorityState) {
      case event {
        TodoCreated(id, desc, priority, due_date, created_at) -> {
          let view =
            TodoView(id, desc, priority, due_date, Active, created_at, "")
          Ok(add_to_priority(state, view))
        }
        PriorityUpdated(id, new_priority) -> {
          let state = remove_from_all_priorities(state, id)
          case find_view_in_priorities(state, id) {
            Some(view) -> {
              let updated = TodoView(..view, priority: new_priority)
              Ok(add_to_priority(state, updated))
            }
            None -> Ok(state)
          }
        }
        TodoCompleted(id, _) -> {
          // Mark as Completed (keep in list so TodoReopened can restore)
          Ok(update_in_all_priorities(state, id, fn(v) {
            TodoView(..v, status: Completed)
          }))
        }
        TodoDeleted(id) -> Ok(remove_from_all_priorities(state, id))
        TodoReopened(id) -> {
          Ok(update_in_all_priorities(state, id, fn(v) {
            TodoView(..v, status: Active, completed_at: "")
          }))
        }
        _ -> Ok(state)
      }
    },
  )
}

/// By due date projection - sorted list
pub fn by_due_date_projection() -> ProjectionConfig(
  TodoEvent,
  List(TodoView),
) {
  projection.new(
    name: "by_due_date",
    initial_state: [],
    handle_event: fn(event, _recorded, state: List(TodoView)) {
      case event {
        TodoCreated(id, desc, priority, due_date, created_at) -> {
          let view =
            TodoView(id, desc, priority, due_date, Active, created_at, "")
          let new_state = [view, ..state]
          Ok(sort_by_due_date(new_state))
        }
        DueDateUpdated(id, due_date) -> {
          let new_state =
            list.map(state, fn(v) {
              case v.id == id {
                True -> TodoView(..v, due_date: due_date)
                False -> v
              }
            })
          Ok(sort_by_due_date(new_state))
        }
        TodoCompleted(id, _) ->
          Ok(list.filter(state, fn(v) { v.id != id }))
        TodoDeleted(id) ->
          Ok(list.filter(state, fn(v) { v.id != id }))
        _ -> Ok(state)
      }
    },
  )
}

// --- Helpers ---

fn update_view(
  state: AllTodosState,
  id: String,
  updater: fn(TodoView) -> TodoView,
) -> AllTodosState {
  case dict.get(state, id) {
    Ok(view) -> dict.insert(state, id, updater(view))
    Error(_) -> state
  }
}

fn is_overdue(due_date: String, today: String) -> Bool {
  due_date != "" && string.compare(due_date, today) == order.Lt
}

fn add_to_priority(state: ByPriorityState, view: TodoView) -> ByPriorityState {
  case view.priority {
    domain.Critical ->
      ByPriorityState(..state, critical: [view, ..state.critical])
    domain.High -> ByPriorityState(..state, high: [view, ..state.high])
    domain.Medium -> ByPriorityState(..state, medium: [view, ..state.medium])
    domain.Low -> ByPriorityState(..state, low: [view, ..state.low])
  }
}

fn remove_from_all_priorities(
  state: ByPriorityState,
  id: String,
) -> ByPriorityState {
  ByPriorityState(
    critical: list.filter(state.critical, fn(v) { v.id != id }),
    high: list.filter(state.high, fn(v) { v.id != id }),
    medium: list.filter(state.medium, fn(v) { v.id != id }),
    low: list.filter(state.low, fn(v) { v.id != id }),
  )
}

fn find_view_in_priorities(
  state: ByPriorityState,
  id: String,
) -> option.Option(TodoView) {
  let all =
    list.flatten([state.critical, state.high, state.medium, state.low])
  case list.find(all, fn(v: TodoView) { v.id == id }) {
    Ok(v) -> Some(v)
    Error(_) -> None
  }
}

fn update_in_all_priorities(
  state: ByPriorityState,
  id: String,
  updater: fn(TodoView) -> TodoView,
) -> ByPriorityState {
  let update_list = fn(items: List(TodoView)) {
    list.map(items, fn(v) {
      case v.id == id {
        True -> updater(v)
        False -> v
      }
    })
  }
  ByPriorityState(
    critical: update_list(state.critical),
    high: update_list(state.high),
    medium: update_list(state.medium),
    low: update_list(state.low),
  )
}

/// Filter a ByPriorityState to only include active items.
pub fn filter_active_priorities(state: ByPriorityState) -> ByPriorityState {
  let keep_active = fn(items: List(TodoView)) {
    list.filter(items, fn(v) { v.status == Active })
  }
  ByPriorityState(
    critical: keep_active(state.critical),
    high: keep_active(state.high),
    medium: keep_active(state.medium),
    low: keep_active(state.low),
  )
}

fn sort_by_due_date(todos: List(TodoView)) -> List(TodoView) {
  list.sort(todos, fn(a, b) { string.compare(a.due_date, b.due_date) })
}
