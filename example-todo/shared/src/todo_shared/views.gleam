//// Read model types for the Todo application.

import todo_shared/domain.{type Priority, type Status}

/// A read-model todo item (for projections and API responses)
pub type TodoView {
  TodoView(
    id: String,
    description: String,
    priority: Priority,
    due_date: String,
    status: Status,
    created_at: String,
    completed_at: String,
  )
}

/// State for grouping todos by priority
pub type ByPriorityState {
  ByPriorityState(
    critical: List(TodoView),
    high: List(TodoView),
    medium: List(TodoView),
    low: List(TodoView),
  )
}
