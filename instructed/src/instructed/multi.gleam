//// Multi - composable aggregate command execution.
////
//// `Multi` allows an aggregate's `execute` function to run multiple
//// command stages in sequence, accumulating events from each stage.
//// If any stage fails, the error short-circuits the chain and **all
//// accumulated events are discarded** — the final `to_result` returns the
//// error. This satisfies Invariant 15: "Multi errors discard all events —
//// atomic".
////
//// This is the Gleam equivalent of `Commanded.Aggregates.Multi`.
////
//// ## How it works
////
//// 1. `new(state)` — start a Multi chain with the current aggregate state
//// 2. `execute(multi, fn(state) -> Result(events, error))` — run a command
////    stage; if there is already an error in the chain this is a no-op
//// 3. `apply(multi, apply_fn)` — fold the accumulated events into the state
////    so that the NEXT execute stage sees an up-to-date aggregate state
//// 4. `to_result(multi)` — collapse the chain into a single
////    `Result(List(event), String)`
////
//// The aggregate's `execute` function returns `Result(List(event), String)`,
//// so Multi integrates transparently — just call `to_result` at the end.
////
//// ## Example
////
//// ```gleam
//// import instructed/multi
////
//// // Inside an aggregate execute callback:
//// execute: fn(state, cmd) {
////   case cmd {
////     ComplexCommand(..) ->
////       multi.new(state)
////       |> multi.execute(fn(s) { validate_and_emit_first_events(s, cmd) })
////       |> multi.apply(apply_event)   // update state so next stage sees changes
////       |> multi.execute(fn(s) { emit_second_events(s, cmd) })
////       |> multi.to_result()
////     _ -> Error("unknown command")
////   }
//// }
//// ```
////
//// ## Reducing a list of commands
////
//// ```gleam
//// let items = [item1, item2, item3]
////
//// multi.new(state)
//// |> multi.reduce(items, fn(s, item) { process_item(s, item) }, apply_event)
//// |> multi.to_result()
//// ```

import gleam/list
import gleam/option.{type Option, None, Some}

/// An opaque builder that accumulates events from multiple command stages.
///
/// Type parameters:
/// - `state` — the aggregate state type
/// - `event` — the domain event type
pub opaque type Multi(state, event) {
  Multi(
    /// The current aggregate state (updated via `apply` between stages)
    aggregate: state,
    /// Events accumulated so far (in order, oldest first)
    events: List(event),
    /// The first error encountered (subsequent stages are skipped)
    error: Option(String),
  )
}

/// Start a new Multi chain with the given aggregate state.
///
/// This is the entry point — call this with the current aggregate state
/// at the start of a command handler.
pub fn new(aggregate: state) -> Multi(state, event) {
  Multi(aggregate: aggregate, events: [], error: None)
}

/// Execute a command stage against the current aggregate state.
///
/// - If the Multi already has an error, this is a no-op (the error is
///   preserved and no new events are added).
/// - If `f(state)` returns `Ok(events)`, those events are appended to
///   the accumulated list.
/// - If `f(state)` returns `Error(reason)`, the error is recorded and
///   all subsequent stages are skipped.
pub fn execute(
  multi: Multi(state, event),
  f: fn(state) -> Result(List(event), String),
) -> Multi(state, event) {
  case multi.error {
    Some(_) ->
      // Already failed — short-circuit
      multi
    None ->
      case f(multi.aggregate) {
        Ok(new_events) ->
          Multi(
            ..multi,
            events: list.append(multi.events, new_events),
          )
        Error(reason) ->
          Multi(..multi, error: Some(reason))
      }
  }
}

/// Apply all events accumulated so far to the aggregate state.
///
/// This updates the internal aggregate state by folding the accumulated
/// events through `apply_fn`. The result is that subsequent `execute`
/// stages see the aggregate state AFTER the previously emitted events —
/// exactly as if those events had already been persisted and replayed.
///
/// Call this between `execute` stages when later stages depend on the
/// state changes produced by earlier stages.
///
/// If the Multi is in an error state, this is a no-op.
pub fn apply(
  multi: Multi(state, event),
  apply_fn: fn(state, event) -> state,
) -> Multi(state, event) {
  case multi.error {
    Some(_) -> multi
    None -> {
      let new_state =
        list.fold(multi.events, multi.aggregate, apply_fn)
      Multi(..multi, aggregate: new_state)
    }
  }
}

/// Reduce a list of items, running an execute + apply step for each.
///
/// Equivalent to calling `execute` and `apply` for every item in the list.
/// Each step sees the aggregate state as mutated by all previous steps.
///
/// If any step fails, the chain short-circuits and the remaining items
/// are skipped.
///
/// ```gleam
/// multi.new(state)
/// |> multi.reduce(items, fn(s, item) { handle_item(s, item) }, apply_event)
/// |> multi.to_result()
/// ```
pub fn reduce(
  multi: Multi(state, event),
  items: List(item),
  execute_fn: fn(state, item) -> Result(List(event), String),
  apply_fn: fn(state, event) -> state,
) -> Multi(state, event) {
  list.fold(items, multi, fn(acc, item) {
    acc
    |> execute(fn(s) { execute_fn(s, item) })
    |> apply(apply_fn)
  })
}

/// Collapse the Multi chain into a `Result`.
///
/// - Returns `Ok(all_events)` if every stage succeeded.
/// - Returns `Error(first_error)` if any stage failed.
///
/// When an error is returned, **all accumulated events are discarded**
/// (Invariant 15: Multi errors are atomic — no partial state changes).
pub fn to_result(multi: Multi(state, event)) -> Result(List(event), String) {
  case multi.error {
    Some(reason) -> Error(reason)
    None -> Ok(multi.events)
  }
}

/// Read the current aggregate state inside the Multi chain.
///
/// Useful when you need to inspect the current state in a conditional
/// before deciding which execute stage to run next.
pub fn get_state(multi: Multi(state, event)) -> state {
  multi.aggregate
}

/// Read the events accumulated so far.
///
/// Mostly useful for debugging. Do not depend on this for correctness —
/// use `to_result` to get the final list.
pub fn get_events(multi: Multi(state, event)) -> List(event) {
  multi.events
}

/// Check whether the Multi chain is in a failed state.
pub fn has_error(multi: Multi(state, event)) -> Bool {
  case multi.error {
    Some(_) -> True
    None -> False
  }
}
