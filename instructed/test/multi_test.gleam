import gleam/list
import gleeunit/should
import instructed/multi

// --- Domain types for testing ---

type State {
  State(count: Int, items: List(String))
}

type Event {
  Incremented(by: Int)
  ItemAdded(item: String)
  ItemRemoved(item: String)
}

fn empty_state() -> State {
  State(count: 0, items: [])
}

fn apply_event(state: State, event: Event) -> State {
  case event {
    Incremented(by) -> State(..state, count: state.count + by)
    ItemAdded(item) -> State(..state, items: [item, ..state.items])
    ItemRemoved(item) ->
      State(..state, items: list.filter(state.items, fn(i) { i != item }))
  }
}

// --- Tests ---

pub fn multi_new_test() {
  let m = multi.new(empty_state())
  should.equal(multi.get_state(m), empty_state())
  should.equal(multi.get_events(m), [])
  should.equal(multi.has_error(m), False)
}

pub fn multi_single_execute_ok_test() {
  let result =
    multi.new(empty_state())
    |> multi.execute(fn(_s) { Ok([Incremented(1)]) })
    |> multi.to_result()

  should.be_ok(result)
  let assert Ok(events) = result
  should.equal(events, [Incremented(1)])
}

pub fn multi_single_execute_error_test() {
  let result =
    multi.new(empty_state())
    |> multi.execute(fn(_s) { Error("something went wrong") })
    |> multi.to_result()

  should.be_error(result)
  let assert Error(msg) = result
  should.equal(msg, "something went wrong")
}

pub fn multi_chained_execute_ok_test() {
  let result =
    multi.new(empty_state())
    |> multi.execute(fn(_s) { Ok([Incremented(5)]) })
    |> multi.execute(fn(_s) { Ok([ItemAdded("foo")]) })
    |> multi.to_result()

  should.be_ok(result)
  let assert Ok(events) = result
  should.equal(events, [Incremented(5), ItemAdded("foo")])
}

pub fn multi_error_short_circuits_test() {
  // After an error, subsequent stages must NOT be executed
  let m =
    multi.new(empty_state())
    |> multi.execute(fn(_s) { Ok([Incremented(1)]) })
    |> multi.execute(fn(_s) { Error("stage 2 failed") })
    |> multi.execute(fn(_s) { Ok([ItemAdded("should not appear")]) })

  // Only the first error matters; no later events added
  let result = multi.to_result(m)
  should.be_error(result)
  let assert Error(msg) = result
  should.equal(msg, "stage 2 failed")
}

pub fn multi_error_discards_all_events_test() {
  // Invariant 15: Multi errors discard ALL events — including events from
  // stages that succeeded before the failure
  let result =
    multi.new(empty_state())
    |> multi.execute(fn(_s) { Ok([Incremented(10)]) })
    |> multi.execute(fn(_s) { Error("abort") })
    |> multi.to_result()

  // Error returned, no events
  should.be_error(result)
}

pub fn multi_apply_updates_state_test() {
  // After apply, the next execute stage sees updated state
  let result =
    multi.new(empty_state())
    |> multi.execute(fn(_s) { Ok([Incremented(3)]) })
    |> multi.apply(apply_event)
    |> multi.execute(fn(s) {
      // State should now have count = 3
      case s.count == 3 {
        True -> Ok([Incremented(s.count)])
        False -> Error("unexpected count: " <> "?")
      }
    })
    |> multi.to_result()

  should.be_ok(result)
  let assert Ok(events) = result
  should.equal(events, [Incremented(3), Incremented(3)])
}

pub fn multi_apply_without_error_test() {
  // apply with no preceding error updates state correctly
  let m =
    multi.new(empty_state())
    |> multi.execute(fn(_s) { Ok([ItemAdded("a"), ItemAdded("b")]) })
    |> multi.apply(apply_event)

  let state = multi.get_state(m)
  // Items are present in the state (order depends on fold/prepend semantics)
  should.equal(list.contains(state.items, "a"), True)
  should.equal(list.contains(state.items, "b"), True)
  should.equal(list.length(state.items), 2)
}

pub fn multi_apply_with_error_noop_test() {
  // apply in error state is a no-op
  let m =
    multi.new(empty_state())
    |> multi.execute(fn(_s) { Error("boom") })
    |> multi.apply(apply_event)
  // State should still be empty (apply was skipped)
  should.equal(multi.get_state(m), empty_state())
  should.equal(multi.has_error(m), True)
}

pub fn multi_empty_events_ok_test() {
  // execute returning Ok([]) is a valid no-op
  let result =
    multi.new(empty_state())
    |> multi.execute(fn(_s) { Ok([]) })
    |> multi.to_result()

  should.be_ok(result)
  let assert Ok(events) = result
  should.equal(events, [])
}

pub fn multi_reduce_ok_test() {
  let items = ["a", "b", "c"]

  let result =
    multi.new(empty_state())
    |> multi.reduce(
      items,
      fn(_s, item) { Ok([ItemAdded(item)]) },
      apply_event,
    )
    |> multi.to_result()

  should.be_ok(result)
  let assert Ok(events) = result
  should.equal(events, [ItemAdded("a"), ItemAdded("b"), ItemAdded("c")])
}

pub fn multi_reduce_error_short_circuits_test() {
  let items = ["a", "b", "c"]

  let result =
    multi.new(empty_state())
    |> multi.reduce(
      items,
      fn(_s, item) {
        case item == "b" {
          True -> Error("cannot add b")
          False -> Ok([ItemAdded(item)])
        }
      },
      apply_event,
    )
    |> multi.to_result()

  should.be_error(result)
  let assert Error(msg) = result
  should.equal(msg, "cannot add b")
}

pub fn multi_reduce_empty_items_test() {
  let result =
    multi.new(empty_state())
    |> multi.reduce([], fn(_s, _item: String) { Ok([]) }, apply_event)
    |> multi.to_result()

  should.be_ok(result)
  let assert Ok(events) = result
  should.equal(events, [])
}

pub fn multi_get_state_reflects_apply_test() {
  let m =
    multi.new(empty_state())
    |> multi.execute(fn(_s) { Ok([Incremented(7)]) })
    |> multi.apply(apply_event)

  should.equal(multi.get_state(m).count, 7)
}

pub fn multi_get_events_intermediate_test() {
  let m =
    multi.new(empty_state())
    |> multi.execute(fn(_s) { Ok([Incremented(1)]) })
    |> multi.execute(fn(_s) { Ok([Incremented(2)]) })

  // get_events returns all accumulated events so far
  should.equal(multi.get_events(m), [Incremented(1), Incremented(2)])
}

pub fn multi_has_error_true_test() {
  let m =
    multi.new(empty_state())
    |> multi.execute(fn(_s) { Error("fail") })

  should.equal(multi.has_error(m), True)
}

pub fn multi_has_error_false_test() {
  let m =
    multi.new(empty_state())
    |> multi.execute(fn(_s) { Ok([Incremented(1)]) })

  should.equal(multi.has_error(m), False)
}


