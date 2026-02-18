import gleam/dict
import gleam/erlang/process
import gleam/list
import gleam/option.{None}
import gleeunit/should
import instructed/event.{EventData}
import instructed/event_store.{AnyVersion}
import instructed/in_memory_event_store
import instructed/telemetry

// --- Basic emit & handler tests ---

pub fn telemetry_set_and_clear_handler_test() {
  let received = process.new_subject()

  telemetry.set_handler(fn(event) { process.send(received, event) })

  telemetry.emit(telemetry.CommandDispatchStart(
    command_id: "cmd-1",
    aggregate_stream_id: "agg-1",
    system_time: 0,
  ))

  let assert Ok(ev) = process.receive(received, 500)
  case ev {
    telemetry.CommandDispatchStart(command_id: "cmd-1", ..) -> should.be_true(True)
    _ -> should.fail()
  }

  telemetry.clear_handler()
}

pub fn telemetry_no_handler_no_crash_test() {
  // Without a handler set, emit should be a no-op — no crash
  telemetry.clear_handler()
  telemetry.emit(telemetry.CommandDispatchStart(
    command_id: "x",
    aggregate_stream_id: "y",
    system_time: 0,
  ))
  should.be_true(True)
}

pub fn telemetry_all_event_types_test() {
  let received = process.new_subject()
  telemetry.set_handler(fn(event) { process.send(received, event) })

  let events_to_emit = [
    telemetry.CommandDispatchStart("c1", "s1", 0),
    telemetry.CommandDispatchStop("c1", "s1", 0, 2),
    telemetry.CommandDispatchException("c1", "s1", 0, "err"),
    telemetry.AggregateExecuteStart("agg-1", 0),
    telemetry.AggregateExecuteStop("agg-1", 100, 3),
    telemetry.AggregateExecuteException("agg-1", 100, "agg-err"),
    telemetry.EventHandleStart("handler-1", "SomeEvent", 42, 0),
    telemetry.EventHandleStop("handler-1", "SomeEvent", 42, 200),
    telemetry.EventHandleException("handler-1", "SomeEvent", 42, 200, "handle-err"),
    telemetry.ProcessManagerHandleStart("pm-1", "SomeEvent", 7, 0),
    telemetry.ProcessManagerHandleStop("pm-1", "SomeEvent", 7, 150, 1),
    telemetry.ProcessManagerHandleException("pm-1", "SomeEvent", 7, 150, "pm-err"),
  ]

  list.each(events_to_emit, telemetry.emit)

  // Collect all 12 events
  let collected =
    list.repeat(0, 12)
    |> list.map(fn(_) { process.receive(received, 200) })
    |> list.filter_map(fn(r) { r })

  should.equal(list.length(collected), 12)

  telemetry.clear_handler()
}

pub fn telemetry_dispatch_start_helper_test() {
  let received = process.new_subject()
  telemetry.set_handler(fn(event) { process.send(received, event) })

  telemetry.dispatch_start("cmd-help", "stream-help")

  let assert Ok(ev) = process.receive(received, 500)
  case ev {
    telemetry.CommandDispatchStart(command_id: "cmd-help", aggregate_stream_id: "stream-help", ..) ->
      should.be_true(True)
    _ -> should.fail()
  }

  telemetry.clear_handler()
}

pub fn telemetry_dispatch_stop_helper_test() {
  let received = process.new_subject()
  telemetry.set_handler(fn(event) { process.send(received, event) })

  let start = telemetry.system_time()
  telemetry.dispatch_stop("cmd-2", "stream-2", start, 5)

  let assert Ok(ev) = process.receive(received, 500)
  case ev {
    telemetry.CommandDispatchStop(command_id: "cmd-2", event_count: 5, ..) ->
      should.be_true(True)
    _ -> should.fail()
  }

  telemetry.clear_handler()
}

pub fn telemetry_dispatch_exception_helper_test() {
  let received = process.new_subject()
  telemetry.set_handler(fn(event) { process.send(received, event) })

  let start = telemetry.system_time()
  telemetry.dispatch_exception("cmd-3", "stream-3", start, "version conflict")

  let assert Ok(ev) = process.receive(received, 500)
  case ev {
    telemetry.CommandDispatchException(
      command_id: "cmd-3",
      error: "version conflict",
      ..,
    ) -> should.be_true(True)
    _ -> should.fail()
  }

  telemetry.clear_handler()
}

pub fn telemetry_aggregate_helpers_test() {
  let received = process.new_subject()
  telemetry.set_handler(fn(event) { process.send(received, event) })

  let start = telemetry.aggregate_start("agg-stream")
  // start emits AggregateExecuteStart
  let assert Ok(ev1) = process.receive(received, 500)
  case ev1 {
    telemetry.AggregateExecuteStart(aggregate_stream_id: "agg-stream", ..) ->
      should.be_true(True)
    _ -> should.fail()
  }

  telemetry.aggregate_stop("agg-stream", start, 3)
  let assert Ok(ev2) = process.receive(received, 500)
  case ev2 {
    telemetry.AggregateExecuteStop(aggregate_stream_id: "agg-stream", event_count: 3, ..) ->
      should.be_true(True)
    _ -> should.fail()
  }

  telemetry.aggregate_exception("agg-stream", start, "exec-error")
  let assert Ok(ev3) = process.receive(received, 500)
  case ev3 {
    telemetry.AggregateExecuteException(error: "exec-error", ..) ->
      should.be_true(True)
    _ -> should.fail()
  }

  telemetry.clear_handler()
}

pub fn telemetry_event_handle_helpers_test() {
  let received = process.new_subject()
  telemetry.set_handler(fn(event) { process.send(received, event) })

  let start = telemetry.event_handle_start("my-handler", "OrderPlaced", 99)
  let assert Ok(ev1) = process.receive(received, 500)
  case ev1 {
    telemetry.EventHandleStart(
      handler_name: "my-handler",
      event_type: "OrderPlaced",
      event_number: 99,
      ..,
    ) -> should.be_true(True)
    _ -> should.fail()
  }


  telemetry.event_handle_stop("my-handler", "OrderPlaced", 99, start)
  let assert Ok(ev2) = process.receive(received, 500)
  case ev2 {
    telemetry.EventHandleStop(handler_name: "my-handler", event_number: 99, ..) ->
      should.be_true(True)
    _ -> should.fail()
  }

  telemetry.event_handle_exception("my-handler", "OrderPlaced", 99, start, "oops")
  let assert Ok(ev3) = process.receive(received, 500)
  case ev3 {
    telemetry.EventHandleException(error: "oops", ..) -> should.be_true(True)
    _ -> should.fail()
  }

  telemetry.clear_handler()
}

pub fn telemetry_pm_handle_helpers_test() {
  let received = process.new_subject()
  telemetry.set_handler(fn(event) { process.send(received, event) })

  let start = telemetry.pm_handle_start("my-pm", "OrderShipped", 12)
  let assert Ok(ev1) = process.receive(received, 500)
  case ev1 {
    telemetry.ProcessManagerHandleStart(
      pm_name: "my-pm",
      event_type: "OrderShipped",
      event_number: 12,
      ..,
    ) -> should.be_true(True)
    _ -> should.fail()
  }

  telemetry.pm_handle_stop("my-pm", "OrderShipped", 12, start, 2)
  let assert Ok(ev2) = process.receive(received, 500)
  case ev2 {
    telemetry.ProcessManagerHandleStop(commands_dispatched: 2, ..) ->
      should.be_true(True)
    _ -> should.fail()
  }

  telemetry.pm_handle_exception("my-pm", "OrderShipped", 12, start, "pm-fail")
  let assert Ok(ev3) = process.receive(received, 500)
  case ev3 {
    telemetry.ProcessManagerHandleException(error: "pm-fail", ..) ->
      should.be_true(True)
    _ -> should.fail()
  }

  telemetry.clear_handler()
}

pub fn telemetry_system_time_increases_test() {
  let t1 = telemetry.system_time()
  let t2 = telemetry.system_time()
  // Monotonic time should be non-decreasing (note: monotonic time can be
  // negative relative to Erlang's reference point — we only check ordering)
  should.be_true(t2 >= t1)
}

pub fn telemetry_duration_in_stop_is_nonnegative_test() {
  let received = process.new_subject()
  telemetry.set_handler(fn(event) { process.send(received, event) })

  let start = telemetry.system_time()
  telemetry.dispatch_stop("dur-cmd", "dur-stream", start, 1)

  let assert Ok(ev) = process.receive(received, 500)
  case ev {
    telemetry.CommandDispatchStop(duration_ns: dur, ..) ->
      should.be_true(dur >= 0)
    _ -> should.fail()
  }

  telemetry.clear_handler()
}

pub fn telemetry_handler_replaced_test() {
  let received1 = process.new_subject()
  let received2 = process.new_subject()

  telemetry.set_handler(fn(event) { process.send(received1, event) })
  // Replace with new handler
  telemetry.set_handler(fn(event) { process.send(received2, event) })

  telemetry.emit(telemetry.CommandDispatchStart("x", "y", 0))

  // First handler should NOT have received it
  let res1 = process.receive(received1, 50)
  should.be_error(res1)

  // Second handler SHOULD have received it
  let assert Ok(_) = process.receive(received2, 500)

  telemetry.clear_handler()
}

// --- Integration: telemetry via event store append ---

pub fn telemetry_integrated_with_event_store_test() {
  // We wire telemetry around an append call to validate the timing helpers
  telemetry.clear_handler()
  let received = process.new_subject()
  telemetry.set_handler(fn(event) { process.send(received, event) })

  let assert Ok(store_subject) = in_memory_event_store.start()
  let store = in_memory_event_store.to_event_store(store_subject)

  let stream_id = "telemetry-test-stream"
  let cmd_id = "telemetry-cmd-1"
  let start = telemetry.system_time()

  telemetry.emit(telemetry.CommandDispatchStart(
    command_id: cmd_id,
    aggregate_stream_id: stream_id,
    system_time: start,
  ))

  let evt = EventData(
    data: "test-event",
    event_type: "TestEvent",
    causation_id: None,
    correlation_id: None,
    metadata: dict.new(),
  )

  let result = store.append_to_stream(stream_id, AnyVersion, [evt])
  let event_count = case result {
    Ok(_) -> 1
    Error(_) -> 0
  }

  telemetry.emit(telemetry.CommandDispatchStop(
    command_id: cmd_id,
    aggregate_stream_id: stream_id,
    duration_ns: telemetry.system_time() - start,
    event_count: event_count,
  ))

  // Should have received 2 events: start + stop
  let assert Ok(ev1) = process.receive(received, 500)
  case ev1 {
    telemetry.CommandDispatchStart(..) -> should.be_true(True)
    _ -> should.fail()
  }

  let assert Ok(ev2) = process.receive(received, 500)
  case ev2 {
    telemetry.CommandDispatchStop(event_count: 1, ..) -> should.be_true(True)
    _ -> should.fail()
  }

  telemetry.clear_handler()
}
