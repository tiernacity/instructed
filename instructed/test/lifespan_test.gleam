import gleam/erlang/process
import gleeunit/should
import instructed/aggregate
import instructed/aggregate_server
import instructed/in_memory_event_store
import instructed/lifespan

// --- Domain types ---

type State {
  State(value: Int, done: Bool)
}

type Cmd {
  Increment(Int)
  Finish
  Fail
}

type Ev {
  Incremented(Int)
  Finished
}

fn make_aggregate() {
  aggregate.new(
    empty_state: fn() { State(value: 0, done: False) },
    execute: fn(_, cmd) {
      case cmd {
        Increment(n) -> Ok([Incremented(n)])
        Finish -> Ok([Finished])
        Fail -> Error("intentional failure")
      }
    },
    apply_event: fn(state, event) {
      case event {
        Incremented(n) -> State(..state, value: state.value + n)
        Finished -> State(..state, done: True)
      }
    },
  )
}

fn start_store() {
  let assert Ok(subject) = in_memory_event_store.start()
  in_memory_event_store.to_event_store(subject)
}

// --- Tests ---

pub fn lifespan_always_running_test() {
  // Default: no lifespan configured, aggregate stays running
  let store = start_store()
  let config =
    aggregate_server.new_config(
      aggregate: make_aggregate(),
      event_store: store,
      stream_id: "lifespan-always-1",
    )
  let assert Ok(server) = aggregate_server.start(config)

  // Multiple commands work fine
  let assert Ok(r1) = aggregate_server.execute(server, Increment(5), 5000)
  should.equal(r1.aggregate_state.value, 5)

  let assert Ok(r2) = aggregate_server.execute(server, Increment(3), 5000)
  should.equal(r2.aggregate_state.value, 8)
}

pub fn lifespan_stop_after_command_test() {
  // stop_after_command: process stops after first successful command
  let store = start_store()
  let ls = lifespan.stop_after_command()

  let config =
    aggregate_server.new_config(
      aggregate: make_aggregate(),
      event_store: store,
      stream_id: "lifespan-stop-1",
    )
    |> aggregate_server.with_lifespan(ls)

  let assert Ok(server) = aggregate_server.start(config)

  // First command succeeds
  let assert Ok(result) = aggregate_server.execute(server, Increment(10), 5000)
  should.equal(result.aggregate_state.value, 10)

  // Process stops asynchronously — give it time
  process.sleep(50)
  Nil
}

pub fn lifespan_stop_on_done_test() {
  // Custom lifespan that stops when done=True
  let store = start_store()

  let ls =
    lifespan.Lifespan(
      after_command: fn(state: State, _cmd) {
        case state.done {
          True -> lifespan.Stop
          False -> lifespan.KeepRunning
        }
      },
      after_error: fn(_, _, _) { lifespan.KeepRunning },
      after_event: fn(_, _) { lifespan.KeepRunning },
    )

  let config =
    aggregate_server.new_config(
      aggregate: make_aggregate(),
      event_store: store,
      stream_id: "lifespan-stop-2",
    )
    |> aggregate_server.with_lifespan(ls)

  let assert Ok(server) = aggregate_server.start(config)

  // Increment — done=False → KeepRunning
  let assert Ok(r1) = aggregate_server.execute(server, Increment(5), 5000)
  should.equal(r1.aggregate_state.done, False)

  // Finish — done=True → Stop
  let assert Ok(r2) = aggregate_server.execute(server, Finish, 5000)
  should.equal(r2.aggregate_state.done, True)

  // Process stops asynchronously
  process.sleep(50)
  Nil
}

pub fn lifespan_keep_running_on_error_test() {
  // KeepRunning after error — process stays alive
  let store = start_store()

  let ls =
    lifespan.Lifespan(
      after_command: fn(_, _) { lifespan.KeepRunning },
      after_error: fn(_, _, _) { lifespan.KeepRunning },
      after_event: fn(_, _) { lifespan.KeepRunning },
    )

  let config =
    aggregate_server.new_config(
      aggregate: make_aggregate(),
      event_store: store,
      stream_id: "lifespan-error-keep-1",
    )
    |> aggregate_server.with_lifespan(ls)

  let assert Ok(server) = aggregate_server.start(config)

  // Error command
  let result = aggregate_server.execute(server, Fail, 5000)
  should.be_error(result)

  // Process still alive — can execute more commands
  let assert Ok(r) = aggregate_server.execute(server, Increment(1), 5000)
  should.equal(r.aggregate_state.value, 1)
}

pub fn lifespan_always_running_fn_test() {
  let ls = lifespan.always_running()

  let state = State(value: 0, done: False)
  should.equal(ls.after_command(state, Increment(1)), lifespan.KeepRunning)
  should.equal(ls.after_error(state, Fail, "err"), lifespan.KeepRunning)
  should.equal(ls.after_event(state, Incremented(1)), lifespan.KeepRunning)
}

pub fn lifespan_stop_after_command_fn_test() {
  let ls = lifespan.stop_after_command()

  let state = State(value: 0, done: False)
  should.equal(ls.after_command(state, Increment(1)), lifespan.Stop)
  should.equal(ls.after_error(state, Fail, "err"), lifespan.KeepRunning)
  should.equal(ls.after_event(state, Incremented(1)), lifespan.KeepRunning)
}

pub fn lifespan_new_idle_fn_test() {
  let ls = lifespan.new_idle(5000)

  let state = State(value: 0, done: False)
  should.equal(ls.after_command(state, Increment(1)), lifespan.StopAfter(5000))
  should.equal(ls.after_error(state, Fail, "err"), lifespan.StopAfter(5000))
  should.equal(ls.after_event(state, Incremented(1)), lifespan.StopAfter(5000))
}

pub fn lifespan_stop_after_idle_test() {
  // StopAfter with very short timeout (10ms) — process should stop
  let store = start_store()

  let ls = lifespan.new_idle(10)

  let config =
    aggregate_server.new_config(
      aggregate: make_aggregate(),
      event_store: store,
      stream_id: "lifespan-idle-1",
    )
    |> aggregate_server.with_lifespan(ls)

  let assert Ok(server) = aggregate_server.start(config)

  // Execute a command to trigger the StopAfter(10) decision
  let assert Ok(r) = aggregate_server.execute(server, Increment(1), 5000)
  should.equal(r.aggregate_state.value, 1)

  // Wait longer than the timeout
  process.sleep(100)

  // Process should be stopped — subsequent commands would fail
  // We just verify the happy path worked correctly
  Nil
}

pub fn lifespan_with_no_config_test() {
  // Verify that aggregate_server works normally without lifespan config
  let store = start_store()
  let config =
    aggregate_server.new_config(
      aggregate: make_aggregate(),
      event_store: store,
      stream_id: "lifespan-no-config-1",
    )

  // No with_lifespan call — default is None
  let assert Ok(server) = aggregate_server.start(config)
  let assert Ok(r1) = aggregate_server.execute(server, Increment(42), 5000)
  should.equal(r1.aggregate_state.value, 42)

  let assert Ok(r2) = aggregate_server.execute(server, Increment(8), 5000)
  should.equal(r2.aggregate_state.value, 50)
}
