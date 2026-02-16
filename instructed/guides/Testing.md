# Testing with Instructed

Instructed is designed to be easily testable. The in-memory event store makes it simple to write fast, isolated tests.

## Testing Aggregates

Test aggregates directly without the event store:

```gleam
import gleeunit/should
import instructed/aggregate

pub fn open_account_test() {
  let agg = bank_aggregate()
  let state = agg.empty_state()

  let result = agg.execute(state, OpenAccount("ACC1", 100))
  should.be_ok(result)

  let assert Ok(events) = result
  should.equal(events, [AccountOpened("ACC1", 100)])
}

pub fn reject_invalid_command_test() {
  let agg = bank_aggregate()
  let state = agg.empty_state()

  let result = agg.execute(state, OpenAccount("ACC1", -50))
  should.be_error(result)
}
```

## Testing State Rebuilding

```gleam
pub fn rebuild_state_test() {
  let agg = bank_aggregate()
  let events = [
    AccountOpened("ACC1", 100),
    MoneyDeposited(50, 150),
  ]

  let state = aggregate.rebuild_state(agg, events)
  should.equal(state.balance, 150)
}
```

## Testing with Event Store

```gleam
import instructed/in_memory_event_store
import instructed/router

pub fn full_dispatch_test() {
  let assert Ok(store_subject) = in_memory_event_store.start()
  let store = in_memory_event_store.to_event_store(store_subject)

  let r = router.new(
    aggregate: bank_aggregate(),
    event_store: store,
    identity: fn(cmd) { cmd.account_number },
  )

  let assert Ok(result) = router.dispatch(r, OpenAccount("ACC1", 100))
  should.equal(result.aggregate_state.balance, 100)

  let assert Ok(result) = router.dispatch(r, DepositMoney("ACC1", 50))
  should.equal(result.aggregate_state.balance, 150)
}
```

## Testing Projections

```gleam
import gleam/erlang/process
import instructed/projection

pub fn projection_test() {
  let assert Ok(store_subject) = in_memory_event_store.start()
  let store = in_memory_event_store.to_event_store(store_subject)

  let proj = projection.new(
    name: "test_proj",
    initial_state: 0,
    handle_event: fn(_event, _recorded, count) { Ok(count + 1) },
  )

  let assert Ok(p) = projection.start(proj, store)

  // Add events...
  // Wait for processing
  process.sleep(100)

  let count = projection.get_state(p, 5000)
  should.equal(count, expected_count)
}
```

## Testing Middleware

```gleam
import instructed/middleware

pub fn middleware_halt_test() {
  let mw = middleware.new(
    before_dispatch: fn(pipeline) {
      middleware.halt(pipeline)
    },
    after_dispatch: fn(p) { p },
    after_failure: fn(p) { p },
  )

  let r = router.new(...)
    |> router.with_middleware(mw)

  let result = router.dispatch(r, some_command)
  should.be_error(result)
}
```

## Resetting the Event Store

For test isolation, reset the event store between tests:

```gleam
let assert Ok(Nil) = store.reset()
```

## Best Practices

1. **Test aggregates in isolation** - They're pure functions
2. **Use in-memory event store** for fast tests
3. **Test command validation** with both valid and invalid commands
4. **Test event application** with all event variants
5. **Test projections** with real event sequences
6. **Reset state** between tests for isolation
