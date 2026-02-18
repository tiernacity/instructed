import gleam/dict
import gleam/option.{None}
import gleeunit/should
import instructed/aggregate
import instructed/aggregate_server
import instructed/event.{type RecordedEvent, EventData, RecordedEvent}
import gleam/int
import instructed/event_store.{NoStream}
import instructed/in_memory_event_store
import instructed/upcast

// --- Tests for upcast module ---

type TestEv {
  OldEvent(value: Int)
  NewEvent(value: Int, extra: String)
}

fn make_recorded(n: Int, ev: TestEv) -> RecordedEvent(TestEv) {
  RecordedEvent(
    event_id: "id-" <> int.to_string(n),
    event_number: n,
    stream_id: "test",
    stream_version: n,
    event_type: case ev {
      OldEvent(_) -> "OldEvent"
      NewEvent(_, _) -> "NewEvent"
    },
    causation_id: None,
    correlation_id: None,
    data: ev,
    metadata: dict.new(),
    created_at: 0,
  )
}


pub fn upcast_identity_test() {
  let u = upcast.identity()
  let ev = make_recorded(1, OldEvent(42))
  let result = upcast.apply(u, ev)
  should.equal(result, ev)
}

pub fn upcast_transform_test() {
  // Upcaster: OldEvent(n) → NewEvent(n, "upcast")
  let u =
    upcast.Upcaster(upcast: fn(recorded) {
      case recorded.data {
        OldEvent(n) ->
          RecordedEvent(..recorded, data: NewEvent(n, "upcast"))
        other -> RecordedEvent(..recorded, data: other)
      }
    })

  let ev = make_recorded(1, OldEvent(10))
  let result = upcast.apply(u, ev)
  should.equal(result.data, NewEvent(10, "upcast"))
  // Other fields preserved
  should.equal(result.event_id, ev.event_id)
  should.equal(result.stream_version, ev.stream_version)
}

pub fn upcast_apply_all_test() {
  let u =
    upcast.Upcaster(upcast: fn(recorded) {
      case recorded.data {
        OldEvent(n) ->
          RecordedEvent(..recorded, data: NewEvent(n, "up"))
        other -> RecordedEvent(..recorded, data: other)
      }
    })

  let events = [
    make_recorded(1, OldEvent(1)),
    make_recorded(2, NewEvent(2, "already")),
    make_recorded(3, OldEvent(3)),
  ]

  let results = upcast.apply_all(u, events)
  let assert [r1, r2, r3] = results

  should.equal(r1.data, NewEvent(1, "up"))
  should.equal(r2.data, NewEvent(2, "already"))
  should.equal(r3.data, NewEvent(3, "up"))
}

pub fn upcast_chain_test() {
  // v1→v2: multiply by 2
  let u1 =
    upcast.Upcaster(upcast: fn(r) {
      case r.data {
        OldEvent(n) -> RecordedEvent(..r, data: OldEvent(n * 2))
        other -> RecordedEvent(..r, data: other)
      }
    })
  // v2→v3: add "chain" extra
  let u2 =
    upcast.Upcaster(upcast: fn(r) {
      case r.data {
        OldEvent(n) -> RecordedEvent(..r, data: NewEvent(n, "chain"))
        other -> RecordedEvent(..r, data: other)
      }
    })

  let chained = upcast.chain(u1, u2)
  let ev = make_recorded(1, OldEvent(5))
  let result = upcast.apply(chained, ev)
  // 5 * 2 = 10, then OldEvent(10) → NewEvent(10, "chain")
  should.equal(result.data, NewEvent(10, "chain"))
}

pub fn upcast_chain_all_test() {
  let u1 =
    upcast.Upcaster(upcast: fn(r) {
      case r.data {
        OldEvent(n) -> RecordedEvent(..r, data: OldEvent(n + 1))
        other -> RecordedEvent(..r, data: other)
      }
    })
  let u2 =
    upcast.Upcaster(upcast: fn(r) {
      case r.data {
        OldEvent(n) -> RecordedEvent(..r, data: OldEvent(n + 10))
        other -> RecordedEvent(..r, data: other)
      }
    })

  let chained = upcast.chain_all([u1, u2])
  let ev = make_recorded(1, OldEvent(0))
  let result = upcast.apply(chained, ev)
  // 0 + 1 = 1, then 1 + 10 = 11
  should.equal(result.data, OldEvent(11))
}

pub fn upcast_chain_all_empty_test() {
  // Empty chain = identity
  let chained = upcast.chain_all([])
  let ev = make_recorded(1, OldEvent(99))
  let result = upcast.apply(chained, ev)
  should.equal(result.data, OldEvent(99))
}

// --- Integration test: upcasting in aggregate server ---

type AggrEv {
  V1Created(n: Int)
  V2Created(n: Int, tag: String)
}

pub fn upcast_aggregate_server_integration_test() {
  // An aggregate that only understands V2Created.
  // We configure an upcaster that transforms V1Created → V2Created.
  let assert Ok(store_subject) = in_memory_event_store.start()
  let store = in_memory_event_store.to_event_store(store_subject)

  // First: append a V1Created event directly to the store
  let _ =
    store.append_to_stream("upcast-agg-1", NoStream, [
      EventData(
        data: V1Created(42),
        event_type: "V1Created",
        causation_id: None,
        correlation_id: None,
        metadata: dict.new(),
      ),
    ])

  // The aggregate only handles V2Created
  let agg =
    aggregate.new(
      empty_state: fn() { 0 },
      execute: fn(_state, _cmd: Nil) { Ok([]) },
      apply_event: fn(state, ev) {
        case ev {
          V2Created(n, _) -> state + n
          V1Created(_) -> state
          // V1 should not arrive (upcasted away)
        }
      },
    )

  // Create an upcaster: V1Created → V2Created
  let u =
    upcast.Upcaster(upcast: fn(recorded) {
      case recorded.data {
        V1Created(n) ->
          RecordedEvent(..recorded, data: V2Created(n, "migrated"))
        other -> RecordedEvent(..recorded, data: other)
      }
    })

  let config =
    aggregate_server.new_config(
      aggregate: agg,
      event_store: store,
      stream_id: "upcast-agg-1",
    )
    |> aggregate_server.with_upcaster(u)

  let assert Ok(server) = aggregate_server.start(config)

  // Execute a no-op command to trigger state loading
  let assert Ok(result) = aggregate_server.execute(server, Nil, 5000)

  // State should be 42 (V1Created(42) was upcast to V2Created(42, "migrated"))
  should.equal(result.aggregate_state, 42)
}
