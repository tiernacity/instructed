//// Conformance test runner for the in-memory event store adapter.

import instructed/conformance/append_events
import instructed/conformance/concurrency
import instructed/conformance/snapshot
import instructed/conformance/subscription
import instructed/conformance/test_event.{type TestEvent}
import instructed/conformance/transient_subscription
import instructed/event_store.{type EventStore}
import instructed/in_memory_event_store

fn memory_factory() -> EventStore(TestEvent) {
  let assert Ok(subject) = in_memory_event_store.start()
  in_memory_event_store.to_event_store(subject)
}

pub fn append_conformance_test() -> Nil {
  append_events.run_all(memory_factory)
}

pub fn snapshot_conformance_test() -> Nil {
  snapshot.run_all(memory_factory)
}

pub fn subscription_conformance_test() -> Nil {
  subscription.run_all(memory_factory)
}

pub fn transient_subscription_conformance_test() -> Nil {
  transient_subscription.run_all(memory_factory)
}

pub fn concurrency_conformance_test() -> Nil {
  concurrency.run_all(memory_factory)
}
