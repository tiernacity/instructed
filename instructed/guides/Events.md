# Events

Domain events represent facts that have occurred in your system. They are immutable and are the source of truth for aggregate state.

## Domain Events

Define events as Gleam custom types:

```gleam
type BankEvent {
  AccountOpened(account_number: String, initial_balance: Int)
  MoneyDeposited(amount: Int, new_balance: Int)
  MoneyWithdrawn(amount: Int, new_balance: Int)
  AccountClosed
}
```

## Event Data

Before persistence, events are wrapped in `EventData`:

```gleam
type EventData(event) {
  EventData(
    data: event,
    causation_id: Option(String),
    correlation_id: Option(String),
    metadata: Dict(String, String),
  )
}
```

## Recorded Events

After persistence, events are wrapped in `RecordedEvent` with storage metadata:

```gleam
type RecordedEvent(event) {
  RecordedEvent(
    event_id: String,
    event_number: Int,
    stream_id: String,
    stream_version: Int,
    causation_id: Option(String),
    correlation_id: Option(String),
    data: event,
    metadata: Dict(String, String),
    created_at: Int,
  )
}
```

## Event Handlers

Event handlers subscribe to domain events and process them:

```gleam
import instructed/event_handler

let email_handler = event_handler.new(
  name: "email_notifier",
  handle_event: fn(event, recorded_event, state) {
    case event {
      AccountOpened(num, _) -> {
        send_welcome_email(num)
        Ok(state)
      }
      _ -> Ok(state)
    }
  },
  initial_state: Nil,
)

let assert Ok(_) = event_handler.start(email_handler, event_store)
```

### Stream-specific Handlers

Subscribe to events from a specific stream:

```gleam
let handler = event_handler.new(...)
  |> event_handler.for_stream("bank-ACC1")
```

### Handler State

Event handlers can maintain state across events:

```gleam
let counter_handler = event_handler.new(
  name: "event_counter",
  handle_event: fn(_event, _recorded, count) {
    Ok(count + 1)
  },
  initial_state: 0,
)
```

## Event Metadata

Events carry metadata through the system:

```gleam
let metadata = event.enrich_metadata(recorded_event)
// EventMetadata with event_id, stream_id, causation_id, etc.
```

## Causation and Correlation

- **Causation ID**: Identifies what caused this event (typically the command ID)
- **Correlation ID**: Groups related commands/events together

These are automatically propagated when dispatching commands with context:

```gleam
router.dispatch_with_context(
  router, command,
  "cmd-123",
  option.Some("cause-456"),
  option.Some("correlation-789"),
  dict.new(),
)
```
