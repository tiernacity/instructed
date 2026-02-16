//// Instructed - A CQRS/ES Framework for Gleam
////
//// Instructed is a strongly-typed CQRS/ES (Command Query Responsibility 
//// Segregation / Event Sourcing) framework for Gleam. It is a port of the 
//// Elixir `Commanded` library, redesigned to leverage Gleam's type system.
////
//// ## Features
////
//// - **Aggregates**: Event-sourced domain entities with type-safe commands and events
//// - **Command Routing**: Type-safe command dispatch to aggregates
//// - **Event Handlers**: Subscribe to and process domain events
//// - **Process Managers**: Coordinate multiple aggregates (sagas)
//// - **Projections**: Build read models from event streams
//// - **Middleware**: Extensible command processing pipeline
//// - **Event Store**: Pluggable event storage (in-memory included)
//// - **Supervision**: OTP supervisor support for fault tolerance
//// - **Snapshotting**: Aggregate state snapshots for performance
////

/// The version of the Instructed library.
pub const version = "1.0.0"
