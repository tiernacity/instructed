//// Error types used throughout the Instructed framework.

/// Errors that can occur during command dispatch.
pub type DispatchError {
  /// The aggregate returned a domain error
  AggregateError(reason: String)
  /// The command was halted by middleware
  Halted
  /// Command execution timed out
  Timeout
  /// Wrong expected version when appending events
  WrongExpectedVersion
  /// The aggregate process could not be started
  AggregateStartError(reason: String)
  /// The event store returned an error
  EventStoreError(reason: String)
  /// A middleware error
  MiddlewareError(reason: String)
  /// Concurrency error - too many retry attempts
  TooManyAttempts
}

/// Errors that can occur in the event store.
pub type EventStoreError {
  /// The expected version didn't match the current stream version
  VersionConflict
  /// The requested stream does not exist
  StreamNotFound
  /// The stream already exists (returned when NoStream expected version fails)
  StreamAlreadyExists
  /// The requested snapshot does not exist
  SnapshotNotFound
  /// A subscription with this name already exists
  SubscriptionAlreadyExists
  /// The requested subscription was not found
  SubscriptionNotFound
  /// A storage or I/O error
  StorageError(reason: String)
}
