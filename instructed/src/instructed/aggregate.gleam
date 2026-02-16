//// Aggregate behaviour for event-sourced domain entities.
////
//// An aggregate is the core building block in CQRS/ES. It receives commands,
//// validates them against current state, and produces domain events.
//// State is rebuilt by replaying events through the `apply_event` function.
////
//// ## Design
////
//// In Gleam, we use records of functions instead of behaviours/traits.
//// The `Aggregate` type takes three type parameters:
//// - `state`: The aggregate's state type
//// - `command`: The command type the aggregate handles
//// - `event`: The domain event type the aggregate produces
////
//// ## Example
////
//// ```gleam
//// import instructed/aggregate.{type Aggregate}
////
//// type BankAccount {
////   BankAccount(account_number: String, balance: Int)
//// }
////
//// type BankCommand {
////   OpenAccount(account_number: String, initial_balance: Int)
////   DepositMoney(amount: Int)
//// }
////
//// type BankEvent {
////   AccountOpened(account_number: String, initial_balance: Int)
////   MoneyDeposited(amount: Int, new_balance: Int)
//// }
////
//// fn bank_aggregate() -> Aggregate(BankAccount, BankCommand, BankEvent) {
////   aggregate.new(
////     empty_state: fn() { BankAccount("", 0) },
////     execute: fn(state, cmd) {
////       case cmd {
////         OpenAccount(num, balance) if balance > 0 ->
////           Ok([AccountOpened(num, balance)])
////         OpenAccount(_, _) ->
////           Error("Initial balance must be positive")
////         DepositMoney(amount) ->
////           Ok([MoneyDeposited(amount, state.balance + amount)])
////       }
////     },
////     apply_event: fn(state, event) {
////       case event {
////         AccountOpened(num, balance) ->
////           BankAccount(num, balance)
////         MoneyDeposited(_, new_balance) ->
////           BankAccount(..state, balance: new_balance)
////       }
////     },
////   )
//// }
//// ```

import gleam/list

/// An aggregate definition with strongly-typed state, commands, and events.
pub type Aggregate(state, command, event) {
  Aggregate(
    /// Function to create the initial empty state
    empty_state: fn() -> state,
    /// Function to execute a command against state, returning events or an error
    execute: fn(state, command) -> Result(List(event), String),
    /// Function to apply an event to state, returning new state.
    /// This must never fail - events are facts that have already occurred.
    apply_event: fn(state, event) -> state,
  )
}

/// Create a new aggregate definition.
pub fn new(
  empty_state empty_state: fn() -> state,
  execute execute: fn(state, command) -> Result(List(event), String),
  apply_event apply_event: fn(state, event) -> state,
) -> Aggregate(state, command, event) {
  Aggregate(empty_state:, execute:, apply_event:)
}

/// Rebuild aggregate state from a list of events.
pub fn rebuild_state(
  aggregate: Aggregate(state, command, event),
  events: List(event),
) -> state {
  list.fold(events, aggregate.empty_state(), aggregate.apply_event)
}
