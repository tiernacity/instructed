import gleeunit/should
import instructed/aggregate

// --- Test domain types ---

type BankAccount {
  BankAccount(account_number: String, balance: Int, open: Bool)
}

type BankCommand {
  OpenAccount(account_number: String, initial_balance: Int)
  DepositMoney(amount: Int)
  WithdrawMoney(amount: Int)
  CloseAccount
}

type BankEvent {
  AccountOpened(account_number: String, initial_balance: Int)
  MoneyDeposited(amount: Int, new_balance: Int)
  MoneyWithdrawn(amount: Int, new_balance: Int)
  AccountClosed
}

fn bank_aggregate() -> aggregate.Aggregate(BankAccount, BankCommand, BankEvent) {
  aggregate.new(
    empty_state: fn() { BankAccount("", 0, False) },
    execute: fn(state, cmd) {
      case cmd {
        OpenAccount(num, balance) ->
          case state.open {
            True -> Error("Account already open")
            False ->
              case balance > 0 {
                True -> Ok([AccountOpened(num, balance)])
                False -> Error("Initial balance must be positive")
              }
          }
        DepositMoney(amount) ->
          case state.open {
            False -> Error("Account not open")
            True ->
              case amount > 0 {
                True -> Ok([MoneyDeposited(amount, state.balance + amount)])
                False -> Error("Amount must be positive")
              }
          }
        WithdrawMoney(amount) ->
          case state.open {
            False -> Error("Account not open")
            True ->
              case amount > 0 && amount <= state.balance {
                True -> Ok([MoneyWithdrawn(amount, state.balance - amount)])
                False -> Error("Invalid withdrawal amount")
              }
          }
        CloseAccount ->
          case state.open {
            False -> Error("Account not open")
            True -> Ok([AccountClosed])
          }
      }
    },
    apply_event: fn(state, event) {
      case event {
        AccountOpened(num, balance) ->
          BankAccount(account_number: num, balance: balance, open: True)
        MoneyDeposited(_, new_balance) ->
          BankAccount(..state, balance: new_balance)
        MoneyWithdrawn(_, new_balance) ->
          BankAccount(..state, balance: new_balance)
        AccountClosed -> BankAccount(..state, open: False)
      }
    },
  )
}

// --- Tests ---

pub fn empty_state_test() {
  let agg = bank_aggregate()
  let state = agg.empty_state()
  should.equal(state.account_number, "")
  should.equal(state.balance, 0)
  should.equal(state.open, False)
}

pub fn execute_open_account_test() {
  let agg = bank_aggregate()
  let state = agg.empty_state()
  let result = agg.execute(state, OpenAccount("ACC1", 100))
  should.be_ok(result)
  let assert Ok(events) = result
  should.equal(events, [AccountOpened("ACC1", 100)])
}

pub fn execute_open_account_negative_balance_test() {
  let agg = bank_aggregate()
  let state = agg.empty_state()
  let result = agg.execute(state, OpenAccount("ACC1", -50))
  should.be_error(result)
}

pub fn execute_deposit_on_closed_account_test() {
  let agg = bank_aggregate()
  let state = agg.empty_state()
  let result = agg.execute(state, DepositMoney(50))
  should.be_error(result)
}

pub fn apply_events_test() {
  let agg = bank_aggregate()
  let events = [
    AccountOpened("ACC1", 100),
    MoneyDeposited(50, 150),
    MoneyWithdrawn(25, 125),
  ]
  let state = aggregate.rebuild_state(agg, events)
  should.equal(state.account_number, "ACC1")
  should.equal(state.balance, 125)
  should.equal(state.open, True)
}

pub fn execute_after_rebuild_test() {
  let agg = bank_aggregate()
  let events = [AccountOpened("ACC1", 100)]
  let state = aggregate.rebuild_state(agg, events)

  let result = agg.execute(state, DepositMoney(50))
  should.be_ok(result)
  let assert Ok(new_events) = result
  should.equal(new_events, [MoneyDeposited(50, 150)])
}

pub fn execute_duplicate_open_test() {
  let agg = bank_aggregate()
  let state =
    aggregate.rebuild_state(agg, [AccountOpened("ACC1", 100)])
  let result = agg.execute(state, OpenAccount("ACC2", 200))
  should.be_error(result)
}

pub fn execute_close_and_reopen_test() {
  let agg = bank_aggregate()
  let state =
    aggregate.rebuild_state(agg, [AccountOpened("ACC1", 100), AccountClosed])
  should.equal(state.open, False)
  should.equal(state.balance, 100)

  // Can't deposit to closed account
  let result = agg.execute(state, DepositMoney(50))
  should.be_error(result)
}

pub fn withdraw_exceeding_balance_test() {
  let agg = bank_aggregate()
  let state =
    aggregate.rebuild_state(agg, [AccountOpened("ACC1", 100)])
  let result = agg.execute(state, WithdrawMoney(200))
  should.be_error(result)
}
