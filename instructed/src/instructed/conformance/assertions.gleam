//// Simple assertion helpers for conformance tests.
//// No dependency on gleeunit, so these can live in src/.

import gleam/string

pub fn equal(a: t, b: t) -> Nil {
  case a == b {
    True -> Nil
    False ->
      panic as {
        "Assertion failed: expected equal values, got "
        <> string.inspect(a)
        <> " vs "
        <> string.inspect(b)
      }
  }
}

pub fn not_equal(a: t, b: t) -> Nil {
  case a != b {
    True -> Nil
    False ->
      panic as {
        "Assertion failed: expected values to differ, got "
        <> string.inspect(a)
      }
  }
}

pub fn be_true(value: Bool) -> Nil {
  case value {
    True -> Nil
    False -> panic as "Assertion failed: expected True"
  }
}

pub fn be_ok(result: Result(a, b)) -> Nil {
  case result {
    Ok(_) -> Nil
    Error(e) ->
      panic as {
        "Assertion failed: expected Ok, got Error("
        <> string.inspect(e)
        <> ")"
      }
  }
}

pub fn be_error(result: Result(a, b)) -> Nil {
  case result {
    Error(_) -> Nil
    Ok(v) ->
      panic as {
        "Assertion failed: expected Error, got Ok("
        <> string.inspect(v)
        <> ")"
      }
  }
}
