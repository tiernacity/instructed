# example_todo

[![Package Version](https://img.shields.io/hexpm/v/example_todo)](https://hex.pm/packages/example_todo)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/example_todo/)

```sh
gleam add example_todo@1
```
```gleam
import example_todo

pub fn main() -> Nil {
  // TODO: An example of the project in use
}
```

Further documentation can be found at <https://hexdocs.pm/example_todo>.

## Development

```sh
gleam run   # Run the project
gleam test  # Run the tests
```

## Building a Binary

Build a self-contained `todo` executable (requires Erlang and `rebar3`):

```sh
make build
```

This produces a `todo` escript binary (~1 MB) that can be run directly:

```sh
./todo help
./todo add "Buy groceries" high 2026-02-20
./todo list
```

The binary is portable to any system with a compatible Erlang/OTP installation.
