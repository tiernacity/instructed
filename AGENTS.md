# Agent Notes

## Gleam: Warnings as Errors

`gleam.toml` does **not** support `warnings_as_errors`. Use the CLI flag:

```sh
gleam build --warnings-as-errors
```

This is configured in the Makefiles. Run `make check` from the repo root to build all projects with warnings as errors.
