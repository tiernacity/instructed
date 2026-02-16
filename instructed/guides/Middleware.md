# Middleware

Middleware provides extension points for the command dispatch pipeline. Use it for validation, authorization, logging, and other cross-cutting concerns.

## Defining Middleware

```gleam
import instructed/middleware

let my_middleware = middleware.new(
  before_dispatch: fn(pipeline) {
    // Called before command execution
    pipeline
  },
  after_dispatch: fn(pipeline) {
    // Called after successful execution
    pipeline
  },
  after_failure: fn(pipeline) {
    // Called after failed execution
    pipeline
  },
)
```

## Pipeline

The pipeline carries the command and its context through middleware:

```gleam
type Pipeline(command) {
  Pipeline(
    command: command,
    command_id: String,
    causation_id: Option(String),
    correlation_id: Option(String),
    metadata: Dict(String, String),
    assigns: Dict(String, String),
    halted: Bool,
    response: Option(PipelineResponse),
  )
}
```

## Halting the Pipeline

To prevent a command from being dispatched:

```gleam
let auth_middleware = middleware.new(
  before_dispatch: fn(pipeline) {
    case is_authorized(pipeline.command) {
      True -> pipeline
      False -> middleware.halt(pipeline)
    }
  },
  after_dispatch: fn(p) { p },
  after_failure: fn(p) { p },
)
```

## Assigning Values

Store data in the pipeline's assigns map:

```gleam
let enrichment = middleware.new(
  before_dispatch: fn(pipeline) {
    middleware.assign(pipeline, "processed_at", timestamp())
  },
  after_dispatch: fn(p) { p },
  after_failure: fn(p) { p },
)
```

## Logging Middleware

```gleam
let logger = middleware.new(
  before_dispatch: fn(pipeline) {
    io.println("Dispatching: " <> string.inspect(pipeline.command))
    pipeline
  },
  after_dispatch: fn(pipeline) {
    io.println("Success: " <> pipeline.command_id)
    pipeline
  },
  after_failure: fn(pipeline) {
    io.println("Failed: " <> pipeline.command_id)
    pipeline
  },
)
```

## Adding Middleware to Router

```gleam
let router = router.new(...)
  |> router.with_middleware(logger)
  |> router.with_middleware(auth_middleware)
  |> router.with_middleware(enrichment)
```

Middleware executes in the order added.
