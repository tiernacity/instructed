> [!CAUTION]
> This repository is **entirely agent-generated**. Its primary purpose is as a medium-complexity test case for exercising different AI coding agent workflows (e.g. [ralph-wiggum](https://ghuntley.com/ralph/)). The code may contain bugs, incomplete features, or questionable design decisions. Use at your own risk.

# Instructed

A strongly-typed CQRS/ES (Command Query Responsibility Segregation / Event Sourcing) framework for [Gleam](https://gleam.run), inspired by the Elixir [Commanded](https://github.com/commanded/commanded) library. It provides aggregates, command routing, event handlers, projections, process managers, middleware, and snapshotting — all with compile-time type safety via Gleam's type system.

## Repository Structure

### `instructed/`

The core framework. Defines the `EventStore` interface, aggregates, command routing, projections, event handlers, process managers, middleware, and an in-memory event store for testing.

### `instructed-postgres/`

PostgreSQL event store adapter. Implements the `EventStore` interface using `pog` for persistent, production-grade event storage. Requires a running PostgreSQL instance.

### `instructed-sqlite/`

SQLite event store adapter. Implements the `EventStore` interface using `sqlight` (which bundles SQLite via `esqlite` — no separate installation required). All operations are serialized through an OTP actor. Optimistic locking is enforced via a `UNIQUE(stream_id, stream_version)` constraint.

### `example-todo/`

A CLI todo application demonstrating the framework. Uses the SQLite adapter for persistence. Showcases aggregates, command dispatch, event-driven projections (active/completed/overdue/by-priority/by-due-date views), and the full CQRS/ES lifecycle.
