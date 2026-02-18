> [!CAUTION]
> **DO NOT USE THIS LIBRARY.** This code is broken and incomplete. It is missing critical CQRS/ES guarantees including command serialization, idempotency, error handling, and consistency controls. Using it in production will result in data corruption, lost events, and silent failures. See `REVIEW.md` for a full list of issues.

> [!NOTE]
> This repository is **entirely agent-generated**. Its primary purpose is as a medium-complexity test case for exercising different AI coding agent workflows (e.g. [ralph-wiggum](https://ghuntley.com/ralph/)). The code may contain bugs, incomplete features, or questionable design decisions.

# Instructed

An **incomplete and broken** attempt at a Gleam port of the Elixir [Commanded](https://github.com/commanded/commanded) CQRS/ES library.

## Status: Not Fit For Purpose

A comprehensive review against Commanded (`REVIEW.md`) found:

- **3 CRITICAL issues**: No command serialization per aggregate, errors silently swallowed, no idempotency (duplicate processing on restart)
- **3 HIGH severity gaps**: No supervision tree, no strong/eventual consistency model, process managers lose state on restart
- **16 missing or incomplete features** out of 22 reviewed

The library has the scaffolding of a CQRS/ES framework (types, interfaces, basic wiring) but is missing the guarantees that make such a system actually work. It is roughly equivalent to a prototype or proof-of-concept.

## What's Here

### `instructed/`

The core framework. Has types for aggregates, command routing, event handlers, projections, process managers, middleware, and an in-memory event store. Most of these have significant correctness gaps — see `REVIEW.md` for details.

### `instructed-postgres/`

PostgreSQL event store adapter. Has a race condition in the append path (version check and insert are not atomic).

### `instructed-sqlite/`

SQLite event store adapter. Operations are serialized through an OTP actor so the race condition doesn't apply here.

### `example-todo/`

A CLI todo application demonstrating basic usage. Works for the happy path but inherits all framework issues.

## Documents

- **`REVIEW.md`** — Comprehensive feature-by-feature comparison against Commanded (22 areas reviewed)
- **`instructed-iterate.md`** — Instructions for an iterative process to bring this to feature parity with Commanded
- **`instructed-review-instructions.md`** — Instructions for re-running the review at any time
