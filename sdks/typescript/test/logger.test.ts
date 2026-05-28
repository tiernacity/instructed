/**
 * Unit tests for the pluggable logger surface (TODO #16).
 *
 * Covers the three observable promises:
 *
 *   1. Levels supplied on the `ILoggerImpl` receive their messages
 *      (eager string or thunk-resolved string).
 *   2. Levels NOT supplied are true no-ops: a thunk passed to that
 *      level is not invoked. This is the optimisation that lets
 *      trace sites be scattered without worrying about cost.
 *   3. `child(prefix)` layers prefixes; the prefix is applied only
 *      on wired levels (no allocation cost on unwired levels).
 */

import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { Logger, DEFAULT_LOGGER_IMPL, NOOP_LOGGER_IMPL } from "../src/logger.ts";

describe("Logger -- wired levels", () => {
  test("eager string passes through to the underlying impl", () => {
    const seen: string[] = [];
    const log = Logger.fromImpl({ info: (m) => seen.push(m) });
    log.info("hello");
    assert.deepEqual(seen, ["hello"]);
  });

  test("thunk is invoked exactly once when level is wired", () => {
    const seen: string[] = [];
    let calls = 0;
    const log = Logger.fromImpl({ warn: (m) => seen.push(m) });
    log.warn(() => {
      calls += 1;
      return "lazy";
    });
    assert.equal(calls, 1);
    assert.deepEqual(seen, ["lazy"]);
  });

  test("each level routes to its own method", () => {
    const seen: Array<[string, string]> = [];
    const log = Logger.fromImpl({
      info: (m) => seen.push(["info", m]),
      warn: (m) => seen.push(["warn", m]),
      error: (m) => seen.push(["error", m]),
      trace: (m) => seen.push(["trace", m]),
    });
    log.info("i");
    log.warn("w");
    log.error("e");
    log.trace("t");
    assert.deepEqual(seen, [
      ["info", "i"],
      ["warn", "w"],
      ["error", "e"],
      ["trace", "t"],
    ]);
  });
});

describe("Logger -- unwired levels are no-ops", () => {
  test("undefined impl: no thunk ever invoked", () => {
    let trace = 0;
    let warn = 0;
    const log = Logger.fromImpl(undefined);
    log.trace(() => {
      trace += 1;
      return "x";
    });
    log.warn(() => {
      warn += 1;
      return "y";
    });
    assert.equal(trace, 0);
    assert.equal(warn, 0);
  });

  test("partial impl: only wired levels evaluate the thunk", () => {
    const seen: string[] = [];
    let traceCalls = 0;
    let infoCalls = 0;
    // Only `warn` is wired; `trace`/`info`/`error` are no-ops.
    const log = Logger.fromImpl({ warn: (m) => seen.push(m) });
    log.warn(() => {
      // wired -> invoked
      return "w";
    });
    log.trace(() => {
      traceCalls += 1;
      return "should-not-build";
    });
    log.info(() => {
      infoCalls += 1;
      return "should-not-build";
    });
    assert.deepEqual(seen, ["w"]);
    assert.equal(traceCalls, 0);
    assert.equal(infoCalls, 0);
  });

  test("Logger.noop never invokes any thunk", () => {
    let calls = 0;
    const log = Logger.noop();
    for (const level of ["info", "warn", "error", "trace"] as const) {
      log[level](() => {
        calls += 1;
        return "x";
      });
    }
    assert.equal(calls, 0);
  });
});

describe("Logger -- prefix and child()", () => {
  test("constructor prefix is prepended on wired levels", () => {
    const seen: string[] = [];
    const log = Logger.fromImpl(
      { info: (m) => seen.push(m) },
      "[root]",
    );
    log.info("hi");
    log.info(() => "ho");
    assert.deepEqual(seen, ["[root] hi", "[root] ho"]);
  });

  test("prefix is not built on unwired levels (thunk skipped)", () => {
    let calls = 0;
    // `trace` is unwired; the prefix path should not even reach the
    // user's thunk.
    const log = Logger.fromImpl({ info: () => {} }, "[root]");
    log.trace(() => {
      calls += 1;
      return "x";
    });
    assert.equal(calls, 0);
  });

  test("child appends to the parent prefix", () => {
    const seen: string[] = [];
    const root = Logger.fromImpl(
      { warn: (m) => seen.push(m) },
      "[app]",
    );
    const worker = root.child("[w1#Balances]");
    worker.warn("slow batch");
    assert.deepEqual(seen, ["[app] [w1#Balances] slow batch"]);
  });

  test("child of a no-prefix logger uses just the child prefix", () => {
    const seen: string[] = [];
    const root = Logger.fromImpl({ warn: (m) => seen.push(m) });
    const worker = root.child("[w1#Balances]");
    worker.warn("oops");
    assert.deepEqual(seen, ["[w1#Balances] oops"]);
  });
});

describe("DEFAULT_LOGGER_IMPL", () => {
  test("info/warn/error are present; trace is absent", () => {
    assert.equal(typeof DEFAULT_LOGGER_IMPL.info, "function");
    assert.equal(typeof DEFAULT_LOGGER_IMPL.warn, "function");
    assert.equal(typeof DEFAULT_LOGGER_IMPL.error, "function");
    assert.equal(DEFAULT_LOGGER_IMPL.trace, undefined);
  });

  test("a Logger built over the default treats trace as a no-op", () => {
    let calls = 0;
    const log = Logger.fromImpl(DEFAULT_LOGGER_IMPL);
    log.trace(() => {
      calls += 1;
      return "x";
    });
    assert.equal(calls, 0);
  });
});

describe("NOOP_LOGGER_IMPL", () => {
  test("has no methods; every level on a wrapping Logger is a no-op", () => {
    assert.deepEqual(Object.keys(NOOP_LOGGER_IMPL), []);
    let calls = 0;
    const log = Logger.fromImpl(NOOP_LOGGER_IMPL);
    for (const level of ["info", "warn", "error", "trace"] as const) {
      log[level](() => {
        calls += 1;
        return "x";
      });
    }
    assert.equal(calls, 0);
  });
});
