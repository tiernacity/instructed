/**
 * Pluggable logger surface (TODO #16).
 *
 * Two-layer design, separated by audience:
 *
 *   - {@link ILoggerImpl} — what the application supplies. Four
 *     optional methods (`info` / `warn` / `error` / `trace`) that
 *     take eager strings. No knowledge of laziness required.
 *   - {@link Logger} — what the SDK and `ctx.logger` expose. A
 *     concrete class with all four methods always present, each
 *     accepting `string | (() => string)`. Levels that the
 *     application's {@link ILoggerImpl} did not wire resolve to a
 *     true no-op: the thunk is not invoked, so liberally-scattered
 *     `trace(() => `...${expensive}...`)` calls cost only the
 *     arrow-allocation when no trace sink is wired.
 *
 * Default behaviour when the application does not supply a logger:
 * `info` / `warn` / `error` route to the corresponding `console`
 * methods; `trace` is silent. See {@link DEFAULT_LOGGER_IMPL}.
 * Applications wanting a fully-silent default pass `logger: {}`
 * to `Instructed`.
 */

/**
 * Application-supplied logger interface. All four methods are
 * optional; supply only those you want output for. Implementations
 * are expected to be cheap on the hot path but otherwise
 * unconstrained (route to pino / winston / console / nothing).
 */
export interface ILoggerImpl {
  info?(msg: string): void
  warn?(msg: string): void
  error?(msg: string): void
  trace?(msg: string): void
}

/**
 * A log message: either a ready-built string, or a thunk that
 * builds one. The thunk form is the optimisation: it is invoked
 * only when the corresponding level is wired on the underlying
 * {@link ILoggerImpl}. Use the thunk form for messages whose body
 * costs more than an arrow allocation (template-string
 * interpolation with multiple substitutions, `JSON.stringify`,
 * iterating a collection, etc.).
 */
export type LogMessage = string | (() => string)

/**
 * Concrete logger handed to the SDK and to user handlers via
 * `ctx.logger`. All four methods are always present; unwired
 * levels are true no-ops. Prefix (set at construction time) is
 * prepended only when emitting, so it costs nothing for unwired
 * levels.
 *
 * Construct with the static factories; do not call `new` directly.
 *
 *   - {@link Logger.fromImpl} — wrap an {@link ILoggerImpl}, with
 *     an optional prefix. `impl === undefined` produces a fully
 *     no-op logger.
 *   - {@link Logger.noop} — a fully no-op logger; convenient for
 *     tests and for synthesising a `ctx` without a real sink.
 */
export class Logger {
  readonly info: (msg: LogMessage) => void
  readonly warn: (msg: LogMessage) => void
  readonly error: (msg: LogMessage) => void
  readonly trace: (msg: LogMessage) => void

  private readonly impl: ILoggerImpl | undefined
  private readonly prefix: string | undefined

  private constructor(impl: ILoggerImpl | undefined, prefix?: string) {
    this.impl = impl
    this.prefix = prefix
    this.info = bind(impl, 'info', prefix)
    this.warn = bind(impl, 'warn', prefix)
    this.error = bind(impl, 'error', prefix)
    this.trace = bind(impl, 'trace', prefix)
  }

  /**
   * Build a Logger over an application-supplied {@link ILoggerImpl}.
   * When `impl` is `undefined`, every level is a no-op. When a
   * specific method on `impl` is absent, that level is a no-op:
   * the message thunk (if used) is not invoked.
   */
  static fromImpl(impl: ILoggerImpl | undefined, prefix?: string): Logger {
    return new Logger(impl, prefix)
  }

  /**
   * A logger whose every method is a no-op. Equivalent to
   * `Logger.fromImpl(NOOP_LOGGER_IMPL)`. Intended for tests that
   * synthesise a `ctx` directly; application code wanting silence
   * passes `NOOP_LOGGER_IMPL` to {@link Instructed} instead and
   * lets the facade build the `Logger`.
   */
  static noop(): Logger {
    return new Logger(undefined, undefined)
  }

  /**
   * Derive a child Logger that appends `extra` to this logger's
   * prefix (separated by a space). Cheap; constructs a fresh
   * bound-method set over the same underlying {@link ILoggerImpl}.
   */
  child(extra: string): Logger {
    const next = this.prefix ? `${this.prefix} ${extra}` : extra
    return new Logger(this.impl, next)
  }
}

/**
 * Used by `Instructed` when no `logger` option is supplied.
 * Routes `info` / `warn` / `error` to `console`; `trace` is
 * intentionally absent (i.e. silent) so that the lazy-thunk
 * optimisation continues to apply by default. Applications that
 * want trace output supply their own `ILoggerImpl` with a `trace`
 * method.
 */
export const DEFAULT_LOGGER_IMPL: ILoggerImpl = {
  // eslint-disable-next-line no-console
  info: (msg) => console.info(msg),
  // eslint-disable-next-line no-console
  warn: (msg) => console.warn(msg),
  // eslint-disable-next-line no-console
  error: (msg) => console.error(msg),
}

/**
 * A fully-silent {@link ILoggerImpl}. No method is defined, so a
 * `Logger` built over it treats every level as a no-op (the lazy
 * thunk is never invoked). Use for tests that don't want runtime
 * log output, or for applications that want to opt out of the
 * default `console` wiring on `Instructed`.
 *
 *   - Application: `new Instructed({ db, logger: NOOP_LOGGER_IMPL })`
 *     — silent app.
 *   - L2 direct callers: `{ logger: Logger.fromImpl(NOOP_LOGGER_IMPL) }`
 *     as `opts.ctx` on `runCommand` etc.
 */
export const NOOP_LOGGER_IMPL: ILoggerImpl = {}

const NOOP: (m: LogMessage) => void = () => {
  /* unwired level: thunk argument is intentionally not evaluated */
}

function bind(
  impl: ILoggerImpl | undefined,
  level: 'info' | 'warn' | 'error' | 'trace',
  prefix: string | undefined,
): (msg: LogMessage) => void {
  const fn = impl?.[level]
  if (!fn) return NOOP
  // Bind to `impl` so methods that rely on `this` (e.g. a class
  // method delegating to internal state) work as expected.
  const bound = fn.bind(impl)
  if (prefix === undefined) {
    return (m) => bound(typeof m === 'function' ? m() : m)
  }
  return (m) => bound(`${prefix} ${typeof m === 'function' ? m() : m}`)
}
