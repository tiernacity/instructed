/**
 * Foundational primitives: the connection-like object the SDK accepts,
 * and the JSON value shape used for event data / metadata / snapshot data.
 */

import type * as pg from 'pg'

/**
 * A connection-like object accepted by the SDK. Matches absurd's idiom:
 * any pg.Pool / pg.Client / pg.PoolClient (anything with `.query`).
 */
export type Queryable = Pick<pg.Client, 'query'> | Pick<pg.PoolClient, 'query'>

/** A JSON value, used for event data / metadata / snapshot data. */
export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue }
