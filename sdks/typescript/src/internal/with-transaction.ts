/**
 * Internal helper: open a short SDK-internal transaction on the
 * persist client's pool and run a callback against a dedicated
 * session-bound {@link Client}.
 *
 * The PM worker uses this to make the snapshot write + cursor advance
 * atomic — the *only* place the SDK opens
 * a multi-procedure transaction. The handler itself is opaque
 * (D-0016) and runs outside any SDK transaction.
 *
 * If the underlying connection is a pg.Pool (has `.connect`), the
 * helper checks out a dedicated client. Otherwise (a pg.Client or
 * pg.PoolClient), it issues BEGIN/COMMIT on the existing session.
 */

import type * as pg from "pg";
import { Client } from "../client/index.ts";
import { mapPgError } from "../errors/index.ts";
import type { Queryable } from "../types/index.ts";

interface PoolLike {
  connect(): Promise<pg.PoolClient>;
}

function isPoolLike(con: unknown): con is PoolLike {
  return (
    typeof con === "object" &&
    con !== null &&
    typeof (con as { connect?: unknown }).connect === "function"
  );
}

export async function withTransaction<T>(
  client: Client,
  fn: (tx: Client) => Promise<T>,
): Promise<T> {
  const con = client.con as unknown;
  if (isPoolLike(con)) {
    const pc = await con.connect();
    try {
      await pc.query("BEGIN");
      let result: T;
      try {
        result = await fn(new Client(pc as unknown as Queryable));
      } catch (err) {
        try {
          await pc.query("ROLLBACK");
        } catch {
          // ignore
        }
        throw err;
      }
      await pc.query("COMMIT");
      return result;
    } finally {
      pc.release();
    }
  }
  // Already a single session: inline BEGIN/COMMIT.
  const session = con as Queryable;
  try {
    await session.query("BEGIN");
  } catch (err) {
    throw mapPgError(err);
  }
  let result: T;
  try {
    result = await fn(client);
  } catch (err) {
    try {
      await session.query("ROLLBACK");
    } catch {
      // ignore
    }
    throw err;
  }
  await session.query("COMMIT");
  return result;
}
