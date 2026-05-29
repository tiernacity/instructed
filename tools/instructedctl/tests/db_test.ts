// Unit tests for connection-config resolution and URI sanitisation. These
// manipulate the environment, so each test snapshots and restores the vars it
// touches.

import { assertEquals } from "@std/assert";
import { configFromOptions, sanitizeUri } from "../src/db.ts";

const ENV_VARS = [
  "INSTRUCTED_DATABASE_URL",
  "PGDATABASE",
  "PGHOST",
  "PGPORT",
  "PGUSER",
  "PGPASSWORD",
];

function withEnv(
  overrides: Record<string, string | undefined>,
  fn: () => void,
): void {
  const saved = new Map<string, string | undefined>();
  for (const key of ENV_VARS) {
    saved.set(key, Deno.env.get(key));
    Deno.env.delete(key);
  }
  try {
    for (const [key, value] of Object.entries(overrides)) {
      if (value !== undefined) Deno.env.set(key, value);
    }
    fn();
  } finally {
    for (const key of ENV_VARS) {
      const value = saved.get(key);
      if (value === undefined) Deno.env.delete(key);
      else Deno.env.set(key, value);
    }
  }
}

Deno.test("configFromOptions: explicit URI flag wins", () => {
  withEnv({ INSTRUCTED_DATABASE_URL: "postgresql://env/db" }, () => {
    const config = configFromOptions({ database: "postgresql://flag/db" });
    assertEquals(config.uri, "postgresql://flag/db");
  });
});

Deno.test("configFromOptions: INSTRUCTED_DATABASE_URL over PGDATABASE", () => {
  withEnv(
    {
      INSTRUCTED_DATABASE_URL: "postgresql://env/db",
      PGDATABASE: "postgresql://pg/db",
    },
    () => {
      const config = configFromOptions({});
      assertEquals(config.uri, "postgresql://env/db");
    },
  );
});

Deno.test("configFromOptions: default URI when nothing set", () => {
  withEnv({}, () => {
    const config = configFromOptions({});
    assertEquals(config.uri, "postgresql://localhost/instructed");
  });
});

Deno.test("configFromOptions: discrete fields when database is a bare name", () => {
  withEnv({ PGPASSWORD: "secret" }, () => {
    const config = configFromOptions({
      database: "mydb",
      host: "db.example",
      port: "6543",
      user: "alice",
    });
    assertEquals(config.uri, undefined);
    assertEquals(config.host, "db.example");
    assertEquals(config.port, 6543);
    assertEquals(config.user, "alice");
    assertEquals(config.password, "secret");
    assertEquals(config.database, "mydb");
  });
});

Deno.test("configFromOptions: bare-name database falls back to PG* vars", () => {
  withEnv(
    { PGDATABASE: "envdb", PGHOST: "envhost", PGPORT: "7777", PGUSER: "bob" },
    () => {
      const config = configFromOptions({});
      assertEquals(config.uri, undefined);
      assertEquals(config.host, "envhost");
      assertEquals(config.port, 7777);
      assertEquals(config.user, "bob");
      assertEquals(config.database, "envdb");
    },
  );
});

Deno.test("sanitizeUri: redacts the password", () => {
  assertEquals(
    sanitizeUri("postgresql://user:secret@localhost:5432/db"),
    "postgresql://user:****@localhost:5432/db",
  );
});

Deno.test("sanitizeUri: leaves a password-less URI untouched", () => {
  assertEquals(
    sanitizeUri("postgresql://localhost:5432/db"),
    "postgresql://localhost:5432/db",
  );
});
