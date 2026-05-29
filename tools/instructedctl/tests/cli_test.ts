// Unit tests for the argument parser and shared option resolution. No
// database required.

import { assertEquals } from "@std/assert";
import { flagBool, makeContext, parseArgs } from "../src/cli.ts";

Deno.test("parseArgs: positionals only", () => {
  const { positionals, flags } = parseArgs(["read-stream", "demo-1"]);
  assertEquals(positionals, ["read-stream", "demo-1"]);
  assertEquals(flags.size, 0);
});

Deno.test("parseArgs: --flag value", () => {
  const { flags } = parseArgs(["--database", "mydb"]);
  assertEquals(flags.get("database"), "mydb");
});

Deno.test("parseArgs: --flag=value", () => {
  const { flags } = parseArgs(["--database=mydb"]);
  assertEquals(flags.get("database"), "mydb");
});

Deno.test("parseArgs: boolean flag (no value)", () => {
  const { flags } = parseArgs(["--verbose"]);
  assertEquals(flags.get("verbose"), true);
});

Deno.test("parseArgs: boolean flag before positional stays boolean", () => {
  // A following token that itself looks like a flag must not be consumed.
  const { positionals, flags } = parseArgs(["--verbose", "--help"]);
  assertEquals(flags.get("verbose"), true);
  assertEquals(flags.get("help"), true);
  assertEquals(positionals, []);
});

Deno.test("parseArgs: short flag with value", () => {
  const { flags } = parseArgs(["-d", "mydb", "-v"]);
  assertEquals(flags.get("d"), "mydb");
  assertEquals(flags.get("v"), true);
});

Deno.test("parseArgs: -- stops flag parsing", () => {
  const { positionals, flags } = parseArgs(["--verbose", "--", "--not-a-flag"]);
  assertEquals(flags.get("verbose"), true);
  assertEquals(positionals, ["--not-a-flag"]);
});

Deno.test("parseArgs: mixed positionals and flags", () => {
  const { positionals, flags } = parseArgs([
    "demo-1",
    "--from",
    "10",
    "--count=5",
    "tail",
  ]);
  assertEquals(positionals, ["demo-1", "tail"]);
  assertEquals(flags.get("from"), "10");
  assertEquals(flags.get("count"), "5");
});

Deno.test("flagBool: recognises any of several names", () => {
  const args = parseArgs(["-v"]);
  assertEquals(flagBool(args, "verbose", "v"), true);
  assertEquals(flagBool(args, "verbose"), false);
});

Deno.test("makeContext: verbose flag flows through", () => {
  const ctx = makeContext(parseArgs(["--verbose"]));
  assertEquals(ctx.verbose, true);
});

Deno.test("makeContext: --database URI produces a uri config", () => {
  const ctx = makeContext(
    parseArgs(["--database", "postgresql://h/db"]),
  );
  assertEquals(ctx.dbConfig.uri, "postgresql://h/db");
});
