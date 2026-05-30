// CLI smoke tests: drive the Cliffy command tree end-to-end via parse(),
// pointing it at a throwaway database through INSTRUCTED_DATABASE_URL. These
// confirm the thin wrapper wires global options + commands to core; the
// behaviour itself is covered by the core tests.

import { assertEquals, assertStringIncludes } from "@std/assert";
import { buildCli } from "../src/cli/main.ts";
import { capture, createThrowawayDb, uriFor } from "./support.ts";

async function runCli(
  uri: string,
  argv: string[],
): Promise<{ stdout: string; stderr: string }> {
  const prev = Deno.env.get("INSTRUCTED_DATABASE_URL");
  Deno.env.set("INSTRUCTED_DATABASE_URL", uri);
  try {
    const { stdout, stderr } = await capture(() => buildCli().parse(argv));
    return { stdout, stderr };
  } finally {
    if (prev === undefined) Deno.env.delete("INSTRUCTED_DATABASE_URL");
    else Deno.env.set("INSTRUCTED_DATABASE_URL", prev);
  }
}

Deno.test("cli: schema version prints the version", async () => {
  const tw = await createThrowawayDb();
  try {
    const { stdout } = await runCli(uriFor(tw), ["schema", "version"]);
    assertEquals(stdout.trim(), "main");
  } finally {
    await tw.drop();
  }
});

Deno.test("cli: schema (default subcommand) prints status", async () => {
  const tw = await createThrowawayDb();
  try {
    const { stdout } = await runCli(uriFor(tw), ["schema"]);
    assertStringIncludes(stdout, "schema version");
    assertStringIncludes(stdout, "$all head");
  } finally {
    await tw.drop();
  }
});

Deno.test("cli: subscriptions list --json emits JSON", async () => {
  const tw = await createThrowawayDb();
  try {
    const { stdout } = await runCli(uriFor(tw), ["subscriptions", "list", "--json"]);
    assertEquals(JSON.parse(stdout), []);
  } finally {
    await tw.drop();
  }
});

Deno.test("cli: subs alias resolves to subscriptions", async () => {
  const tw = await createThrowawayDb();
  try {
    const { stdout } = await runCli(uriFor(tw), ["subs", "list", "--json"]);
    assertEquals(JSON.parse(stdout), []);
  } finally {
    await tw.drop();
  }
});
