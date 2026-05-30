// `health` command: a quick store-soundness check. Exits non-zero if any check
// fails, so it is usable as a monitoring probe.

import { Command } from "@cliffy/command";
import { checkHealth } from "../../core/index.ts";
import { action, type GlobalOptions, runWith } from "../options.ts";
import { printJson, printTable } from "../output.ts";

export function healthCommand() {
  return new Command()
    .description("Check store soundness ($all contiguity, orphans, zombies)")
    .action((opts) =>
      action(async () => {
        const g = opts as unknown as GlobalOptions;
        const report = await runWith(g, checkHealth);
        if (g.json) {
          printJson(report);
        } else {
          printTable(
            ["CHECK", "STATUS", "DETAIL"],
            report.checks.map((c) => [c.name, c.ok ? "ok" : "FAIL", c.detail]),
          );
        }
        if (!report.ok) Deno.exit(1);
      })
    );
}
