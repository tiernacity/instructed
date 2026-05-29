// Command registry. New commands are added here; main.ts and the help text
// derive their behaviour from this single list so the surface stays in one
// place.

import type { Command } from "./cli.ts";
import { installCommand } from "./commands/install.ts";
import { statusCommand } from "./commands/status.ts";
import { schemaVersionCommand } from "./commands/schema-version.ts";

export const COMMANDS: Command[] = [
  installCommand,
  statusCommand,
  schemaVersionCommand,
];

export function findCommand(name: string): Command | undefined {
  return COMMANDS.find((c) => c.name === name);
}
