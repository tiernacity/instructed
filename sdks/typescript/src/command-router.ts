/**
 * L3 command router.
 *
 * A {@link CommandRouter} maps an application-level {@link Command} to
 * the aggregate that owns it: a `(aggregateType, aggregateId)` pair.
 * The SDK uses this resolution to:
 *
 *   - look up the {@link AggregateDefinition} in the facade's
 *     registry, and
 *   - derive the underlying stream name via the aggregate's
 *     `streamName(id)` (defaulting to {@link prefixType}).
 *
 * The application identifies aggregates by `(type, id)` and never
 * needs to construct stream names; stream-naming is a storage-layer
 * concern under D-0026 / step-3 of the API rework.
 *
 * # Why a separate router (and not a method on `Command`)?
 *
 * Commands are plain data — they cross language boundaries (HTTP,
 * queues, audit logs) and we don't want to bind aggregate identity
 * to the command type at declaration time. The router is the
 * deployment-level wiring: "this process knows that
 * `DepositToAccount` lives on the `Account` aggregate, keyed by
 * `accountId`."
 *
 * # Off-the-shelf builder
 *
 * {@link commandRouter} is the standard static-map builder: pass a
 * record keyed by command `type`, with each entry naming the
 * aggregate and an `id(cmd)` extractor. The type-checker enforces
 * exhaustive coverage over the command union and narrows `cmd` to
 * the matching variant inside each `id` callback.
 *
 * Applications wanting non-static routing (e.g. consulting external
 * state) write a `CommandRouter` directly.
 */

import type { AggregateDefinition, DispatchContext } from "./aggregate/index.ts";
import type { Command } from "./types/index.ts";

/**
 * Pure resolution from a command to its target aggregate
 * `(type, id)`. Synchronous by contract: command routing is a
 * deployment-level lookup, not a database access. A throwing
 * router surfaces as a dispatch error.
 */
export type CommandRouter = (
  command: Command,
  ctx: DispatchContext,
) => { aggregateType: string; aggregateId: string };

/**
 * Per-command-type route entry: which aggregate owns this command
 * and how to extract the aggregate id from the command payload.
 *
 * `cmd` is narrowed to the union member matching `type` inside
 * `id`, so e.g. `id: (cmd) => cmd.accountId` type-checks against
 * the `DepositToAccount` variant only.
 */
export interface CommandRoute<C extends Command, K extends C["type"]> {
  aggregate: AggregateDefinition<any, any, any>;
  id: (cmd: Extract<C, { type: K }>) => string;
}

/**
 * Build a static command router from a record keyed by command type.
 *
 * Type-parameterised by the application's command union; the record
 * type forces exhaustive coverage and narrows each `id` callback to
 * the matching variant. A command whose `type` isn't in the table
 * raises at dispatch time.
 *
 * @example
 *
 *     const router = commandRouter<AccountCommand | TransferCommand>({
 *       OpenAccount:         { aggregate: Account,  id: (c) => c.owner },
 *       DepositToAccount:    { aggregate: Account,  id: (c) => c.accountId },
 *       WithdrawFromAccount: { aggregate: Account,  id: (c) => c.accountId },
 *       RequestTransfer:     { aggregate: Transfer, id: (c) => c.transferId },
 *     });
 */
export function commandRouter<C extends Command>(
  routes: { [K in C["type"]]: CommandRoute<C, K> },
): CommandRouter {
  // The mapped-type domain is exactly the discriminator union of
  // `C`; we erase to a plain Record for runtime lookup.
  const table = routes as unknown as Record<
    string,
    CommandRoute<Command, string>
  >;
  return (command, _ctx) => {
    const entry = table[command.type];
    if (!entry) {
      throw new Error(
        `commandRouter: no route for command type "${command.type}"`,
      );
    }
    return {
      aggregateType: entry.aggregate.type,
      aggregateId: entry.id(command as never),
    };
  };
}
