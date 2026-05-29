/**
 * Structural contract for a command.
 *
 * Same pattern as `Event` (see `event.ts`): applications declare commands
 * as discriminated unions over the literal `type` field; SDK APIs that
 * accept commands type-check the user's union against this shape.
 */
export interface Command<T extends string = string> {
  type: T;
}
