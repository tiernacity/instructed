/**
 * `AccountOpened` — the first event on every Account stream.
 *
 * Value/type share a name: in value position `AccountOpened` is the
 * literal-string discriminator (`"AccountOpened"`); in type position
 * it's the event's TypeScript shape. The SDK consumes the type
 * structurally via `Event<T,D>` — no inheritance required.
 */
export const AccountOpened = "AccountOpened" as const;
export type AccountOpened = {
  type: typeof AccountOpened;
  data: { owner: string };
};
