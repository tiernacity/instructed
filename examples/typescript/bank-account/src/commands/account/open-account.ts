/**
 * `OpenAccount` — request to open a new account.
 *
 * Commands carry their target aggregate id directly on the
 * command (here `accountId`); the command router uses this
 * extractor to resolve the dispatch to `(Account, accountId)`.
 * No stream-name construction in app code.
 */
export const OpenAccount = "OpenAccount" as const;
export type OpenAccount = {
  type: typeof OpenAccount;
  accountId: string;
  owner: string;
};
