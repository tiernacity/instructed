/** L1 — IS020 / IS021 / IS022: subscription errors. */

import { InstructedError } from "./base.ts";

export class SubscriptionError extends InstructedError {
  readonly streamUuid?: string;
  readonly subscriptionName?: string;
  readonly holder?: string;
  constructor(
    message: string,
    opts: {
      code?: string;
      detail?: string;
      hint?: string;
      streamUuid?: string;
      subscriptionName?: string;
      holder?: string;
      cause?: unknown;
    } = {},
  ) {
    super(message, opts);
    this.streamUuid = opts.streamUuid;
    this.subscriptionName = opts.subscriptionName;
    this.holder = opts.holder;
  }
}

export class SubscriptionNotFound extends SubscriptionError {}
/** Reserved by the SQL catalogue; never thrown in v1. */
export class SubscriptionAlreadyClaimed extends SubscriptionError {}
export class SubscriptionLeaseLost extends SubscriptionError {}
