// Typed return shapes for the core API. These are the data contracts the CLI
// (and any other consumer) formats; keep them free of presentation concerns.

export interface StoreStatus {
  schemaVersion: string;
  allHead: number;
  streams: number;
  events: number;
  subscriptions: number;
}

export interface InstallResult {
  // Whether a schema already existed before this call.
  alreadyExisted: boolean;
  // 'installed' on a fresh database, 'reinstalled' when --force dropped an
  // existing schema first.
  action: "installed" | "reinstalled";
  schemaVersion: string;
}

export interface SubscriptionSummary {
  subscriptionName: string;
  streamUuid: string;
  lastSeen: number;
  // How far behind $all's head this subscription is (allHead - lastSeen).
  lag: number;
  claimedBy: string | null;
  claimExpiresAt: Date | null;
  createdAt: Date;
}
