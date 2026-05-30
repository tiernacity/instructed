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

export interface StreamSummary {
  streamUuid: string;
  // Current head version of the stream.
  head: number;
  // Number of events on the stream.
  eventCount: number;
}

export interface EventRecord {
  eventId: string;
  // Position in $all.
  eventNumber: number;
  // Original stream identity (not $all).
  streamUuid: string;
  streamVersion: number;
  eventType: string;
  causationId: string | null;
  correlationId: string | null;
  data: unknown;
  metadata: unknown;
  createdAt: Date;
}

export interface Snapshot {
  sourceUuid: string;
  sourceType: string;
  sourceVersion: number;
  data: unknown;
  metadata: unknown;
  createdAt: Date;
}

export interface WorkItemCounts {
  subscriptionName: string;
  pending: number;
  claimed: number;
  failed: number;
  done: number;
  total: number;
  // Lowest event_number still in a non-terminal state (pending/claimed/failed),
  // or null when the subscription has no active work.
  oldestActiveEventNumber: number | null;
}

export interface FailedWorkItem {
  subscriptionName: string;
  partitionKey: string;
  eventNumber: number;
  failedAt: Date | null;
  errorText: string | null;
}

export interface HealthCheck {
  name: string;
  ok: boolean;
  detail: string;
}

export interface HealthReport {
  ok: boolean;
  checks: HealthCheck[];
}

export interface ClaimResult {
  // 'claimed' when this call took the lease; 'already_claimed' when another
  // worker holds a live lease.
  result: "claimed" | "already_claimed";
  lastSeen: number;
  claimedBy: string | null;
  claimExpiresAt: Date | null;
}
