/**
 * Shared `RunningWorker` interface used by every worker module
 * (routing, processing, projection, PM, and the layer-5 facade
 * composite). Lifted here in SUB-A slice 9 when the legacy
 * `subscription.ts` module was removed.
 */

export interface RunningWorker {
  /** Idempotent. Stops the loop, releases the lease best-effort. */
  close(): Promise<void>;
  /** Resolves when the loop has exited. Never rejects. */
  readonly stopped: Promise<void>;
}
