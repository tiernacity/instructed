/**
 * Shared `RunningWorker` interface used by every worker module
 * (routing, processing, projection, PM, and the layer-5 facade
 * composite). Lifted here in SUB-A slice 9 when the legacy
 * `subscription.ts` module was removed.
 */

export interface RunningWorker {
  /**
   * Idempotent. Stops the loop, releases the lease best-effort.
   * Returns the same promise as {@link stopped}, so
   * `await worker.stop()` waits for the loop to fully exit.
   */
  stop(): Promise<void>
  /**
   * Resolves when the loop has exited — whether stopped via
   * {@link stop}, by a worker-internal error policy escalation,
   * or by any other means. Never rejects.
   */
  readonly stopped: Promise<void>
}
