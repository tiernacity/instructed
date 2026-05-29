/**
 * Default worker-id generator. Format: `${hostname}:${pid}:${random}`.
 *
 * The store treats `worker_id` as an opaque string (per
 * `sql/instructed.sql` :: claim_subscription); this default just makes
 * stuck leases easy to track down in operator dashboards.
 */
import { hostname } from 'node:os'

export function defaultWorkerId(): string {
  const h = (() => {
    try {
      return hostname() || 'host'
    } catch {
      return 'host'
    }
  })()
  const pid = typeof process !== 'undefined' ? process.pid : 0
  // 8 hex chars from a fresh UUID. Plenty of uniqueness for one process.
  const rand = globalThis.crypto.randomUUID().replace(/-/g, '').slice(0, 8)
  return `${h}:${pid}:${rand}`
}
