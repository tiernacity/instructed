/**
 * Layer 0 client (barrel).
 *
 * This is a barrel: it contains nothing but re-exports. The `Client`
 * class and its public `ClientOptions` live in `client.ts`; the L2→L1
 * input packers and L1→L2 row mappers live in `pack-event.ts` and
 * `row-mappers.ts` respectively (internal to the client layer, not part
 * of the public surface).
 */

export { Client, type ClientOptions } from './client.ts'
