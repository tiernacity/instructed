/**
 * instructed TypeScript SDK — public surface.
 *
 * See docs/sdk-design.md for the layered design. This file re-exports
 * only the public contract; everything under `src/internal/` is private.
 */

export { Client, type ClientOptions } from "./client.ts";
export * from "./errors.ts";
export * from "./types.ts";
