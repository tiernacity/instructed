/**
 * Event-shaped contracts: the structural `Event` declaration pattern,
 * the append input (`NewEvent`), and the recorded-event read shapes
 * (`AppendedEvent`, `RecordedEvent`).
 *
 * Mirrors the SQL contract in `sql/instructed.sql`.
 */

/**
 * Structural contract for a domain event.
 *
 * Applications declare their own event types (typically a discriminated
 * union over the literal `type` field) and pass them to SDK APIs; the
 * type-checker enforces compatibility with this shape. The SDK does NOT
 * require user code to `extends Event<...>` — `Event` is the contract,
 * not a base class.
 *
 * Recommended declaration pattern (no SDK import needed in the event
 * file itself):
 *
 *     export const AccountDepositedTo = "AccountDepositedTo" as const;
 *     export type AccountDepositedTo = {
 *       type: typeof AccountDepositedTo;
 *       data: { amount: number; transferId?: string };
 *     };
 *
 * The value-and-type-share-a-name pattern lets the same identifier
 * stand in for the literal-string discriminator (in value position)
 * and the event's TypeScript type (in type position).
 */
export interface Event<T extends string = string, D = unknown> {
  type: T
  data: D
}

/**
 * An event the caller wants to append. The SDK fills `event_id`,
 * `causation_id`, and `correlation_id` when omitted (§11.2 / §11.8).
 *
 * Generic `E` is the application's event union (each member extending
 * {@link Event}); the type distributes over the union so each branch
 * carries its own `type` literal and `data` shape. Pass the union
 * (e.g. `NewEvent<AccountEvent>`) to get discriminated-union narrowing
 * inside switches on `type`. Default `E = Event` gives the historical
 * open shape (`type: string`, `data: unknown`).
 *
 * Note: the field is `type`, not `event_type` — the underlying SQL
 * column is `event_type`, but the SDK normalises the TypeScript
 * surface to a single `type` field. The `Client.appendToStream` and
 * `Client.readStream` boundary maps between the two (L2-only rename;
 * L1 wire / SQL column unchanged).
 */
export interface NewEvent<E = unknown> {
  event_id?: string
  type: string
  data: E
  metadata?: unknown
  causation_id?: string
  correlation_id?: string
}

/** One row returned by `append_to_stream`, in append order.
 *
 *  `stream_uuid` is populated client-side from the `appendToStream`
 *  argument (the SQL procedure doesn't echo it back). It is
 *  load-bearing for the CON-B cross-stream guard in
 *  `waitForProjection`. */
export interface AppendedEvent {
  event_id: string
  stream_uuid: string
  stream_version: bigint
  event_number: bigint
  created_at: Date
}

/** Bookkeeping fields shared by every recorded event, independent of
 *  the domain payload. Internal building block for {@link RecordedEvent};
 *  not part of the porting-checklist surface. */
export interface RecordedEventFields {
  event_id: string
  event_number: bigint
  stream_uuid: string
  stream_version: bigint
  causation_id: string | null
  correlation_id: string | null
  metadata: unknown
  created_at: Date
}

/** A recorded event row, the shape returned by read_stream / read_all /
 *  list_pm_rebuild_events.
 *
 *  Generic `E` is the application's event union (each member extending
 *  {@link Event}); the type distributes so each branch carries its own
 *  `type` literal and `data` shape. Inside
 *  `switch (event.type) { case "Foo": ... }`, `event.data` is narrowed
 *  to the matching branch's data — no casting needed. Default
 *  `E = Event` gives the historical open shape.
 *
 *  Note: `type` (not `event_type`) — see {@link NewEvent}. */
export type RecordedEvent<E extends Event = Event> = E extends Event
  ? RecordedEventFields & { type: E['type']; data: E['data'] }
  : never

/** Options for `append_to_stream`. v1 has no recognised keys. */
export interface AppendOptions {
  /** Reserved for future use; currently unused. */
}
