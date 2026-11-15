export * as ClientKV from "./client-kv.js"

import { Schema } from "effect"
import { ephemeral, inventory } from "./event.js"

export const Space = Schema.String.check(Schema.isPattern(/^[A-Za-z0-9._-]{1,128}$/u)).annotate({
  identifier: "ClientKV.Space",
  description: "Client-chosen namespace: 1-128 letters, digits, '.', '_' or '-'.",
})
export type Space = typeof Space.Type

export const Key = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(1024)).annotate({
  identifier: "ClientKV.Key",
})
export type Key = typeof Key.Type

/** Any JSON but a top-level null, which the store cannot hold. */
export const Value = Schema.Union([
  Schema.String,
  Schema.Finite,
  Schema.Boolean,
  Schema.Array(Schema.Json),
  Schema.Record(Schema.String, Schema.Json),
]).annotate({ identifier: "ClientKV.Value" })
export type Value = typeof Value.Type

export interface Entry extends Schema.Schema.Type<typeof Entry> {}
export const Entry = Schema.Struct({ key: Key, value: Value }).annotate({ identifier: "ClientKV.Entry" })

/** Every entry of a space a subscriber requested, sent once when its event stream opens. Never published on the bus. */
const Snapshot = ephemeral({
  type: "experimental.kv.snapshot",
  schema: { space: Space, entries: Schema.Array(Entry) },
})
const Updated = ephemeral({
  type: "experimental.kv.updated",
  schema: { space: Space, key: Key, value: Value },
})
const Deleted = ephemeral({
  type: "experimental.kv.deleted",
  schema: { space: Space, key: Key },
})
export const Event = { Snapshot, Updated, Deleted, Definitions: inventory(Snapshot, Updated, Deleted) }
