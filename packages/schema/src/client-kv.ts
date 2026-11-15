export * as ClientKV from "./client-kv.js"

import { Schema } from "effect"
import { ephemeral, inventory } from "./event.js"
import { optional } from "./schema.js"

export const Space = Schema.String.check(Schema.isPattern(/^[A-Za-z0-9._-]{1,128}$/u)).annotate({
  identifier: "ClientKV.Space",
  description: "Client-chosen namespace: 1-128 letters, digits, '.', '_' or '-'.",
})
export type Space = typeof Space.Type

export const Key = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(1024)).annotate({
  identifier: "ClientKV.Key",
})
export type Key = typeof Key.Type

/** Any JSON but a top-level null, which marks an unset key. */
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

export interface Page extends Schema.Schema.Type<typeof Page> {}
export const Page = Schema.Struct({
  entries: Schema.Array(Entry),
  next: Key.pipe(optional).annotate({ description: "Pass as `after` to read the next page." }),
}).annotate({ identifier: "ClientKV.Page" })

/** Invalidation only: readers fetch the current value. Published after the write commits. */
const Updated = ephemeral({
  type: "experimental.kv.updated",
  schema: { space: Space, key: Key },
})
export const Event = { Updated, Definitions: inventory(Updated) }
