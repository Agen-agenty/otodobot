import { ClientKV } from "@opencode/schema/client-kv"
import { Schema } from "effect"
import { HttpApiEndpoint, HttpApiSchema, OpenApi } from "effect/http-api"

const params = { space: ClientKV.Space, key: ClientKV.Key }
const annotations = (identifier: string, summary: string, description: string) =>
  OpenApi.annotations({
    identifier: `experimental.kv.${identifier}`,
    summary,
    description: `${description} Experimental: may change without compatibility guarantees.`,
    // The routes share the `server.experimental` group with the persistent PTY prototype; tag them on their own.
    transform: (operation) => ({ ...operation, tags: ["experimental"] }),
  })

/** Lives in the `server.experimental` group (see `groups/experimental.ts`). */
export const KVEndpoints = [
  HttpApiEndpoint.get("kv.list", "/api/experimental/kv/:space", {
    params: { space: ClientKV.Space },
    query: { after: Schema.optional(ClientKV.Key) },
    success: Schema.Struct({ data: ClientKV.Page }),
  }).annotateMerge(
    annotations(
      "list",
      "List values",
      "Read a page of up to 1000 entries in a space, ordered by key. Subscribe to `experimental.kv.updated` on /api/event first, then read this snapshot, and read it again after reconnecting.",
    ),
  ),
  HttpApiEndpoint.get("kv.get", "/api/experimental/kv/:space/:key", {
    params,
    success: Schema.Struct({ data: Schema.NullOr(ClientKV.Value) }),
  }).annotateMerge(annotations("get", "Get value", "Read a client-defined value; `data` is null when unset.")),
  HttpApiEndpoint.put("kv.set", "/api/experimental/kv/:space/:key", {
    params,
    payload: Schema.Struct({ value: ClientKV.Value }),
    success: HttpApiSchema.NoContent,
  }).annotateMerge(
    annotations(
      "set",
      "Set value",
      "Store a client-defined JSON value, replacing any previous value, then publish `experimental.kv.updated`. The server does not interpret it.",
    ),
  ),
  HttpApiEndpoint.delete("kv.delete", "/api/experimental/kv/:space/:key", {
    params,
    success: HttpApiSchema.NoContent,
  }).annotateMerge(
    annotations(
      "delete",
      "Delete value",
      "Remove a stored value, then publish `experimental.kv.updated`. Removing an unset key succeeds.",
    ),
  ),
] as const
