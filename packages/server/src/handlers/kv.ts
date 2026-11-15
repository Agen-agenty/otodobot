import { Bus } from "@opencode/core/bus"
import { KV } from "@opencode/core/kv"
import { ClientKV } from "@opencode/schema/client-kv"
import { Effect } from "effect"
import { HttpApiSchema } from "effect/http-api"

type Params = { readonly space: ClientKV.Space; readonly key: ClientKV.Key }

/** `kv.*` in the `server.experimental` group: opaque client values in the server's existing KV table. */
export const makeKV = Effect.gen(function* () {
  const kv = yield* KV.Service
  const bus = yield* Bus.Service
  // Spaces cannot contain ":", so keys in different spaces never collide.
  const prefix = (space: ClientKV.Space) => `client:${space}:`
  const updated = (params: Params) =>
    bus
      .publish(ClientKV.Event.Updated, { space: params.space, key: params.key })
      .pipe(Effect.as(HttpApiSchema.NoContent.make()))

  return {
    list: (space: ClientKV.Space, after?: ClientKV.Key) =>
      kv
        .scan({ prefix: prefix(space), after: after === undefined ? undefined : prefix(space) + after, limit: 1000 })
        .pipe(
          Effect.map((page) => ({
            data: {
              entries: page.entries.flatMap((entry) =>
                entry.value === null ? [] : [{ key: entry.key.slice(prefix(space).length), value: entry.value }],
              ),
              next: page.next?.slice(prefix(space).length),
            },
          })),
        ),
    get: (params: Params) =>
      kv.get(prefix(params.space) + params.key).pipe(Effect.map((data) => ({ data: data ?? null }))),
    set: (params: Params, value: ClientKV.Value) =>
      kv.set(prefix(params.space) + params.key, value).pipe(Effect.andThen(updated(params))),
    remove: (params: Params) => kv.remove(prefix(params.space) + params.key).pipe(Effect.andThen(updated(params))),
  }
})
