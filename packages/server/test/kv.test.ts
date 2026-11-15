import path from "node:path"
import { expect } from "bun:test"
import { OpenCode } from "@opencode/client"
import { Effect } from "effect"
import { tmpdirScoped } from "../../core/test/fixture/tmpdir"
import { it } from "../../core/test/lib/effect"
import { ServerFetch } from "../src/fetch"

const client = (handler: (request: Request) => Promise<Response>, password?: string) =>
  OpenCode.make({
    baseUrl: "http://opencode.local",
    headers: password === undefined ? undefined : { authorization: `Basic ${btoa(`opencode:${password}`)}` },
    fetch: Object.assign((input: string | URL | Request, init?: RequestInit) => handler(new Request(input, init)), {
      preconnect: fetch.preconnect,
    }),
  })

it.live(
  "syncs client values between two authenticated clients over the event feed and keeps them across a restart",
  () =>
    Effect.gen(function* () {
      const dir = (yield* tmpdirScoped("opencode-kv-api-")).path
      const boot = ServerFetch.make({
        app: { version: "test" },
        password: "secret",
        database: { path: path.join(dir, "opencode.db") },
        config: { directory: dir, project: false, claude: false },
        models: { fetch: false },
        fs: { filewatcher: false },
      })

      yield* Effect.scoped(
        Effect.gen(function* () {
          const handler = yield* boot
          const writer = client(handler, "secret")
          const reader = client(handler, "secret")
          yield* Effect.promise(async () => {
            await writer.experimental.kv.set({ space: "app", key: "draft/1", value: { text: "hi", n: [1, null] } })
            await writer.experimental.kv.set({ space: "other", key: "draft/1", value: "elsewhere" })

            // Subscribe first, then read the snapshot, so no write between the two is missed.
            const events = reader.event.subscribe({ types: ["experimental.kv.updated"] })[Symbol.asyncIterator]()
            expect((await events.next()).value).toMatchObject({ type: "server.connected" })
            expect(await reader.experimental.kv.list({ space: "app" })).toEqual({
              entries: [{ key: "draft/1", value: { text: "hi", n: [1, null] } }],
            })

            await writer.experimental.kv.set({ space: "app", key: "draft/2", value: 2 })
            expect((await events.next()).value).toMatchObject({
              type: "experimental.kv.updated",
              data: { space: "app", key: "draft/2" },
            })
            expect(await reader.experimental.kv.get({ space: "app", key: "draft/2" })).toBe(2)

            await writer.experimental.kv.delete({ space: "app", key: "draft/2" })
            expect((await events.next()).value).toMatchObject({ data: { space: "app", key: "draft/2" } })
            expect(await reader.experimental.kv.get({ space: "app", key: "draft/2" })).toBeNull()
            await events.return?.()

            await expect(client(handler).experimental.kv.get({ space: "app", key: "draft/1" })).rejects.toMatchObject({
              _tag: "UnauthorizedError",
            })
            await expect(writer.experimental.kv.get({ space: "a:b", key: "c" })).rejects.toMatchObject({
              _tag: "InvalidRequestError",
            })
            const empty = await handler(
              new Request("http://opencode.local/api/experimental/kv/app/c", {
                method: "PUT",
                headers: { authorization: `Basic ${btoa("opencode:secret")}`, "content-type": "application/json" },
                body: JSON.stringify({ value: null }),
              }),
            )
            expect(empty.status).toBe(400)
          })
        }),
      )

      const handler = yield* boot
      yield* Effect.promise(async () => {
        expect(await client(handler, "secret").experimental.kv.list({ space: "app" })).toEqual({
          entries: [{ key: "draft/1", value: { text: "hi", n: [1, null] } }],
        })
        expect(await client(handler, "secret").experimental.kv.get({ space: "other", key: "draft/1" })).toBe(
          "elsewhere",
        )
      })
    }).pipe(Effect.scoped),
)
