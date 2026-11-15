import path from "node:path"
import { expect } from "bun:test"
import { OpenCode } from "@opencode/client"
import { Effect } from "effect"
import { tmpdirScoped } from "../../core/test/fixture/tmpdir"
import { it } from "../../core/test/lib/effect"
import { ServerFetch } from "../src/fetch"

const authorization = `Basic ${btoa("opencode:secret")}`

const client = (handler: (request: Request) => Promise<Response>, headers?: Record<string, string>) =>
  OpenCode.make({
    baseUrl: "http://opencode.local",
    headers,
    fetch: Object.assign((input: string | URL | Request, init?: RequestInit) => handler(new Request(input, init)), {
      preconnect: fetch.preconnect,
    }),
  })

// Reads the wire stream directly: the SDK's shared event subscription has no `experimental.kv` option.
const subscribe = async (
  handler: (request: Request) => Promise<Response>,
  query: string,
  headers: Record<string, string> = { authorization },
) => {
  const response = await handler(new Request(`http://opencode.local/api/event?${query}`, { headers }))
  const reader = response.body?.pipeThrough(new TextDecoderStream()).getReader()
  const frames: string[] = []
  let buffer = ""
  return {
    status: response.status,
    next: async (): Promise<{ readonly type: string; readonly data: unknown }> => {
      while (frames.length === 0) {
        const chunk = await reader?.read()
        if (!chunk || chunk.done) throw new Error("event stream closed")
        const parts = (buffer + chunk.value).split("\n\n")
        buffer = parts.pop() ?? ""
        frames.push(...parts.filter((part) => part.startsWith("data: ")))
      }
      const event = JSON.parse(frames.shift()?.slice("data: ".length) ?? "")
      return { type: event.type, data: event.data }
    },
    close: () => reader?.cancel(),
  }
}

it.live("streams snapshots and later writes of requested spaces to another client and survives a restart", () =>
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
        const writer = client(handler, { authorization })
        yield* Effect.promise(async () => {
          await writer.experimental.kv.set({ space: "app", key: "draft/1", value: { text: "hi", n: [1, null] } })
          await writer.experimental.kv.set({ space: "other", key: "draft/1", value: "elsewhere" })

          const reader = await subscribe(handler, "types=session.created&experimental.kv=app&experimental.kv=empty")
          const plain = await subscribe(handler, "")
          expect(await reader.next()).toEqual({ type: "server.connected", data: {} })
          expect(await reader.next()).toEqual({
            type: "experimental.kv.snapshot",
            data: { space: "app", entries: [{ key: "draft/1", value: { text: "hi", n: [1, null] } }] },
          })
          expect(await reader.next()).toEqual({
            type: "experimental.kv.snapshot",
            data: { space: "empty", entries: [] },
          })
          expect(await plain.next()).toEqual({ type: "server.connected", data: {} })

          await writer.experimental.kv.set({ space: "other", key: "draft/1", value: "unseen" })
          await writer.experimental.kv.set({ space: "app", key: "draft/2", value: 2 })
          await writer.experimental.kv.delete({ space: "app", key: "draft/1" })
          await writer.session.create({ location: { directory: dir } })
          expect(await reader.next()).toEqual({
            type: "experimental.kv.updated",
            data: { space: "app", key: "draft/2", value: 2 },
          })
          expect(await reader.next()).toEqual({
            type: "experimental.kv.deleted",
            data: { space: "app", key: "draft/1" },
          })
          // `types` still selects the other events, and a plain subscriber sees no kv events at all.
          expect((await reader.next()).type).toBe("session.created")
          expect((await plain.next()).type).not.toStartWith("experimental.kv.")
          await reader.close()
          await plain.close()

          const reconnected = await subscribe(handler, "experimental.kv=app")
          await reconnected.next()
          expect(await reconnected.next()).toEqual({
            type: "experimental.kv.snapshot",
            data: { space: "app", entries: [{ key: "draft/2", value: 2 }] },
          })
          await reconnected.close()

          expect((await subscribe(handler, "experimental.kv=app", {})).status).toBe(401)
          expect((await subscribe(handler, "experimental.kv=a:b")).status).toBe(400)
          await expect(client(handler).experimental.kv.set({ space: "app", key: "x", value: 1 })).rejects.toMatchObject(
            {
              _tag: "UnauthorizedError",
            },
          )
          const empty = await handler(
            new Request("http://opencode.local/api/experimental/kv/app/c", {
              method: "PUT",
              headers: { authorization, "content-type": "application/json" },
              body: JSON.stringify({ value: null }),
            }),
          )
          expect(empty.status).toBe(400)
        })
      }),
    )

    const handler = yield* boot
    yield* Effect.promise(async () => {
      const reader = await subscribe(handler, "experimental.kv=app&experimental.kv=other")
      await reader.next()
      expect(await reader.next()).toEqual({
        type: "experimental.kv.snapshot",
        data: { space: "app", entries: [{ key: "draft/2", value: 2 }] },
      })
      expect(await reader.next()).toEqual({
        type: "experimental.kv.snapshot",
        data: { space: "other", entries: [{ key: "draft/1", value: "unseen" }] },
      })
      await reader.close()
    })
  }).pipe(Effect.scoped),
)
