import { ClientKV } from "@opencode/schema/client-kv"
import { Event } from "@opencode/schema/event"
import { Clock, Effect, Stream } from "effect"
import { HttpServerResponse } from "effect/http"
import { HttpApiBuilder } from "effect/http-api"
import { Api } from "../api"
import { ClientKVStore } from "../client-kv-store"
import { EventFeed } from "../event-feed"

export const EventHandler = HttpApiBuilder.group(Api, "server.event", (handlers) =>
  Effect.gen(function* () {
    const feed = yield* EventFeed.Service
    const store = yield* ClientKVStore.Service
    return handlers.handleRaw("event.subscribe", (ctx) =>
      Effect.gen(function* () {
        const connected = {
          id: Event.ID.create(),
          type: "server.connected",
          data: {},
        } as const
        const spaces = Array.from(new Set(ctx.query["experimental.kv"]))
        const output = Stream.unwrap(
          Effect.gen(function* () {
            if (spaces.length === 0) {
              const live = yield* feed.subscribe(ctx.query.types)
              return Stream.make(EventFeed.frame(connected)).pipe(Stream.concat(live))
            }
            const [live, snapshots] = yield* store.open(spaces, feed.subscribe(ctx.query.types, spaces))
            const created = yield* Clock.currentTimeMillis
            return Stream.make(
              EventFeed.frame(connected),
              ...snapshots.map((data) =>
                EventFeed.frame({ id: Event.ID.create(), created, type: ClientKV.Event.Snapshot.type, data }),
              ),
            ).pipe(Stream.concat(live))
          }),
        )
        const heartbeat = Stream.tick("15 seconds").pipe(Stream.map(() => ": heartbeat\n\n"))
        return HttpServerResponse.stream(
          output.pipe(Stream.merge(heartbeat, { haltStrategy: "left" }), Stream.encodeText),
          {
            contentType: "text/event-stream",
            headers: {
              "Cache-Control": "no-cache, no-transform",
              "X-Accel-Buffering": "no",
              "X-Content-Type-Options": "nosniff",
            },
          },
        )
      }),
    )
  }),
)
