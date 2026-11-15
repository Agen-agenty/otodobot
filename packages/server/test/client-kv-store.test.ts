import { expect } from "bun:test"
import { Agent } from "@opencode/core/agent"
import { Bus } from "@opencode/core/bus"
import { KV } from "@opencode/core/kv"
import { makeGlobalNode } from "@opencode/util/effect/app-node"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Context, Deferred, Effect, Fiber, Layer, Stream } from "effect"
import { it } from "../../core/test/lib/effect"
import { ClientKVStore } from "../src/client-kv-store"
import { EventFeed } from "../src/event-feed"

// Real store, bus and feed over an in-memory database. `hold(value)` pauses the next write of `value` after it commits,
// while it still holds the store lock, so a test can line up competing writes and subscriptions behind it.
const setup = Effect.gen(function* () {
  const gates = new Map<string, { committed: Deferred.Deferred<void>; release: Deferred.Deferred<void> }>()
  const paused = makeGlobalNode({
    service: KV.Service,
    layer: Layer.effect(
      KV.Service,
      Effect.gen(function* () {
        const kv = yield* KV.Service
        return KV.Service.of({
          ...kv,
          set: (key, value) =>
            kv.set(key, value).pipe(
              Effect.andThen(
                Effect.suspend(() => {
                  const gate = gates.get(JSON.stringify(value))
                  if (!gate) return Effect.void
                  gates.delete(JSON.stringify(value))
                  return Deferred.succeed(gate.committed, undefined).pipe(Effect.andThen(Deferred.await(gate.release)))
                }),
              ),
            ),
        })
      }),
    ).pipe(Layer.provide(LayerNode.compile(KV.node))),
    deps: [],
  })
  const context = yield* Layer.build(
    LayerNode.compile(LayerNode.group([ClientKVStore.node, Bus.node]), { replacements: [KV.node.replace(paused)] }),
  )
  const bus = Context.get(context, Bus.Service)
  const hold = Effect.fnUntraced(function* (value: string) {
    const gate = { committed: yield* Deferred.make<void>(), release: yield* Deferred.make<void>() }
    gates.set(JSON.stringify(value), gate)
    return gate
  })
  return { store: Context.get(context, ClientKVStore.Service), bus, feed: yield* EventFeed.make(bus.listen), hold }
})

const take = (live: Stream.Stream<string, EventFeed.Error>, count: number) =>
  live.pipe(
    Stream.take(count),
    Stream.runCollect,
    Effect.map((frames) =>
      Array.from(frames, (frame) => {
        const event = JSON.parse(frame.slice("data: ".length))
        return { type: event.type, data: event.data }
      }),
    ),
  )

const now = { startImmediately: true } as const

it.live("delivers racing writes in commit order", () =>
  Effect.gen(function* () {
    const { store, feed, hold } = yield* setup
    const [live] = yield* store.open(["app"], feed.subscribe(undefined, ["app"]))

    const first = yield* hold("a")
    const a = yield* store.set("app", "k", "a").pipe(Effect.forkChild(now))
    yield* Deferred.await(first.committed)
    // Blocked behind "a": without the lock it would commit and publish "b" before "a" is published.
    const b = yield* store.set("app", "k", "b").pipe(Effect.forkChild(now))
    yield* Deferred.succeed(first.release, undefined)
    yield* Fiber.join(a)
    yield* Fiber.join(b)

    expect(yield* take(live, 2)).toEqual([
      { type: "experimental.kv.updated", data: { space: "app", key: "k", value: "a" } },
      { type: "experimental.kv.updated", data: { space: "app", key: "k", value: "b" } },
    ])
  }),
)

it.live("a subscription opened during a write gets it in the snapshot or as an update, exactly once", () =>
  Effect.gen(function* () {
    const { store, feed, hold } = yield* setup
    yield* store.set("app", "k", "old")

    const pending = yield* hold("new")
    const write = yield* store.set("app", "k", "new").pipe(Effect.forkChild(now))
    yield* Deferred.await(pending.committed)
    // "new" is committed but not yet published. Both of these queue behind it, in this order.
    const opening = yield* store.open(["app"], feed.subscribe(undefined, ["app"])).pipe(Effect.forkChild(now))
    const next = yield* store.set("app", "k", "newer").pipe(Effect.forkChild(now))
    yield* Deferred.succeed(pending.release, undefined)
    yield* Fiber.join(write)
    const [live, snapshots] = yield* Fiber.join(opening)
    yield* Fiber.join(next)
    yield* store.remove("app", "k")

    expect(snapshots).toEqual([{ space: "app", entries: [{ key: "k", value: "new" }] }])
    // Without the lock the open would register before "new" is published and receive it again after the snapshot.
    expect(yield* take(live, 2)).toEqual([
      { type: "experimental.kv.updated", data: { space: "app", key: "k", value: "newer" } },
      { type: "experimental.kv.deleted", data: { space: "app", key: "k" } },
    ])
  }),
)

it.live("sends kv events only to subscribers of their space and leaves other events alone", () =>
  Effect.gen(function* () {
    const { store, feed, bus } = yield* setup
    const app = yield* feed.subscribe([Agent.Event.Updated.type], ["app"])
    const other = yield* feed.subscribe(undefined, ["other"])
    const plain = yield* feed.subscribe()

    yield* store.set("app", "k", { n: 1 })
    yield* bus.publish(Agent.Event.Updated, {})

    expect(yield* take(app, 2)).toEqual([
      { type: "experimental.kv.updated", data: { space: "app", key: "k", value: { n: 1 } } },
      { type: "agent.updated", data: {} },
    ])
    expect(yield* take(other, 1)).toEqual([{ type: "agent.updated", data: {} }])
    expect(yield* take(plain, 1)).toEqual([{ type: "agent.updated", data: {} }])
  }),
)
