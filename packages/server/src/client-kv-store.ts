export * as ClientKVStore from "./client-kv-store"

import { Bus } from "@opencode/core/bus"
import { KV } from "@opencode/core/kv"
import { Event, type Entry, type Key, type Space, type Value } from "@opencode/schema/client-kv"
import { makeGlobalNode } from "@opencode/util/effect/app-node"
import { Context, Effect, Layer, Semaphore } from "effect"

export interface Snapshot {
  readonly space: Space
  readonly entries: ReadonlyArray<Entry>
}

export interface Interface {
  readonly set: (space: Space, key: Key, value: Value) => Effect.Effect<void>
  readonly remove: (space: Space, key: Key) => Effect.Effect<void>
  /**
   * Runs `register`, which must start buffering this store's events, then reads each space, with no write in
   * between: every write is then either in a snapshot or delivered to the registration, never both or neither.
   */
  readonly open: <A, E, R>(
    spaces: ReadonlyArray<Space>,
    register: Effect.Effect<A, E, R>,
  ) => Effect.Effect<readonly [A, ReadonlyArray<Snapshot>], E, R>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/server/ClientKVStore") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const kv = yield* KV.Service
    const bus = yield* Bus.Service
    // Bus listeners run inline, so publishing under the lock queues the event for every registered subscriber before
    // the next write or snapshot starts, and subscribers see writes in commit order.
    const lock = yield* Semaphore.make(1)
    // Spaces cannot contain ":", so keys in different spaces never collide.
    const prefix = (space: Space) => `client:${space}:`
    const read = Effect.fnUntraced(function* (space: Space) {
      const entries: Entry[] = []
      let after: string | undefined
      while (true) {
        const page = yield* kv.scan({ prefix: prefix(space), after, limit: 1000 })
        entries.push(
          ...page.entries.flatMap((entry) =>
            entry.value === null ? [] : [{ key: entry.key.slice(prefix(space).length), value: entry.value }],
          ),
        )
        if (page.next === undefined) return { space, entries }
        after = page.next
      }
    })

    return Service.of({
      // Uninterruptible so a dropped request cannot commit a write that subscribers never hear about.
      set: Effect.fn("ClientKVStore.set")(function* (space, key, value) {
        yield* lock.withPermit(
          Effect.uninterruptible(
            kv.set(prefix(space) + key, value).pipe(Effect.andThen(bus.publish(Event.Updated, { space, key, value }))),
          ),
        )
      }),
      remove: Effect.fn("ClientKVStore.remove")(function* (space, key) {
        yield* lock.withPermit(
          Effect.uninterruptible(
            kv.remove(prefix(space) + key).pipe(Effect.andThen(bus.publish(Event.Deleted, { space, key }))),
          ),
        )
      }),
      open: (spaces, register) =>
        lock.withPermit(
          Effect.gen(function* () {
            const registered = yield* register
            return [registered, yield* Effect.forEach(spaces, read)] as const
          }),
        ),
    })
  }),
)

export const node = makeGlobalNode({ service: Service, layer, deps: [KV.node, Bus.node] })
