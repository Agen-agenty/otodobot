export * as VideoGeneration from "./video-generation.js"

import { Media } from "@opencode/ai/effect"
import {
  VideoID,
  type VideoCompleted,
  type VideoError,
  type VideoJob,
  type VideoRequest,
  type VideoRunning,
} from "@opencode/schema/generate"
import type { Model } from "@opencode/schema/model"
import { makeGlobalNode, Node } from "@opencode/util/effect/app-node"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Global } from "@opencode/util/global"
import { Context, Deferred, Duration, Effect, Exit, FiberMap, Layer, Option, Schedule, Schema } from "effect"
import { BlobStore } from "./blob-store.js"
import { Generate } from "./generate.js"
import { Location } from "./location.js"
import { LocationServiceMap } from "./location-service-map.js"
import { AbsolutePath } from "./schema.js"

export class NotFoundError extends Schema.TaggedError<NotFoundError>()("VideoGeneration.NotFoundError", {
  id: Schema.String,
  message: Schema.String,
}) {}

const INTERVAL = Duration.seconds(5)
const DEADLINE = Duration.hours(2)

/** A submitted job, with the provider's job ID a caller keeps to `resume` it after a restart. */
export interface Started extends VideoRunning {
  readonly providerJobID: string
}

export interface StartOptions {
  /** The Location whose models and credentials run the job. Defaults to the server's base config Location. */
  readonly location?: Location.Ref
}

export interface ResumeInput extends StartOptions {
  readonly id: VideoID
  readonly model: Model.Ref
  readonly providerJobID: string
}

export type Settled = VideoCompleted | VideoError

export interface Interface {
  /** Returns once the provider accepted the job; the server then polls it and stores the video as a blob. */
  readonly start: (input: Generate.VideoInput, options?: StartOptions) => Effect.Effect<Started, Generate.Error>
  readonly get: (id: VideoID) => Effect.Effect<VideoJob, NotFoundError>
  /** Polls a job again from the provider job ID its caller kept. Does nothing for a job already tracked or finished. */
  readonly resume: (input: ResumeInput) => Effect.Effect<void>
  /** Waits until the job completes or fails. */
  readonly wait: (id: VideoID) => Effect.Effect<Settled, NotFoundError>
  /** Stops polling a running job, which then fails as cancelled, and asks the provider to cancel it if it can. */
  readonly cancel: (id: VideoID) => Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/VideoGeneration") {}

type Tracked = {
  job: VideoRunning | VideoError
  readonly done: Deferred.Deferred<Settled>
  readonly model: Model.Ref
  readonly providerJobID: string
  readonly location?: Location.Ref
}

/**
 * Running and failed jobs live in memory only, and a finished job is read from its blobs, so it outlives a restart
 * until they expire. A restart forgets running jobs; a caller that kept the provider job ID from `start` polls one
 * again with `resume`.
 */
const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const global = yield* Global.Service
    const locations = yield* LocationServiceMap.Service
    const blobs = yield* BlobStore.Service
    // The same base-config Location the generate routes use.
    const base = Location.Ref.make({ directory: AbsolutePath.make(global.config) })
    const fibers = yield* FiberMap.make<VideoID>()
    const jobs = new Map<VideoID, Tracked>()

    const generate = <A>(
      location: Location.Ref | undefined,
      run: (generate: Generate.Interface) => Effect.Effect<A, Generate.Error>,
    ): Effect.Effect<A, Generate.Error> =>
      Generate.Service.pipe(
        Effect.flatMap(run),
        Effect.provide(locations.get(location ?? base)),
        Effect.catchTag("FileSystem.DirectoryNotFoundError", (error) =>
          Effect.fail(new Generate.UnavailableError({ message: `Location not found: ${error.directory}` })),
        ),
      )

    // Each video becomes blob `<id>_<n>`, so a restarted server finds them from the job's ID alone.
    const blobID = (id: VideoID, index: number) => BlobStore.ID.make(`${id}_${index}`)

    const store = Effect.fnUntraced(function* (id: VideoID, tracked: Tracked) {
      const videos = yield* generate(tracked.location, (service) =>
        service.video.result(tracked.model, tracked.providerJobID),
      )
      const stored = yield* Effect.forEach(videos, (video, index) =>
        blobs.write(blobID(id, index), video.stream).pipe(
          Effect.flatMap((blob) =>
            blob.bytes > 0
              ? Effect.succeed(blob)
              : Effect.fail(
                  new Generate.ProviderRequestError({
                    providerID: tracked.model.providerID,
                    reason: "provider",
                    message: "The provider returned an empty video",
                  }),
                ),
          ),
        ),
      ).pipe(
        // All or nothing: a job never reports only some of its videos.
        Effect.onError(() => Effect.forEach(videos, (_, index) => blobs.remove(blobID(id, index)), { discard: true })),
      )
      return { id, status: "completed", blobs: stored } satisfies VideoJob
    })

    // A finished job outlives a restart through its blobs: `<id>_0`, `<id>_1`, … up to the first missing one.
    const stored = (id: VideoID): Effect.Effect<readonly BlobStore.Info[]> =>
      Effect.gen(function* () {
        const found: BlobStore.Info[] = []
        while (true) {
          const blob = yield* blobs.read(blobID(id, found.length)).pipe(Effect.option)
          if (Option.isNone(blob)) return found
          found.push({
            id: blob.value.id,
            mime: blob.value.mime,
            bytes: blob.value.bytes,
            expiresAt: blob.value.expiresAt,
          })
        }
      })

    // A status read that fails is retried at the next interval: the provider may be briefly unavailable.
    const poll = (id: VideoID, tracked: Tracked) =>
      generate(tracked.location, (service) => service.video.status(tracked.model, tracked.providerJobID)).pipe(
        Effect.map((status) => (status.status === "running" ? undefined : status)),
        Effect.catch((error) =>
          Effect.logWarning("video status read failed", { id, error: error.message }).pipe(Effect.as(undefined)),
        ),
        Effect.repeat({ schedule: Schedule.spaced(INTERVAL), until: (status) => status !== undefined }),
        Effect.timeoutOption(DEADLINE),
      )

    const settle = Effect.fnUntraced(function* (id: VideoID, tracked: Tracked) {
      const settled = yield* poll(id, tracked)
      if (Option.isNone(settled) || settled.value === undefined)
        return {
          id,
          status: "error",
          reason: "unknown",
          error: `The provider had no result ${Duration.format(DEADLINE)} after submission`,
        } satisfies VideoJob
      if (settled.value.status === "error") {
        const reason = settled.value.reason ?? "failed"
        return { id, status: "error", reason, error: `The provider reported the job as ${reason}` } satisfies VideoJob
      }
      return yield* store(id, tracked).pipe(
        Effect.retry({ times: 2, schedule: Schedule.exponential("1 second") }),
        Effect.catch((error) =>
          Effect.succeed({ id, status: "error", reason: "failed", error: error.message } satisfies VideoJob),
        ),
      )
    })

    // A failure is kept as long as a blob would be, so the map holds at most one retention's worth of jobs.
    const forget = (id: VideoID) =>
      Effect.sleep(BlobStore.RETENTION).pipe(Effect.andThen(Effect.sync(() => jobs.delete(id))), Effect.asVoid)

    // Runs in the job's fiber, once it is in `jobs`.
    const track = Effect.fnUntraced(function* (id: VideoID, tracked: Tracked) {
      const settled = yield* settle(id, tracked)
      // A finished job's blobs are its record from here on.
      if (settled.status === "completed") jobs.delete(id)
      if (settled.status === "error") tracked.job = settled
      yield* Deferred.succeed(tracked.done, settled)
      if (settled.status === "error") yield* forget(id)
    })

    const start = Effect.fn("VideoGeneration.start")(function* (input: Generate.VideoInput, options?: StartOptions) {
      const id = VideoID.create()
      const submitted = yield* Deferred.make<Started, Generate.Error>()
      // Runs in the server's fibers, so a client that disconnects can't interrupt a paid submission midway.
      yield* FiberMap.run(
        fibers,
        id,
        Effect.gen(function* () {
          const started = yield* generate(options?.location, (service) => service.video.start(input))
          const tracked: Tracked = {
            job: { id, status: "running" },
            done: yield* Deferred.make<Settled>(),
            model: input.model,
            providerJobID: started.id,
            location: options?.location,
          }
          jobs.set(id, tracked)
          yield* Deferred.succeed(submitted, { id, status: "running", providerJobID: started.id } as const)
          yield* track(id, tracked)
        }).pipe(
          // Once the submission succeeded, this does nothing.
          Effect.onExit((exit) => (Exit.isFailure(exit) ? Deferred.failCause(submitted, exit.cause) : Effect.void)),
          Effect.ignore,
        ),
      )
      return yield* Deferred.await(submitted)
    })

    const resume = Effect.fn("VideoGeneration.resume")(function* (input: ResumeInput) {
      if (jobs.has(input.id) || (yield* stored(input.id)).length > 0) return
      const tracked: Tracked = {
        job: { id: input.id, status: "running" },
        done: yield* Deferred.make<Settled>(),
        model: input.model,
        providerJobID: input.providerJobID,
        location: input.location,
      }
      jobs.set(input.id, tracked)
      yield* FiberMap.run(fibers, input.id, track(input.id, tracked))
    })

    const get = Effect.fn("VideoGeneration.get")(function* (id: VideoID) {
      const tracked = jobs.get(id)
      if (tracked) return tracked.job
      const found = yield* stored(id)
      if (found.length === 0) return yield* new NotFoundError({ id, message: `Video not found: ${id}` })
      return { id, status: "completed", blobs: found } satisfies VideoJob
    })

    const wait = Effect.fn("VideoGeneration.wait")(function* (id: VideoID) {
      const tracked = jobs.get(id)
      if (tracked) return yield* Deferred.await(tracked.done)
      const found = yield* stored(id)
      if (found.length === 0) return yield* new NotFoundError({ id, message: `Video not found: ${id}` })
      return { id, status: "completed", blobs: found } satisfies Settled
    })

    const cancel = Effect.fn("VideoGeneration.cancel")(function* (id: VideoID) {
      const tracked = jobs.get(id)
      if (tracked?.job.status !== "running") return
      const cancelled = {
        id,
        status: "error",
        reason: "cancelled",
        error: "The video generation was cancelled",
      } satisfies VideoError
      tracked.job = cancelled
      // Replacing the polling fiber interrupts it, along with any download in progress.
      yield* FiberMap.run(fibers, id, forget(id))
      yield* Deferred.succeed(tracked.done, cancelled)
      // Best effort: some providers have no cancel call, and the paid job then runs to completion on their side.
      yield* generate(tracked.location, (service) => service.video.cancel(tracked.model, tracked.providerJobID)).pipe(
        Effect.ignore,
      )
    })

    return Service.of({ start, get, resume, wait, cancel })
  }),
)

/** The media of an HTTP request, which arrives as inline files. */
export function fromRequest(input: VideoRequest): Generate.VideoInput {
  return {
    ...input,
    start: input.start && Media.image(Media.base64(input.start.data, input.start.mime)),
    end: input.end && Media.image(Media.base64(input.end.data, input.end.mime)),
    references: input.references?.map((file) => Media.base64(file.data, file.mime)),
  }
}

/** Videos are stored as blobs, which need a local disk that Durable Objects don't have. */
export const unsupportedLayer = Layer.succeed(
  Service,
  Service.of({
    start: () =>
      Effect.fail(new Generate.UnavailableError({ message: "Video generation is not supported in this runtime" })),
    get: (id) => Effect.fail(new NotFoundError({ id, message: `Video not found: ${id}` })),
    resume: () => Effect.void,
    wait: (id) => Effect.fail(new NotFoundError({ id, message: `Video not found: ${id}` })),
    cancel: () => Effect.void,
  }),
)

// Typed by hand: the Location services this reads include this service, so its type can't be inferred.
export const node: LayerNode.Provider<Service, never, typeof Node.tags.values.global> = makeGlobalNode({
  service: Service,
  layer,
  deps: [BlobStore.node, Global.node, LocationServiceMap.node],
})
