import { VideoID } from "@opencode/schema/generate"
import { Schema } from "effect"
import { SessionSchema } from "../session/schema.js"

const Shell = Schema.Struct({
  kind: Schema.Literal("shell"),
  sessionID: SessionSchema.ID,
  shellID: Schema.String,
  command: Schema.String,
})
const Image = Schema.Struct({
  kind: Schema.Literal("image"),
  sessionID: SessionSchema.ID,
  path: Schema.String,
  model: Schema.String,
})
const Video = Schema.Struct({
  kind: Schema.Literal("video"),
  sessionID: SessionSchema.ID,
  path: Schema.String,
  model: Schema.String,
})
const subagent = {
  kind: Schema.Literal("subagent"),
  childSessionID: SessionSchema.ID,
  agent: Schema.String,
  description: Schema.String,
}

const OutstandingItem = Schema.Union([Shell, Image, Video, Schema.Struct({ ...subagent, sessionID: SessionSchema.ID })])
export type OutstandingItem = typeof OutstandingItem.Type

export const RecoverySchema = Schema.Union([
  Shell,
  Image,
  Schema.Struct({
    ...subagent,
    parentSessionID: SessionSchema.ID,
    outstanding: Schema.optionalKey(Schema.Array(OutstandingItem)),
  }),
  Schema.Struct({
    ...Video.fields,
    /** The server's video ID, whose blobs hold the finished videos. */
    videoID: VideoID,
    /** The provider's job ID, so a restarted server can keep polling the same job. */
    providerJobID: Schema.String,
  }),
])

export type Recovery = typeof RecoverySchema.Type
