import { assert, describe, it } from "@effect/vitest"
import { Channel, Effect } from "effect"
import * as Inspection from "effect/Inspection"

describe("Channel observation", () => {
  it.effect("retains a pulled batch after the source has been consumed", () =>
    Effect.gen(function*() {
      let read: (() => any) | undefined
      const inspection = Inspection.make({
        register(_owner, kind, reader) {
          if (kind === "Channel.flattenArray") read = reader
          return () => {}
        }
      })
      const pull = yield* Channel.succeed([1, 2, 3]).pipe(
        Channel.flattenArray,
        Channel.toPull,
        Effect.provideService(Inspection.CurrentInspection, inspection)
      )
      assert.isDefined(read)
      assert.strictEqual(yield* pull, 1)
      assert.deepStrictEqual(read!().array, [1, 2, 3])
      assert.strictEqual(read!().index, 1)
      assert.strictEqual(yield* pull, 2)
      assert.strictEqual(read!().index, 2)
      assert.strictEqual(yield* pull, 3)
      assert.isUndefined(read!().array)
      assert.strictEqual(read!().index, 0)
    }))
})
