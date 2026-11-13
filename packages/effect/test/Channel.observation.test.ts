import { assert, describe, it } from "@effect/vitest"
import { Channel, Effect, Exit, MutableList, Scheduler } from "effect"
import * as Inspection from "effect/Inspection"

describe("Channel observation", () => {
  it("retains worker output at the native pre-offer yield", () => {
    const tasks: Array<() => void> = []
    const scheduler: Scheduler.Scheduler = {
      executionMode: "async",
      shouldYield: () => false,
      makeDispatcher: () => ({
        scheduleTask: (task) => tasks.push(task),
        flush() {
          while (tasks.length > 0) tasks.shift()!()
        }
      })
    }
    let read: (() => any) | undefined
    const inspection = Inspection.make({
      register(_owner, kind, reader) {
        if (kind === "Channel.mergeAll") read = reader
        return () => {}
      }
    })
    const fiber = Effect.runFork(
      Channel.succeed(Channel.succeed(42)).pipe(
        Channel.mergeAll({ concurrency: 1, switch: true }),
        Channel.runCollect,
        Effect.provideService(Inspection.CurrentInspection, inspection),
        Effect.provideService(Scheduler.Scheduler, scheduler)
      )
    )
    try {
      assert.isDefined(read)
      for (
        let i = 0;
        i < 20 && !Array.from(read!().workers.values()).some((worker: any) => worker.state._tag === "Yielding");
        i++
      ) {
        assert.isAbove(tasks.length, 0)
        tasks.shift()!()
      }
      const worker = Array.from(read!().workers.values())[0] as any
      assert.deepStrictEqual(worker.state, { _tag: "Yielding", value: 42 })
      assert.deepStrictEqual(MutableList.toArray(read!().queue.messages), [])
      while (tasks.length > 0) tasks.shift()!()
      assert.deepStrictEqual(fiber.pollUnsafe(), Exit.succeed([42]))
    } finally {
      fiber.interruptUnsafe()
      while (tasks.length > 0) tasks.shift()!()
    }
  })

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
