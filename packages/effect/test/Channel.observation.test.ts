import { assert, describe, it } from "@effect/vitest"
import { Channel, Effect, Exit, Latch, MutableList, Queue, Scheduler, Scope, Stream } from "effect"
import * as Inspection from "effect/Inspection"

describe("Channel observation", () => {
  it.effect("retains the Stream mapper identity through Channel lowering", () =>
    Effect.gen(function*() {
      let observed: any
      const inspection = Inspection.make({
        register(_owner, kind, read) {
          if (kind === "Channel.mergeAll") observed = read()
          return () => {}
        }
      })
      const source = Stream.make(1, 2)
      const mapper = (value: number) => Stream.succeed(value + 1)
      const stream = Stream.switchMap(source, mapper)
      yield* Stream.runCollect(stream).pipe(Effect.provideService(Inspection.CurrentInspection, inspection))
      assert.strictEqual(observed.provenance.stream.source, source)
      assert.strictEqual(observed.provenance.stream.mapper, mapper)
      assert.strictEqual(observed.channel, stream.channel)
    }))

  it.effect("retains a retiring worker and its constructed replacement through cleanup", () =>
    Effect.gen(function*() {
      const entered = yield* Latch.make()
      const closing = yield* Latch.make()
      const release = yield* Latch.make()
      const source = yield* Queue.unbounded<Channel.Channel<number>>()
      const scope = yield* Scope.make()
      let read: (() => any) | undefined
      const inspection = Inspection.make({
        register(_owner, kind, reader) {
          if (kind === "Channel.mergeAll") read = reader
          return () => {
            if (kind === "Channel.mergeAll") read = undefined
          }
        }
      })
      const first = Channel.fromTransform((_upstream, childScope) =>
        Effect.gen(function*() {
          yield* Scope.addFinalizer(childScope, Effect.andThen(closing.open, release.await))
          return Effect.andThen(entered.open, Effect.never)
        })
      )
      const replacement = Channel.succeed(7)
      const pull = yield* Channel.fromQueue(source).pipe(
        Channel.mergeAll({ concurrency: 1, switch: true }),
        Channel.toPull,
        Scope.provide(scope),
        Effect.provideService(Inspection.CurrentInspection, inspection)
      )
      yield* Queue.offer(source, first)
      yield* entered.await
      yield* Queue.offer(source, replacement)
      yield* closing.await
      assert.isDefined(read)
      const state = read!()
      assert.strictEqual(state.selected.channel, replacement)
      assert.strictEqual(state.fibers.size, 0)
      assert.strictEqual(state.driverSite, "Interrupting")
      assert.strictEqual(state.retiring.scope.state._tag, "Closed")
      assert.strictEqual(state.workers.get(state.retiring.fiber).state._tag, "Closing")
      yield* release.open
      assert.strictEqual(yield* pull, 7)
      yield* Scope.close(scope, Exit.void)
      assert.isUndefined(read)
    }))

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
