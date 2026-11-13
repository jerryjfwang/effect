import { assert, describe, it } from "@effect/vitest"
import { type Cause, Channel, Effect, Exit, Latch, MutableList, Queue, Scheduler, Scope, Stream } from "effect"
import * as Inspection from "effect/Inspection"

describe("Channel observation", () => {
  it.effect("exposes the actual callback queue and setup fiber without consuming messages", () =>
    Effect.gen(function*() {
      const finish = yield* Latch.make()
      const scope = yield* Scope.make()
      let read: (() => any) | undefined
      const inspection = Inspection.make({
        register(_owner, kind, reader) {
          if (kind === "Channel.callbackArray") read = reader
          return () => {
            if (kind === "Channel.callbackArray") read = undefined
          }
        }
      })
      let actualQueue: Queue.Queue<number, Cause.Done> | undefined
      const callback = Effect.fnUntraced(function*(queue: Queue.Queue<number, Cause.Done>) {
        actualQueue = queue
        yield* Queue.offerAll(queue, [1, 2])
        yield* finish.await
      })
      const stream = Stream.callback(callback)
      const pull = yield* Stream.toPull(stream).pipe(
        Scope.provide(scope),
        Effect.provideService(Inspection.CurrentInspection, inspection)
      )
      assert.isDefined(read)
      assert.strictEqual(read!().queue, actualQueue)
      assert.strictEqual(read!().callback, callback)
      assert.strictEqual(read!().channel, stream.channel)
      assert.isDefined(read!().setupFiber)
      assert.isUndefined(read!().setupFiber.pollUnsafe())
      assert.deepStrictEqual(MutableList.toArray(read!().queue.messages), [1, 2])
      assert.deepStrictEqual(yield* pull, [1, 2])
      yield* Scope.close(scope, Exit.void)
      assert.isUndefined(read)
    }))

  it.effect("retains distinct switch inputs when the mapper reuses one channel", () =>
    Effect.gen(function*() {
      const entered = yield* Latch.make()
      const closing = yield* Latch.make()
      const release = yield* Latch.make()
      const source = yield* Queue.unbounded<number>()
      const scope = yield* Scope.make()
      const inputs: Array<number> = []
      let read: (() => any) | undefined
      const inspection = Inspection.make({
        register(_owner, kind, reader) {
          if (kind === "Channel.mergeAll") read = reader
          return () => {}
        }
      })
      const original = Channel.fromTransform((_upstream, childScope) =>
        Effect.gen(function*() {
          yield* Scope.addFinalizer(childScope, Effect.andThen(closing.open, release.await))
          return Effect.andThen(entered.open, Effect.never)
        })
      )
      const mapper = (input: number) => {
        inputs.push(input)
        return original
      }
      yield* Channel.fromQueue(source).pipe(
        Channel.switchMap(mapper),
        Channel.toPull,
        Scope.provide(scope),
        Effect.provideService(Inspection.CurrentInspection, inspection),
        Effect.asVoid
      )
      yield* Queue.offer(source, 1)
      yield* entered.await
      yield* Queue.offer(source, 2)
      yield* closing.await
      try {
        const state = read!()
        const old = state.workers.get(state.retiring.fiber).selected
        assert.deepStrictEqual(inputs, [1, 2])
        assert.strictEqual(old.capture.input, 1)
        assert.strictEqual(state.selected.capture.input, 2)
        assert.strictEqual(old.capture.channel, original)
        assert.strictEqual(state.selected.capture.channel, original)
        assert.strictEqual(state.selected.capture.mapper, mapper)
        assert.notStrictEqual(old.channel, state.selected.channel)
      } finally {
        release.openUnsafe()
      }
      yield* Scope.close(scope, Exit.void)
    }))

  it("retains output while its bounded offer waits for a consumer", () => {
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
    const consume = Latch.makeUnsafe()
    let read: (() => any) | undefined
    const inspection = Inspection.make({
      register(_owner, kind, reader) {
        if (kind === "Channel.mergeAll") read = reader
        return () => {}
      }
    })
    const fiber = Effect.runFork(
      Effect.scoped(Effect.gen(function*() {
        const pull = yield* Channel.succeed(Channel.fromIterable([1, 2, 3])).pipe(
          Channel.mergeAll({ concurrency: 1, switch: true, bufferSize: 1 }),
          Channel.toPull
        )
        yield* consume.await
        return [yield* pull, yield* pull, yield* pull]
      })).pipe(
        Effect.provideService(Inspection.CurrentInspection, inspection),
        Effect.provideService(Scheduler.Scheduler, scheduler)
      )
    )
    try {
      while (tasks.length > 0) tasks.shift()!()
      assert.isDefined(read)
      const state = read!()
      const worker = Array.from(state.workers.values())[0] as any
      assert.deepStrictEqual(worker.state, { _tag: "Offering", value: 2 })
      assert.deepStrictEqual(MutableList.toArray(state.queue.messages), [1])
      assert.strictEqual(state.driverSite, "Completing")
      assert.isDefined(state.terminal)
      consume.openUnsafe()
      while (tasks.length > 0) tasks.shift()!()
      assert.deepStrictEqual(fiber.pollUnsafe(), Exit.succeed([1, 2, 3]))
    } finally {
      fiber.interruptUnsafe()
      while (tasks.length > 0) tasks.shift()!()
    }
  })

  it.effect("retains the next child while acquisition waits before replacing the current worker", () =>
    Effect.gen(function*() {
      const started = yield* Latch.make()
      const acquiring = yield* Latch.make()
      const acquired = yield* Latch.make()
      const source = yield* Queue.unbounded<Channel.Channel<number>>()
      const scope = yield* Scope.make()
      let read: (() => any) | undefined
      const inspection = Inspection.make({
        register(_owner, kind, reader) {
          if (kind === "Channel.mergeAll") read = reader
          return () => {}
        }
      })
      const first = Channel.fromEffect(Effect.andThen(started.open, Effect.never))
      const next = Channel.fromTransform(() =>
        Effect.gen(function*() {
          yield* acquiring.open
          yield* acquired.await
          return Effect.succeed(7)
        })
      )
      const pull = yield* Channel.fromQueue(source).pipe(
        Channel.mergeAll({ concurrency: 1, switch: true }),
        Channel.toPull,
        Scope.provide(scope),
        Effect.provideService(Inspection.CurrentInspection, inspection)
      )
      yield* Queue.offer(source, first)
      yield* started.await
      yield* Queue.offer(source, next)
      yield* acquiring.await
      try {
        const state = read!()
        assert.strictEqual(state.driverSite, "AcquiringChild")
        assert.strictEqual(state.selected.channel, next)
        assert.isUndefined(state.selected.pull)
        assert.strictEqual(state.fibers.size, 1)
        assert.isUndefined(state.retiring)
      } finally {
        acquired.openUnsafe()
      }
      assert.strictEqual(yield* pull, 7)
      yield* Scope.close(scope, Exit.void)
    }))

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
