import { assert, describe, it } from "@effect/vitest"
import { Context, Effect, Exit, Fiber, Inspection, Latch, Queue, Scheduler } from "effect"

describe("Scheduler observation", () => {
  it("observes Queue work scheduled after its creating Fiber has completed", () => {
    const tasks: Array<() => void> = []
    const readers = new Map<object, () => any>()
    const inspection = Inspection.make({
      register(owner, kind, read) {
        if (kind === "Scheduler.MixedDispatcher") readers.set(owner, read)
        return () => readers.delete(owner)
      }
    })
    const scheduler = new Scheduler.MixedScheduler("async", (task) => {
      tasks.push(task)
      return () => {}
    })
    const context = Context.make(Inspection.CurrentInspection, inspection).pipe(
      Context.add(Scheduler.Scheduler, scheduler)
    )
    const created = Effect.runForkWith(context)(Queue.unbounded<number>()).pollUnsafe()!
    assert.isTrue(Exit.isSuccess(created))
    if (Exit.isFailure(created)) return
    assert.strictEqual(readers.size, 0)
    const take = Effect.runForkWith(context)(Queue.take(created.value))
    Queue.offerUnsafe(created.value, 42)
    try {
      assert.strictEqual(readers.size, 1)
      const state = Array.from(readers.values())[0]()
      assert.strictEqual(state.fiber, undefined)
      assert.strictEqual(state.scheduler, scheduler)
      assert.isAbove(state.buckets.length, 0)
    } finally {
      while (tasks.length > 0) tasks.shift()!()
      take.interruptUnsafe()
    }
    assert.deepStrictEqual(take.pollUnsafe(), Exit.succeed(42))
    assert.strictEqual(readers.size, 0)
  })

  it("leaves an unknown dispatcher visible without invoking its prototype trap", () => {
    const tasks: Array<() => void> = []
    let observed: unknown
    let registeredNative = false
    let probes = 0
    const inspection = Inspection.make({
      register(_owner, kind, read) {
        if (kind === "Fiber") observed = (read() as any).dispatcher
        if (kind === "Scheduler.MixedDispatcher") registeredNative = true
        return () => {}
      }
    })
    const dispatcher = new Proxy<Scheduler.SchedulerDispatcher>({
      scheduleTask(task) {
        tasks.push(task)
      },
      flush() {
        while (tasks.length > 0) tasks.shift()!()
      }
    }, {
      getPrototypeOf() {
        probes++
        throw new Error("unavailable prototype")
      }
    })
    const scheduler: Scheduler.Scheduler = {
      executionMode: "async",
      shouldYield: () => false,
      makeDispatcher: () => dispatcher
    }
    const context = Context.make(Inspection.CurrentInspection, inspection).pipe(
      Context.add(Scheduler.Scheduler, scheduler)
    )
    const fiber = Effect.runForkWith(context)(Effect.yieldNow)
    while (tasks.length > 0) tasks.shift()!()
    assert.deepStrictEqual(fiber.pollUnsafe(), Exit.void)
    assert.strictEqual(observed, dispatcher)
    assert.strictEqual(registeredNative, false)
    assert.strictEqual(probes, 0)
  })

  it("keeps a replaced dispatcher and a completed Fiber's dispatcher until their actual tasks finish", () => {
    const tasks: Array<() => void> = []
    const events: Array<string> = []
    const readers = new Map<object, () => any>()
    const inspection = Inspection.make({
      register(owner, kind, read) {
        if (kind === "Scheduler.MixedDispatcher") readers.set(owner, read)
        return () => readers.delete(owner)
      }
    })
    const schedule = (task: () => void) => {
      let active = true
      tasks.push(() => {
        if (active) task()
      })
      return () => {
        active = false
      }
    }
    const first = new Scheduler.MixedScheduler("async", schedule)
    const second = new Scheduler.MixedScheduler("async", schedule)
    const context = Context.make(Inspection.CurrentInspection, inspection).pipe(Context.add(Scheduler.Scheduler, first))
    const release = Latch.makeUnsafe()
    let oldDispatcher: Scheduler.SchedulerDispatcher | undefined
    let currentDispatcher: Scheduler.SchedulerDispatcher | undefined
    const program = Effect.gen(function*() {
      const fiber = yield* Effect.fiber
      oldDispatcher = fiber.currentDispatcher
      oldDispatcher.scheduleTask(() => events.push("old"), 0)
      yield* Effect.gen(function*() {
        currentDispatcher = fiber.currentDispatcher
        currentDispatcher.scheduleTask(() => events.push("current"), 0)
        yield* release.await
      }).pipe(Effect.provideService(Scheduler.Scheduler, second))
    })
    const fiber = Effect.runForkWith(context)(program)
    try {
      assert.strictEqual(readers.size, 2)
      const old = readers.get(oldDispatcher!)!()
      assert.strictEqual(old.scheduler, first)
      assert.strictEqual(old.setImmediate, schedule)
      assert.strictEqual(old.fiber, undefined)
      assert.strictEqual(old.buckets[0][1].length, 1)
      assert.isDefined(old.running)
      tasks.shift()!()
      assert.deepStrictEqual(events, ["old"])
      assert.isFalse(readers.has(oldDispatcher!))
      release.openUnsafe()
      assert.deepStrictEqual(fiber.pollUnsafe(), Exit.void)
      assert.strictEqual(readers.size, 1)
      assert.strictEqual(readers.get(currentDispatcher!)!().fiber, undefined)
      while (tasks.length > 0) tasks.shift()!()
      assert.deepStrictEqual(events, ["old", "current"])
      assert.strictEqual(readers.size, 0)
    } finally {
      release.openUnsafe()
      fiber.interruptUnsafe()
      while (tasks.length > 0) tasks.shift()!()
    }
  })

  it("exposes both native runTasks frames during a reentrant flush", () => {
    const tasks: Array<() => void> = []
    const readers = new Map<object, () => any>()
    const depths: Array<number> = []
    const events: Array<string> = []
    const inspection = Inspection.make({
      register(owner, kind, read) {
        if (kind === "Scheduler.MixedDispatcher") readers.set(owner, read)
        return () => readers.delete(owner)
      }
    })
    const scheduler = new Scheduler.MixedScheduler("async", (task) => {
      let active = true
      tasks.push(() => {
        if (active) task()
      })
      return () => {
        active = false
      }
    })
    const context = Context.make(Inspection.CurrentInspection, inspection).pipe(
      Context.add(Scheduler.Scheduler, scheduler)
    )
    const program = Effect.sync(() => {
      const dispatcher = Fiber.getCurrent()!.currentDispatcher
      const read = () => readers.get(dispatcher)!()
      dispatcher.scheduleTask(() => {
        depths.push(read().frames.length)
        assert.strictEqual(read().frames[0].j, 0)
        dispatcher.scheduleTask(() => {
          depths.push(read().frames.length)
          assert.strictEqual(read().frames[0].j, 0)
          assert.strictEqual(read().frames[1].j, 0)
          events.push("inner")
        }, 0)
        dispatcher.flush()
        depths.push(read().frames.length)
        events.push("outer")
      }, 0)
      dispatcher.scheduleTask(() => events.push("next"), 0)
    })
    const fiber = Effect.runForkWith(context)(program)
    assert.deepStrictEqual(fiber.pollUnsafe(), Exit.void)
    while (tasks.length > 0) tasks.shift()!()
    assert.deepStrictEqual(depths, [1, 2, 1])
    assert.deepStrictEqual(events, ["inner", "outer", "next"])
    assert.strictEqual(readers.size, 0)
  })

  it("preserves a thrown native task and retires its detached observation", () => {
    const tasks: Array<() => void> = []
    const readers = new Map<object, () => unknown>()
    const inspection = Inspection.make({
      register(owner, kind, read) {
        if (kind === "Scheduler.MixedDispatcher") readers.set(owner, read)
        return () => readers.delete(owner)
      }
    })
    const scheduler = new Scheduler.MixedScheduler("async", (task) => {
      tasks.push(task)
      return () => {}
    })
    const defect = new Error("task")
    const context = Context.make(Inspection.CurrentInspection, inspection).pipe(
      Context.add(Scheduler.Scheduler, scheduler)
    )
    const fiber = Effect.runForkWith(context)(Effect.sync(() => {
      Fiber.getCurrent()!.currentDispatcher.scheduleTask(() => {
        throw defect
      }, 0)
    }))
    assert.deepStrictEqual(fiber.pollUnsafe(), Exit.void)
    assert.strictEqual(readers.size, 1)
    let thrown: unknown
    try {
      tasks.shift()!()
    } catch (error) {
      thrown = error
    }
    assert.strictEqual(thrown, defect)
    assert.strictEqual(readers.size, 0)
  })
})
