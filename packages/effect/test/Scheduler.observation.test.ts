import { assert, describe, it } from "@effect/vitest"
import { Context, Effect, Exit, Fiber, Inspection, Latch, Queue, Scheduler, Schema, Scope } from "effect"

import { Rpc, RpcClient, RpcGroup, type RpcMessage } from "effect/rpc"

describe("Scheduler observation", () => {
  for (const phase of ["AfterCompletion", "CompletionObserver"] as const) {
    for (const mode of ["Absent", "Present", "Register", "Unregister"] as const) {
      it(`retains first inspection affinity for ${phase} dispatcher access with ${mode}`, () => {
        const tasks: Array<() => void> = []
        const events: Array<number> = []
        const snapshots: Array<any> = []
        let fiberRegistrations = 0
        let foreignDispatchers = 0
        const inspection = Inspection.make({
          register(_owner, kind, read) {
            if (kind === "Fiber") fiberRegistrations++
            if (kind === "Scheduler.MixedDispatcher") snapshots.push(read())
            if (mode === "Register") throw new Error("register")
            return () => {
              if (mode === "Unregister") throw new Error("unregister")
            }
          }
        })
        const other = Inspection.make({
          register(_owner, kind) {
            if (kind === "Scheduler.MixedDispatcher") foreignDispatchers++
            return () => {}
          }
        })
        const scheduler = new Scheduler.MixedScheduler("async", (task) => {
          tasks.push(task)
          return () => {}
        })
        const context = Context.make(Scheduler.Scheduler, scheduler).pipe(
          Context.add(Inspection.CurrentInspection, mode === "Absent" ? undefined : inspection)
        )
        let resume: (effect: Effect.Effect<void>) => void = () => {}
        const fiber = Effect.runForkWith(context)(Effect.callback<void>((complete) => {
          resume = complete
        }))
        let dispatcher: Scheduler.SchedulerDispatcher | undefined
        const before = fiberRegistrations
        if (phase === "CompletionObserver") {
          fiber.addObserver(() => {
            dispatcher = fiber.currentDispatcher
            dispatcher.flush()
            assert.strictEqual(snapshots.length, 0)
            dispatcher.scheduleTask(() => events.push(1), 0)
          })
          resume(Effect.void)
        } else {
          resume(Effect.void)
          const accessed = Effect.runForkWith(Context.make(Inspection.CurrentInspection, other))(
            Effect.sync(() => fiber.currentDispatcher)
          ).pollUnsafe()!
          assert.isTrue(Exit.isSuccess(accessed))
          if (Exit.isFailure(accessed)) return
          dispatcher = accessed.value
          dispatcher.flush()
          assert.strictEqual(snapshots.length, 0)
          dispatcher.scheduleTask(() => events.push(1), 0)
        }
        assert.deepStrictEqual(fiber.pollUnsafe(), Exit.void)
        assert.strictEqual(fiberRegistrations, before)
        assert.strictEqual(snapshots.length, mode === "Absent" ? 0 : 1)
        if (mode !== "Absent") assert.strictEqual(snapshots[0].fiber, undefined)
        while (tasks.length > 0) tasks.shift()!()
        dispatcher!.scheduleTask(() => events.push(2), 0)
        while (tasks.length > 0) tasks.shift()!()
        assert.deepStrictEqual(events, [1, 2])
        assert.strictEqual(fiberRegistrations, before)
        assert.strictEqual(foreignDispatchers, 0)
        assert.strictEqual(snapshots.length, mode === "Absent" ? 0 : 2)
        assert.strictEqual(inspection.failure()._tag, mode === "Register" || mode === "Unregister" ? "Some" : "None")
      })
    }
  }

  it("observes the native RPC sender interruption scheduled after its caller completes", () => {
    const tasks: Array<() => void> = []
    const readers = new Map<object, { kind: string; read: () => any }>()
    const inspection = Inspection.make({
      register(owner, kind, read) {
        readers.set(owner, { kind, read })
        return () => readers.delete(owner)
      }
    })
    const scheduler = new Scheduler.MixedScheduler("async", (task) => {
      tasks.push(task)
      return () => {}
    })
    const scope = Scope.makeUnsafe()
    const context = Context.make(Inspection.CurrentInspection, inspection).pipe(
      Context.add(Scheduler.Scheduler, scheduler),
      Context.add(Scope.Scope, scope)
    )
    const run = Effect.runForkWith(context)
    const group = RpcGroup.make(Rpc.make("Ping", { success: Schema.String }))
    let requestId: RpcMessage.RequestId | undefined
    let sender: Fiber.Fiber<unknown, unknown> | undefined
    const created = run(RpcClient.makeNoSerialization(group, {
      disableTracing: true,
      onFromClient: ({ message }) => {
        if (message._tag !== "Request") return Effect.void
        requestId = message.id
        return Effect.callback(() => {
          sender = Fiber.getCurrent()
        })
      }
    })).pollUnsafe()!
    assert.isTrue(Exit.isSuccess(created))
    if (Exit.isFailure(created)) return
    const { client, write } = created.value
    const caller = run(client.Ping())
    try {
      assert.isUndefined(caller.pollUnsafe())
      const response = run(write({ _tag: "Exit", clientId: 0, requestId: requestId!, exit: Exit.succeed("reply") }))
      assert.deepStrictEqual(caller.pollUnsafe(), Exit.succeed("reply"))
      assert.deepStrictEqual(response.pollUnsafe(), Exit.void)
      assert.isUndefined(sender!.pollUnsafe())
      const dispatchers = Array.from(readers.values()).filter((entry) => entry.kind === "Scheduler.MixedDispatcher")
      assert.strictEqual(dispatchers.length, 1)
      assert.strictEqual(dispatchers[0].read().fiber, undefined)
      assert.isFalse(readers.has(caller))
      assert.strictEqual(tasks.length, 1)
      while (tasks.length > 0) tasks.shift()!()
      assert.isTrue(Exit.isFailure(sender!.pollUnsafe()!))
    } finally {
      while (tasks.length > 0) tasks.shift()!()
      const closing = run(Scope.close(scope, Exit.void))
      while (tasks.length > 0) tasks.shift()!()
      assert.deepStrictEqual(closing.pollUnsafe(), Exit.void)
    }
    assert.strictEqual(readers.size, 0)
  })

  for (const enabled of [false, true]) {
    it(`preserves transparent native dispatcher proxies with inspection ${enabled}`, () => {
      const tasks: Array<() => void> = []
      const events: Array<string> = []
      let nativeReaders = 0
      let observed: unknown
      const inspection = Inspection.make({
        register(_owner, kind, read) {
          if (kind === "Fiber") observed = (read() as any).dispatcher
          if (kind === "Scheduler.MixedDispatcher") nativeReaders++
          return () => {}
        }
      })
      const nativeScheduler = new Scheduler.MixedScheduler("async", (task) => {
        events.push("schedule")
        let active = true
        tasks.push(() => {
          if (active) task()
        })
        return () => {
          active = false
          events.push("cancel")
        }
      })
      const proxy = new Proxy(nativeScheduler.makeDispatcher(), {})
      const scheduler: Scheduler.Scheduler = {
        executionMode: "async",
        shouldYield: () => false,
        makeDispatcher: () => proxy
      }
      const context = Context.make(Scheduler.Scheduler, scheduler).pipe(
        Context.add(Inspection.CurrentInspection, enabled ? inspection : undefined)
      )
      const fiber = Effect.runForkWith(context)(Effect.as(Effect.yieldNow, 7))
      proxy.flush()
      while (tasks.length > 0) tasks.shift()!()
      assert.deepStrictEqual(fiber.pollUnsafe(), Exit.succeed(7))
      assert.deepStrictEqual(events, ["schedule", "cancel"])
      if (enabled) assert.strictEqual(observed === proxy, true)
      assert.strictEqual(nativeReaders, 0)
      const defect = new Error("native proxy task")
      proxy.scheduleTask(() => {
        throw defect
      }, 0)
      let thrown: unknown
      try {
        proxy.flush()
      } catch (error) {
        thrown = error
      }
      while (tasks.length > 0) tasks.shift()!()
      assert.strictEqual(thrown, defect)
      assert.deepStrictEqual(events, ["schedule", "cancel", "schedule", "cancel"])
      proxy.flush()
    })
  }

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
    assert.strictEqual(observed === dispatcher, true)
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
