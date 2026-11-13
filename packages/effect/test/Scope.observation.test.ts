import { assert, describe, it } from "@effect/vitest"
import { Effect, Exit, Fiber, Latch, Queue, Scheduler, Scope, Stream } from "effect"
import * as Inspection from "effect/Inspection"

describe("Scope observation", () => {
  it.effect("observes concurrent executions of saved cleanup separately", () =>
    Effect.gen(function*() {
      const scope = yield* Scope.make()
      const started = yield* Queue.unbounded<number>()
      const releases = [yield* Latch.make(), yield* Latch.make()]
      let next = 0
      const readers = new Map<object, () => unknown>()
      const inspection = Inspection.make({
        register(owner, kind, read) {
          if (kind === "Scope.close" && (read() as any).scope === scope) readers.set(owner, read)
          return () => readers.delete(owner)
        }
      })
      yield* Scope.addFinalizer(
        scope,
        Effect.gen(function*() {
          const index = next++
          yield* Queue.offer(started, index)
          yield* releases[index].await
        })
      )
      const saved = Scope.closeUnsafe(scope, Exit.void)!
      const run = Effect.uninterruptible(saved).pipe(Effect.provideService(Inspection.CurrentInspection, inspection))
      const first = yield* Effect.forkChild(run)
      assert.strictEqual(yield* Queue.take(started), 0)
      const second = yield* Effect.forkChild(run)
      assert.strictEqual(yield* Queue.take(started), 1)
      try {
        assert.strictEqual(readers.size, 2)
        yield* releases[0].open
        yield* Fiber.join(first)
        assert.strictEqual(readers.size, 1)
      } finally {
        releases[0].openUnsafe()
        releases[1].openUnsafe()
      }
      yield* Fiber.join(second)
      assert.strictEqual(readers.size, 0)
    }))

  for (const result of [Exit.void, Exit.die("defect"), Exit.interrupt(123)]) {
    it("preserves a finalizer's native Exit identity when close is immediate", () => {
      const scope = Scope.makeUnsafe()
      Effect.runSync(Scope.addFinalizer(scope, result))
      const cleanup = Scope.closeUnsafe(scope, Exit.void)
      assert.strictEqual(cleanup, result)
      assert.strictEqual(Effect.runSyncExit(cleanup!), result)
    })
  }

  it("returns cleanup outside a fiber and runs it in the later execution", () => {
    const scope = Scope.makeUnsafe()
    const events: Array<string> = []
    Effect.runSync(Scope.addFinalizerExit(scope, () => {
      events.push("constructed")
      return Effect.sync(() => events.push("executed"))
    }))
    const cleanup = Scope.closeUnsafe(scope, Exit.void)
    assert.deepStrictEqual(events, ["constructed"])
    assert.isDefined(cleanup)
    Effect.runSync(Effect.uninterruptible(cleanup!))
    assert.deepStrictEqual(events, ["constructed", "executed"])
  })

  it.effect("keeps readers until cleanup saved by another fiber actually completes", () =>
    Effect.gen(function*() {
      const scope = yield* Scope.make()
      const started = yield* Latch.make()
      const release = yield* Latch.make()
      const readers = new Map<object, string>()
      const inspection = Inspection.make({
        register(owner, kind) {
          readers.set(owner, kind)
          return () => readers.delete(owner)
        }
      })
      yield* Stream.toPull(Stream.make(1).pipe(Stream.changes)).pipe(
        Scope.provide(scope),
        Effect.provideService(Inspection.CurrentInspection, inspection),
        Effect.asVoid
      )
      yield* Scope.addFinalizer(scope, Effect.andThen(started.open, release.await))
      const saved = yield* Effect.sync(() => Scope.closeUnsafe(scope, Exit.void)).pipe(
        Effect.provideService(Inspection.CurrentInspection, inspection),
        Effect.forkChild,
        Effect.flatMap(Fiber.join)
      )
      assert.include(Array.from(readers.values()), "Stream.changesWith")
      const cleanup = yield* Effect.uninterruptible(saved!).pipe(
        Effect.provideService(Inspection.CurrentInspection, inspection),
        Effect.forkChild
      )
      yield* started.await
      try {
        assert.include(Array.from(readers.values()), "Stream.changesWith")
        assert.include(Array.from(readers.values()), "Scope.close")
      } finally {
        release.openUnsafe()
      }
      yield* Fiber.join(cleanup)
      assert.strictEqual(readers.size, 0)
    }))

  for (
    const [name, end] of [["success", Effect.void], ["failure", Effect.fail("subject")], [
      "defect",
      Effect.die("defect")
    ], ["interruption", Effect.interrupt]] as const
  ) {
    it(`preserves native instruction counts through nested close after ${name}`, () => {
      const run = (enabled: boolean) => {
        const operations: Array<number> = []
        const readers = new Set<object>()
        const inspection = Inspection.make({
          register(owner) {
            readers.add(owner)
            return () => readers.delete(owner)
          }
        })
        const scheduler: Scheduler.Scheduler = {
          executionMode: "sync",
          makeDispatcher: () => new Scheduler.MixedScheduler("sync").makeDispatcher(),
          shouldYield(fiber) {
            operations.push(fiber.currentOpCount)
            return false
          }
        }
        const exit = Effect.runSyncExit(
          Effect.scoped(Effect.gen(function*() {
            const child = yield* Scope.make()
            yield* Scope.addFinalizer(child, Effect.void)
            yield* Effect.addFinalizer(() => Scope.close(child, Exit.void))
            return yield* end
          })).pipe(
            Effect.provideService(Inspection.CurrentInspection, enabled ? inspection : undefined),
            Effect.provideService(Scheduler.Scheduler, scheduler)
          )
        )
        assert.strictEqual(readers.size, 0)
        return { operations, tag: exit._tag }
      }
      assert.deepStrictEqual(run(true), run(false))
    })
  }

  it.effect("retains a new reader registered during a single suspended finalizer", () =>
    Effect.gen(function*() {
      const scope = yield* Scope.make()
      const entered = yield* Latch.make()
      const release = yield* Latch.make()
      const readers = new Map<object, string>()
      const inspection = Inspection.make({
        register(owner, kind) {
          readers.set(owner, kind)
          return () => readers.delete(owner)
        }
      })
      yield* Scope.addFinalizer(
        scope,
        Effect.gen(function*() {
          const pull = yield* Stream.toPull(Stream.make(1).pipe(Stream.changes)).pipe(Scope.provide(scope))
          yield* entered.open
          yield* release.await
          assert.deepStrictEqual(yield* Effect.orDie(pull), [1])
        })
      )
      const fiber = yield* Scope.close(scope, Exit.void).pipe(
        Effect.provideService(Inspection.CurrentInspection, inspection),
        Effect.forkChild
      )
      yield* entered.await
      try {
        assert.include(Array.from(readers.values()), "Scope.close")
        assert.include(Array.from(readers.values()), "Stream.changesWith")
        yield* Scope.close(scope, Exit.void)
        assert.include(Array.from(readers.values()), "Stream.changesWith")
      } finally {
        release.openUnsafe()
      }
      yield* Fiber.join(fiber)
      assert.strictEqual(readers.size, 0)
    }))

  it.effect("retains remaining finalizers and earlier failures while cleanup is suspended", () =>
    Effect.gen(function*() {
      const scope = yield* Scope.make()
      const entered = yield* Latch.make()
      const release = yield* Latch.make()
      let read: (() => any) | undefined
      const inspection = Inspection.make({
        register(_owner, kind, reader) {
          if (kind !== "Scope.close" || (reader() as any).scope !== scope) return () => {}
          read = reader
          return () => {
            read = undefined
          }
        }
      })
      const ran: Array<string> = []
      yield* Scope.addFinalizer(scope, Effect.sync(() => ran.push("last")))
      yield* Scope.addFinalizer(scope, Effect.andThen(entered.open, release.await))
      yield* Scope.addFinalizer(scope, Effect.die("first failed"))
      const fiber = yield* Scope.close(scope, Exit.void).pipe(
        Effect.provideService(Inspection.CurrentInspection, inspection),
        Effect.forkChild
      )
      yield* entered.await
      try {
        assert.strictEqual(scope.state._tag, "Closed")
        assert.isDefined(read)
        assert.strictEqual(read!().index, 1)
        assert.deepStrictEqual(read!().exits, [Exit.die("first failed")])
        assert.deepStrictEqual(ran, [])
      } finally {
        release.openUnsafe()
      }
      assert.deepStrictEqual(yield* Fiber.await(fiber), Exit.die("first failed"))
      assert.deepStrictEqual(ran, ["last"])
      assert.isUndefined(read)
    }))
})
