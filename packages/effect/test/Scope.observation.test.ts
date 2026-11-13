import { assert, describe, it } from "@effect/vitest"
import { Effect, Exit, Fiber, Latch, Scheduler, Scope, Stream } from "effect"
import * as Inspection from "effect/Inspection"

describe("Scope observation", () => {
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
          assert.deepStrictEqual(yield* pull, [1])
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
        register(owner, kind, reader) {
          if (owner !== scope || kind !== "Scope.close") return () => {}
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
