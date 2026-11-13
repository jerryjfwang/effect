import { assert, describe, it } from "@effect/vitest"
import { Effect, Exit, Fiber, Latch, Scope } from "effect"
import * as Inspection from "effect/Inspection"

describe("Scope observation", () => {
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
