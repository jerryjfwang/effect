import { assert, describe, it } from "@effect/vitest"
import { Effect, Exit, Stream } from "effect"
import * as Inspection from "effect/Inspection"
import * as Option from "effect/Option"

describe("Inspection", () => {
  for (const mode of ["Absent", "Present", "Register", "Unregister"] as const) {
    it.effect(`preserves stream failure and release when inspection is ${mode}`, () =>
      Effect.gen(function*() {
        const events: Array<string> = []
        const inspection = Inspection.make({
          register() {
            if (mode === "Register") throw new Error("register")
            return () => {
              if (mode === "Unregister") throw new Error("unregister")
            }
          }
        })
        const result = yield* Stream.fromEffect(Effect.acquireRelease(
          Effect.sync(() => {
            events.push("acquire")
            return 1
          }),
          () => Effect.sync(() => events.push("release"))
        )).pipe(
          Stream.changesWith((a, b) => a === b),
          Stream.switchMap(() => Stream.fail("subject failed")),
          Stream.runCollect,
          Effect.scoped,
          Effect.provideService(Inspection.CurrentInspection, mode === "Absent" ? undefined : inspection),
          Effect.exit
        )
        assert.deepStrictEqual(result, Exit.fail("subject failed"))
        assert.deepStrictEqual(events, ["acquire", "release"])
        assert.strictEqual(Option.isSome(inspection.failure()), mode === "Register" || mode === "Unregister")
      }))
  }

  it("retains diagnostic registration failure without throwing into the owner", () => {
    const owner = {}
    const cause = new Error("registration failed")
    const inspection = Inspection.make({
      register() {
        throw cause
      }
    })
    inspection.register(owner, "owner", () => 1)()
    assert.deepStrictEqual(inspection.failure(), Option.some({ owner, kind: "owner", phase: "Register", cause }))
  })
})
