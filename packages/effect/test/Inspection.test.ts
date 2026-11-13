import { assert, describe, it } from "@effect/vitest"
import * as Inspection from "effect/Inspection"
import * as Option from "effect/Option"

describe("Inspection", () => {
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
