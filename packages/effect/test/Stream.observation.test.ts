import { assert, describe, it } from "@effect/vitest"
import { Effect, Exit, Scope, Stream } from "effect"
import * as Inspection from "effect/Inspection"

describe("Stream observation", () => {
  it.effect("observes the native drop count without changing chunk delivery", () =>
    Effect.gen(function*() {
      let read: (() => any) | undefined
      const inspection = Inspection.make({
        register(_owner, kind, reader) {
          if (kind === "Stream.drop") read = reader
          return () => {}
        }
      })
      const pull = yield* Stream.make(1, 2, 3, 4).pipe(
        Stream.rechunk(3),
        Stream.drop(2),
        Stream.toPull,
        Effect.provideService(Inspection.CurrentInspection, inspection)
      )
      assert.isDefined(read)
      assert.strictEqual(read!().dropped, 0)
      assert.deepStrictEqual(yield* pull, [3])
      assert.strictEqual(read!().count, 2)
      assert.strictEqual(read!().dropped, 3)
      assert.deepStrictEqual(yield* pull, [4])
      assert.strictEqual(read!().dropped, 3)
    }))

  it.effect("retains each acquisition's comparison anchor and original comparator", () =>
    Effect.gen(function*() {
      const readers = new Map<object, () => any>()
      const inspection = Inspection.make({
        register(owner, kind, read) {
          if (kind !== "Stream.changesWith") return () => {}
          readers.set(owner, read)
          return () => readers.delete(owner)
        }
      })
      const comparator = (a: number, b: number) => a === b
      const stream = Stream.make(1, 1, 2).pipe(Stream.rechunk(1), Stream.changesWith(comparator))
      const scope = yield* Scope.make()
      const acquire = Stream.toPull(stream).pipe(
        Scope.provide(scope),
        Effect.provideService(Inspection.CurrentInspection, inspection)
      )
      const first = yield* acquire
      const second = yield* acquire
      assert.strictEqual(readers.size, 2)
      const [readFirst, readSecond] = Array.from(readers.values())
      assert.strictEqual(readFirst().first, true)
      assert.deepStrictEqual(yield* first, [1])
      assert.strictEqual(readFirst().last, 1)
      assert.strictEqual(readFirst().comparator, comparator)
      assert.strictEqual(readSecond().first, true)
      assert.deepStrictEqual(yield* first, [2])
      assert.strictEqual(readFirst().last, 2)
      assert.deepStrictEqual(yield* second, [1])
      yield* Scope.close(scope, Exit.void)
      assert.strictEqual(readers.size, 0)
    }))
})
