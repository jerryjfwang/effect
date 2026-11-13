import { assert, describe, it } from "@effect/vitest"
import { Context, Effect, Exit, Fiber, Inspection, Scheduler, Scope } from "effect"

describe("Fiber observation", () => {
  it("observes synchronous root creation and releases its reader after native completion", () => {
    const readers = new Map<object, () => any>()
    const inspection = Inspection.make({
      register(owner, kind, read) {
        if (kind === "Fiber") readers.set(owner, read)
        return () => readers.delete(owner)
      }
    })
    const program = Effect.sync(() => {
      const fiber = Fiber.getCurrent()!
      const read = readers.get(fiber)
      assert.isDefined(read)
      assert.strictEqual(read!().running, true)
      assert.strictEqual(read!().creation.site, "runForkWith")
      assert.strictEqual(read!().creation.initialEffect, program)
      assert.strictEqual(read!().currentInspection, inspection)
      assert.strictEqual(read!().dispatcher, undefined)
      return 42
    })
    const fiber = Effect.runForkWith(Context.make(Inspection.CurrentInspection, inspection))(program)
    assert.deepStrictEqual(fiber.pollUnsafe(), Exit.succeed(42))
    assert.strictEqual(readers.size, 0)
  })

  it("refreshes through the first inspector when the service or scheduler temporarily changes", () => {
    const observed: Array<{ scheduler: Scheduler.Scheduler; inspection: Inspection.Inspection | undefined }> = []
    const first = new Scheduler.MixedScheduler("sync")
    const second = new Scheduler.MixedScheduler("sync")
    const inspection = Inspection.make({
      register(_owner, kind, read) {
        if (kind === "Fiber") {
          const state = read() as any
          observed.push({ scheduler: state.scheduler, inspection: state.currentInspection })
        }
        return () => {}
      }
    })
    const context = Context.make(Inspection.CurrentInspection, inspection).pipe(Context.add(Scheduler.Scheduler, first))
    const program = Effect.gen(function*() {
      yield* Effect.void.pipe(Effect.provideService(Inspection.CurrentInspection, undefined))
      yield* Effect.void.pipe(Effect.provideService(Scheduler.Scheduler, second))
    })
    const fiber = Effect.runForkWith(context)(program)
    assert.deepStrictEqual(fiber.pollUnsafe(), Exit.void)
    assert.isTrue(observed.some((state) => state.inspection === undefined && state.scheduler === first))
    assert.isTrue(observed.some((state) => state.scheduler === second && state.inspection === inspection))
    assert.deepStrictEqual(observed.at(-1), { scheduler: first, inspection })
  })

  it("marks a first late inspection entry as having unobserved creation", () => {
    const creations: Array<any> = []
    const inspection = Inspection.make({
      register(_owner, kind, read) {
        if (kind === "Fiber") creations.push((read() as any).creation)
        return () => {}
      }
    })
    Effect.runSync(Effect.void.pipe(Effect.provideService(Inspection.CurrentInspection, inspection)))
    assert.isAbove(creations.length, 0)
    assert.isTrue(creations.every((creation) => creation.site === "Unobserved"))
  })

  it("records deferred, detached, cached and closed-scope native creation sites", () => {
    const creations: Array<any> = []
    const tasks: Array<() => void> = []
    const inspection = Inspection.make({
      register(_owner, kind, read) {
        if (kind === "Fiber") creations.push((read() as any).creation)
        return () => {}
      }
    })
    const scheduler = new Scheduler.MixedScheduler("async", (task) => {
      tasks.push(task)
      return () => {}
    })
    const context = Context.make(Inspection.CurrentInspection, inspection).pipe(
      Context.add(Scheduler.Scheduler, scheduler)
    )
    const program = Effect.gen(function*() {
      const parent = yield* Effect.fiber
      const child = yield* Effect.forkChild(Effect.void)
      yield* Fiber.join(child)
      const detached = yield* Effect.forkDetach(Effect.void, { startImmediately: true })
      yield* Fiber.join(detached)
      const cached = yield* Effect.cached(Effect.succeed(1))
      yield* cached
      const scope = yield* Scope.make()
      yield* Scope.close(scope, Exit.void)
      const closed = yield* Effect.forkIn(Effect.void, scope)
      assert.isTrue(Exit.isFailure(closed.pollUnsafe()!))
      return parent
    })
    const fiber = Effect.runForkWith(context)(program)
    while (tasks.length > 0) tasks.shift()!()
    assert.isTrue(Exit.isSuccess(fiber.pollUnsafe()!))
    assert.isTrue(
      creations.some((creation) =>
        creation.site === "forkUnsafe" && creation.parent === fiber && !creation.immediate && !creation.daemon
      )
    )
    assert.isTrue(
      creations.some((creation) =>
        creation.site === "forkUnsafe" && creation.parent === fiber && creation.immediate && creation.daemon
      )
    )
    assert.isTrue(creations.some((creation) => creation.site === "cached" && creation.parent === fiber))
    assert.isTrue(creations.some((creation) => creation.site === "forkIn.closed" && creation.parent === fiber))
  })
})
