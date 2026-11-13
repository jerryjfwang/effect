import { assert, describe, it } from "@effect/vitest"
import { Effect, Option, PubSub, Result } from "effect"

describe("PubSub observation", () => {
  it.effect("refuses bounded storage without consuming it", () =>
    Effect.gen(function*() {
      const pubsub = yield* PubSub.bounded<number>(2)
      const subscription = yield* PubSub.subscribe(pubsub)
      yield* PubSub.publish(pubsub, 1)
      assert.deepStrictEqual(
        PubSub.observeSubscriptionUnsafe(subscription),
        Result.fail(
          {
            _tag: "UnsupportedSubscriptionObservation",
            reason: "Storage"
          } as const
        )
      )
      assert.deepStrictEqual(yield* PubSub.takeAll(subscription), [1])
    }))

  it.effect("observes pending replay slides without synchronizing the cursor", () =>
    Effect.gen(function*() {
      const pubsub = yield* PubSub.unbounded<number>({ replay: 2 })
      yield* PubSub.publishAll(pubsub, [1, 2])
      const subscription = yield* PubSub.subscribe(pubsub)
      yield* PubSub.publish(pubsub, 3)
      pubsub.pubsub.slide()
      const before = PubSub.observeSubscriptionUnsafe(subscription)
      assert.isTrue(Result.isSuccess(before))
      if (Result.isFailure(before)) return
      assert.deepStrictEqual(before.success.unread, [])
      assert.deepStrictEqual(before.success.replay?.values, [1, 2])
      assert.strictEqual(before.success.replay?.slideIndex, 0)
      assert.strictEqual(before.success.replay?.buffer.index, 1)
      assert.deepStrictEqual(before.success.replay?.buffer.slideValues, [{ value: 3, index: 2 }])
      assert.deepStrictEqual(PubSub.observeSubscriptionUnsafe(subscription), before)
      assert.deepStrictEqual(yield* PubSub.takeAll(subscription), [2, 3])
    }))

  it.effect("distinguishes an unread undefined payload from shutdown and ending", () =>
    Effect.gen(function*() {
      const pubsub = yield* PubSub.unbounded<undefined>()
      const subscription = yield* PubSub.subscribe(pubsub)
      yield* PubSub.publish(pubsub, undefined)
      const before = PubSub.observeSubscriptionUnsafe(subscription)
      assert.isTrue(Result.isSuccess(before))
      if (Result.isFailure(before)) return
      assert.deepStrictEqual(before.success.unread, [undefined])
      assert.deepStrictEqual(before.success.ended, Option.none())
      yield* PubSub.end(pubsub, undefined)
      yield* PubSub.shutdown(pubsub)
      const after = PubSub.observeSubscriptionUnsafe(subscription)
      assert.isTrue(Result.isSuccess(after))
      if (Result.isFailure(after)) return
      assert.deepStrictEqual(after.success.unread, [])
      assert.strictEqual(after.success.cursor.unsubscribed, true)
      assert.strictEqual(after.success.shutdown, true)
      assert.deepStrictEqual(after.success.ended, Option.some(undefined))
    }))

  it.effect("keeps copied replay metadata stable while publishers advance", () =>
    Effect.gen(function*() {
      const pubsub = yield* PubSub.unbounded<number>({ replay: 2 })
      yield* PubSub.publish(pubsub, 1)
      const subscription = yield* PubSub.subscribe(pubsub)
      const observed = PubSub.observeSubscriptionUnsafe(subscription)
      assert.isTrue(Result.isSuccess(observed))
      if (Result.isFailure(observed)) return
      yield* PubSub.publish(pubsub, 2)
      assert.strictEqual(observed.success.publisher.replayBuffer?.publisherIndex, 1)
      assert.deepStrictEqual(yield* PubSub.takeAll(subscription), [1, 2])
    }))

  it.effect("observes unread values without advancing either subscriber", () =>
    Effect.gen(function*() {
      const pubsub = yield* PubSub.unbounded<number>({ replay: 2 })
      yield* PubSub.publishAll(pubsub, [1, 2])
      const first = yield* PubSub.subscribe(pubsub)
      const second = yield* PubSub.subscribe(pubsub)
      yield* PubSub.publishAll(pubsub, [3, 4])
      assert.strictEqual(yield* PubSub.take(first), 1)

      const observed = PubSub.observeSubscriptionUnsafe(first)
      assert.isTrue(Result.isSuccess(observed))
      if (Result.isFailure(observed)) return
      assert.deepStrictEqual(observed.success.unread, [3, 4])
      assert.deepStrictEqual<unknown>(observed.success.replay?.values, [Symbol.for("effect/PubSub/AbsentValue"), 2])
      assert.strictEqual(observed.success.replay?.index, 1)
      assert.strictEqual(observed.success.replay?.remaining, 1)
      assert.strictEqual(observed.success.publisher.owner, first.pubsub)
      assert.strictEqual(observed.success.cursor.owner, first.subscription)
      assert.deepStrictEqual(PubSub.observeSubscriptionUnsafe(first), observed)

      assert.deepStrictEqual(yield* PubSub.takeAll(first), [2, 3, 4])
      assert.deepStrictEqual(yield* PubSub.takeAll(second), [1, 2, 3, 4])
    }))
})
