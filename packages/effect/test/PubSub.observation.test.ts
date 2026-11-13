import { assert, describe, it } from "@effect/vitest"
import { Effect, PubSub, Result } from "effect"

describe("PubSub observation", () => {
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
      assert.deepStrictEqual(observed.success.replay?.values, [Symbol.for("effect/PubSub/AbsentValue"), 2])
      assert.strictEqual(observed.success.replay?.index, 1)
      assert.strictEqual(observed.success.replay?.remaining, 1)
      assert.strictEqual(observed.success.publisher.owner, first.pubsub)
      assert.strictEqual(observed.success.cursor.owner, first.subscription)
      assert.deepStrictEqual(PubSub.observeSubscriptionUnsafe(first), observed)

      assert.deepStrictEqual(yield* PubSub.takeAll(first), [2, 3, 4])
      assert.deepStrictEqual(yield* PubSub.takeAll(second), [1, 2, 3, 4])
    }))
})
