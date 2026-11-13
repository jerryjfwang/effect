/**
 * Optional inspection of native resource state. Inspection does not establish
 * state equivalence or describe arbitrary suspended Effect continuations.
 *
 * @stability unstable
 * @since 4.1.3
 */
import * as Context from "./Context.ts"
import { constUndefined, constVoid } from "./Function.ts"
import * as Option from "./Option.ts"

/**
 * A diagnostic callback failure, retained independently of resource execution.
 *
 * @stability unstable
 * @category models
 * @since 4.1.3
 */
export interface Failure {
  readonly owner: object
  readonly kind: string
  readonly phase: "Register" | "Unregister"
  readonly cause: unknown
}

/**
 * Registers native readers. Consumers must neither mutate observed references
 * nor treat them as a complete logical state. A failure invalidates inspection
 * without interrupting the resource or its cleanup.
 *
 * @stability unstable
 * @category models
 * @since 4.1.3
 */
export interface Inspection {
  readonly register: (owner: object, kind: string, read: () => unknown) => () => void
  readonly failure: () => Option.Option<Failure>
}

/**
 * Makes an inspection service that retains its first callback failure.
 *
 * @stability unstable
 * @category constructors
 * @since 4.1.3
 */
export const make = (options: Pick<Inspection, "register">): Inspection => {
  let failure: Option.Option<Failure> = Option.none()
  return {
    register(owner, kind, read) {
      try {
        const unregister = options.register(owner, kind, read)
        return () => {
          try {
            unregister()
          } catch (cause) {
            if (Option.isNone(failure)) failure = Option.some({ owner, kind, phase: "Unregister", cause })
          }
        }
      } catch (cause) {
        if (Option.isNone(failure)) failure = Option.some({ owner, kind, phase: "Register", cause })
        return constVoid
      }
    },
    failure: () => failure
  }
}

/**
 * Native inspection is disabled unless explicitly provided.
 *
 * @stability unstable
 * @category services
 * @since 4.1.3
 */
export const CurrentInspection: Context.Reference<Inspection | undefined> = Context.Reference(
  "effect/Inspection",
  { defaultValue: constUndefined }
)
