import type * as Inspection from "../Inspection.ts"
import type * as Scope from "../Scope.ts"
import * as effect from "./effect.ts"

/** @internal */
export const register = (
  scope: Scope.Scope,
  inspection: Inspection.Inspection,
  owner: object,
  kind: string,
  read: () => unknown
): void => {
  const unregister = inspection.register(owner, kind, read)
  if (scope.state._tag === "Closed") {
    unregister()
  } else {
    effect.scopeAddFinalizerUnsafe(scope, {}, () => effect.sync(unregister))
  }
}
