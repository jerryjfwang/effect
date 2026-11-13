import type * as Inspection from "../Inspection.ts"
import type * as Scope from "../Scope.ts"

/** @internal */
export const scopeObservers = new WeakMap<Scope.Scope, {
  readonly disposers: Set<() => void>
  readonly closings: Set<object>
}>()

/** @internal */
export const switchMapSources = new WeakMap<object, {
  readonly source: object
  readonly mapper: unknown
  readonly stream?: { readonly source: object; readonly mapper: unknown }
}>()

/** @internal */
export const switchMapInputs = new WeakMap<object, {
  readonly input: unknown
  readonly mapper: unknown
  readonly channel: object
}>()

/** @internal */
export const register = (
  scope: Scope.Scope,
  inspection: Inspection.Inspection,
  owner: object,
  kind: string,
  read: () => unknown
): void => {
  const unregister = inspection.register(owner, kind, read)
  let observers = scopeObservers.get(scope)
  if (observers === undefined && scope.state._tag === "Closed") {
    unregister()
    return
  }
  if (observers === undefined) scopeObservers.set(scope, observers = { disposers: new Set(), closings: new Set() })
  observers.disposers.add(unregister)
}
