import { cleanup } from "@testing-library/react"
import { afterEach } from "vitest"

import type { StoreModule, StoreProxy, StoreSnapshot } from "@musubi/client"

afterEach(() => {
  cleanup()
})

/**
 * Minimal stand-in for a real `StoreProxy<M, R>` used by the React adapter
 * tests. The fake exposes the four reserved runtime members and a settable
 * snapshot/dispatch impl, but skips proxy field access — tests assert on
 * snapshot values, not on bracket-style field reads through the proxy.
 */
export class FakeStoreProxy<M extends StoreModule<R>, R> {
  readonly __musubi_store_id__: string[] = []

  private snapshotValue: StoreSnapshot<M, R>
  private readonly subscribers = new Set<() => void>()

  readonly dispatchCalls: Array<{ name: string; payload: unknown }> = []
  private dispatchImpl: (name: string, payload: unknown) => Promise<unknown> = async () =>
    undefined

  private readonly eventHandlers = new Map<string, Set<(payload: unknown) => void>>()

  constructor(initialSnapshot: StoreSnapshot<M, R>) {
    this.snapshotValue = initialSnapshot
  }

  subscribe = (listener: () => void): (() => void) => {
    this.subscribers.add(listener)
    return () => {
      this.subscribers.delete(listener)
    }
  }

  snapshot = (): StoreSnapshot<M, R> => this.snapshotValue

  handleEvent = (name: string, handler: (payload: unknown) => void): (() => void) => {
    const handlers = this.eventHandlers.get(name) ?? new Set<(payload: unknown) => void>()
    handlers.add(handler)
    this.eventHandlers.set(name, handlers)
    return () => {
      handlers.delete(handler)
      if (handlers.size === 0) this.eventHandlers.delete(name)
    }
  }

  emitEvent(name: string, payload: unknown): void {
    for (const handler of this.eventHandlers.get(name) ?? []) handler(payload)
  }

  dispatchCommand = async (name: string, payload: unknown): Promise<unknown> => {
    this.dispatchCalls.push({ name, payload })
    return this.dispatchImpl(name, payload)
  }

  setSnapshot(next: StoreSnapshot<M, R>): void {
    this.snapshotValue = next
    for (const listener of this.subscribers) listener()
  }

  onDispatch(impl: (name: string, payload: unknown) => Promise<unknown>): void {
    this.dispatchImpl = impl
  }

  asProxy(): StoreProxy<M, R> {
    return this as StoreProxy<M, R>
  }
}
