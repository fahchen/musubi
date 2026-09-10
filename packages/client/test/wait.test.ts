import { describe, expect, test } from "vitest"

import { nextSnapshot, waitFor } from "../src/wait"

import type { AsyncResult, StoreProxy, StoreSnapshot } from "../src/types"

type WaitStores = {
  "Wait.Test.Root": Musubi.StoreDef<
    "Wait.Test.Root",
    {
      title: string
      report: Musubi.AsyncField<string>
    },
    {}
  >
}

type Root = "Wait.Test.Root"
type Snapshot = StoreSnapshot<Root, WaitStores>

describe("nextSnapshot", () => {
  test("shares one promise per proxy until it settles", async () => {
    const fake = fakeProxy()

    const first = nextSnapshot(fake.proxy)
    expect(nextSnapshot(fake.proxy)).toBe(first)

    fake.push({ title: "Inbox" })
    await first

    expect(fake.subscriberCount).toBe(0)
    expect(nextSnapshot(fake.proxy)).not.toBe(first)
  })

  test("does not cache a promise that settled during subscribe", async () => {
    // Guards the sync-notify path: `settle()` runs inside the promise executor,
    // so a naive `set()` after the constructor re-caches an already-settled
    // promise and every later waiter resolves instantly.
    const fake = fakeProxy(undefined, { notifyOnSubscribe: true })

    const first = nextSnapshot(fake.proxy)
    await first

    expect(fake.subscriberCount).toBe(0)
    expect(nextSnapshot(fake.proxy)).not.toBe(first)
  })
})

describe("waitFor", () => {
  test("resolves from the current snapshot without subscribing", async () => {
    const fake = fakeProxy({ title: "Inbox" })

    await expect(waitFor(fake.proxy, (snapshot) => snapshot?.title)).resolves.toBe("Inbox")
    expect(fake.subscriberCount).toBe(0)
  })

  test("resolves once a later patch satisfies the selector", async () => {
    const fake = fakeProxy()
    const pending = waitFor(fake.proxy, (snapshot) => snapshot?.title)

    fake.push({ title: "Inbox" })

    await expect(pending).resolves.toBe("Inbox")
  })

  test("keeps waiting while the selector returns undefined", async () => {
    const fake = fakeProxy()
    const pending = waitFor(fake.proxy, (snapshot) =>
      snapshot?.title === "Archive" ? snapshot.title : undefined
    )

    fake.push({ title: "Inbox" })
    await Promise.resolve()
    fake.push({ title: "Archive" })

    await expect(pending).resolves.toBe("Archive")
  })

  test("rejects when the selector throws — the async `failed` path", async () => {
    const fake = fakeProxy()
    const pending = waitFor(fake.proxy, (snapshot) => {
      const report = snapshot?.report
      if (report === undefined || report.status === "loading") return undefined
      if (report.status === "failed") throw new Error("report failed")
      return report.data
    })

    fake.push({ title: "Inbox", report: loading() })
    await Promise.resolve()
    fake.push({ title: "Inbox", report: failed() })

    await expect(pending).rejects.toThrow("report failed")
  })
})

function loading(): AsyncResult<string> {
  return { status: "loading", data: null, error: null }
}

function failed(): AsyncResult<string> {
  return { status: "failed", data: null, error: { kind: "error", value: "boom" } }
}

type FakeProxy = {
  proxy: StoreProxy<Root, WaitStores>
  push: (fields: { title: string; report?: AsyncResult<string> }) => void
  readonly subscriberCount: number
}

function fakeProxy(
  initial?: { title: string; report?: AsyncResult<string> },
  options?: { notifyOnSubscribe?: boolean }
): FakeProxy {
  const subscribers = new Set<() => void>()
  let snapshot: Snapshot | undefined = initial ? buildSnapshot(initial) : undefined

  const proxy = {
    __musubi_store_id__: [],
    subscribe(listener: () => void): () => void {
      subscribers.add(listener)
      // Models a transport that notifies from inside `subscribe` itself.
      if (options?.notifyOnSubscribe) listener()
      return () => {
        subscribers.delete(listener)
      }
    },
    snapshot: () => snapshot
  } as unknown as StoreProxy<Root, WaitStores>

  return {
    proxy,
    push(fields) {
      snapshot = buildSnapshot(fields)
      for (const listener of [...subscribers]) listener()
    },
    get subscriberCount() {
      return subscribers.size
    }
  }
}

function buildSnapshot(fields: { title: string; report?: AsyncResult<string> }): Snapshot {
  return {
    __musubi_store_id__: [],
    title: fields.title,
    report: fields.report ?? loading()
  } as Snapshot
}
