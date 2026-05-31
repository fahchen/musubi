# Musubi 0.6.1 — `mountConnectionRoot` keys by `id` only, ignoring `module`

Filed by: coloured_flow_dashboard (dashboard/ui, musubi 0.6.1, @musubi/react workspace)

**Status**: fixed. `packages/client/src/runtime.ts` now keys the local
`connectionState.roots` Map by `(module, id)`. Patch routing iterates by
wire `root_id` and prefers the server-confirmed entry (`version >= 1`),
which keeps the existing protocol intact while turning silent corruption
into either correct dedup (same `(module, id)`) or a loud
`:already_mounted` rejection (distinct module reusing a sibling's `id`).
Regression covered by `packages/client/test/connect.test.ts` →
"mountStore does not dedup across distinct modules sharing one id".

## Symptom

Operator navigates from `/` (`InboxStore`) to `/flows` (`FlowCatalogStore`)
via the in-app sidebar link. The destination page renders the empty state
("No flows registered") even though the server has flows. Hard-reloading
`/flows` shows the catalog correctly. Reverse navigation `/flows → /` exhibits
the mirror failure (Inbox shows empty).

No console error, no toast, no rejected mount push. Snapshot just returns
the wrong store's state — silent corruption.

## Repro

Two `Musubi.Store, root: true` modules share a client-side mount id:

```ts
// src/components/InboxNotifier.tsx (always mounted in RootLayout)
useMusubiRoot({ module: "InboxStore",       id: "default" })

// src/routes/InboxPage.tsx
useMusubiRootSuspense({ module: "InboxStore", id: "default" })

// src/routes/FlowCatalogPage.tsx
useMusubiRootSuspense({ module: "FlowCatalogStore", id: "default" })
```

Sequence:

1. App boots at `/`. `InboxNotifier` mounts → client `mountConnectionRoot`
   creates `roots["default"] = { module: "InboxStore", refCount: 1 }`.
   `InboxPage` mounts → finds existing root_id="default", `refCount++` → 2.
   Both correctly point at InboxStore.
2. Operator clicks `/flows`. `InboxPage` unmounts → `unmountConnectionRoot`
   → `refCount-- → 1`. `InboxNotifier` is still mounted at the layout
   level, so the entry stays alive as InboxStore.
3. `FlowCatalogPage` mounts → `mountConnectionRoot({ module:
   "FlowCatalogStore", id: "default" })`. `mountConnectionRoot` looks up
   `roots["default"]`, finds the InboxStore entry, `refCount++ → 2`, returns
   its `ready` promise. The promise resolves immediately
   (`connection.version >= 1`), so no server-side mount is even attempted.
4. React commits. `getRootProxy<FlowCatalogStore>(connection)` returns a
   proxy bound to the InboxStore root's connection state. `useMusubiSnapshot`
   reads InboxStore's `:workitems` / `:counts` tree; the FlowCatalogStore
   render function never ran for this client. `snapshot.flows` is
   `undefined` → empty state.

## Observed vs expected

- **Observed**: second mount silently aliases an existing root that has a
  different `module`. Server is never told FlowCatalogStore is needed for
  this connection. SPA renders empty.
- **Expected**: distinct `(module, id)` pairs map to distinct
  `RootConnection` entries. A second mount of `id="default"` with a
  different `module` should either (a) be a separate connection, or
  (b) reject with an explicit "id already mounted with module X" error so
  the consumer can fix it.

## Root cause (file:line)

`packages/client/src/runtime.ts:155-205` — `mountConnectionRoot` keys
`connectionState.roots` purely by `options.id`:

```ts
export function mountConnectionRoot(
  connectionState: ConnectionState,
  options: MountConnectionRootOptions
): { connection: RootConnection; ready: Promise<void> } {
  const rootId = options.id
  const existing = connectionState.roots.get(rootId)

  if (existing) {
    existing.refCount += 1
    return { connection: existing, ready: ensureConnectionRootMounted(existing) }
  }
  // …
  connectionState.roots.set(rootId, connection)
  // …
}
```

`module` is stored on the `RootConnection` but never compared against the
caller's `options.module`. The companion `unmountConnectionRoot`
(`packages/client/src/runtime.ts:207`) is keyed the same way.

`@musubi/react`'s `pendingRootMounts` correctly uses
`${id}|${module}|${params}` as the React-layer key
(`packages/react/src/index.tsx:792`), so React thinks each consumer has
its own shared mount. The collision is purely at the client layer.

## What the dashboard tried

**Workaround (shipped)**: assign each root store a unique mount id so two
distinct modules never share a rootId on the same connection.

```ts
useMusubiRoot({       module: "InboxStore",         id: "inbox"          })
useMusubiRootSuspense({ module: "InboxStore",       id: "inbox"          })
useMusubiRootSuspense({ module: "FlowCatalogStore", id: "flow-catalog"   })
useMusubiRootSuspense({ module: "EnactmentListStore", id: "enactment-list" })
useMusubiRootSuspense({ module: "TelemetryFeedStore", id: "global"        })
useMusubiRootSuspense({ module: "EnactmentDetailStore", id: enactmentId   }) // already unique
```

This works because each pair `(module, id)` now yields a unique `id` in
isolation. Pages that share a module + id (InboxNotifier + InboxPage on
"inbox") still correctly share a single server-side mount via existing
refCount reuse.

The workaround is fragile: any future store added with id="default" (or
any id collision) re-introduces the silent corruption with no test or
runtime signal. Tests mock `@musubi/react` so they never exercise the
real client path.

## Suggested fix direction

Pick one:

1. **Compose key by `(id, module)`** in `mountConnectionRoot` /
   `unmountConnectionRoot` so the existing dedupe is correct. Smallest
   diff. Stores keyed `${id}|${module}` (params can stay out of the key
   if server treats them as cosmetic).
2. **Reject mismatched module on existing id**: keep `id`-keyed roots
   but throw when `existing.module !== options.module`. Surfaces the
   bug loudly; consumer must pass distinct ids. Smaller behavior change
   but worse ergonomics — every multi-store app needs unique ids.
3. **Server-side `:already_mounted` reply** for `(connection, root_id)`
   with mismatched module. Belt-and-suspenders alongside (1) — protects
   against future client regressions.

(1) is the principled fix; module is already on the `RootConnection`
struct, just plumb it into the lookup key.

## Footprint

Three call sites in `packages/client/src/runtime.ts` (mount, unmount, and
the `recoverConnectionRootFromVersionMismatch` re-mount path at line
631-643 — currently safe because it uses the original
`connection.id`). `@musubi/react`'s `rootMountKey` already composes
`(id, module, params)`; the change just aligns the client with what the
React layer already promised.

No public-API surface change. `MountStoreOptions.id` and `.module` stay
where they are.
