# @musubi/react

React bindings for Musubi stores. Provides `MusubiProvider`, `useMusubiRoot`,
`useMusubiSnapshot`, `useMusubiCommand`, and `useMusubiEvent` on top of
`@musubi/client`.

## Install

`@musubi/react` ships inside the Musubi Hex package under
`deps/musubi/packages/react`. After `mix deps.get` populates `deps/musubi/`,
reference both packages by local path from the frontend project's
`package.json` (adjust the relative path so it points at
`deps/musubi/packages/<name>` from the JS app root):

```json
{
  "dependencies": {
    "@musubi/client": "file:../deps/musubi/packages/client",
    "@musubi/react": "file:../deps/musubi/packages/react"
  }
}
```

Then install once:

```sh
pnpm install   # or npm install / yarn install
```

`react` and `react-dom` are peer dependencies — install them in the
consumer app, not inside this package.

Both `@musubi/client` and `@musubi/react` ship TypeScript source directly;
the consumer bundler (Vite, Phoenix esbuild) transpiles on demand — no
build step required.

## Avoiding Duplicate React Copies

When the consumer's package manager copies the linked package into
`node_modules/@musubi/react/`, it may also bring along a second copy of
`react` via the package's own dependency tree. The symptom is
`Invalid hook call. Hooks can only be called inside of the body of a
function component` with a stack pointing deep into `@musubi/react`
internals — the bug is the dual install, not Musubi.

### Vite

Add to `vite.config.ts`:

```ts
import path from "node:path"
import { defineConfig } from "vite"

export default defineConfig({
  resolve: {
    dedupe: ["react", "react-dom"],
    alias: {
      react: path.resolve(__dirname, "node_modules/react"),
      "react-dom": path.resolve(__dirname, "node_modules/react-dom"),
    },
  },
})
```

### Webpack / Next.js

Use `resolve.alias` with the same paths.

### Verifying the Fix

After configuring, run `pnpm why react` (or `npm ls react`) in the
consumer app. Every entry should resolve to the same path on disk. If
they diverge, the bundler config has not deduplicated yet.
