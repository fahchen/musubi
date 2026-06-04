# Releasing Musubi

How to cut a release of the `musubi` Elixir package. The JS packages
(`@musubi/client`, `@musubi/react`) are not in lockstep yet and are
not covered here — they stay at their workspace versions.

## Source of truth

| Surface | Field | Notes |
| :-- | :-- | :-- |
| `mix.exs` | `@version` | The only place the version is declared. `mix.exs:version: @version` reads it. |
| `CHANGELOG.md` | `## [X.Y.Z] — YYYY-MM-DD` section + bottom ref-link table | Adheres to [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). |
| `README.md` | install snippet `{:musubi, "~> X.Y.Z"}` | Conventionally refreshed on every bump. |
| `guides/getting-started.md` | install snippet | Same. |

Publishing to Hex is automated: pushing a tag matching `v*` to
GitHub fires `.github/workflows/publish.yml`, which runs
`mix hex.publish --yes` with the `HEX_API_KEY` repo secret.

## Pick the bump

SemVer per the project README:

- **Patch (`0.6.0` → `0.6.1`)** — bug fixes only. No API change. No
  behavior change beyond fixing the bug. Existing `~> 0.6.0`
  consumers pick it up automatically.
- **Minor (`0.6.0` → `0.7.0`)** — backwards-compatible additions
  (new functions, new DSL features, new options).
- **Major (`0.x` → `1.0`)** — only when leaving the 0.x train, or
  for any breaking change once stable.

Pre-1.0, breaking changes go in **minor** bumps; CHANGELOG should
flag them with **Breaking** prefixes (see the 0.5.0 entry for the
shape).

## The procedure

Assume the current version is `X.Y.Z` and the target is `X.Y.Z+1`
(adjust for minor/major).

### 1. Pre-flight on `main`

```bash
git checkout main
git pull
mix precommit              # format + credo + dialyzer + tests
pnpm typecheck             # JS workspace clean
pnpm test                  # if any JS changed
```

All green. No outstanding PRs you wanted in this release.

### 2. Branch

```bash
git checkout -b chore/bump-X.Y.Z+1
```

### 3. Bump `mix.exs`

```elixir
@version "X.Y.Z+1"
```

### 4. Finalize `CHANGELOG.md`

Replace:

```markdown
## [Unreleased]

### Fixed
- ...
```

with:

```markdown
## [Unreleased]

## [X.Y.Z+1] — YYYY-MM-DD

### Fixed
- ...
```

Leave the empty `## [Unreleased]` header in place so the next cycle
has somewhere to land entries.

### 5. Refresh CHANGELOG ref-links (bottom of file)

This step is easy to forget — the section header renders as a
broken link without it. Update the `[Unreleased]` target and add a
new `[X.Y.Z+1]` line:

```markdown
[Unreleased]: https://github.com/fahchen/musubi/compare/vX.Y.Z+1...HEAD
[X.Y.Z+1]: https://github.com/fahchen/musubi/compare/vX.Y.Z...vX.Y.Z+1
[X.Y.Z]: https://github.com/fahchen/musubi/compare/vX.Y.Z-1...vX.Y.Z
...
```

### 6. Refresh install snippets

Bump every `{:musubi, "~> X.Y.Z"}` you find — currently:

- `README.md` (Installation section)
- `guides/getting-started.md` (step 1)

`~> X.Y.0` resolves to `X.Y.Z+1` under Hex's SemVer rules, so this
is cosmetic for consumers but expected by repo convention.

### 7. Verify

```bash
mix precommit              # warnings-as-errors + format + credo + dialyzer + test
```

Quick sanity: search the repo for the OLD version string. Any
remaining references besides historic CHANGELOG entries are bugs:

```bash
rg -n 'X.Y.Z\b' -g '!CHANGELOG.md' -g '!_build' -g '!deps' -g '!node_modules'
```

### 8. Commit + PR

```bash
git add mix.exs CHANGELOG.md README.md guides/getting-started.md
git commit -m "chore(release): bump to X.Y.Z+1"
git push -u origin chore/bump-X.Y.Z+1
gh pr create --title "chore(release): bump to X.Y.Z+1" --body "..."
```

PR body should summarize what's in this release (link the PRs that
fed the CHANGELOG entries). The PR template is the standard one;
CI runs the usual matrix.

### 9. Merge

Squash merge into `main` once CI is green. The merge commit becomes
the release commit on `main`.

### 10. Cut the release (tags, publishes, and generates notes)

```bash
git checkout main
git pull
gh release create vX.Y.Z+1 --target main --title "vX.Y.Z+1" --generate-notes
```

The `git checkout main` / `git pull` just sync your local `main`; the
`gh release create` line is the actual release and does everything in
one step — no separate `git tag`, no hand-pasted notes:

- `gh release create` creates and pushes the `vX.Y.Z+1` tag at `main`.
- The tag push fires `.github/workflows/publish.yml`, which runs
  `mix hex.publish --yes` against the production Hex account (+ HexDocs).
- `--generate-notes` fills the release body automatically from the PRs
  merged since the previous release.

The maintained `CHANGELOG.md` (steps 4–5) stays the human-curated
record; the GitHub release notes are the auto-generated PR rollup.
They don't need to match.

### 11. Verify the publish

- Watch the GitHub Actions run for the tag.
- Confirm the new version on [hex.pm/packages/musubi](https://hex.pm/packages/musubi).
- Check that the [HexDocs](https://hexdocs.pm/musubi) site picked
  up the new version (HexDocs is built and uploaded by the same
  `mix hex.publish` run).

## Rollback

If a release is broken in a way that warrants withdrawal:

- `mix hex.retire musubi X.Y.Z+1 invalid --message "..."` keeps the
  package on Hex but flags it as retired. Existing pins keep
  working; new resolutions skip it.
- Cut a fresh patch (`X.Y.Z+2`) with the fix.
- Never delete the tag or force-push to `main`.

## What NOT to do

- Don't bump `mix.exs` without finalizing the CHANGELOG section and
  ref-links in the same PR — the next contributor will trip over the
  half-finished state.
- Don't point the release `--target` at anything but `main`.
- Don't hand-write release notes or split out a separate `git tag` +
  `git push` — `gh release create --generate-notes` does both in one
  step (see step 10).
- Don't manually run `mix hex.publish` from your laptop — the
  workflow has the credentials and the warnings-as-errors compile
  gate. Local publishes bypass the gate and confuse the audit trail.
- Don't bump the JS packages here. They follow their own (currently
  ad-hoc) cadence.
