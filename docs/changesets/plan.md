# Plan: Adopt Changesets for Release Management (#28)

## Classification: Medium

Multiple files across root config, CI workflow, and docs. Single concern (release tooling), no domain logic changes.

## Context

Two publishable packages: `@dcb-es/event-store` and `@dcb-es/event-store-postgres`, both at `7.0.0-alpha.2`. The postgres adapter depends on the core package (`^7.0.0-alpha.2`). Three private packages (`event-store-bench`, two examples) are excluded from publishing.

Current release process is a manual `publish-all` script in root `package.json` that sequentially cleans, builds, lints, tests, and publishes both packages with `--tag alpha`. No changelogs, no automated versioning.

## Design Decisions

### 1. Fixed vs independent versioning

**Decision: fixed versioning** (all publishable packages share one version).

The two packages are tightly coupled — `event-store-postgres` implements the interfaces defined by `event-store` and takes a caret range on it. Every breaking change in core is a breaking change in postgres. Independent versioning would create version matrix confusion for a two-package monorepo with a single maintainer.

Changesets calls this `"fixed"` groups — all packages in a group receive the same version bump when any member is bumped.

**Alternative considered:** Independent versioning. Rejected because the packages don't have independent consumers — you always need core + an adapter, and they evolve in lockstep.

### 2. Pre-release management

Both packages are currently on `7.0.0-alpha.2` (pre-release). Changesets supports pre-release mode via `npx changeset pre enter alpha`, which prefixes all version bumps with the pre-release tag.

**Decision:** Enter pre-release mode (`alpha`) immediately after init. This preserves the existing `alpha` tag convention and lets normal changesets flow produce versions like `7.0.0-alpha.3`, `7.0.0-alpha.4`, etc. When ready for GA, exit pre-release mode (`npx changeset pre exit`) and the next version will be `7.0.0`.

**Alternative considered:** Skip pre-release mode, manually set versions. Rejected because it breaks the changeset version calculation and requires manual intervention on every release.

### 3. Scope: Changesets only, no publish automation

Issue #41 covers automated npm publishing via GitHub Actions. This issue (#28) adopts the Changesets tooling and workflow only. The `release.yml` workflow is out of scope — it will be added under #41 using the Changesets infrastructure set up here.

What **is** in scope: the Changesets config, the `version` script, and a `release.yml` that opens a "Version Packages" PR (the standard Changesets bot behaviour). Actual npm publish automation belongs to #41.

### 4. Access config

Packages are scoped (`@dcb-es/*`) and public. Changesets config needs `"access": "public"` to pass `--access public` during publish.

## Implementation Steps

### Step 1 — Install and initialise Changesets

- `pnpm add -Dw @changesets/cli`
- `pnpm changeset init` (creates `.changeset/` directory with `config.json` and `README.md`)

### Step 2 — Configure `.changeset/config.json`

```json
{
  "$schema": "https://unpkg.com/@changesets/config@3.1.1/schema.json",
  "changelog": "@changesets/cli/changelog",
  "commit": false,
  "fixed": [["@dcb-es/event-store", "@dcb-es/event-store-postgres"]],
  "linked": [],
  "access": "public",
  "baseBranch": "main",
  "updateInternalDependencies": "patch",
  "ignore": [
    "@dcb-es/event-store-bench",
    "@dcb-es/examples-course-manager-cli",
    "@dcb-es/examples-course-manager-cli-with-readmodel"
  ],
  "privatePackages": { "version": false, "tag": false }
}
```

Key settings:
- **`fixed`**: Both publishable packages always get the same version
- **`ignore`**: Private packages excluded from versioning
- **`privatePackages`**: Don't version or tag private packages
- **`commit: false`**: Don't auto-commit version bumps — the Version Packages PR handles this
- **`updateInternalDependencies: "patch"`**: When core bumps, auto-update postgres's dependency range

### Step 3 — Enter pre-release mode

```bash
npx changeset pre enter alpha
```

This creates `.changeset/pre.json` tracking pre-release state. All subsequent `changeset version` runs produce alpha-tagged versions.

### Step 4 — Update root package.json scripts

Replace the manual `publish-all` script:

```json
{
  "scripts": {
    "changeset": "changeset",
    "version-packages": "changeset version",
    "release": "pnpm run clean && pnpm run build && changeset publish"
  }
}
```

Remove the old `publish-all` script.

### Step 5 — Add `release.yml` GitHub Action

A workflow that runs on push to `main` and either:
- Opens/updates a "Version Packages" PR aggregating pending changesets, OR
- Does nothing if no changesets are pending

This uses `changesets/action@v1` with `version: pnpm run version-packages` and **no** `publish` command (publish automation is #41).

```yaml
name: Release

on:
  push:
    branches: [main]

concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: false    # don't cancel mid-version

permissions:
  contents: write
  pull-requests: write

jobs:
  release:
    name: Version Packages
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: 22
      - run: corepack enable pnpm
      - run: pnpm install --frozen-lockfile

      - uses: changesets/action@v1
        with:
          title: "chore: version packages"
          commit: "chore: version packages"
          version: pnpm run version-packages
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

### Step 6 — Clean up

- Remove `publish-all` from root `package.json` scripts
- Verify `.changeset/` directory is committed (not gitignored)

## Out of Scope

- **Automated npm publish** (#41) — `release.yml` will be extended there to add the `publish` command
- **CONTRIBUTING.md** (#29) — release process docs belong there
- **Changesets bot / GitHub App** — not needed; `changesets/action` handles the PR workflow

## Test Strategy

No domain logic changes — this is purely tooling/config. Verification:

1. Run `pnpm changeset` and confirm it prompts for a package selection (only the two publishable packages)
2. Run `pnpm changeset version` and confirm it bumps both packages to the same alpha version
3. Run `pnpm run build && pnpm run lint && pnpm run test` to confirm nothing is broken
4. Verify `.changeset/config.json` is valid by running `pnpm changeset status`
5. After push, verify the `release.yml` workflow runs and the changesets action behaves correctly (opens a PR or reports no changesets)
