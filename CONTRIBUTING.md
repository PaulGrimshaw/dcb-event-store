# Contributing

## Development

```bash
pnpm install
pnpm run build
pnpm run test
pnpm run lint
```

## Releasing

This project uses [Changesets](https://github.com/changesets/changesets) for version management and publishing.

### Adding a changeset

When you make a change that should be released, run:

```bash
pnpm changeset
```

This prompts you to select the affected packages and semver bump type, then creates a Markdown file in `.changeset/` describing the change. Commit this file with your PR.

### How releases work

1. PRs with changesets merge to `main`
2. The Release workflow opens (or updates) a **Version Packages** PR that aggregates pending changesets, bumps versions, and updates changelogs
3. Merging the Version Packages PR triggers `changeset publish`, which publishes updated packages to npm

### Manual release (fallback)

```bash
pnpm run version-packages   # apply changesets, bump versions, update changelogs
pnpm run release             # build, lint, test, and publish to npm
```

Requires `NPM_TOKEN` set in your environment or `npm login` to the `@dcb-es` scope.
