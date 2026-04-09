# Contributing

## Getting Started

1. Clone the repo and run `make install` to set up your environment and pre-commit hooks.
2. Copy `.env.example` to `.env` and fill in any required values.

## Branch Naming

Use the format: `type/short-description`

| Type | Use for |
|------|---------|
| `feat/` | New features |
| `fix/` | Bug fixes |
| `docs/` | Documentation changes |
| `chore/` | Maintenance, tooling, config |
| `test/` | Adding or fixing tests |

Examples: `feat/user-authentication`, `fix/null-pointer-on-startup`

## Pull Requests

- Keep PRs small and focused on one change.
- Fill out the PR template.
- At least one approval is required before merging.
- Squash merge is preferred to keep main history clean.

## Code Style

This project uses [ruff](https://docs.astral.sh/ruff/) for linting and formatting.

Run `make lint` to check, `make format` to auto-fix.

Pre-commit hooks run ruff automatically on every commit.

## Running Tests

```bash
make test
```

All tests must pass before a PR can be merged.
