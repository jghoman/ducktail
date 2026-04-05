# ducktail — Claude Code Instructions

## Development Principles
- **TDD**: Write tests first. All new code must have corresponding tests.
- **Static typing**: Use type annotations everywhere. Code must pass mypy strict mode and pyright strict mode.

## Tooling
- **uv** for dependency management (`uv sync`, `uv run`)
- **just** for task running (`just test`, `just lint`, `just fmt`, `just typecheck`)
- **flox** for environment management
- **ruff** for linting and formatting
- **mypy** + **pyright** for type checking
- **pytest** for testing

## Workflow
- Run `just ci` for the full check suite (format, lint, typecheck, test).
- Use agents for code review when making substantial changes.

## Post-Code Checks (REQUIRED)
After any code task, run ALL of these and fix any issues before reporting done:
1. `uv run ruff check src/ tests/` — must be 0 errors
2. `uv run ruff format --check src/ tests/` — must pass
3. `uv run mypy src/` — must be 0 errors
4. `uv run python -m pytest tests/ --ignore=tests/integration -q --tb=short` — all tests must pass

If any check fails, fix the issues before returning results. Do not report success with lint or type errors outstanding.

## Demo
- `just demo-up` — start Postgres metadata backend (Docker)
- `just demo-producer` — run the demo producer (inserts, updates, deletes)
- `just demo-tail` — tail in pager mode
- `just demo-tail-interactive` — tail in interactive mode
- `just demo-down` — stop Postgres and clean data
