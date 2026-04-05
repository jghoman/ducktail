# Ducktail — Tail DuckLake tables via CDC

# Default recipe: list all available recipes
default:
    @just --list

# === Dev ===

# Install dependencies
[group('dev')]
sync:
    uv sync

# Format code
[group('dev')]
fmt:
    uv run ruff format src/ tests/

# Check formatting
[group('dev')]
fmt-check:
    uv run ruff format --check src/ tests/

# Lint code
[group('dev')]
lint:
    uv run ruff check src/ tests/

# Lint and fix
[group('dev')]
lint-fix:
    uv run ruff check --fix src/ tests/

# Type check with mypy
[group('dev')]
typecheck:
    uv run mypy src/

# Type check with pyright
[group('dev')]
typecheck-pyright:
    uv run pyright src/

# === Test ===

# Run unit tests (excludes integration)
[group('test')]
test:
    uv run python -m pytest tests/ --ignore=tests/integration

# Run integration tests (requires Docker)
[group('test')]
test-integration:
    uv run python -m pytest tests/integration -m integration -v

# Run all tests
[group('test')]
test-all: test test-integration

# Full CI check
[group('test')]
ci: fmt-check lint typecheck test

# === Build ===

# Build wheel and sdist
[group('build')]
build:
    uv build

# Build wheel only
[group('build')]
wheel:
    uv build --wheel

# Clean build artifacts
[group('build')]
clean:
    rm -rf .venv dist *.egg-info __pycache__ src/ducktail/__pycache__

# === Docs ===

# Serve documentation locally
[group('docs')]
docs:
    uv run mkdocs serve

# Build static documentation site
[group('docs')]
docs-build:
    uv run mkdocs build

# === Demo ===

demo_pg := "postgres:dbname=ducklake host=localhost port=5433 user=ducklake password=ducklake"
demo_data := justfile_directory() / "demo" / "data"

# Start the demo Postgres metadata backend
[group('demo')]
demo-up:
    docker compose up -d
    @echo "Postgres ready on localhost:5433"

# Stop the demo stack and clean up data
[group('demo')]
demo-down:
    docker compose down -v
    rm -rf demo/data

# Run the demo producer (inserts, updates, deletes)
[group('demo')]
demo-producer:
    uv run python demo/producer.py

# Tail the demo table in pager mode
[group('demo')]
demo-tail:
    uv run ducktail tail events \
        -C "{{demo_pg}}" \
        -c demo \
        -d "{{demo_data}}"

# Tail the demo table in interactive mode
[group('demo')]
demo-tail-interactive:
    uv run ducktail tail events \
        -C "{{demo_pg}}" \
        -c demo \
        -d "{{demo_data}}" \
        -m interactive
