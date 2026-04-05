# Demo

The repo includes a self-contained demo: a Postgres metadata backend and a
producer that generates a continuous stream of inserts, deletes, and updates.

## Prerequisites

- Docker (for Postgres)
- [flox](https://flox.dev/) (or just, uv, and Python 3.12 installed manually)

## Run it

### 1. Start the infrastructure

```bash
just demo-up
```

This starts a Postgres 17 container on `localhost:5433` with a `ducklake`
database. Data files are stored locally in `demo/data/`.

### 2. Start the producer

```bash
just demo-producer
```

The producer creates an `events` table and generates batches of 5 rows every 3
seconds. It randomly mixes in deletes (~30% chance per cycle) and updates (~40%
chance per cycle):

```
[14:30:01] +5 inserts  (total=5)
[14:30:07] +5 inserts  (total=10)
[14:30:10] -1 delete   id=6
[14:30:13] +5 inserts  (total=15)
[14:30:16] Δ1 update   id=3 status=warning amount=858.07
```

### 3. Tail the table

In a separate terminal:

```bash
# Pager mode — plain text stream
just demo-tail

# Interactive mode — full-screen TUI
just demo-tail-interactive
```

### 4. Clean up

```bash
just demo-down
```

This stops Postgres and removes `demo/data/`.

## Table schema

The demo `events` table has the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `id` | `int` | Auto-incrementing sequence |
| `event_id` | `string` | UUID |
| `event_type` | `string` | `pageview`, `click`, `purchase`, `signup`, `logout`, `error` |
| `user_id` | `string` | `user_0000` through `user_0049` |
| `page` | `string` | URL path |
| `browser` | `string` | `Chrome`, `Firefox`, `Safari`, `Edge` |
| `status` | `string` | `ok`, `warning`, `error` |
| `amount` | `double` | 0.0, or random value up to 500.0 (~30% of rows) |
| `ts` | `timestamptz` | Event timestamp (UTC) |

## Customizing the producer

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `DUCKLAKE_META` | `postgres:dbname=ducklake host=localhost port=5433 ...` | Metadata connection string |
| `DUCKLAKE_DATA_PATH` | `demo/data` (absolute path) | Data file location |
