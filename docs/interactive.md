# Interactive Mode

Interactive mode provides a full-screen terminal UI with color-coded changes,
powered by [Rich](https://github.com/Textualize/rich).

## Launch

```bash
ducktail tail events -C meta.duckdb -m interactive
```

## Display

The screen clears on startup and shows a table with three columns:

| Column | Content |
|--------|---------|
| **Type** | `+` (green), `-` (red), or `Δ` (yellow) |
| **Timestamp** | UTC time when the change was received |
| **Details** | Column values (inserts/deletes) or `column: old → new` (updates) |

New changes appear at the bottom. Once the screen is full, older entries scroll
off the top — the display always shows the most recent changes.

## Controls

- **Ctrl+C** — exit cleanly

## Options that work with interactive mode

All standard options apply:

```bash
# Show only specific columns
ducktail tail events -C meta.duckdb -m interactive -col user_id -col status

# Server-side filter
ducktail tail events -C meta.duckdb -m interactive -f "event_type = 'purchase'"

# Faster polling
ducktail tail events -C meta.duckdb -m interactive -i 0.5
```

## When to use which mode

| | Pager | Interactive |
|---|---|---|
| Piping to other tools | Yes | No |
| Logging to a file | Yes | No |
| Watching live in a terminal | Works | Better |
| Color-coded by change type | No | Yes |
| Timestamps per change | No | Yes |
| Scrollback in terminal | Yes | No (rolling window) |
