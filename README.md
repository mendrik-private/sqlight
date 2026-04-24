# sqv — terminal SQLite viewer

A fast, keyboard-first TUI SQLite browser written in Rust. Warm rose-pine-moon aesthetic, virtual scrolling over million-row tables, rich cell editing, foreign-key navigation, sorting, filtering, and export.

![sqv screenshot showing filtered tracks table with alphabet rail](.github/screenshot.png)

## Install

```bash
cargo install --path .
```

Or run directly:

```bash
cargo run --release -- path/to/database.db
```

## Usage

```
sqv <DB_PATH> [--readonly]
sqv check-terminal
```

- `DB_PATH` — path to a SQLite file, or `:memory:` for an empty in-memory DB
- `--readonly` — open in read-only mode (disables all writes)
- `check-terminal` — print detected terminal capabilities

## Keybindings

### Navigation

| Key | Action |
|-----|--------|
| `↑ ↓ ← →` / `h j k l` | Move focused cell |
| `Home` / `End` | First / last column in row |
| `Ctrl-Home` / `Ctrl-End` | First / last cell in table |
| `PgUp` / `PgDn` / `Ctrl-↑` / `Ctrl-↓` | Scroll one viewport |
| `Mouse wheel` | Scroll 3 rows |
| `Shift-wheel` | Scroll 3 columns |
| `Click cell` | Focus cell |
| `Click row number` | Select row |

### Sidebar & Tabs

| Key | Action |
|-----|--------|
| `Ctrl-b` | Toggle sidebar |
| `Tab` | Cycle focus: sidebar ↔ grid |
| `Enter` (sidebar) | Open table in new tab |
| `Ctrl-Tab` / `Ctrl-Shift-Tab` | Next / previous tab |
| `x` / middle-click tab | Close tab |

### Sorting & Filtering

| Key | Action |
|-----|--------|
| `s` | Cycle sort on focused column: none → ↑ → ↓ |
| `Click column header` | Cycle sort |
| `f` | Open filter popup for focused column |
| `Shift-F` | Open filter popup (all columns) |
| `Ctrl-f` | Filter to rows like focused cell value |
| `Letter key` (TEXT sort active) | Jump to first row starting with that letter |

### Editing

| Key | Action |
|-----|--------|
| `Enter` | Edit focused cell (opens appropriate popup) |
| `Esc` | Cancel / close popup |
| `Ctrl-Enter` | Commit edit (TEXT multiline) |
| `i` | Insert new row |
| `d` | Delete focused row (confirm with `y`) |
| `Ctrl-z` | Undo last write |

### Foreign Keys

| Key | Action |
|-----|--------|
| `j` | Jump to referenced row in target table |
| `Enter` (FK cell) | Open FK picker popup |
| `Backspace` | Pop jump stack (navigate back) |

### Other

| Key | Action |
|-----|--------|
| `Ctrl-Shift-P` | Command palette |
| `Right-click cell` | Context menu |
| `Ctrl-Q` | Quit |

## config.toml

Location: `$XDG_CONFIG_HOME/sqv/config.toml` (typically `~/.config/sqv/config.toml`)

```toml
# Use Nerd Font icons (table/view/index glyphs, PK/FK markers)
# Set false if your terminal font lacks Nerd Font glyphs
nerd_font = true
```

## Column-sizing algorithm

Width computation runs on every viewport change, filter change, or sort change:

1. **Content width**: sample up to 200 rows in the current virtual window. Width = widest grapheme-measured cell, capped by type (TEXT→80, INT/REAL→20, DATE→10, DATETIME→19, BLOB→18, BOOL→1). Add 2 padding.
2. **Header width**: column name + 3-char type badge + optional PK/FK glyph + 2 padding. Column minimum = `max(header_width, 6)`.
3. Let `avail = viewport_cols - gutter_width - 1 (scrollbar)`.
4. **Fits easily** (`total_content ≤ avail`): TEXT columns grow to content width first, then remaining slack distributed proportionally. Non-TEXT stay at content width.
5. **Needs shrinking** (`total_content > avail` but `total_min ≤ avail`): start every column at minimum, distribute remaining space proportionally with TEXT columns getting **2× weight** — this keeps text readable while numerics shrink to minimum.
6. **Overflows** (`total_min > avail`): every column at minimum, enable horizontal scrollbar.

Columns have hysteresis: recompute only when new width differs by ≥ 3 cells to avoid jitter during vertical scrolling. User-resized columns (`Ctrl-←/→` or drag) are pinned until `=` resets them.

## Export

From the command palette (`Ctrl-Shift-P`):

- **Export CSV** → `~/sqv_export.csv`
- **Export JSON** → `~/sqv_export.json`
- **Export SQL** → `~/sqv_export.sql` (INSERT statements)

All exports respect the current filter and sort.

## Filter persistence

Filters are saved per `(database_path, table_name)` at:

```
$XDG_STATE_HOME/sqv/filters/<db_basename>/<table_name>.toml
```

They survive across sessions and are loaded automatically when you open a table.
