# sqv

**sqv** is a keyboard-first terminal viewer for SQLite databases. It is built for fast table browsing, in-place editing, filtering, sorting, and foreign-key navigation without leaving the terminal.

<img width="2880" height="1555" alt="image" src="https://github.com/user-attachments/assets/38ea5b40-a57e-4c49-b3bd-19033f8c7a48" />
<br/>
<img width="2880" height="1555" alt="image" src="https://github.com/user-attachments/assets/32771472-7b33-424b-9138-516edb54c427" />


## Highlights

- Fast virtual scrolling for large tables
- Rich cell editors for text, enums, dates, datetimes, and foreign keys
- Per-column filters, sorting, and alphabet jump navigation
- Export current views to CSV, JSON, or SQL
- Read-only mode for safe inspection

## Install

### apt (GitHub Pages repository)

The project publishes an `amd64` Debian package to a GitHub-hosted apt repository.

```bash
echo "deb [trusted=yes arch=amd64] https://mendrik-private.github.io/sqv stable main" \
  | sudo tee /etc/apt/sources.list.d/sqv.list
sudo apt update
sudo apt install sqv
```

`trusted=yes` is required because the repository is published from GitHub Pages without apt signing.

### Release binary

Download the latest Linux binary archive from GitHub Releases and install it into `/usr/local/bin`:

```bash
curl -fsSL -o sqv-linux-x86_64.tar.gz \
  https://github.com/mendrik-private/sqv/releases/latest/download/sqv-linux-x86_64.tar.gz
tar -xzf sqv-linux-x86_64.tar.gz
sudo install -m 0755 sqv /usr/local/bin/sqv
```

### From source

```bash
cargo install --path .
```

Or run directly from the checkout:

```bash
cargo run --release -- path/to/database.db
```

## Usage

```text
sqv <DB_PATH> [--readonly]
sqv check-terminal
```

- `DB_PATH`: path to a SQLite database, or `:memory:`
- `--readonly`: disable writes
- `check-terminal`: print detected terminal capabilities

## Keybindings

### Navigation

| Key | Action |
|-----|--------|
| `↑ ↓ ← →` / `h j k l` | Move focused cell |
| `Home` / `End` | First / last column in row |
| `Ctrl-Home` / `Ctrl-End` | First / last cell in table |
| `PgUp` / `PgDn` / `Ctrl-↑` / `Ctrl-↓` | Scroll one viewport |
| `Mouse wheel` | Scroll rows |
| `Shift-wheel` | Scroll columns |
| `Click cell` | Focus cell |

### Editing

| Key | Action |
|-----|--------|
| `Enter` | Open the focused cell picker/editor |
| `Alt-Enter` | Insert a newline in the direct text editor |
| `e` | Open the focused cell in the direct editor |
| `n` | Set the focused cell to `NULL` when allowed |
| `Esc` | Close popup |
| `Ctrl-Enter` | Save an inserted row from the staged row editor |
| `i` | Insert row in a staged editor, then validate on save |
| `d` | Delete row |
| `Ctrl-z` | Undo last write |

### Filtering, sorting, and other actions

| Key | Action |
|-----|--------|
| `s` | Cycle sort on focused column |
| `f` | Open filter popup for focused column |
| `Shift-F` | Clear filters |
| `j` | Jump through a foreign key |
| `Backspace` | Jump back |
| `Ctrl-b` | Toggle sidebar |
| `Ctrl-Shift-P` | Command palette |
| `Ctrl-Q` | Quit |

## Configuration

Configuration is read from:

```text
$XDG_CONFIG_HOME/sqv/config.toml
```

Example:

```toml
nerd_font = true
```

## Development

### Local validation

```bash
cargo fmt
cargo clippy -- -D warnings
cargo test
```

### Release pipeline

GitHub Actions provides:

1. **CI** on pushes and pull requests: format check, clippy, and tests
2. **Tagged releases** on `v*` tags:
    - build the release binary
    - build a Debian package
    - publish GitHub Release assets
    - publish an apt repository to GitHub Pages
3. **Manual releases** from the Actions tab:
    - use the selected branch's `Cargo.toml` version as the release tag
    - create or update the matching GitHub Release for that commit

To cut a release:

```bash
git tag v0.1.3
git push origin v0.1.3
```

Before the first tagged release, enable **GitHub Pages** for the repository so the apt repository can be deployed from GitHub Actions. Tagged releases now wait for a successful Pages deployment before publishing release assets.
