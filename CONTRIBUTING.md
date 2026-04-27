# Contributing to sqv

Thanks for your interest in contributing to `sqv`.

`sqv` is a keyboard-first terminal viewer for SQLite databases. Contributions are welcome, including bug reports, documentation improvements, small fixes, and feature ideas.

## Getting started

1. Fork the repository.
2. Clone your fork:

   ```bash
   git clone https://github.com/YOUR_USERNAME/sqv.git
   cd sqv
   ```

3. Create a new branch:

   ```bash
   git checkout -b your-change-name
   ```

4. Build or run the project:

   ```bash
   cargo run --release -- path/to/database.db
   ```

## Development checks

Before opening a pull request, please run:

```bash
cargo fmt
cargo clippy -- -D warnings
cargo test
```

Please make sure your changes pass these checks.

## Reporting bugs

When reporting a bug, include:

- What you were trying to do
- What happened
- What you expected to happen
- Your operating system and terminal
- The `sqv` version or commit you are using
- A small example database or reproduction steps, if possible

## Suggesting features

Feature requests are welcome. Please describe:

- The problem you want to solve
- The behavior you would like
- Any alternatives you considered

## Pull requests

When opening a pull request:

- Keep the change focused
- Explain what changed and why
- Add or update tests when relevant
- Update documentation if user-facing behavior changes

## Code style

Follow the existing Rust style in the project.

Use:

```bash
cargo fmt
```

to format code before submitting.

## License

By contributing to this project, you agree that your contributions will be licensed under the same license as the project.
