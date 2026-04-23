# Architecture Decisions

## LIMIT/OFFSET vs Keyset Pagination

**Decision**: Use `LIMIT`/`OFFSET` for pagination.

**Rationale**: Keyset pagination requires a stable, ordered key for the "last seen" value. SQLite's `rowid` could serve this purpose, but it's not always monotonic (after deletes and re-inserts). Additionally, random access (jumping to row N or jumping to a letter in the alphabet rail) is natural with LIMIT/OFFSET but awkward with keyset. The performance of OFFSET on SQLite is acceptable for tables up to ~10M rows; beyond that, we would need a more sophisticated approach.

## r2d2 + rusqlite vs sqlx

**Decision**: Use `r2d2` connection pool with `rusqlite` instead of `sqlx`.

**Rationale**:
- `rusqlite` gives direct access to `ValueRef` for zero-copy type inspection, critical for efficient cell rendering.
- Custom SQLite functions (REGEXP) require `rusqlite`'s `create_scalar_function`, not available in `sqlx`.
- `rusqlite` supports `unchecked_transaction()` for fine-grained transaction control in write operations.
- `sqlx` would introduce async overhead for a local file database where latency is near-zero.
- `r2d2` provides a simple synchronous pool that works naturally with `tokio::task::spawn_blocking`.

## Column Sizing Algorithm

**Decision**: Three-case greedy algorithm with TEXT columns getting 2× weight.

**Rationale**:
- Case 1 (fits): Prefer content-driven widths for readability. Give TEXT extra slack because long strings benefit most from wider columns.
- Case 2 (doesn't fit but headers fit): Show at least column headers. Distribute remaining space weighted toward TEXT columns since they're most useful to read.
- Case 3 (headers don't fit): Everything at minimum; horizontal scrolling is required.
- The weight-2 for TEXT is a heuristic that feels natural — numeric columns rarely need extra width, while text columns benefit greatly from a few extra chars.
- Unicode grapheme width is measured with `unicode-width` to handle CJK and emoji correctly.

## No Animation Except the Loading Stripe

**Decision**: Only one animation (the loading stripe in the gutter when fetching rows).

**Rationale**:
- Animations in terminal apps are distracting for data-focused work.
- The loading stripe is the minimal signal needed to indicate background activity without being intrusive.
- The 30Hz render loop (33ms tick) provides sufficient responsiveness for smooth cursor navigation without wasting CPU on decorative animations.
- Slide-in/slide-out animations for toasts and popups would add complexity (interpolation state, timer management) for minimal UX benefit in a terminal.
