use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{block::BorderType, Block, Borders, Paragraph},
    Frame,
};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use crate::{
    config::Config,
    db::{schema::Column, types::SqlValue},
    theme::Theme,
};

pub struct FindState {
    pub query: String,
    pub table_name: String,
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<SqlValue>>,
    pub selected: usize,
    pub loading: bool,
}

pub struct FindHit {
    pub abs_row_index: usize,
    pub first_match_col: usize,
    pub matched_ranges: Vec<Option<(usize, usize)>>,
}

#[derive(Clone, Copy)]
struct HighlightStyles {
    base: Style,
    matched: Style,
}

impl FindState {
    pub fn new(table_name: String, columns: Vec<Column>) -> Self {
        Self {
            query: String::new(),
            table_name,
            columns,
            rows: Vec::new(),
            selected: 0,
            loading: true,
        }
    }

    pub fn push_char(&mut self, ch: char) {
        self.query.push(ch);
        self.selected = 0;
    }

    pub fn pop_char(&mut self) {
        self.query.pop();
        self.selected = 0;
    }

    pub fn move_up(&mut self) {
        self.selected = self.selected.saturating_sub(1);
    }

    pub fn move_down(&mut self) {
        let count = self.hit_count();
        if self.selected + 1 < count {
            self.selected += 1;
        }
    }

    fn hit_count(&self) -> usize {
        if self.query.trim().is_empty() {
            self.rows.len()
        } else {
            self.visible_hits().len()
        }
    }

    pub fn visible_hits(&self) -> Vec<FindHit> {
        if self.query.trim().is_empty() {
            return self
                .rows
                .iter()
                .enumerate()
                .map(|(i, row)| FindHit {
                    abs_row_index: i,
                    first_match_col: 0,
                    matched_ranges: vec![None; row.len()],
                })
                .collect();
        }

        let needle = self.query.trim().to_lowercase();
        let needle_chars = needle.chars().count();
        self.rows
            .iter()
            .enumerate()
            .filter_map(|(i, row)| {
                let mut found = false;
                let mut first_match_col = 0;
                let mut matched_ranges = Vec::with_capacity(row.len());
                for (col_idx, val) in row.iter().enumerate() {
                    let display = val_display(val);
                    let lower = display.to_lowercase();
                    let range = lower.find(&needle).map(|byte_start| {
                        let start = lower[..byte_start].chars().count();
                        if !found {
                            found = true;
                            first_match_col = col_idx;
                        }
                        (start, start + needle_chars)
                    });
                    matched_ranges.push(range);
                }
                found.then_some(FindHit {
                    abs_row_index: i,
                    first_match_col,
                    matched_ranges,
                })
            })
            .collect()
    }
}

fn val_display(val: &SqlValue) -> String {
    match val {
        SqlValue::Null => String::new(),
        SqlValue::Integer(n) => n.to_string(),
        SqlValue::Real(f) => f.to_string(),
        SqlValue::Text(s) => s.clone(),
        SqlValue::Blob(b) => format!("<blob {} bytes>", b.len()),
    }
}

pub fn render(frame: &mut Frame, area: Rect, state: &FindState, theme: &Theme, _config: &Config) {
    let popup_width = ((area.width * 4) / 5).max(60).min(area.width);
    let popup_height = ((area.height * 3) / 5).max(12).min(area.height);
    let x = area.x + (area.width.saturating_sub(popup_width)) / 2;
    let y = area.y + (area.height.saturating_sub(popup_height)) / 2;
    let popup_area = Rect {
        x,
        y,
        width: popup_width,
        height: popup_height,
    };

    super::paint_popup_surface(frame, popup_area, theme);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(theme.accent))
        .title(format!(" Find in {} ", state.table_name))
        .style(Style::default().bg(theme.bg_raised));
    let inner = block.inner(popup_area);
    frame.render_widget(block, popup_area);

    if state.loading {
        frame.render_widget(
            Paragraph::new(" Loading…")
                .style(Style::default().fg(theme.fg_dim).bg(theme.bg_raised)),
            inner,
        );
        return;
    }

    if inner.height < 3 || inner.width == 0 {
        return;
    }

    let input_area = Rect {
        x: inner.x,
        y: inner.y,
        width: inner.width,
        height: 1,
    };
    let footer_y = inner.y + inner.height.saturating_sub(1);
    let footer_area = Rect {
        x: inner.x,
        y: footer_y,
        width: inner.width,
        height: 1,
    };
    let list_top = inner.y + 1;
    let list_height = footer_y.saturating_sub(list_top) as usize;

    // Search input
    let input_line = Line::from(vec![
        Span::styled(" Search: ", Style::default().fg(theme.fg_dim)),
        Span::styled(
            format!(" {} ", state.query),
            Style::default().fg(theme.fg).bg(theme.bg_soft),
        ),
        Span::styled("▌", Style::default().fg(theme.accent).bg(theme.bg_soft)),
    ]);
    frame.render_widget(
        Paragraph::new(input_line).style(Style::default().bg(theme.bg_raised)),
        input_area,
    );

    let hits = state.visible_hits();
    let match_count = hits.len();
    let total = state.rows.len();

    let scroll_start = if list_height > 0 && state.selected >= list_height {
        state.selected - list_height + 1
    } else {
        0
    };

    let list_area = Rect {
        x: inner.x,
        y: list_top,
        width: inner.width,
        height: footer_y.saturating_sub(list_top),
    };

    if list_area.height > 0 {
        if hits.is_empty() && !state.query.trim().is_empty() {
            let buf = frame.buffer_mut();
            for row_y in list_area.y..list_area.y + list_area.height {
                buf.set_string(
                    list_area.x,
                    row_y,
                    " ".repeat(list_area.width as usize),
                    Style::default().bg(theme.bg_raised),
                );
            }
            buf.set_string(
                list_area.x + 1,
                list_area.y,
                "No matches",
                Style::default().fg(theme.fg_faint).bg(theme.bg_raised),
            );
        } else {
            render_hit_list(
                frame.buffer_mut(),
                list_area,
                state,
                &hits,
                scroll_start,
                list_height,
                theme,
            );
        }
    }

    // Footer
    let count_text = if state.query.trim().is_empty() {
        format!(" {} rows", total)
    } else {
        format!(" {} of {} matched", match_count, total)
    };
    let footer_line = Line::from(vec![
        Span::styled(count_text, Style::default().fg(theme.fg_mute)),
        Span::styled(
            "  ↵ go · ↑↓ select · esc cancel",
            Style::default().fg(theme.fg_faint),
        ),
    ]);
    frame.render_widget(
        Paragraph::new(footer_line).style(Style::default().bg(theme.bg_raised)),
        footer_area,
    );
}

const ROW_NUM_W: u16 = 6;

fn render_hit_list(
    buf: &mut Buffer,
    area: Rect,
    state: &FindState,
    hits: &[FindHit],
    scroll_start: usize,
    list_height: usize,
    theme: &Theme,
) {
    let available_w = area.width.saturating_sub(ROW_NUM_W) as usize;

    // Fill background for all rows first
    for row_y in area.y..area.y + area.height {
        buf.set_string(
            area.x,
            row_y,
            " ".repeat(area.width as usize),
            Style::default().bg(theme.bg_raised),
        );
    }

    for (view_idx, hit) in hits.iter().enumerate().skip(scroll_start).take(list_height) {
        let row_y = area.y + (view_idx - scroll_start) as u16;
        let is_selected = view_idx == state.selected;
        let bg = if is_selected {
            theme.bg_soft
        } else {
            theme.bg_raised
        };
        let base_style = Style::default().bg(bg);

        // Fill row background
        buf.set_string(area.x, row_y, " ".repeat(area.width as usize), base_style);

        // Row number gutter
        if is_selected {
            buf.set_string(area.x, row_y, "▸", Style::default().fg(theme.accent).bg(bg));
            let num_str = format!("{:>4} ", hit.abs_row_index + 1);
            buf.set_string(
                area.x + 1,
                row_y,
                &num_str,
                Style::default().fg(theme.fg_mute).bg(bg),
            );
        } else {
            let num_str = format!("{:>5} ", hit.abs_row_index + 1);
            buf.set_string(
                area.x,
                row_y,
                &num_str,
                Style::default().fg(theme.fg_mute).bg(bg),
            );
        }

        if available_w == 0 {
            continue;
        }

        let row_vals = &state.rows[hit.abs_row_index];
        let mut x_cursor = area.x + ROW_NUM_W;
        let mut used_w = 0usize;
        let mut first_col = true;

        for (col_idx, (col, val)) in state.columns.iter().zip(row_vals.iter()).enumerate() {
            let display = val_display(val);
            if display.is_empty() {
                continue;
            }

            let sep = if first_col { "" } else { " · " };
            let prefix = format!("{}{}: ", sep, col.name);
            let prefix_w = prefix.width();

            if used_w + prefix_w >= available_w {
                if used_w < available_w {
                    buf.set_string(
                        x_cursor,
                        row_y,
                        "…",
                        Style::default().fg(theme.fg_faint).bg(bg),
                    );
                }
                break;
            }

            buf.set_string(
                x_cursor,
                row_y,
                &prefix,
                Style::default()
                    .fg(if first_col {
                        theme.fg_dim
                    } else {
                        theme.fg_faint
                    })
                    .bg(bg),
            );
            x_cursor += prefix_w as u16;
            used_w += prefix_w;

            let val_remaining = available_w.saturating_sub(used_w);
            let matched = hit.matched_ranges.get(col_idx).copied().flatten();
            let base_fg = if is_selected { theme.fg } else { theme.fg_dim };
            let drawn_w = draw_truncated_text(
                buf,
                x_cursor,
                row_y,
                val_remaining,
                &display,
                matched,
                HighlightStyles {
                    base: Style::default().fg(base_fg).bg(bg),
                    matched: Style::default()
                        .fg(theme.accent)
                        .bg(bg)
                        .add_modifier(Modifier::BOLD),
                },
            );

            x_cursor += drawn_w as u16;
            used_w += drawn_w;
            first_col = false;

            if used_w >= available_w {
                break;
            }
        }
    }
}

fn draw_truncated_text(
    buf: &mut Buffer,
    x: u16,
    y: u16,
    max_width: usize,
    value: &str,
    matched_range: Option<(usize, usize)>,
    styles: HighlightStyles,
) -> usize {
    if max_width == 0 {
        return 0;
    }

    let chars: Vec<(usize, char)> = value.chars().enumerate().collect();
    let total_width: usize = chars
        .iter()
        .map(|(_, ch)| UnicodeWidthChar::width(*ch).unwrap_or(1))
        .sum();
    let needs_ellipsis = total_width > max_width;
    let content_limit = if needs_ellipsis && max_width > 3 {
        max_width - 3
    } else {
        max_width
    };

    let mut cursor = x;
    let mut used = 0usize;

    for (idx, ch) in &chars {
        let ch_w = UnicodeWidthChar::width(*ch).unwrap_or(1);
        if used + ch_w > content_limit {
            break;
        }
        let style = if matched_range.is_some_and(|(start, end)| *idx >= start && *idx < end) {
            styles.matched
        } else {
            styles.base
        };
        buf.set_string(cursor, y, ch.to_string(), style);
        cursor += ch_w as u16;
        used += ch_w;
    }

    if needs_ellipsis && max_width > 0 {
        buf.set_string(cursor, y, "...", styles.base);
        used += 3;
    }

    used.min(max_width)
}
