pub mod layout;

use std::collections::HashMap;

use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    Frame,
};
use unicode_width::UnicodeWidthStr;

use crate::{
    config::Config,
    db::{
        schema::Column,
        types::{affinity, ColAffinity, SqlValue},
    },
    theme::Theme,
};

#[allow(dead_code)]
pub struct GridState {
    pub table_name: String,
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<SqlValue>>,
    pub total_rows: i64,
    pub focused_row: usize,
    pub focused_col: usize,
    pub col_widths: Vec<u16>,
    pub h_scroll: usize,
    pub fk_cols: Vec<bool>,
    pub manual_widths: HashMap<usize, u16>,
}

impl GridState {
    pub fn new(
        table_name: String,
        columns: Vec<Column>,
        fk_cols: Vec<bool>,
        rows: Vec<Vec<SqlValue>>,
        total_rows: i64,
        area_width: u16,
    ) -> Self {
        let col_count = columns.len();
        let col_widths =
            layout::compute_col_widths(&columns, &rows, area_width, &HashMap::new(), &fk_cols);
        let fk_cols_safe = if fk_cols.len() == col_count {
            fk_cols
        } else {
            vec![false; col_count]
        };
        Self {
            table_name,
            columns,
            rows,
            total_rows,
            focused_row: 0,
            focused_col: 0,
            col_widths,
            h_scroll: 0,
            fk_cols: fk_cols_safe,
            manual_widths: HashMap::new(),
        }
    }
}

// ── rendering helpers ────────────────────────────────────────────────────────

fn digits(n: i64) -> usize {
    if n == 0 {
        1
    } else {
        n.unsigned_abs().to_string().len()
    }
}

fn format_thousands(n: i64) -> String {
    let abs_str = n.unsigned_abs().to_string();
    let sign = if n < 0 { "-" } else { "" };
    let chars: Vec<char> = abs_str.chars().collect();
    let len = chars.len();
    let mut result = String::with_capacity(len + len / 3 + 1);
    for (i, &c) in chars.iter().enumerate() {
        if i > 0 && (len - i).is_multiple_of(3) {
            result.push('\u{202F}');
        }
        result.push(c);
    }
    format!("{}{}", sign, result)
}

fn truncate_to_display_width(s: &str, max_w: usize) -> String {
    if max_w == 0 {
        return String::new();
    }
    let mut result = String::new();
    let mut cur_w = 0usize;
    for c in s.chars() {
        let cw = unicode_width::UnicodeWidthChar::width(c).unwrap_or(1);
        if cur_w + cw > max_w {
            break;
        }
        result.push(c);
        cur_w += cw;
    }
    result
}

fn col_badge(col: &Column) -> &'static str {
    let upper = col.col_type.to_uppercase();
    if upper.contains("DATETIME") || upper.contains("TIMESTAMP") {
        return "DT ";
    }
    if upper.contains("DATE") {
        return "DAT";
    }
    match affinity(&col.col_type) {
        ColAffinity::Integer => "INT",
        ColAffinity::Real => "REA",
        ColAffinity::Text => "TXT",
        ColAffinity::Blob => "BLB",
        ColAffinity::Numeric => "NUM",
    }
}

fn badge_color(col: &Column, theme: &Theme) -> Color {
    let upper = col.col_type.to_uppercase();
    if upper.contains("DATETIME") || upper.contains("TIMESTAMP") || upper.contains("DATE") {
        return theme.pink;
    }
    match affinity(&col.col_type) {
        ColAffinity::Integer => theme.yellow,
        ColAffinity::Real => theme.blue,
        ColAffinity::Text => theme.teal,
        ColAffinity::Blob => theme.purple,
        ColAffinity::Numeric => theme.yellow,
    }
}

#[derive(Clone, Copy)]
enum CellAlign {
    Left,
    Right,
    Center,
}

fn format_cell_content(val: &SqlValue, col: &Column, inner_w: usize) -> (String, CellAlign) {
    let col_upper = col.col_type.to_uppercase();
    match val {
        SqlValue::Null => (truncate_to_display_width("NULL", inner_w), CellAlign::Left),
        SqlValue::Integer(n) => {
            if col_upper.contains("BOOL") {
                let s = if *n != 0 { "✓" } else { "·" };
                (s.to_string(), CellAlign::Center)
            } else {
                (
                    truncate_to_display_width(&format_thousands(*n), inner_w),
                    CellAlign::Right,
                )
            }
        }
        SqlValue::Real(f) => (
            truncate_to_display_width(&format!("{:.6}", f), inner_w),
            CellAlign::Right,
        ),
        SqlValue::Text(t) => (truncate_to_display_width(t, inner_w), CellAlign::Left),
        SqlValue::Blob(b) => (
            truncate_to_display_width(&format!("<blob {} bytes>", b.len()), inner_w),
            CellAlign::Left,
        ),
    }
}

fn cell_val_style(val: &SqlValue, col: &Column, theme: &Theme, is_focused: bool) -> Style {
    if is_focused {
        return Style::default()
            .fg(theme.accent)
            .add_modifier(Modifier::BOLD);
    }
    let col_upper = col.col_type.to_uppercase();
    match val {
        SqlValue::Null => Style::default()
            .fg(theme.fg_faint)
            .add_modifier(Modifier::ITALIC),
        SqlValue::Blob(_) => Style::default()
            .fg(theme.purple)
            .add_modifier(Modifier::ITALIC),
        SqlValue::Integer(n) => {
            if col_upper.contains("BOOL") {
                if *n != 0 {
                    Style::default().fg(theme.green)
                } else {
                    Style::default().fg(theme.fg_faint)
                }
            } else {
                Style::default().fg(theme.fg)
            }
        }
        SqlValue::Real(_) => Style::default().fg(theme.blue),
        SqlValue::Text(_) => {
            if col_upper.contains("DATETIME")
                || col_upper.contains("TIMESTAMP")
                || col_upper.contains("DATE")
            {
                Style::default().fg(theme.pink)
            } else {
                Style::default().fg(theme.fg_dim)
            }
        }
    }
}

// ── sub-render functions ─────────────────────────────────────────────────────

fn render_header(
    buf: &mut ratatui::buffer::Buffer,
    area: Rect,
    gutter_width: u16,
    visible_cols: &[usize],
    state: &GridState,
    theme: &Theme,
) {
    let header_y = area.y;
    let header_style = Style::default().bg(theme.bg_raised);
    buf.set_style(
        Rect {
            x: area.x,
            y: header_y,
            width: area.width,
            height: 1,
        },
        header_style,
    );
    // gutter
    let gutter_str = " ".repeat(gutter_width as usize);
    buf.set_string(area.x, header_y, &gutter_str, header_style);

    let mut col_x = area.x + gutter_width;
    for &col_idx in visible_cols {
        if col_x >= area.x + area.width {
            break;
        }
        let col = &state.columns[col_idx];
        let cell_w = state.col_widths[col_idx];
        let actual_w = cell_w.min(area.x + area.width - col_x);

        let badge = col_badge(col);
        let bcolor = badge_color(col, theme);
        let is_pk = col.is_pk;
        let is_fk = state.fk_cols.get(col_idx).copied().unwrap_or(false);
        let pfx = if is_pk {
            "K "
        } else if is_fk {
            "F "
        } else {
            ""
        };

        // clear cell
        buf.set_string(
            col_x,
            header_y,
            " ".repeat(actual_w as usize),
            header_style,
        );

        let badge_len = UnicodeWidthStr::width(badge);
        let max_name_w = (actual_w as usize).saturating_sub(badge_len + 2);
        let name_raw = format!(" {}{}", pfx, col.name);
        let name_truncated = truncate_to_display_width(&name_raw, max_name_w);
        buf.set_string(
            col_x,
            header_y,
            &name_truncated,
            Style::default()
                .bg(theme.bg_raised)
                .fg(theme.fg)
                .add_modifier(Modifier::BOLD),
        );

        let badge_x = col_x + actual_w - badge_len as u16 - 1;
        if badge_x > col_x && badge_x < area.x + area.width {
            buf.set_string(
                badge_x,
                header_y,
                badge,
                Style::default()
                    .bg(theme.bg_raised)
                    .fg(bcolor)
                    .add_modifier(Modifier::DIM),
            );
        }

        col_x += cell_w;
    }
}

fn render_data_rows(
    buf: &mut ratatui::buffer::Buffer,
    area: Rect,
    gutter_width: u16,
    gutter_digits: usize,
    visible_cols: &[usize],
    state: &GridState,
    theme: &Theme,
) {
    for row_idx in 0..state.rows.len() {
        let row_y = area.y + 1 + row_idx as u16;
        if row_y >= area.y + area.height {
            break;
        }

        let is_focused = row_idx == state.focused_row;
        let row_bg = if is_focused {
            Color::Rgb(0x2a, 0x23, 0x20)
        } else if row_idx % 2 == 0 {
            theme.bg
        } else {
            theme.bg_soft
        };

        buf.set_style(
            Rect {
                x: area.x,
                y: row_y,
                width: area.width,
                height: 1,
            },
            Style::default().bg(row_bg),
        );

        // gutter
        let row_num_str = format!("{:>width$} ", row_idx + 1, width = gutter_digits);
        let gutter_fg = if is_focused {
            theme.accent
        } else {
            theme.fg_faint
        };
        buf.set_string(
            area.x,
            row_y,
            &row_num_str,
            Style::default().bg(row_bg).fg(gutter_fg),
        );

        let mut col_x = area.x + gutter_width;
        for &col_idx in visible_cols {
            if col_x >= area.x + area.width {
                break;
            }
            let col = &state.columns[col_idx];
            let cell_w = state.col_widths[col_idx];
            let actual_w = cell_w.min(area.x + area.width - col_x);
            let inner_w = (actual_w as usize).saturating_sub(2);

            let is_focused_cell = is_focused && col_idx == state.focused_col;

            if let Some(val) = state.rows[row_idx].get(col_idx) {
                let (content, align) = format_cell_content(val, col, inner_w);
                let style = cell_val_style(val, col, theme, is_focused_cell).bg(row_bg);
                let display_w = UnicodeWidthStr::width(content.as_str());
                let content_x = match align {
                    CellAlign::Left => col_x + 1,
                    CellAlign::Right => col_x + 1 + inner_w.saturating_sub(display_w) as u16,
                    CellAlign::Center => col_x + 1 + (inner_w.saturating_sub(display_w) / 2) as u16,
                };
                buf.set_string(content_x, row_y, &content, style);
            }

            col_x += cell_w;
        }
    }
}

fn render_focused_border(
    buf: &mut ratatui::buffer::Buffer,
    area: Rect,
    gutter_width: u16,
    visible_cols: &[usize],
    state: &GridState,
    theme: &Theme,
) {
    let focused_row = state.focused_row;
    let focused_col = state.focused_col;

    let vis_pos = match visible_cols.iter().position(|&c| c == focused_col) {
        Some(p) => p,
        None => return,
    };

    let cell_y = area.y + 1 + focused_row as u16;
    if cell_y >= area.y + area.height {
        return;
    }

    let mut cell_x = area.x + gutter_width;
    for &col_idx in &visible_cols[..vis_pos] {
        cell_x += state.col_widths[col_idx];
    }
    let cell_w = if focused_col < state.col_widths.len() {
        state.col_widths[focused_col]
    } else {
        return;
    };

    if cell_x >= area.x + area.width || cell_w < 2 {
        return;
    }

    let border_style = Style::default().fg(theme.accent);
    let right_x = cell_x + cell_w - 1;

    // top border
    if cell_y > area.y {
        let ty = cell_y - 1;
        buf.set_string(cell_x, ty, "┌", border_style);
        if cell_w > 2 {
            let mid = "─".repeat((cell_w - 2) as usize);
            buf.set_string(cell_x + 1, ty, &mid, border_style);
        }
        if right_x < area.x + area.width {
            buf.set_string(right_x, ty, "┐", border_style);
        }
    }

    // left/right sides
    buf.set_string(cell_x, cell_y, "│", border_style);
    if right_x < area.x + area.width {
        buf.set_string(right_x, cell_y, "│", border_style);
    }

    // bottom border
    let by = cell_y + 1;
    if by < area.y + area.height {
        buf.set_string(cell_x, by, "└", border_style);
        if cell_w > 2 {
            let mid = "─".repeat((cell_w - 2) as usize);
            buf.set_string(cell_x + 1, by, &mid, border_style);
        }
        if right_x < area.x + area.width {
            buf.set_string(right_x, by, "┘", border_style);
        }
    }
}

// ── public render entry point ────────────────────────────────────────────────

pub fn render_grid(
    frame: &mut Frame,
    area: Rect,
    state: &GridState,
    theme: &Theme,
    _config: &Config,
) {
    if area.width == 0 || area.height == 0 {
        return;
    }

    let gutter_digits = digits(state.total_rows.max(1));
    let gutter_width = (gutter_digits + 1) as u16;
    let data_width = area.width.saturating_sub(gutter_width).saturating_sub(1);

    // Determine visible columns
    let mut visible_cols: Vec<usize> = Vec::new();
    let mut cumul = 0u16;
    for col_idx in state.h_scroll..state.col_widths.len() {
        let w = state.col_widths[col_idx];
        if visible_cols.is_empty() || cumul + w <= data_width {
            visible_cols.push(col_idx);
            cumul += w;
        } else {
            break;
        }
    }

    let buf = frame.buffer_mut();
    buf.set_style(area, Style::default().bg(theme.bg));

    if area.height >= 1 {
        render_header(buf, area, gutter_width, &visible_cols, state, theme);
    }
    if area.height >= 2 {
        render_data_rows(
            buf,
            area,
            gutter_width,
            gutter_digits,
            &visible_cols,
            state,
            theme,
        );
        render_focused_border(buf, area, gutter_width, &visible_cols, state, theme);
    }
}
