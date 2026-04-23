pub mod alphabet_rail;
pub mod layout;
pub mod virtual_scroll;

use std::collections::HashMap;

use ratatui::{
    buffer::Buffer,
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

#[derive(Debug, Clone, PartialEq)]
pub enum SortDir {
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
pub struct SortSpec {
    pub col_idx: usize,
    pub direction: SortDir,
}

#[allow(dead_code)]
pub struct GridState {
    pub table_name: String,
    pub columns: Vec<Column>,
    pub window: virtual_scroll::VirtualWindow,
    pub focused_row: usize,
    pub focused_col: usize,
    pub col_widths: Vec<u16>,
    pub h_scroll: usize,
    pub fk_cols: Vec<bool>,
    pub manual_widths: HashMap<usize, u16>,
    pub needs_fetch: bool,
    pub viewport_start: i64,
    pub avail_col_width: u16,
    pub sort: Option<SortSpec>,
    pub filter: crate::filter::FilterSet,
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
            window: virtual_scroll::VirtualWindow::new(0, rows, total_rows),
            focused_row: 0,
            focused_col: 0,
            col_widths,
            h_scroll: 0,
            fk_cols: fk_cols_safe,
            manual_widths: HashMap::new(),
            needs_fetch: false,
            viewport_start: 0,
            avail_col_width: 80,
            sort: None,
            filter: crate::filter::FilterSet::default(),
        }
    }

    pub fn scroll_down(&mut self, n: usize) {
        let max_row = (self.window.total_rows - 1).max(0);
        let new_focused = (self.focused_row as i64 + n as i64).min(max_row);
        self.focused_row = new_focused as usize;
        self.adjust_viewport();
        self.check_needs_fetch();
    }

    pub fn scroll_up(&mut self, n: usize) {
        self.focused_row = self.focused_row.saturating_sub(n);
        self.adjust_viewport();
        self.check_needs_fetch();
    }

    pub fn scroll_to_row(&mut self, abs_row: i64) {
        let max_row = (self.window.total_rows - 1).max(0);
        self.focused_row = abs_row.clamp(0, max_row) as usize;
        self.adjust_viewport();
        self.check_needs_fetch();
    }

    pub fn scroll_to_end(&mut self) {
        let max_row = (self.window.total_rows - 1).max(0) as usize;
        self.focused_row = max_row;
        self.adjust_viewport();
        self.check_needs_fetch();
    }

    fn adjust_viewport(&mut self) {
        let vp = self.window.viewport_rows.max(1);
        let fr = self.focused_row as i64;
        let total = self.window.total_rows;

        if fr < self.viewport_start {
            self.viewport_start = fr;
        } else if fr >= self.viewport_start + vp as i64 {
            self.viewport_start = fr - vp as i64 + 1;
        }

        self.viewport_start = self.viewport_start.max(0);
        let max_start = (total - vp as i64).max(0);
        self.viewport_start = self.viewport_start.min(max_start);
    }

    fn check_needs_fetch(&mut self) {
        if !self.window.fetch_in_flight && self.window.needs_prefetch(self.focused_row as i64) {
            self.needs_fetch = true;
        }
    }

    pub fn move_col_right(&mut self) {
        if self.focused_col + 1 < self.columns.len() {
            self.focused_col += 1;
            self.adjust_h_scroll();
        }
    }

    pub fn move_col_left(&mut self) {
        if self.focused_col > 0 {
            self.focused_col -= 1;
            self.adjust_h_scroll();
        }
    }

    pub fn move_col_first(&mut self) {
        self.focused_col = 0;
        self.h_scroll = 0;
    }

    pub fn move_col_last(&mut self) {
        if !self.columns.is_empty() {
            self.focused_col = self.columns.len() - 1;
            self.adjust_h_scroll();
        }
    }

    pub fn focus_cell(&mut self, row: usize, col: usize) {
        if self.columns.is_empty() {
            self.focused_row = row.min(self.window.total_rows.saturating_sub(1) as usize);
            self.focused_col = 0;
            return;
        }
        self.focused_row = row.min(self.window.total_rows.saturating_sub(1) as usize);
        self.focused_col = col.min(self.columns.len() - 1);
        self.adjust_viewport();
        self.adjust_h_scroll();
        self.check_needs_fetch();
    }

    fn adjust_h_scroll(&mut self) {
        if self.focused_col < self.h_scroll {
            self.h_scroll = self.focused_col;
            return;
        }
        let avail = self.avail_col_width as usize;
        if avail == 0 {
            return;
        }
        let mut cumul = 0usize;
        let mut visible_end = self.h_scroll;
        for col_idx in self.h_scroll..self.col_widths.len() {
            let w = self.col_widths[col_idx] as usize;
            if cumul + w > avail {
                break;
            }
            cumul += w;
            visible_end = col_idx + 1;
        }
        while self.focused_col >= visible_end && self.h_scroll < self.focused_col {
            self.h_scroll += 1;
            cumul = 0;
            visible_end = self.h_scroll;
            for col_idx in self.h_scroll..self.col_widths.len() {
                let w = self.col_widths[col_idx] as usize;
                if cumul + w > avail {
                    break;
                }
                cumul += w;
                visible_end = col_idx + 1;
            }
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
    buf: &mut Buffer,
    area: Rect,
    gutter_width: u16,
    visible_cols: &[usize],
    state: &GridState,
    theme: &Theme,
) {
    let header_y = area.y;
    let header_style = Style::default().bg(theme.bg_raised);
    buf.set_string(
        area.x,
        header_y,
        " ".repeat(area.width as usize),
        header_style,
    );

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

        buf.set_string(col_x, header_y, " ".repeat(actual_w as usize), header_style);

        let sort_arrow: Option<&str> = if let Some(s) = &state.sort {
            if s.col_idx == col_idx {
                Some(if s.direction == SortDir::Asc {
                    "▲"
                } else {
                    "▼"
                })
            } else {
                None
            }
        } else {
            None
        };

        let filter_active = state
            .filter
            .columns
            .get(&col.name)
            .is_some_and(|cf| cf.rules.iter().any(|r| r.enabled));

        let arrow_reserve = if sort_arrow.is_some() { 2usize } else { 0usize };
        let filter_reserve = if filter_active { 2usize } else { 0usize };
        let badge_len = UnicodeWidthStr::width(badge);
        let max_name_w =
            (actual_w as usize).saturating_sub(badge_len + 2 + arrow_reserve + filter_reserve);
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

        if let Some(arrow) = sort_arrow {
            let arrow_x = badge_x.saturating_sub(2);
            if arrow_x > col_x && arrow_x < area.x + area.width {
                buf.set_string(
                    arrow_x,
                    header_y,
                    arrow,
                    Style::default().fg(theme.accent).bg(theme.bg_raised),
                );
            }
        }

        if filter_active {
            let filter_x = badge_x.saturating_sub((arrow_reserve + 2) as u16);
            if filter_x > col_x && filter_x < area.x + area.width {
                buf.set_string(
                    filter_x,
                    header_y,
                    "F",
                    Style::default().fg(theme.accent).bg(theme.bg_raised),
                );
            }
        }

        col_x += cell_w;
    }

    if state.h_scroll + visible_cols.len() < state.col_widths.len() {
        let chevron_reserve = if alphabet_rail::should_show_rail(state) {
            alphabet_rail::RAIL_WIDTH + 1
        } else {
            1
        };
        let chevron_x = area.x + area.width.saturating_sub(chevron_reserve + 1);
        if chevron_x >= area.x + gutter_width && chevron_x < area.x + area.width {
            buf.set_string(
                chevron_x,
                header_y,
                "▸",
                Style::default().fg(theme.fg_mute).bg(theme.bg_raised),
            );
        }
    }
}

fn render_data_rows(
    buf: &mut Buffer,
    area: Rect,
    gutter_width: u16,
    gutter_digits: usize,
    visible_cols: &[usize],
    state: &GridState,
    theme: &Theme,
) {
    let viewport_rows = state.window.viewport_rows;
    for row_in_view in 0..viewport_rows {
        let abs_row = state.viewport_start + row_in_view as i64;
        if abs_row >= state.window.total_rows {
            break;
        }
        let row_y = area.y + 1 + row_in_view as u16;
        if row_y >= area.y + area.height {
            break;
        }

        let is_focused = abs_row == state.focused_row as i64;
        let row_bg = if is_focused {
            Color::Rgb(0x2a, 0x23, 0x20)
        } else if abs_row % 2 == 0 {
            theme.bg
        } else {
            theme.bg_soft
        };

        buf.set_string(
            area.x,
            row_y,
            " ".repeat(area.width as usize),
            Style::default().bg(row_bg),
        );

        let row_num_str = format!("{:>width$} ", abs_row + 1, width = gutter_digits);
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

        if let Some(row_data) = state.window.get_row(abs_row) {
            for &col_idx in visible_cols {
                if col_x >= area.x + area.width {
                    break;
                }
                let col = &state.columns[col_idx];
                let cell_w = state.col_widths[col_idx];
                let actual_w = cell_w.min(area.x + area.width - col_x);
                let inner_w = (actual_w as usize).saturating_sub(2);

                let is_focused_cell = is_focused && col_idx == state.focused_col;
                if actual_w > 0 {
                    buf.set_string(
                        col_x,
                        row_y,
                        " ".repeat(actual_w as usize),
                        Style::default().bg(row_bg),
                    );
                }

                if let Some(val) = row_data.get(col_idx) {
                    let (content, align) = format_cell_content(val, col, inner_w);
                    let style = cell_val_style(val, col, theme, is_focused_cell).bg(row_bg);
                    let display_w = UnicodeWidthStr::width(content.as_str());
                    let content_x = match align {
                        CellAlign::Left => col_x + 1,
                        CellAlign::Right => col_x + 1 + inner_w.saturating_sub(display_w) as u16,
                        CellAlign::Center => {
                            col_x + 1 + (inner_w.saturating_sub(display_w) / 2) as u16
                        }
                    };
                    buf.set_string(content_x, row_y, &content, style);
                }

                col_x += cell_w;
            }
        } else {
            buf.set_string(
                area.x + gutter_width,
                row_y,
                "…",
                Style::default().fg(theme.fg_faint).bg(row_bg),
            );
        }
    }
}

fn render_focused_border(
    buf: &mut Buffer,
    area: Rect,
    gutter_width: u16,
    visible_cols: &[usize],
    state: &GridState,
    theme: &Theme,
) {
    let focused_row_in_view = state.focused_row as i64 - state.viewport_start;
    if focused_row_in_view < 0 || focused_row_in_view >= state.window.viewport_rows as i64 {
        return;
    }
    let focused_row_in_view = focused_row_in_view as usize;
    let focused_col = state.focused_col;

    let vis_pos = match visible_cols.iter().position(|&c| c == focused_col) {
        Some(p) => p,
        None => return,
    };

    let cell_y = area.y + 1 + focused_row_in_view as u16;
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

    buf.set_string(cell_x, cell_y, "│", border_style);
    if right_x < area.x + area.width {
        buf.set_string(right_x, cell_y, "│", border_style);
    }

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

fn render_vertical_scrollbar(buf: &mut Buffer, area: Rect, state: &GridState, theme: &Theme) {
    let total = state.window.total_rows;
    if total <= 0 {
        return;
    }
    let track_height = area.height.saturating_sub(1) as i64;
    if track_height <= 0 {
        return;
    }
    let track_x = area.x + area.width - 1;
    let track_y_start = area.y + 1;

    for ty in 0..track_height {
        buf.set_string(
            track_x,
            track_y_start + ty as u16,
            "│",
            Style::default().fg(theme.line_soft),
        );
    }

    let thumb_height = ((state.window.viewport_rows as i64 * track_height) / total.max(1))
        .max(1)
        .min(track_height);
    let thumb_offset = if total > 1 {
        (state.focused_row as i64 * (track_height - thumb_height)) / (total - 1)
    } else {
        0
    };
    let max_thumb_offset = (track_height - thumb_height).max(0);
    let thumb_offset = thumb_offset.clamp(0, max_thumb_offset);

    for ty in 0..thumb_height {
        let ty_abs = track_y_start + (thumb_offset + ty) as u16;
        if ty_abs < area.y + area.height {
            buf.set_string(track_x, ty_abs, "█", Style::default().fg(theme.fg_dim));
        }
    }
}

fn render_loading_indicator(buf: &mut Buffer, area: Rect, state: &GridState, theme: &Theme) {
    if !state.window.fetch_in_flight {
        return;
    }
    let steps = area.height.saturating_sub(2) as u64;
    if steps == 0 {
        return;
    }
    let pos = (state.window.tick_count / 15) % steps;
    let y = area.y + 1 + pos as u16;
    if y < area.y + area.height {
        buf.set_string(area.x, y, "●", Style::default().fg(theme.accent));
    }
}

// ── public render entry point ────────────────────────────────────────────────

pub fn render_grid(
    frame: &mut Frame,
    area: Rect,
    state: &mut GridState,
    theme: &Theme,
    _config: &Config,
) {
    if area.width == 0 || area.height == 0 {
        return;
    }

    let viewport_rows = area.height.saturating_sub(1) as usize;
    state.window.viewport_rows = viewport_rows;

    let gutter_digits = digits(state.window.total_rows.max(1));
    let gutter_width = (gutter_digits + 1) as u16;
    let rail_width = if alphabet_rail::should_show_rail(state) {
        alphabet_rail::RAIL_WIDTH
    } else {
        0
    };
    // reserve right-side space for scrollbar and optional alphabet rail
    let data_width = area
        .width
        .saturating_sub(gutter_width)
        .saturating_sub(1 + rail_width);
    state.avail_col_width = data_width;

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

    if state.window.total_rows == 0 && area.height >= 2 {
        buf.set_string(
            area.x + gutter_width,
            area.y + 1,
            "Empty table",
            Style::default().fg(theme.fg_faint).bg(theme.bg),
        );
        return;
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
        render_vertical_scrollbar(buf, area, state, theme);
        render_loading_indicator(buf, area, state, theme);
    }
    let _ = buf;
    alphabet_rail::render_rail(frame, area, state, theme);
}

pub fn hit_test(area: Rect, state: &GridState, x: u16, y: u16) -> Option<GridHit> {
    if area.width == 0
        || area.height == 0
        || x < area.x
        || x >= area.x + area.width
        || y < area.y
        || y >= area.y + area.height
    {
        return None;
    }

    let gutter_digits = digits(state.window.total_rows.max(1));
    let gutter_width = (gutter_digits + 1) as u16;
    let rail_width = if alphabet_rail::should_show_rail(state) {
        alphabet_rail::RAIL_WIDTH
    } else {
        0
    };
    let data_width = area
        .width
        .saturating_sub(gutter_width)
        .saturating_sub(1 + rail_width);

    if x >= area.x + area.width.saturating_sub(1) {
        return Some(GridHit::Scrollbar);
    }

    if y == area.y {
        let col = hit_test_col(area, state, x, gutter_width, data_width)?;
        return Some(GridHit::Header(col));
    }

    if x < area.x + gutter_width {
        let row = hit_test_row(area, state, y)?;
        return Some(GridHit::RowGutter(row));
    }

    let row = hit_test_row(area, state, y)?;
    let col = hit_test_col(area, state, x, gutter_width, data_width)?;
    Some(GridHit::Cell { row, col })
}

pub enum GridHit {
    Header(usize),
    RowGutter(usize),
    Cell { row: usize, col: usize },
    Scrollbar,
}

fn hit_test_row(area: Rect, state: &GridState, y: u16) -> Option<usize> {
    if y <= area.y {
        return None;
    }
    let row_in_view = (y - area.y - 1) as i64;
    let abs_row = state.viewport_start + row_in_view;
    if abs_row < 0 || abs_row >= state.window.total_rows {
        None
    } else {
        Some(abs_row as usize)
    }
}

fn hit_test_col(
    area: Rect,
    state: &GridState,
    x: u16,
    gutter_width: u16,
    data_width: u16,
) -> Option<usize> {
    let mut col_x = area.x + gutter_width;
    let mut cumul = 0u16;
    for col_idx in state.h_scroll..state.col_widths.len() {
        let w = state.col_widths[col_idx];
        if !(cumul == 0 || cumul + w <= data_width) {
            break;
        }
        let col_end = col_x.saturating_add(w);
        if x >= col_x && x < col_end {
            return Some(col_idx);
        }
        cumul += w;
        col_x = col_end;
    }
    None
}
