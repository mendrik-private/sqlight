use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Layout, Rect},
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
    Frame,
};

use crate::{config::Config, db::types::SqlValue, theme::Theme};

pub struct FkPickerState {
    pub target_table: String,
    #[allow(dead_code)]
    pub target_col: String,
    pub display_cols: Vec<String>,
    pub rows: Vec<Vec<SqlValue>>,
    pub filter: String,
    pub selected: usize,
    pub source_table: String,
    pub source_col: String,
    pub source_rowid: i64,
    pub loading: bool,
    pub original: crate::db::types::SqlValue,
}

impl FkPickerState {
    pub fn new(
        target_table: String,
        target_col: String,
        display_cols: Vec<String>,
        source_table: String,
        source_col: String,
        source_rowid: i64,
        original: crate::db::types::SqlValue,
    ) -> Self {
        Self {
            target_table,
            target_col,
            display_cols,
            rows: Vec::new(),
            filter: String::new(),
            selected: 0,
            source_table,
            source_col,
            source_rowid,
            loading: true,
            original,
        }
    }

    pub fn move_up(&mut self) {
        self.selected = self.selected.saturating_sub(1);
    }

    pub fn move_down(&mut self) {
        let visible = self.visible_rows();
        if self.selected + 1 < visible.len() {
            self.selected += 1;
        }
    }

    pub fn push_filter_char(&mut self, ch: char) {
        self.filter.push(ch);
        self.selected = 0;
    }

    pub fn pop_filter_char(&mut self) {
        self.filter.pop();
        self.selected = 0;
    }

    pub fn selected_value(&self) -> Option<&SqlValue> {
        let hit = self.visible_rows().get(self.selected)?.row_index;
        self.rows.get(hit)?.first()
    }

    pub fn visible_rows(&self) -> Vec<RowHit> {
        if self.filter.is_empty() {
            return self
                .rows
                .iter()
                .enumerate()
                .map(|(row_index, _)| RowHit {
                    row_index,
                    matched_col: None,
                    match_range: None,
                })
                .collect();
        }
        let filter_lower = self.filter.to_lowercase();
        self.rows
            .iter()
            .enumerate()
            .filter_map(|(row_index, row)| {
                self.first_match(row, &filter_lower)
                    .map(|(matched_col, match_range)| RowHit {
                        row_index,
                        matched_col: Some(matched_col),
                        match_range: Some(match_range),
                    })
            })
            .collect()
    }

    fn first_match(&self, row: &[SqlValue], needle_lower: &str) -> Option<(usize, (usize, usize))> {
        for (idx, value) in row.iter().enumerate().skip(1) {
            let hay = value_to_string(value);
            let hay_lower = hay.to_lowercase();
            if let Some(start) = hay_lower.find(needle_lower) {
                return Some((idx, (start, start + needle_lower.len())));
            }
        }
        None
    }
}

pub struct RowHit {
    pub row_index: usize,
    pub matched_col: Option<usize>,
    pub match_range: Option<(usize, usize)>,
}

pub fn render(
    frame: &mut Frame,
    area: Rect,
    state: &FkPickerState,
    theme: &Theme,
    _config: &Config,
) {
    let popup_width = (area.width / 2).max(50).min(area.width);
    let popup_height = (area.height * 3 / 5).max(10).min(area.height);
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
        .border_style(Style::default().fg(theme.accent))
        .title(format!(
            " FK: {} → {} ",
            state.source_col, state.target_table
        ))
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

    let chunks = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(0),
        Constraint::Length(1),
    ])
    .split(inner);

    let filter_line = Line::from(vec![
        Span::styled(
            " 🔍 ",
            Style::default().fg(theme.fg_mute).bg(theme.bg_raised),
        ),
        Span::styled(
            state.filter.clone() + "▌",
            Style::default().fg(theme.fg).bg(theme.bg_raised),
        ),
    ]);
    frame.render_widget(
        Paragraph::new(filter_line).style(Style::default().bg(theme.bg_raised)),
        chunks[0],
    );

    let list_area = chunks[1];
    let visible_rows = state.visible_rows();
    let visible_count = list_area.height as usize;
    let start = if state.selected >= visible_count {
        state.selected - visible_count + 1
    } else {
        0
    };

    for (draw_idx, hit) in visible_rows
        .iter()
        .enumerate()
        .skip(start)
        .take(visible_count)
    {
        let row_y = list_area.y + (draw_idx - start) as u16;
        let is_sel = draw_idx == state.selected;
        let bg = if is_sel {
            theme.bg_soft
        } else {
            theme.bg_raised
        };
        let row = &state.rows[hit.row_index];
        let fg = if is_sel { theme.accent } else { theme.fg_dim };
        let key_val = match row.first() {
            Some(SqlValue::Integer(n)) => n.to_string(),
            Some(SqlValue::Text(s)) => s.clone(),
            _ => "–".to_string(),
        };
        let key_width = 10usize.min(list_area.width as usize);
        let preview_width = list_area.width as usize - key_width;
        let row_style = Style::default().fg(fg).bg(bg);
        let buf = frame.buffer_mut();
        buf.set_string(
            list_area.x,
            row_y,
            " ".repeat(list_area.width as usize),
            row_style,
        );
        buf.set_string(
            list_area.x,
            row_y,
            format!(" {:>width$}", key_val, width = key_width.saturating_sub(1)),
            row_style,
        );
        render_preview(
            buf,
            list_area.x + key_width as u16,
            row_y,
            preview_width,
            state,
            row,
            hit,
            theme,
            bg,
            is_sel,
        );
    }

    let footer = Line::from(Span::styled(
        " ↵ select · esc cancel",
        Style::default().fg(theme.fg_faint),
    ));
    frame.render_widget(
        Paragraph::new(footer).style(Style::default().bg(theme.bg_raised)),
        chunks[2],
    );
}

#[allow(clippy::too_many_arguments)]
fn render_preview(
    buf: &mut Buffer,
    x: u16,
    y: u16,
    width: usize,
    state: &FkPickerState,
    row: &[SqlValue],
    hit: &RowHit,
    theme: &Theme,
    bg: ratatui::style::Color,
    is_sel: bool,
) {
    if width == 0 {
        return;
    }

    let mut cursor = x;
    let base = Style::default()
        .fg(if is_sel { theme.fg } else { theme.fg_dim })
        .bg(bg);
    let label_style = Style::default().fg(theme.fg_mute).bg(bg);
    let hi_style = Style::default()
        .fg(theme.accent)
        .bg(bg)
        .add_modifier(Modifier::BOLD);

    if let (Some(col_idx), Some((start, end))) = (hit.matched_col, hit.match_range) {
        let col_name = state
            .display_cols
            .get(col_idx.saturating_sub(1))
            .cloned()
            .unwrap_or_else(|| state.target_col.clone());
        let label = format!(" {}: ", col_name);
        cursor = put(buf, cursor, y, x + width as u16, &label, label_style);
        let remaining = width.saturating_sub(label.chars().count());
        let text = value_to_string(&row[col_idx]);
        let snippet = centered_snippet(&text, start, end, remaining);
        cursor = put(buf, cursor, y, x + width as u16, &snippet.left, base);
        cursor = put(
            buf,
            cursor,
            y,
            x + width as u16,
            &snippet.match_text,
            hi_style,
        );
        let _ = put(buf, cursor, y, x + width as u16, &snippet.right, base);
    } else {
        let summary = row
            .iter()
            .skip(1)
            .take(3)
            .map(value_to_string)
            .collect::<Vec<_>>()
            .join(" · ");
        let preview: String = summary.chars().take(width.saturating_sub(1)).collect();
        let _ = put(
            buf,
            cursor,
            y,
            x + width as u16,
            &format!(" {}", preview),
            base,
        );
    }
}

fn put(buf: &mut Buffer, mut x: u16, y: u16, right: u16, text: &str, style: Style) -> u16 {
    for ch in text.chars() {
        if x >= right {
            break;
        }
        buf.set_string(x, y, ch.to_string(), style);
        x += 1;
    }
    x
}

fn value_to_string(value: &SqlValue) -> String {
    match value {
        SqlValue::Null => "null".to_string(),
        SqlValue::Integer(n) => n.to_string(),
        SqlValue::Real(f) => f.to_string(),
        SqlValue::Text(s) => s.clone(),
        SqlValue::Blob(bytes) => format!("<blob {} bytes>", bytes.len()),
    }
}

struct Snippet {
    left: String,
    match_text: String,
    right: String,
}

fn centered_snippet(text: &str, start: usize, end: usize, max_width: usize) -> Snippet {
    if max_width == 0 {
        return Snippet {
            left: String::new(),
            match_text: String::new(),
            right: String::new(),
        };
    }

    let chars: Vec<char> = text.chars().collect();
    if chars.len() <= max_width {
        return Snippet {
            left: chars[..start.min(chars.len())].iter().collect(),
            match_text: chars[start.min(chars.len())..end.min(chars.len())]
                .iter()
                .collect(),
            right: chars[end.min(chars.len())..].iter().collect(),
        };
    }

    let match_len = end.saturating_sub(start);
    let center = start + match_len / 2;
    let mut window_start = center.saturating_sub(max_width / 2);
    if window_start + max_width > chars.len() {
        window_start = chars.len().saturating_sub(max_width);
    }
    if start < window_start {
        window_start = start;
    }
    let mut window_end = (window_start + max_width).min(chars.len());
    if end > window_end {
        window_end = end.min(chars.len());
        window_start = window_end.saturating_sub(max_width);
    }

    let mut left: String = chars[window_start..start.min(chars.len())].iter().collect();
    let match_text: String = chars[start.min(chars.len())..end.min(chars.len())]
        .iter()
        .collect();
    let mut right: String = chars[end.min(chars.len())..window_end].iter().collect();

    if window_start > 0 && !left.is_empty() {
        left.remove(0);
        left.insert(0, '…');
    }
    if window_end < chars.len() && !right.is_empty() {
        right.pop();
        right.push('…');
    }

    Snippet {
        left,
        match_text,
        right,
    }
}
