use ratatui::{
    layout::Rect,
    style::Style,
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
    Frame,
};

use crate::{config::Config, db::types::SqlValue, theme::Theme};

#[allow(dead_code)]
pub struct TextEditorState {
    pub table: String,
    pub rowid: i64,
    pub col_name: String,
    pub col_type: String,
    pub original: SqlValue,
    pub current: String,
    pub cursor_pos: usize,
    pub is_multiline: bool,
    pub valid: bool,
    pub readonly: bool,
}

impl TextEditorState {
    pub fn new(
        table: String,
        rowid: i64,
        col_name: String,
        col_type: String,
        original: SqlValue,
        readonly: bool,
    ) -> Self {
        let current = match &original {
            SqlValue::Null => String::new(),
            SqlValue::Integer(n) => n.to_string(),
            SqlValue::Real(f) => f.to_string(),
            SqlValue::Text(s) => s.clone(),
            SqlValue::Blob(_) => String::new(),
        };
        let upper = col_type.to_uppercase();
        let is_multiline = upper.contains("TEXT") || upper.contains("CLOB");
        let cursor_pos = current.chars().count();
        Self {
            table,
            rowid,
            col_name,
            col_type,
            original,
            current,
            cursor_pos,
            is_multiline,
            valid: true,
            readonly,
        }
    }

    pub fn insert_char(&mut self, ch: char) {
        if self.readonly {
            return;
        }
        let byte_pos = self
            .current
            .char_indices()
            .nth(self.cursor_pos)
            .map_or(self.current.len(), |(i, _)| i);
        self.current.insert(byte_pos, ch);
        self.cursor_pos += 1;
        self.validate();
    }

    pub fn delete_backward(&mut self) {
        if self.readonly || self.cursor_pos == 0 {
            return;
        }
        let byte_pos = self
            .current
            .char_indices()
            .nth(self.cursor_pos - 1)
            .map(|(i, _)| i)
            .unwrap_or(0);
        let end_pos = self
            .current
            .char_indices()
            .nth(self.cursor_pos)
            .map_or(self.current.len(), |(i, _)| i);
        self.current.replace_range(byte_pos..end_pos, "");
        self.cursor_pos -= 1;
        self.validate();
    }

    pub fn move_cursor_left(&mut self) {
        self.cursor_pos = self.cursor_pos.saturating_sub(1);
    }

    pub fn move_cursor_right(&mut self) {
        let len = self.current.chars().count();
        if self.cursor_pos < len {
            self.cursor_pos += 1;
        }
    }

    fn validate(&mut self) {
        let upper = self.col_type.to_uppercase();
        if upper.contains("INT") {
            self.valid = self.current.is_empty() || self.current.parse::<i64>().is_ok();
        } else if upper.contains("REAL") || upper.contains("FLOAT") || upper.contains("DOUBLE") {
            self.valid = self.current.is_empty() || self.current.parse::<f64>().is_ok();
        } else {
            self.valid = true;
        }
    }

    pub fn as_sql_value(&self) -> SqlValue {
        if self.current.is_empty() {
            return SqlValue::Null;
        }
        let upper = self.col_type.to_uppercase();
        if upper.contains("INT") {
            self.current
                .parse::<i64>()
                .ok()
                .map(SqlValue::Integer)
                .unwrap_or(SqlValue::Null)
        } else if upper.contains("REAL") || upper.contains("FLOAT") || upper.contains("DOUBLE") {
            self.current
                .parse::<f64>()
                .ok()
                .map(SqlValue::Real)
                .unwrap_or(SqlValue::Null)
        } else {
            SqlValue::Text(self.current.clone())
        }
    }
}

pub fn render(
    frame: &mut Frame,
    area: Rect,
    state: &TextEditorState,
    theme: &Theme,
    _config: &Config,
) {
    let popup_width = (area.width * 6 / 10).max(40).min(area.width);
    let popup_height = if state.is_multiline { 10u16 } else { 5u16 };
    let x = area.x + (area.width.saturating_sub(popup_width)) / 2;
    let y = area.y + area.height / 3;
    let popup_area = Rect {
        x,
        y,
        width: popup_width,
        height: popup_height.min(area.height),
    };

    super::paint_popup_surface(frame, popup_area, theme);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(theme.accent))
        .title(format!(" Edit: {} ", state.col_name))
        .style(Style::default().bg(theme.bg_raised));

    let inner = block.inner(popup_area);
    frame.render_widget(block, popup_area);

    let before: String = state.current.chars().take(state.cursor_pos).collect();
    let after: String = state.current.chars().skip(state.cursor_pos).collect();
    let display = format!("{}▌{}", before, after);

    let input_style = Style::default().fg(theme.fg).bg(theme.bg_raised);
    let hint_style = Style::default().fg(theme.fg_faint);

    let upper = state.col_type.to_uppercase();
    let show_validity = upper.contains("INT")
        || upper.contains("REAL")
        || upper.contains("FLOAT")
        || upper.contains("DOUBLE");

    let mut lines = vec![Line::from(Span::styled(
        format!(" {} ", display),
        input_style,
    ))];

    if state.readonly {
        lines.push(Line::from(Span::styled(
            " ⊘ read-only",
            Style::default().fg(theme.red),
        )));
    }
    lines.push(Line::from(Span::styled(
        " Enter commit · Esc cancel",
        hint_style,
    )));
    if show_validity {
        let vi = if state.valid { "✓" } else { "✗" };
        let vc = if state.valid { theme.green } else { theme.red };
        lines.insert(
            0,
            Line::from(Span::styled(
                format!(" {} ", vi),
                Style::default().fg(vc).bg(theme.bg_raised),
            )),
        );
    }

    frame.render_widget(
        Paragraph::new(lines).style(Style::default().bg(theme.bg_raised)),
        inner,
    );
}
