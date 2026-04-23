use ratatui::{
    layout::{Constraint, Layout, Rect},
    style::Style,
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
        if !self.rows.is_empty() && self.selected + 1 < self.rows.len() {
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
        self.rows.get(self.selected)?.first()
    }

    pub fn row_display(&self, row: &[SqlValue]) -> String {
        row.iter()
            .skip(1)
            .take(self.display_cols.len())
            .map(|v| match v {
                SqlValue::Text(s) => s.clone(),
                SqlValue::Integer(n) => n.to_string(),
                SqlValue::Real(f) => f.to_string(),
                _ => "–".to_string(),
            })
            .collect::<Vec<_>>()
            .join(" · ")
    }

    pub fn visible_rows(&self) -> Vec<(usize, &Vec<SqlValue>)> {
        if self.filter.is_empty() {
            return self.rows.iter().enumerate().collect();
        }
        let filter_lower = self.filter.to_lowercase();
        self.rows
            .iter()
            .enumerate()
            .filter(|(_, row)| {
                row.iter().any(|v| match v {
                    SqlValue::Text(s) => s.to_lowercase().contains(&filter_lower),
                    SqlValue::Integer(n) => n.to_string().contains(&filter_lower),
                    _ => false,
                })
            })
            .collect()
    }
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

    for (draw_idx, (orig_idx, row)) in visible_rows
        .iter()
        .enumerate()
        .skip(start)
        .take(visible_count)
    {
        let row_y = list_area.y + (draw_idx - start) as u16;
        let is_sel = *orig_idx == state.selected;
        let bg = if is_sel {
            theme.bg_soft
        } else {
            theme.bg_raised
        };
        let fg = if is_sel { theme.accent } else { theme.fg_dim };
        let key_val = match row.first() {
            Some(SqlValue::Integer(n)) => n.to_string(),
            Some(SqlValue::Text(s)) => s.clone(),
            _ => "–".to_string(),
        };
        let disp = state.row_display(row);
        let line = if disp.is_empty() {
            format!(" {:>6} ", key_val)
        } else {
            format!(" {:>6}  {} ", key_val, disp)
        };
        let truncated: String = line.chars().take(list_area.width as usize).collect();
        let row_style = Style::default().fg(fg).bg(bg);
        frame
            .buffer_mut()
            .set_string(list_area.x, row_y, &truncated, row_style);
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
