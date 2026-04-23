use fuzzy_matcher::{skim::SkimMatcherV2, FuzzyMatcher};
use ratatui::{
    layout::Rect,
    style::Style,
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
    Frame,
};

use crate::{config::Config, db::types::SqlValue, theme::Theme};

#[allow(dead_code)]
pub struct ValuePickerState {
    pub table: String,
    pub rowid: i64,
    pub col_name: String,
    pub values: Vec<String>,
    pub filter: String,
    pub selected: usize,
    pub original: SqlValue,
}

impl ValuePickerState {
    #[allow(dead_code)]
    pub fn new(
        table: String,
        rowid: i64,
        col_name: String,
        values: Vec<String>,
        original: SqlValue,
    ) -> Self {
        Self {
            table,
            rowid,
            col_name,
            values,
            filter: String::new(),
            selected: 0,
            original,
        }
    }

    pub fn filtered_values(&self) -> Vec<(usize, &str, Vec<usize>)> {
        if self.filter.is_empty() {
            return self
                .values
                .iter()
                .enumerate()
                .map(|(i, v)| (i, v.as_str(), vec![]))
                .collect();
        }
        let matcher = SkimMatcherV2::default();
        let mut results: Vec<(usize, &str, i64, Vec<usize>)> = self
            .values
            .iter()
            .enumerate()
            .filter_map(|(i, v)| {
                matcher
                    .fuzzy_indices(v, &self.filter)
                    .map(|(score, indices)| (i, v.as_str(), score, indices))
            })
            .collect();
        results.sort_by(|a, b| b.2.cmp(&a.2));
        results
            .into_iter()
            .map(|(i, v, _, idx)| (i, v, idx))
            .collect()
    }

    pub fn move_up(&mut self) {
        self.selected = self.selected.saturating_sub(1);
    }

    pub fn move_down(&mut self) {
        let filtered = self.filtered_values();
        if self.selected + 1 < filtered.len() {
            self.selected += 1;
        }
    }

    pub fn selected_value(&self) -> Option<&str> {
        let filtered = self.filtered_values();
        filtered.get(self.selected).map(|(_, v, _)| *v)
    }

    pub fn push_filter_char(&mut self, ch: char) {
        self.filter.push(ch);
        self.selected = 0;
    }

    pub fn pop_filter_char(&mut self) {
        self.filter.pop();
        self.selected = 0;
    }
}

pub fn render(
    frame: &mut Frame,
    area: Rect,
    state: &ValuePickerState,
    theme: &Theme,
    _config: &Config,
) {
    let popup_width = (area.width / 2).max(30).min(area.width);
    let popup_height = 15u16.min(area.height);
    let x = area.x + (area.width.saturating_sub(popup_width)) / 2;
    let y = area.y + (area.height.saturating_sub(popup_height)) / 2;
    let popup_area = Rect {
        x,
        y,
        width: popup_width,
        height: popup_height,
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(theme.accent))
        .title(format!(" Pick: {} ", state.col_name))
        .style(Style::default().bg(theme.bg_raised));

    let inner = block.inner(popup_area);
    frame.render_widget(block, popup_area);

    let filter_line = Line::from(vec![
        Span::styled(" Filter: ", Style::default().fg(theme.fg_dim)),
        Span::styled(&state.filter, Style::default().fg(theme.fg)),
        Span::styled("▌", Style::default().fg(theme.accent)),
    ]);

    let filtered = state.filtered_values();
    let list_height = inner.height.saturating_sub(2) as usize;
    let start = if state.selected >= list_height {
        state.selected - list_height + 1
    } else {
        0
    };

    let mut lines = vec![filter_line, Line::from("")];
    for (rel_i, (_, val, matched)) in filtered.iter().skip(start).take(list_height).enumerate() {
        let abs_i = rel_i + start;
        let is_selected = abs_i == state.selected;
        let bg = if is_selected {
            theme.bg_soft
        } else {
            theme.bg_raised
        };

        let mut spans: Vec<Span> = Vec::new();
        if is_selected {
            spans.push(Span::styled(
                " ▶ ",
                Style::default().fg(theme.accent).bg(bg),
            ));
        } else {
            spans.push(Span::styled("   ", Style::default().bg(bg)));
        }
        for (ci, ch) in val.chars().enumerate() {
            let style = if matched.contains(&ci) {
                Style::default().fg(theme.accent).bg(bg)
            } else {
                Style::default().fg(theme.fg).bg(bg)
            };
            spans.push(Span::styled(ch.to_string(), style));
        }
        lines.push(Line::from(spans));
    }

    lines.push(Line::from(Span::styled(
        " ↵ select · esc cancel",
        Style::default().fg(theme.fg_faint),
    )));

    frame.render_widget(
        Paragraph::new(lines).style(Style::default().bg(theme.bg_raised)),
        inner,
    );
}
