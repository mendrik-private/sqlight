use ratatui::{
    layout::Rect,
    style::{Modifier, Style},
    widgets::{block::BorderType, Block, Borders},
    Frame,
};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use crate::{
    config::Config,
    db::types::SqlValue,
    filter::{rule::FilterRule, FilterOp, FilterValue},
    theme::Theme,
};

pub struct FilterPopupState {
    pub col_name: String,
    pub col_type: String,
    pub col_filter: crate::filter::ColumnFilter,
    pub selected_rule: usize,
    pub draft_op: crate::filter::FilterOp,
    pub draft_value: String,
    pub draft_cursor_pos: usize,
    pub focus: FilterPopupFocus,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum FilterPopupFocus {
    Needle,
    Delete,
}

impl FilterPopupState {
    pub fn new(
        col_name: String,
        col_type: String,
        col_filter: crate::filter::ColumnFilter,
    ) -> Self {
        let draft_op = col_filter
            .rules
            .last()
            .map(|rule| rule.op)
            .unwrap_or(crate::filter::FilterOp::Contains);
        Self {
            col_name,
            col_type,
            col_filter,
            selected_rule: 0,
            draft_op,
            draft_value: String::new(),
            draft_cursor_pos: 0,
            focus: FilterPopupFocus::Needle,
        }
    }

    pub fn next_op(&mut self) {
        let ops = popup_ops();
        let idx = ops.iter().position(|op| *op == self.draft_op).unwrap_or(0);
        self.draft_op = ops[(idx + 1) % ops.len()];
    }

    pub fn prev_op(&mut self) {
        let ops = popup_ops();
        let idx = ops.iter().position(|op| *op == self.draft_op).unwrap_or(0);
        self.draft_op = ops[(idx + ops.len() - 1) % ops.len()];
    }

    pub fn select_prev_rule(&mut self) {
        self.selected_rule = self.selected_rule.saturating_sub(1);
    }

    pub fn select_next_rule(&mut self) {
        if self.selected_rule + 1 < self.col_filter.rules.len() {
            self.selected_rule += 1;
        }
    }

    pub fn move_cursor_left(&mut self) {
        self.draft_cursor_pos = self.draft_cursor_pos.saturating_sub(1);
    }

    pub fn move_cursor_right(&mut self) {
        let len = self.draft_value.chars().count();
        if self.draft_cursor_pos < len {
            self.draft_cursor_pos += 1;
        }
    }

    pub fn push_char(&mut self, ch: char) {
        let byte_pos = self
            .draft_value
            .char_indices()
            .nth(self.draft_cursor_pos)
            .map_or(self.draft_value.len(), |(i, _)| i);
        self.draft_value.insert(byte_pos, ch);
        self.draft_cursor_pos += 1;
    }

    pub fn pop_char(&mut self) {
        if self.draft_cursor_pos == 0 {
            return;
        }
        let byte_pos = self
            .draft_value
            .char_indices()
            .nth(self.draft_cursor_pos - 1)
            .map(|(i, _)| i)
            .unwrap_or(0);
        let end_pos = self
            .draft_value
            .char_indices()
            .nth(self.draft_cursor_pos)
            .map_or(self.draft_value.len(), |(i, _)| i);
        self.draft_value.replace_range(byte_pos..end_pos, "");
        self.draft_cursor_pos -= 1;
    }

    pub fn toggle_focus(&mut self) {
        self.focus = match self.focus {
            FilterPopupFocus::Needle if !self.col_filter.rules.is_empty() => {
                FilterPopupFocus::Delete
            }
            _ => FilterPopupFocus::Needle,
        };
    }

    pub fn delete_selected_rule(&mut self) -> bool {
        if self.selected_rule < self.col_filter.rules.len() {
            self.col_filter.rules.remove(self.selected_rule);
            self.clamp_selection();
            if self.col_filter.rules.is_empty() {
                self.focus = FilterPopupFocus::Needle;
            }
            return true;
        }
        false
    }

    pub fn add_rule(&mut self) -> Result<(), String> {
        let rule = self.build_rule()?;
        self.col_filter.rules.push(rule);
        self.selected_rule = self.col_filter.rules.len().saturating_sub(1);
        self.draft_value.clear();
        self.draft_cursor_pos = 0;
        Ok(())
    }

    pub fn display_draft_value(&self) -> String {
        let before: String = self
            .draft_value
            .chars()
            .take(self.draft_cursor_pos)
            .collect();
        let after: String = self
            .draft_value
            .chars()
            .skip(self.draft_cursor_pos)
            .collect();
        format!("{before}▌{after}")
    }

    fn build_rule(&self) -> Result<FilterRule, String> {
        let needle = self.draft_value.trim();
        if needle.is_empty() {
            return Err("Needle is required".to_string());
        }

        let value = match self.draft_op {
            FilterOp::Contains => FilterValue::Pattern(needle.to_string()),
            FilterOp::Regex => FilterValue::Regex(needle.to_string()),
            FilterOp::Eq | FilterOp::Lt | FilterOp::Gt => {
                FilterValue::Literal(self.parse_literal(needle)?)
            }
            _ => return Err("Unsupported filter operator".to_string()),
        };

        Ok(FilterRule {
            op: self.draft_op,
            value,
            enabled: true,
            label: None,
        })
    }

    fn parse_literal(&self, needle: &str) -> Result<SqlValue, String> {
        let upper = self.col_type.to_uppercase();
        if upper.contains("INT") {
            return needle
                .parse::<i64>()
                .map(SqlValue::Integer)
                .map_err(|_| "Needle must be a valid integer".to_string());
        }
        if upper.contains("REAL")
            || upper.contains("FLOAT")
            || upper.contains("DOUBLE")
            || upper.contains("NUM")
        {
            return needle
                .parse::<f64>()
                .map(SqlValue::Real)
                .map_err(|_| "Needle must be a valid number".to_string());
        }
        Ok(SqlValue::Text(needle.to_string()))
    }

    fn clamp_selection(&mut self) {
        if self.selected_rule >= self.col_filter.rules.len() {
            self.selected_rule = self.col_filter.rules.len().saturating_sub(1);
        }
    }
}

pub fn render(
    frame: &mut Frame,
    area: Rect,
    state: &FilterPopupState,
    theme: &Theme,
    _config: &Config,
) {
    let popup_w = (area.width * 7 / 10).max(50).min(area.width);
    let popup_h = ((area.height * 2) / 5).max(9).min(area.height);
    let x = area.x + (area.width.saturating_sub(popup_w)) / 2;
    let y = area.y + (area.height.saturating_sub(popup_h)) / 2;
    let popup_area = Rect {
        x,
        y,
        width: popup_w,
        height: popup_h,
    };

    super::paint_popup_surface(frame, popup_area, theme);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(theme.accent))
        .title(format!(" Filter: {} ", state.col_name))
        .style(Style::default().bg(theme.bg_raised));
    let inner = block.inner(popup_area);
    frame.render_widget(block, popup_area);

    let editor_area = Rect { height: 3, ..inner };
    let divider_y = inner.y + 3;
    let rows_area = Rect {
        y: inner.y + 4,
        height: inner.height.saturating_sub(6),
        ..inner
    };
    let footer_area = Rect {
        y: inner.y + inner.height.saturating_sub(1),
        height: 1,
        ..inner
    };

    let intro = " Same-column rules use OR. Different columns use AND.";
    frame.buffer_mut().set_string(
        editor_area.x,
        editor_area.y,
        truncate(intro, editor_area.width as usize),
        Style::default().fg(theme.fg_faint).bg(theme.bg_raised),
    );

    let op_line = format!(
        " Operator: {}   (↑/↓ to change)",
        popup_op_label(state.draft_op)
    );
    frame.buffer_mut().set_string(
        editor_area.x,
        editor_area.y + 1,
        truncate(&op_line, editor_area.width as usize),
        Style::default()
            .fg(if state.focus == FilterPopupFocus::Needle {
                theme.fg
            } else {
                theme.fg_dim
            })
            .bg(theme.bg_raised),
    );

    let needle_line = format!(" Needle: {}", state.display_draft_value());
    frame.buffer_mut().set_string(
        editor_area.x,
        editor_area.y + 2,
        truncate(&needle_line, editor_area.width as usize),
        Style::default()
            .fg(if state.focus == FilterPopupFocus::Needle {
                theme.fg
            } else {
                theme.fg_dim
            })
            .bg(theme.bg_raised),
    );

    if divider_y < inner.y + inner.height {
        frame.buffer_mut().set_string(
            inner.x,
            divider_y,
            "─".repeat(inner.width as usize),
            Style::default().fg(theme.line).bg(theme.bg_raised),
        );
    }

    if state.col_filter.rules.is_empty() {
        frame.buffer_mut().set_string(
            rows_area.x,
            rows_area.y,
            truncate(
                " No rules yet. Press Enter to add the current operator + needle.",
                rows_area.width as usize,
            ),
            Style::default().fg(theme.fg_faint).bg(theme.bg_raised),
        );
    }

    for (i, rule) in state.col_filter.rules.iter().enumerate() {
        let row_y = rows_area.y + i as u16;
        if row_y >= rows_area.y + rows_area.height {
            break;
        }

        let is_sel = i == state.selected_rule;
        let bg = if is_sel {
            theme.bg_soft
        } else {
            theme.bg_raised
        };
        let fg = if is_sel { theme.fg } else { theme.fg_dim };
        let delete_label = if is_sel { "[x] delete" } else { "[x]" };
        let delete_width = UnicodeWidthStr::width(delete_label);
        let text_width = rows_area
            .width
            .saturating_sub(delete_width as u16)
            .saturating_sub(1) as usize;

        frame.buffer_mut().set_string(
            rows_area.x,
            row_y,
            " ".repeat(rows_area.width as usize),
            Style::default().bg(bg),
        );
        frame.buffer_mut().set_string(
            rows_area.x,
            row_y,
            truncate(&format_rule(rule), text_width),
            Style::default().fg(fg).bg(bg).add_modifier(if is_sel {
                Modifier::BOLD
            } else {
                Modifier::empty()
            }),
        );

        let delete_x = rows_area
            .x
            .saturating_add(rows_area.width.saturating_sub(delete_width as u16));
        frame.buffer_mut().set_string(
            delete_x,
            row_y,
            delete_label,
            Style::default()
                .fg(if is_sel && state.focus == FilterPopupFocus::Delete {
                    theme.red
                } else {
                    theme.fg_mute
                })
                .bg(bg)
                .add_modifier(if is_sel && state.focus == FilterPopupFocus::Delete {
                    Modifier::BOLD
                } else {
                    Modifier::DIM
                }),
        );
    }

    let footer = " ↑/↓ operator  ←/→ cursor  tab needle/delete  enter add/delete  esc close";
    frame.buffer_mut().set_string(
        footer_area.x,
        footer_area.y,
        truncate(footer, footer_area.width as usize),
        Style::default().fg(theme.fg_faint).bg(theme.bg_raised),
    );
}

fn popup_ops() -> &'static [FilterOp] {
    &[
        FilterOp::Lt,
        FilterOp::Gt,
        FilterOp::Eq,
        FilterOp::Contains,
        FilterOp::Regex,
    ]
}

fn popup_op_label(op: FilterOp) -> &'static str {
    match op {
        FilterOp::Lt => "<",
        FilterOp::Gt => ">",
        FilterOp::Eq => "==",
        FilterOp::Contains => "contains",
        FilterOp::Regex => "regexp",
        _ => op.label(),
    }
}

fn format_rule(rule: &FilterRule) -> String {
    let value = match &rule.value {
        FilterValue::Literal(SqlValue::Null) => "NULL".to_string(),
        FilterValue::Literal(SqlValue::Integer(n)) => n.to_string(),
        FilterValue::Literal(SqlValue::Real(f)) => f.to_string(),
        FilterValue::Literal(SqlValue::Text(s)) => format!("\"{}\"", s),
        FilterValue::Literal(SqlValue::Blob(b)) => format!("<blob {} bytes>", b.len()),
        FilterValue::Pattern(s) => format!("\"{}\"", s),
        FilterValue::Regex(s) => format!("\"{}\"", s),
        FilterValue::Range(lo, hi) => format!("{lo:?}..{hi:?}"),
        FilterValue::List(values) => format!("{} values", values.len()),
        FilterValue::Formula(s) => s.clone(),
        FilterValue::N(n) => n.to_string(),
    };
    format!("{} {}", popup_op_label(rule.op), value)
}

fn truncate(text: &str, max_width: usize) -> String {
    if max_width == 0 {
        return String::new();
    }
    let mut out = String::new();
    let mut used = 0usize;
    for ch in text.chars() {
        let width = UnicodeWidthChar::width(ch).unwrap_or(1);
        if used + width > max_width {
            break;
        }
        out.push(ch);
        used += width;
    }
    if UnicodeWidthStr::width(text) > max_width && max_width > 1 {
        out.pop();
        out.push('…');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{FilterPopupFocus, FilterPopupState};
    use crate::{
        db::types::SqlValue,
        filter::{rule::FilterRule, ColumnFilter, FilterOp},
    };

    #[test]
    fn add_rule_parses_numeric_needles() {
        let mut state = FilterPopupState::new(
            "amount".to_string(),
            "INTEGER".to_string(),
            ColumnFilter::default(),
        );
        state.draft_op = FilterOp::Gt;
        state.draft_value = "42".to_string();
        state.draft_cursor_pos = 2;

        state.add_rule().expect("expected valid rule");

        assert_eq!(state.col_filter.rules.len(), 1);
        assert!(matches!(
            state.col_filter.rules[0].value,
            crate::filter::FilterValue::Literal(SqlValue::Integer(42))
        ));
    }

    #[test]
    fn delete_selected_rule_clamps_selection() {
        let mut state = FilterPopupState::new(
            "name".to_string(),
            "TEXT".to_string(),
            ColumnFilter {
                rules: vec![
                    FilterRule {
                        op: FilterOp::Contains,
                        value: crate::filter::FilterValue::Pattern("a".to_string()),
                        enabled: true,
                        label: None,
                    },
                    FilterRule {
                        op: FilterOp::Contains,
                        value: crate::filter::FilterValue::Pattern("b".to_string()),
                        enabled: true,
                        label: None,
                    },
                ],
            },
        );
        state.selected_rule = 1;
        state.focus = FilterPopupFocus::Delete;

        assert!(state.delete_selected_rule());
        assert_eq!(state.selected_rule, 0);
        assert_eq!(state.col_filter.rules.len(), 1);
    }

    #[test]
    fn needle_cursor_edits_in_place() {
        let mut state = FilterPopupState::new(
            "name".to_string(),
            "TEXT".to_string(),
            ColumnFilter::default(),
        );
        state.push_char('a');
        state.push_char('c');
        state.move_cursor_left();
        state.push_char('b');

        assert_eq!(state.draft_value, "abc");
        assert_eq!(state.draft_cursor_pos, 2);
    }
}
