use ratatui::{
    layout::Rect,
    style::Style,
    widgets::{Block, Borders},
    Frame,
};

use crate::{config::Config, filter::FilterValue, theme::Theme};

pub struct FilterPopupState {
    pub col_name: String,
    pub col_filter: crate::filter::ColumnFilter,
    pub selected_rule: usize,
    pub editing: bool,
    pub edit_op: crate::filter::FilterOp,
    pub edit_value: String,
}

impl FilterPopupState {
    pub fn new(col_name: String, col_filter: crate::filter::ColumnFilter) -> Self {
        Self {
            col_name,
            col_filter,
            selected_rule: 0,
            editing: false,
            edit_op: crate::filter::FilterOp::Contains,
            edit_value: String::new(),
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
    let popup_h = (area.height * 3 / 5).max(12).min(area.height);
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
        .border_style(Style::default().fg(theme.accent))
        .title(format!(" Filter: {} ", state.col_name))
        .style(Style::default().bg(theme.bg_raised));
    let inner = block.inner(popup_area);
    frame.render_widget(block, popup_area);

    let rows_area = Rect {
        height: inner.height.saturating_sub(3),
        ..inner
    };
    let editor_area = Rect {
        y: inner.y + inner.height.saturating_sub(3),
        height: 2,
        ..inner
    };
    let footer_area = Rect {
        y: inner.y + inner.height.saturating_sub(1),
        height: 1,
        ..inner
    };

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
        let fg = if is_sel { theme.accent } else { theme.fg_dim };
        let check = if rule.enabled { "[✓]" } else { "[ ]" };
        let label = rule.label.as_deref().unwrap_or(rule.op.label());
        let val_str = match &rule.value {
            FilterValue::Literal(v) => format!("{v:?}"),
            FilterValue::Pattern(s) => format!("\"{s}\""),
            _ => String::new(),
        };
        let line = format!(" {check} {label} {val_str} ");
        let truncated: String = line.chars().take(inner.width as usize).collect();
        frame.buffer_mut().set_string(
            rows_area.x,
            row_y,
            &truncated,
            Style::default().fg(fg).bg(bg),
        );
    }

    if state.editing || state.col_filter.rules.is_empty() {
        let op_line = format!(" Op: {} ", state.edit_op.label());
        let val_line = format!(" Value: {}▌ ", state.edit_value);
        frame.buffer_mut().set_string(
            editor_area.x,
            editor_area.y,
            &op_line,
            Style::default().fg(theme.fg).bg(theme.bg_raised),
        );
        frame.buffer_mut().set_string(
            editor_area.x,
            editor_area.y + 1,
            &val_line,
            Style::default().fg(theme.fg).bg(theme.bg_raised),
        );
    }

    let footer = " Enter apply · n new rule · Del delete · Space toggle · Esc close";
    frame.buffer_mut().set_string(
        footer_area.x,
        footer_area.y,
        footer,
        Style::default().fg(theme.fg_faint).bg(theme.bg_raised),
    );
}
