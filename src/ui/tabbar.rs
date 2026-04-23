use ratatui::{
    layout::Rect,
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::Paragraph,
    Frame,
};

use crate::app::App;

pub fn render_tabbar(frame: &mut Frame, area: Rect, app: &App) {
    let spans: Vec<Span> = if app.open_tabs.is_empty() {
        vec![Span::styled(" sqv ", Style::default().fg(app.theme.accent))]
    } else {
        let mut spans = Vec::new();
        for (idx, tab) in app.open_tabs.iter().enumerate() {
            let is_active = app.active_tab == Some(idx);
            let label = match tab.row_count {
                Some(count) => format!(" {} ({} rows) ", tab.table_name, count),
                None => format!(" {} (…) ", tab.table_name),
            };
            let style = if is_active {
                Style::default()
                    .fg(app.theme.accent)
                    .bg(app.theme.bg_raised)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(app.theme.fg_dim)
            };
            spans.push(Span::styled(label, style));
            if idx < app.open_tabs.len() - 1 {
                spans.push(Span::styled("│", Style::default().fg(app.theme.line)));
            }
        }
        spans
    };

    let paragraph = Paragraph::new(Line::from(spans)).style(Style::default().bg(app.theme.bg_soft));
    frame.render_widget(paragraph, area);
}
