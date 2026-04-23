use ratatui::{
    layout::Rect,
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::Paragraph,
    Frame,
};

use crate::app::App;

pub fn render_statusbar(frame: &mut Frame, area: Rect, app: &App) {
    let mode_span = Span::styled(
        " BROWSE ",
        Style::default()
            .fg(app.theme.bg)
            .bg(app.theme.accent)
            .add_modifier(Modifier::BOLD),
    );

    let table_name = app
        .active_tab
        .and_then(|i| app.open_tabs.get(i))
        .map(|t| format!(" {} ", t.table_name))
        .unwrap_or_else(|| " — ".to_string());

    let table_span = Span::styled(table_name, Style::default().fg(app.theme.fg_dim));

    let line = Line::from(vec![mode_span, table_span]);
    let paragraph = Paragraph::new(line).style(Style::default().bg(app.theme.bg_soft));
    frame.render_widget(paragraph, area);
}
