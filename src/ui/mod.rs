pub mod sidebar;
pub mod statusbar;
pub mod tabbar;

use crate::app::{App, FocusPane};
use ratatui::{
    layout::{Constraint, Direction, Layout},
    Frame,
};

pub fn render(frame: &mut Frame, app: &mut App) {
    let area = frame.area();

    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(0),
            Constraint::Length(1),
        ])
        .split(area);

    tabbar::render_tabbar(frame, vertical[0], app);
    statusbar::render_statusbar(frame, vertical[2], app);

    let main_area = vertical[1];

    let content_area = if app.sidebar_visible {
        let sidebar_width = (main_area.width / 3).clamp(20, 40);
        let horizontal = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Length(sidebar_width), Constraint::Min(0)])
            .split(main_area);

        let focused = matches!(app.focus, FocusPane::Sidebar);
        sidebar::render_sidebar(
            frame,
            horizontal[0],
            &app.schema,
            &mut app.sidebar,
            &app.theme,
            &app.config,
            focused,
        );
        horizontal[1]
    } else {
        main_area
    };

    if let Some(ref grid) = app.grid {
        crate::grid::render_grid(frame, content_area, grid, &app.theme, &app.config);
    } else if let Some(active_idx) = app.active_tab {
        let tab = &app.open_tabs[active_idx];
        let msg = format!(" Loading {}...", tab.table_name);
        frame.render_widget(
            ratatui::widgets::Paragraph::new(msg)
                .style(ratatui::style::Style::default().fg(app.theme.fg_dim)),
            content_area,
        );
    }
}
