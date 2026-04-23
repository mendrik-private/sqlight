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

    if app.sidebar_visible {
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
    }
}
