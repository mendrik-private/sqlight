pub mod sidebar;
pub mod statusbar;

use crate::app::App;
use ratatui::{
    layout::{Constraint, Direction, Layout},
    Frame,
};

pub fn render(frame: &mut Frame, app: &mut App) {
    let area = frame.area();
    let sidebar_width = (area.width / 3).clamp(20, 40);
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(sidebar_width), Constraint::Min(0)])
        .split(area);

    sidebar::render_sidebar(
        frame,
        chunks[0],
        &app.schema,
        &mut app.sidebar,
        &app.theme,
        &app.config,
    );
}
