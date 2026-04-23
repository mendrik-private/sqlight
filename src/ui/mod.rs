pub mod popup;
pub mod sidebar;
pub mod statusbar;
pub mod toast;

use crate::app::{App, FocusPane};
use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Modifier, Style},
    text::Span,
    widgets::{Block, Paragraph},
    Frame,
};

pub fn render(frame: &mut Frame, app: &mut App) {
    let area = frame.area();
    app.tabbar_area = Rect::default();
    app.sidebar_area = None;
    app.grid_outer_area = None;
    app.grid_inner_area = None;

    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(0), Constraint::Length(1)])
        .split(area);

    statusbar::render_statusbar(frame, vertical[1], app);

    let main_area = vertical[0];

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
        app.sidebar_area = Some(horizontal[0]);
        horizontal[1]
    } else {
        main_area
    };

    frame.render_widget(
        Paragraph::new("").style(Style::default().bg(app.theme.bg)),
        content_area,
    );

    if let Some(ref mut grid) = app.grid {
        let border_color = if matches!(app.focus, FocusPane::Grid) {
            app.theme.accent
        } else {
            app.theme.line
        };
        let filter_count = grid
            .filter
            .columns
            .values()
            .map(|cf| cf.rules.iter().filter(|r| r.enabled).count())
            .sum::<usize>();
        let meta = {
            let mut parts = vec![format!("{} rows", fmt_count(grid.window.total_rows))];
            if filter_count > 0 {
                parts.push(format!("{} filters", filter_count));
            }
            if let Some(sort) = &grid.sort {
                if let Some(col) = grid.columns.get(sort.col_idx) {
                    let arrow = if sort.direction == crate::grid::SortDir::Asc {
                        "▲"
                    } else {
                        "▼"
                    };
                    parts.push(format!("sort: {} {}", col.name, arrow));
                }
            }
            parts.join(" · ")
        };

        let block = Block::bordered()
            .style(Style::default().bg(app.theme.bg))
            .border_style(Style::default().fg(border_color))
            .title(Span::styled(
                format!("▌ TABLE · {}", grid.table_name),
                Style::default()
                    .fg(app.theme.accent)
                    .add_modifier(Modifier::BOLD),
            ))
            .title_bottom(Span::styled(meta, Style::default().fg(app.theme.fg_mute)));
        let inner = block.inner(content_area);
        app.grid_outer_area = Some(content_area);
        app.grid_inner_area = Some(inner);
        frame.render_widget(block, content_area);
        crate::grid::render_grid(frame, inner, grid, &app.theme, &app.config);
    } else if let Some(active_idx) = app.active_tab {
        let tab = &app.open_tabs[active_idx];
        let block = Block::bordered()
            .style(Style::default().bg(app.theme.bg))
            .border_style(Style::default().fg(app.theme.line))
            .title(Span::styled(
                format!("▌ TABLE · {}", tab.table_name),
                Style::default()
                    .fg(app.theme.accent)
                    .add_modifier(Modifier::BOLD),
            ));
        let inner = block.inner(content_area);
        app.grid_outer_area = Some(content_area);
        app.grid_inner_area = Some(inner);
        frame.render_widget(block, content_area);
        let msg = format!(" Loading {}...", tab.table_name);
        frame.render_widget(
            ratatui::widgets::Paragraph::new(msg)
                .style(ratatui::style::Style::default().fg(app.theme.fg_dim)),
            inner,
        );
    } else {
        let block = Block::bordered()
            .style(Style::default().bg(app.theme.bg))
            .border_style(Style::default().fg(app.theme.line))
            .title(Span::styled(
                "▌ TABLE",
                Style::default()
                    .fg(app.theme.accent)
                    .add_modifier(Modifier::BOLD),
            ));
        let inner = block.inner(content_area);
        app.grid_outer_area = Some(content_area);
        app.grid_inner_area = Some(inner);
        frame.render_widget(block, content_area);
    }

    if let Some(ref mut popup) = app.popup {
        crate::ui::popup::render_popup(frame, area, popup, &app.theme, &app.config);
    }
    crate::ui::toast::render_toasts(frame, area, &app.toast, &app.theme);
    if let Some(ref confirm) = app.pending_confirm {
        crate::ui::toast::render_confirm(frame, area, &confirm.message, &app.theme);
    }
}

fn fmt_count(n: i64) -> String {
    let s = n.abs().to_string();
    let chars: Vec<char> = s.chars().collect();
    let grouped = chars
        .rchunks(3)
        .rev()
        .map(|chunk| chunk.iter().collect::<String>())
        .collect::<Vec<_>>()
        .join("\u{202F}");
    if n < 0 {
        format!("-{}", grouped)
    } else {
        grouped
    }
}
