use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Modifier, Style},
    Frame,
};

use crate::app::App;

pub fn render_tabbar(frame: &mut Frame, area: Rect, app: &App) {
    if area.width == 0 || area.height == 0 {
        return;
    }

    let theme = &app.theme;
    let buf = frame.buffer_mut();
    buf.set_style(area, Style::default().bg(theme.bg_soft));

    let mut x = area.x;
    let right = area.x + area.width;

    if app.open_tabs.is_empty() {
        buf.set_string(
            x,
            area.y,
            " sqv ",
            Style::default()
                .fg(theme.accent)
                .bg(theme.bg_soft)
                .add_modifier(Modifier::BOLD),
        );
        return;
    }

    for (idx, tab) in app.open_tabs.iter().enumerate() {
        if x >= right {
            break;
        }
        let is_active = app.active_tab == Some(idx);
        let base = if is_active {
            Style::default()
                .fg(theme.fg)
                .bg(theme.bg_raised)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(theme.fg_dim).bg(theme.bg_soft)
        };
        let badge_style = if is_active {
            Style::default()
                .fg(theme.fg)
                .bg(theme.line)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(theme.fg_dim).bg(theme.line_soft)
        };

        let lead = if is_active { "▎ " } else { "  " };
        x = put(
            buf,
            x,
            area.y,
            right,
            lead,
            base.fg(if is_active { theme.accent } else { theme.line }),
        );
        x = put(buf, x, area.y, right, &tab.table_name, base);
        x = put(buf, x, area.y, right, " ", base);

        let badge = match tab.row_count {
            Some(count) => format!(" {} ", count),
            None => " … ".to_string(),
        };
        x = put(buf, x, area.y, right, &badge, badge_style);
        x = put(buf, x, area.y, right, " ", base);
        x = put(
            buf,
            x,
            area.y,
            right,
            "×",
            base.fg(if is_active {
                theme.accent
            } else {
                theme.fg_mute
            }),
        );
        x = put(
            buf,
            x,
            area.y,
            right,
            "  ",
            Style::default().bg(theme.bg_soft),
        );
    }

    if x < right {
        let plus_style = Style::default()
            .fg(theme.fg_mute)
            .bg(theme.bg_soft)
            .add_modifier(Modifier::BOLD);
        let _ = put(buf, x, area.y, right, "+", plus_style);
    }
}

fn put(buf: &mut Buffer, mut x: u16, y: u16, right: u16, text: &str, style: Style) -> u16 {
    for ch in text.chars() {
        if x >= right {
            break;
        }
        let s = ch.to_string();
        buf.set_string(x, y, s, style);
        x += 1;
    }
    x
}
