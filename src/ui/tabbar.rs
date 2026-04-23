use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Modifier, Style},
    Frame,
};

use crate::app::App;

pub enum TabMouseAction {
    Activate(usize),
    Close(usize),
}

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
}

pub fn hit_test(
    area: Rect,
    app: &App,
    x: u16,
    y: u16,
    middle_click: bool,
) -> Option<TabMouseAction> {
    if area.width == 0 || area.height == 0 || y != area.y || x < area.x || x >= area.x + area.width
    {
        return None;
    }

    let mut cursor = area.x;
    let right = area.x + area.width;
    for (idx, tab) in app.open_tabs.iter().enumerate() {
        if cursor >= right {
            break;
        }
        let lead_w = 2u16;
        let name_w = tab.table_name.chars().count() as u16;
        let badge_w = match tab.row_count {
            Some(count) => format!(" {} ", count).chars().count() as u16,
            None => 3,
        };
        let tab_width = lead_w + name_w + 1 + badge_w + 1 + 1 + 2;
        let tab_end = cursor.saturating_add(tab_width).min(right);
        if x >= cursor && x < tab_end {
            let close_x = cursor + lead_w + name_w + 1 + badge_w + 1;
            if middle_click || x == close_x {
                return Some(TabMouseAction::Close(idx));
            }
            return Some(TabMouseAction::Activate(idx));
        }
        cursor = tab_end;
    }

    None
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
