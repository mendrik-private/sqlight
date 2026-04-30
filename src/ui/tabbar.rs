use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Modifier, Style},
    Frame,
};

use crate::app::{App, FocusPane};

pub enum TabMouseAction {
    Activate(usize),
    Close(usize),
}

/// Returns the superscript digit for tabs 0–9, or `None` for higher indices.
/// The sequence is ¹ ² ³ ⁴ ⁵ ⁶ ⁷ ⁸ ⁹ ⁰ so pressing 1–9 activates tab 0–8
/// and pressing 0 activates tab 9.
pub fn superscript_for_tab(idx: usize) -> Option<&'static str> {
    match idx {
        0 => Some("¹"),
        1 => Some("²"),
        2 => Some("³"),
        3 => Some("⁴"),
        4 => Some("⁵"),
        5 => Some("⁶"),
        6 => Some("⁷"),
        7 => Some("⁸"),
        8 => Some("⁹"),
        9 => Some("⁰"),
        _ => None,
    }
}

pub fn render_tabbar(frame: &mut Frame, area: Rect, app: &App) {
    if area.width == 0 || area.height == 0 {
        return;
    }

    let theme = &app.theme;
    let buf = frame.buffer_mut();
    buf.set_style(area, Style::default().bg(theme.bg));

    let top_y = area.y;
    let label_y = area.y + if area.height > 1 { 1 } else { 0 };
    let join_y = area.y + area.height.saturating_sub(1);
    let has_roof = label_y > top_y;
    let has_join = area.height >= 3;

    let mut x = area.x;
    let right = area.x + area.width;

    if app.open_tabs.is_empty() {
        buf.set_string(
            x,
            label_y,
            " sqview ",
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
        let is_focused_active = is_active && matches!(app.focus, FocusPane::Grid);
        let border_style = Style::default()
            .fg(if is_focused_active {
                theme.accent
            } else {
                theme.line
            })
            .bg(theme.bg);
        let base = if is_active {
            Style::default()
                .fg(theme.fg)
                .bg(theme.bg_raised)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(theme.fg_dim).bg(theme.bg_soft)
        };
        let close_style = if is_focused_active {
            base.fg(theme.accent)
        } else {
            base.fg(theme.fg_mute)
        };
        let num_style = Style::default().fg(theme.fg_mute).bg(if is_active {
            theme.bg_raised
        } else {
            theme.bg_soft
        });

        let sup = superscript_for_tab(idx);
        let sup_w: u16 = if sup.is_some() { 1 } else { 0 };
        let name_w = tab.table_name.chars().count() as u16;
        // │ space name [sup] space × space │
        let content_width = 1 + name_w + sup_w + 1 + 1 + 1;
        let tab_width = content_width + 2;
        let tab_x = x;

        if has_roof {
            let mut roof_x = tab_x;
            roof_x = put(buf, roof_x, top_y, right, "╭", border_style);
            if tab_width > 2 {
                roof_x = put(
                    buf,
                    roof_x,
                    top_y,
                    right,
                    &"─".repeat(tab_width.saturating_sub(2) as usize),
                    border_style,
                );
            }
            put(buf, roof_x, top_y, right, "╮", border_style);
        }

        let mut label_x = tab_x;
        label_x = put(buf, label_x, label_y, right, "│", border_style);
        label_x = put(buf, label_x, label_y, right, " ", base);
        label_x = put(buf, label_x, label_y, right, &tab.table_name, base);
        if let Some(s) = sup {
            label_x = put(buf, label_x, label_y, right, s, num_style);
        }
        label_x = put(buf, label_x, label_y, right, " ", base);
        label_x = put(buf, label_x, label_y, right, "×", close_style);
        label_x = put(buf, label_x, label_y, right, " ", base);
        put(buf, label_x, label_y, right, "│", border_style);

        if is_active && has_join {
            let mut join_x = tab_x;
            let left_join = if tab_x == area.x { "│" } else { "┘" };
            let right_join = if tab_x.saturating_add(tab_width) >= right {
                "│"
            } else {
                "└"
            };
            join_x = put(buf, join_x, join_y, right, left_join, border_style);
            if tab_width > 2 {
                join_x = put(
                    buf,
                    join_x,
                    join_y,
                    right,
                    &" ".repeat(tab_width.saturating_sub(2) as usize),
                    base,
                );
            }
            put(buf, join_x, join_y, right, right_join, border_style);
        }

        x = tab_x.saturating_add(tab_width).min(right);
    }
}

pub fn hit_test(
    area: Rect,
    app: &App,
    x: u16,
    y: u16,
    middle_click: bool,
) -> Option<TabMouseAction> {
    if area.width == 0
        || area.height == 0
        || y < area.y
        || y >= area.y + area.height
        || x < area.x
        || x >= area.x + area.width
    {
        return None;
    }

    let mut cursor = area.x;
    let right = area.x + area.width;
    for (idx, tab) in app.open_tabs.iter().enumerate() {
        if cursor >= right {
            break;
        }
        let name_w = tab.table_name.chars().count() as u16;
        let sup_w: u16 = if superscript_for_tab(idx).is_some() {
            1
        } else {
            0
        };
        let tab_width = name_w + sup_w + 6;
        let tab_end = cursor.saturating_add(tab_width).min(right);
        if x >= cursor && x < tab_end {
            // │(1) space(1) name(name_w) [sup(sup_w)] → × is at cursor+2+name_w+sup_w
            let close_x = cursor + 2 + name_w + sup_w;
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
