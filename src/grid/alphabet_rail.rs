use ratatui::{layout::Rect, style::Style, Frame};

use crate::{
    db::types::{affinity, ColAffinity},
    grid::GridState,
    theme::Theme,
};

pub fn should_show_rail(state: &GridState) -> bool {
    if state.window.total_rows < 200 {
        return false;
    }
    if let Some(sort) = &state.sort {
        if let Some(col) = state.columns.get(sort.col_idx) {
            return matches!(affinity(&col.col_type), ColAffinity::Text);
        }
    }
    false
}

pub const RAIL_WIDTH: u16 = 2;

pub fn render_rail(frame: &mut Frame, area: Rect, state: &GridState, theme: &Theme) {
    if !should_show_rail(state) {
        return;
    }

    let track_height = area.height.saturating_sub(1) as usize;
    if track_height == 0 {
        return;
    }

    // Rail is 2 chars wide, to the left of the scrollbar (1 col)
    let rail_x = area.x + area.width.saturating_sub(RAIL_WIDTH + 1);
    let rail_y_start = area.y + 1; // after header

    let letters: Vec<char> = std::iter::once('#').chain('A'..='Z').collect();

    // Determine current letter from viewport
    let current_letter: Option<char> = state
        .window
        .get_row(state.viewport_start)
        .and_then(|row| state.sort.as_ref().and_then(|s| row.get(s.col_idx)))
        .and_then(|val| match val {
            crate::db::types::SqlValue::Text(s) => s.chars().next(),
            _ => None,
        })
        .map(|c| c.to_uppercase().next().unwrap_or(c));

    let letters_to_show: Vec<char> = if track_height >= letters.len() {
        letters.clone()
    } else {
        let step = ((letters.len() as f32 / track_height as f32).ceil() as usize).max(1);
        letters.iter().step_by(step).cloned().collect()
    };

    let buf = frame.buffer_mut();
    for (i, &letter) in letters_to_show.iter().enumerate() {
        let y = rail_y_start + i as u16;
        if y >= area.y + area.height {
            break;
        }

        let is_current = current_letter == Some(letter);
        let style = if is_current {
            Style::default().fg(theme.bg).bg(theme.accent)
        } else {
            Style::default().fg(theme.fg_dim).bg(theme.bg)
        };

        let label = format!("{} ", letter);
        buf.set_string(rail_x, y, &label, style);
    }
}
