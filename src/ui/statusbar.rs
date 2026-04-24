use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Modifier, Style},
    Frame,
};

use crate::{
    app::{App, AppMode, FocusPane},
    db::types::SqlValue,
};

fn fmt_number(n: i64) -> String {
    let s = n.abs().to_string();
    let chars: Vec<char> = s.chars().collect();
    let grouped: String = chars
        .rchunks(3)
        .rev()
        .map(|c| c.iter().collect::<String>())
        .collect::<Vec<_>>()
        .join("\u{202F}");
    if n < 0 {
        format!("-{}", grouped)
    } else {
        grouped
    }
}

pub fn render_statusbar(frame: &mut Frame, area: Rect, app: &App) {
    if area.width == 0 || area.height == 0 {
        return;
    }

    let table_name: String = app
        .active_tab
        .and_then(|i| app.open_tabs.get(i))
        .map(|t| t.table_name.clone())
        .unwrap_or_else(|| "—".to_string());

    let (row_num, total_rows, col_num) = app.grid.as_ref().map_or((0i64, 0i64, 0usize), |g| {
        (
            g.focused_row as i64 + 1,
            g.window.total_rows,
            g.focused_col + 1,
        )
    });

    let cell_preview: String = app
        .grid
        .as_ref()
        .and_then(|g| {
            let abs_row = g.focused_row as i64;
            let col_idx = g.focused_col;
            g.window
                .get_row(abs_row)
                .and_then(|row| row.get(col_idx))
                .map(|val| match val {
                    SqlValue::Null => "NULL".to_string(),
                    SqlValue::Integer(n) => n.to_string(),
                    SqlValue::Real(f) => format!("{}", f),
                    SqlValue::Text(s) => s.chars().take(50).collect(),
                    SqlValue::Blob(b) => format!("<blob {} bytes>", b.len()),
                })
        })
        .unwrap_or_default();

    let pos_str = if total_rows > 0 {
        format!(
            "r {}/{} · col {}",
            fmt_number(row_num),
            fmt_number(total_rows),
            col_num
        )
    } else {
        String::new()
    };

    let theme = &app.theme;

    let filter_count = app
        .grid
        .as_ref()
        .map(|g| {
            g.filter
                .columns
                .values()
                .map(|cf| cf.rules.iter().filter(|r| r.enabled).count())
                .sum::<usize>()
        })
        .unwrap_or(0);

    let sort_str = app
        .grid
        .as_ref()
        .and_then(|g| {
            g.sort.as_ref().and_then(|s| {
                let col_name = g.columns.get(s.col_idx).map(|c| c.name.as_str())?;
                let arrow = if s.direction == crate::grid::SortDir::Asc {
                    "▲"
                } else {
                    "▼"
                };
                Some(format!("{} {}", arrow, col_name))
            })
        })
        .unwrap_or_default();

    let mut segments: Vec<(String, Style)> = vec![
        (
            " BROWSE ".to_string(),
            Style::default()
                .fg(theme.bg)
                .bg(theme.accent)
                .add_modifier(Modifier::BOLD),
        ),
        (
            table_name,
            Style::default()
                .fg(theme.fg_dim)
                .bg(theme.bg)
                .add_modifier(Modifier::BOLD),
        ),
    ];

    if filter_count > 0 {
        segments.push((
            format!("󰈲 {} filters", filter_count),
            Style::default().fg(theme.red).bg(theme.bg_soft),
        ));
    }

    if !sort_str.is_empty() {
        segments.push((
            sort_str,
            Style::default().fg(theme.accent).bg(theme.bg_soft),
        ));
    }

    if !pos_str.is_empty() {
        segments.push((
            pos_str,
            Style::default().fg(theme.fg_mute).bg(theme.bg_soft),
        ));
    }

    if !app.jump_stack.is_empty() {
        let current_table = app.grid.as_ref().map_or("—", |g| g.table_name.as_str());
        let crumb: String = app
            .jump_stack
            .iter()
            .map(|f| f.table.as_str())
            .chain(std::iter::once(current_table))
            .collect::<Vec<_>>()
            .join(" › ");
        segments.push((
            format!("↩ {}", crumb),
            Style::default().fg(theme.accent).bg(theme.bg_soft),
        ));
    }

    if let Some(hints) = action_hint_text(app) {
        segments.push((hints, Style::default().fg(theme.accent).bg(theme.bg_soft)));
    }

    let buf = frame.buffer_mut();
    buf.set_style(area, Style::default().bg(theme.bg_soft));

    let preview = truncate_preview(&cell_preview, area.width as usize / 3);
    let preview_width = preview.chars().count() as u16;
    let preview_x = if preview.is_empty() {
        area.x + area.width
    } else {
        area.x + area.width.saturating_sub(preview_width + 1)
    };

    let mut x = area.x;
    for (idx, (text, style)) in segments.iter().enumerate() {
        if x >= preview_x {
            break;
        }
        x = put(buf, x, area.y, preview_x, text, *style);
        if idx + 1 < segments.len() && x < preview_x {
            x = put(
                buf,
                x,
                area.y,
                preview_x,
                "  │  ",
                Style::default().fg(theme.line).bg(theme.bg_soft),
            );
        }
    }

    if !preview.is_empty() && preview_x < area.x + area.width {
        let _ = put(
            buf,
            preview_x,
            area.y,
            area.x + area.width,
            &preview,
            Style::default().fg(theme.fg).bg(theme.bg_soft),
        );
    }
}

fn truncate_preview(s: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    let mut out: String = s.chars().take(max_chars).collect();
    if s.chars().count() > max_chars && max_chars > 1 {
        out.pop();
        out.push('…');
    }
    out
}

fn action_hint_text(app: &App) -> Option<String> {
    if app.popup.is_some() || app.mode != AppMode::Browse {
        return None;
    }

    let mut hints = Vec::new();

    match app.focus {
        FocusPane::Sidebar => {
            hints.push("[enter] open".to_string());
            if app.sidebar_visible {
                hints.push("[tab] panel".to_string());
            }
        }
        FocusPane::Grid => {
            if app.grid.is_some() && !app.readonly {
                hints.push("[enter] edit".to_string());
            }
            if app.grid.is_some() {
                hints.push("[s] sort".to_string());
                hints.push("[f] filter".to_string());
            }
            if app
                .grid
                .as_ref()
                .and_then(|g| g.fk_cols.get(g.focused_col))
                .copied()
                .unwrap_or(false)
            {
                hints.push("[j] jump".to_string());
            }
            if !app.jump_stack.is_empty() {
                hints.push("[backspace] back".to_string());
            }
            if app.sidebar_visible {
                hints.push("[tab] panel".to_string());
            }
        }
    }

    if app.sidebar_visible {
        hints.push("[ctrl-b] sidebar".to_string());
    }
    hints.push("[ctrl-q] quit".to_string());

    if hints.is_empty() {
        None
    } else {
        Some(hints.join("  "))
    }
}

fn put(buf: &mut Buffer, mut x: u16, y: u16, right: u16, text: &str, style: Style) -> u16 {
    for ch in text.chars() {
        if x >= right {
            break;
        }
        buf.set_string(x, y, ch.to_string(), style);
        x += 1;
    }
    x
}
