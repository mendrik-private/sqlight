use ratatui::{
    layout::Rect,
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::Paragraph,
    Frame,
};

use crate::{app::App, db::types::SqlValue};

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

    let mut spans = vec![
        Span::styled(
            " BROWSE ",
            Style::default()
                .fg(theme.bg)
                .bg(theme.accent)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(" │ ", Style::default().fg(theme.line)),
        Span::styled(
            format!("{} ", table_name),
            Style::default().fg(theme.fg_dim),
        ),
    ];

    if !pos_str.is_empty() {
        spans.push(Span::styled("  ", Style::default()));
        spans.push(Span::styled(pos_str, Style::default().fg(theme.fg_mute)));
    }

    if !cell_preview.is_empty() {
        spans.push(Span::styled("  ", Style::default()));
        spans.push(Span::styled(
            cell_preview,
            Style::default().fg(theme.fg_dim),
        ));
    }

    let line = Line::from(spans);
    let paragraph = Paragraph::new(line).style(Style::default().bg(theme.bg_soft));
    frame.render_widget(paragraph, area);
}
