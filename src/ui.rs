use chrono::{Datelike, Weekday};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{
        Block, Borders, Clear, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState, Wrap,
    },
};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use crate::{
    app::App,
    db::{ColumnKind, parse_bool_text},
    state::{
        DatePickerFocus, DatePickerModal, EditorFocus, EditorModal, FilterJoin, FilterModal,
        Hitbox, ModalState, SortDirection, UiRegion,
    },
};

pub fn render(frame: &mut Frame<'_>, app: &mut App) {
    let area = frame.area();
    frame.render_widget(Block::default().style(Style::default().bg(bg())), area);

    let [status_area, main_area, preview_area, help_area] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(6),
        Constraint::Length(1),
        Constraint::Length(1),
    ])
    .areas(area);

    let [sidebar_area, data_area] =
        Layout::horizontal([Constraint::Length(28), Constraint::Min(24)]).areas(main_area);

    let sidebar_block = panel_block(" Tables ", accent_soft());
    let sidebar_inner = sidebar_block.inner(sidebar_area);
    frame.render_widget(sidebar_block, sidebar_area);

    let table_title = format!(" {} {} ", table_icon(), app.state.table.name);
    let data_block = panel_block(&table_title, accent());
    let data_inner = data_block.inner(data_area);
    frame.render_widget(data_block, data_area);

    let [breadcrumb_area, header_area, body_area, scrollbar_area] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Min(3),
        Constraint::Length(1),
    ])
    .areas(data_inner);

    app.refresh_layout(
        body_area.width.saturating_sub(1),
        body_area.height as usize,
        sidebar_inner.height as usize,
    );
    app.state.hitboxes.clear();

    render_status_bar(frame, app, status_area);
    render_sidebar(frame, app, sidebar_inner, sidebar_area);
    render_breadcrumb(frame, app, breadcrumb_area);
    render_header(frame, app, header_area);
    render_body(frame, app, body_area);
    render_horizontal_scrollbar(frame, app, scrollbar_area);
    render_preview_bar(frame, app, preview_area);
    render_help_bar(frame, app, help_area);

    if let Some(modal) = app.state.modal.as_ref() {
        match modal {
            ModalState::Editor(editor) => render_editor_modal(frame, editor, area),
            ModalState::DatePicker(picker) => render_date_picker_modal(frame, picker, area),
            ModalState::Filter(filter) => render_filter_modal(frame, app, filter, area),
            ModalState::Help => render_help_modal(frame, area),
        }
    }
}

fn render_status_bar(frame: &mut Frame<'_>, app: &App, area: Rect) {
    let mode = match app.state.modal {
        Some(ModalState::Editor(_)) => "EDIT",
        Some(ModalState::DatePicker(_)) => "DATE",
        Some(ModalState::Filter(_)) => "FILTER",
        Some(ModalState::Help) => "HELP",
        None => "BROWSE",
    };
    let sort = app
        .state
        .sort
        .map(|sort| {
            let dir = match sort.direction {
                SortDirection::Asc => "▲",
                SortDirection::Desc => "▼",
            };
            format!(" {} {}", app.state.table.columns[sort.column].name, dir)
        })
        .unwrap_or_else(|| " none".to_owned());
    let row_meta = if app.state.active_filters.is_empty() {
        format!("rows:{} ", app.state.table.rows.len())
    } else {
        format!(
            "rows:{}/{} ",
            app.state.table.rows.len(),
            app.state.source_rows.len()
        )
    };
    let meta = format!(
        " {} tables  {} cols:{}  sort:{} ",
        app.state.tables.len(),
        row_meta,
        app.state.table.columns.len(),
        sort
    );

    let line = Line::from(vec![
        Span::styled(
            format!(" {} ", mode),
            Style::default()
                .fg(Color::Black)
                .bg(accent())
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!(" {} › {} ", app.state.db_label, app.state.table.name),
            Style::default().fg(fg()),
        ),
        Span::styled(meta, Style::default().fg(mutefg())),
    ]);

    frame.render_widget(
        Paragraph::new(line).style(Style::default().bg(panel_bg())),
        area,
    );
}

fn render_sidebar(frame: &mut Frame<'_>, app: &mut App, inner: Rect, block_area: Rect) {
    if inner.width == 0 || inner.height == 0 {
        return;
    }

    let list_width = inner.width.saturating_sub(1).max(1);
    app.state.hitboxes.push(Hitbox {
        area: inner,
        region: UiRegion::TablePanel,
    });

    let visible_end = (app.state.side_scroll + inner.height as usize).min(app.state.tables.len());
    for (draw_index, table_index) in (app.state.side_scroll..visible_end).enumerate() {
        let row_area = Rect::new(inner.x, inner.y + draw_index as u16, list_width, 1);
        let selected = table_index == app.state.current_table;
        let icon = if selected { "▣" } else { "▢" };
        let count = app
            .state
            .table_row_counts
            .get(table_index)
            .copied()
            .unwrap_or(0)
            .to_string();
        let count_width = count.width();
        let name_width = list_width as usize - count_width.saturating_add(3);
        let name = clip_with_ellipsis(&app.state.tables[table_index], name_width.max(1));
        let name_fill = " ".repeat(name_width.saturating_sub(name.width()));

        let style = if selected {
            Style::default()
                .fg(Color::Black)
                .bg(Color::Rgb(132, 187, 255))
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(fg())
        };

        let line = Line::from(vec![
            Span::styled(format!("{icon} "), style),
            Span::styled(
                format!("{name}{name_fill} "),
                if selected {
                    style
                } else {
                    Style::default().fg(fg())
                },
            ),
            Span::styled(
                count,
                if selected {
                    style
                } else {
                    Style::default().fg(mutefg())
                },
            ),
        ]);

        frame.render_widget(Paragraph::new(line), row_area);
        app.state.hitboxes.push(Hitbox {
            area: row_area,
            region: UiRegion::TableItem { index: table_index },
        });
    }

    if app.state.tables.len() > inner.height as usize {
        let mut scrollbar_state = ScrollbarState::default()
            .content_length(app.state.tables.len())
            .viewport_content_length(inner.height as usize)
            .position(app.state.side_scroll);
        frame.render_stateful_widget(
            Scrollbar::new(ScrollbarOrientation::VerticalRight),
            block_area,
            &mut scrollbar_state,
        );
    }
}

fn render_breadcrumb(frame: &mut Frame<'_>, app: &mut App, area: Rect) {
    let line = Line::from(vec![
        Span::styled(app.state.db_label.clone(), Style::default().fg(accent())),
        Span::styled(" › ", Style::default().fg(mutefg())),
        Span::styled(
            app.state.table.name.clone(),
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
    ]);
    frame.render_widget(Paragraph::new(line), area);

    let db_width = app.state.db_label.width() as u16;
    let table_width = app.state.table.name.width() as u16;
    app.state.hitboxes.push(Hitbox {
        area: Rect::new(area.x, area.y, db_width, 1),
        region: UiRegion::Breadcrumb { index: 0 },
    });
    app.state.hitboxes.push(Hitbox {
        area: Rect::new(area.x + db_width + 3, area.y, table_width, 1),
        region: UiRegion::Breadcrumb { index: 1 },
    });
}

fn render_header(frame: &mut Frame<'_>, app: &mut App, area: Rect) {
    let visible_columns = app.visible_columns();
    let mut spans = Vec::new();
    let mut x = area.x;

    for (position, column_index) in visible_columns.iter().copied().enumerate() {
        if position > 0 {
            spans.push(Span::styled("│", Style::default().fg(border())));
            x += 1;
        }

        let width = app.state.grid_layout.column_widths[column_index];
        let column = &app.state.table.columns[column_index];
        let label = format!(
            "{} {} {}",
            type_icon(&column.kind),
            column.name,
            sort_marker(app.state.sort, column_index)
        );
        let text = fit_cell(&label, width, Align::Left);
        spans.push(Span::styled(
            text.clone(),
            Style::default()
                .fg(Color::Black)
                .bg(accent())
                .add_modifier(Modifier::BOLD),
        ));
        app.state.hitboxes.push(Hitbox {
            area: Rect::new(x, area.y, width, 1),
            region: UiRegion::Header { col: column_index },
        });
        x += width;
    }

    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

fn render_body(frame: &mut Frame<'_>, app: &mut App, area: Rect) {
    let visible_columns = app.visible_columns();
    let row_end = (app.state.row_offset + area.height as usize).min(app.state.table.rows.len());
    let list_width = area.width.saturating_sub(1).max(1);

    for (draw_index, row_index) in (app.state.row_offset..row_end).enumerate() {
        let row_area = Rect::new(area.x, area.y + draw_index as u16, list_width, 1);
        let row = &app.state.table.rows[row_index];
        let mut spans = Vec::new();
        let mut x = row_area.x;

        for (position, column_index) in visible_columns.iter().copied().enumerate() {
            if position > 0 {
                spans.push(Span::styled("│", Style::default().fg(border())));
                x += 1;
            }

            let column = &app.state.table.columns[column_index];
            let width = app.state.grid_layout.column_widths[column_index];
            let display = display_cell(&row.cells[column_index], &column.kind);
            let text = fit_cell(&display, width, alignment_for(&column.kind));
            let selected =
                row_index == app.state.selected_row && column_index == app.state.selected_col;
            let row_style = if selected {
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Rgb(182, 214, 122))
                    .add_modifier(Modifier::BOLD)
            } else if draw_index % 2 == 0 {
                Style::default().fg(fg())
            } else {
                Style::default().fg(Color::Rgb(210, 214, 220))
            };

            spans.push(Span::styled(text.clone(), row_style));
            app.state.hitboxes.push(Hitbox {
                area: Rect::new(x, row_area.y, width, 1),
                region: UiRegion::Cell {
                    row: row_index,
                    col: column_index,
                },
            });
            x += width;
        }

        frame.render_widget(Paragraph::new(Line::from(spans)), row_area);
    }

    if app.state.table.rows.len() > area.height as usize {
        let mut scrollbar_state = ScrollbarState::default()
            .content_length(app.state.table.rows.len())
            .viewport_content_length(area.height as usize)
            .position(app.state.row_offset);
        frame.render_stateful_widget(
            Scrollbar::new(ScrollbarOrientation::VerticalRight),
            area,
            &mut scrollbar_state,
        );
    }
}

fn render_horizontal_scrollbar(frame: &mut Frame<'_>, app: &mut App, area: Rect) {
    if app.state.grid_layout.horizontal_overflow {
        let mut scrollbar_state = ScrollbarState::default()
            .content_length(app.state.grid_layout.total_width as usize)
            .viewport_content_length(app.state.grid_layout.viewport_width as usize)
            .position(app.horizontal_scroll_position());
        frame.render_stateful_widget(
            Scrollbar::new(ScrollbarOrientation::HorizontalBottom),
            area,
            &mut scrollbar_state,
        );
        app.state.hitboxes.push(Hitbox {
            area,
            region: UiRegion::HorizontalScrollbarTrack,
        });
    } else {
        let hint = Paragraph::new("smart widths active · no horizontal overflow")
            .style(Style::default().fg(mutefg()));
        frame.render_widget(hint, area);
    }
}

fn render_preview_bar(frame: &mut Frame<'_>, app: &App, area: Rect) {
    let icon = app
        .state
        .table
        .columns
        .get(app.state.selected_col)
        .map(|column| type_icon(&column.kind))
        .unwrap_or("·");
    let rowid = app
        .state
        .table
        .rows
        .get(app.state.selected_row)
        .and_then(|row| row.rowid)
        .map(|rowid| format!("rowid={rowid}  "))
        .unwrap_or_default();
    let value = app.state.current_value().unwrap_or("<empty>");
    let line = format!("{icon} {rowid}{value}");
    frame.render_widget(
        Paragraph::new(clip_with_ellipsis(&line, area.width as usize)).style(
            Style::default()
                .fg(Color::Rgb(230, 218, 140))
                .bg(panel_bg()),
        ),
        area,
    );
}

fn render_help_bar(frame: &mut Frame<'_>, app: &App, area: Rect) {
    let help = Line::from(vec![
        keycap("Tab"),
        label("table"),
        keycap("Arrows"),
        label("move"),
        keycap("Enter"),
        label("edit/toggle"),
        keycap("F"),
        label("filter"),
        keycap("c"),
        label("clear"),
        keycap("Wheel"),
        label("scroll"),
        keycap("Shift+Wheel"),
        label("h-scroll"),
        keycap("s"),
        label("sort"),
        keycap("j"),
        label("fk"),
        keycap("?"),
        label("help"),
        keycap("q"),
        label("quit"),
        Span::styled(
            format!("  {}", app.state.status),
            Style::default().fg(mutefg()),
        ),
    ]);
    frame.render_widget(
        Paragraph::new(help).style(Style::default().bg(panel_bg())),
        area,
    );
}

fn render_editor_modal(frame: &mut Frame<'_>, editor: &EditorModal, area: Rect) {
    let popup = centered_rect(72, 45, area);
    frame.render_widget(Clear, popup);

    let editor_border = Color::Rgb(124, 214, 167);
    let title = format!(" ✎ Edit {} ", editor.column_name);
    let block = modal_block(&title, editor_border);
    let inner = block.inner(popup);
    frame.render_widget(block, popup);

    let [meta_area, body_area, hint_area] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(4),
        Constraint::Length(1),
    ])
    .areas(inner);

    let meta = if editor.unique_values.is_empty() {
        format!("type: {}", kind_name(&editor.column_kind))
    } else {
        format!(
            "type: {}   values: {}   focus: {}",
            kind_name(&editor.column_kind),
            editor.unique_values.len(),
            if editor.focus == EditorFocus::Values {
                "picker"
            } else {
                "input"
            }
        )
    };
    frame.render_widget(
        Paragraph::new(meta).style(Style::default().fg(mutefg())),
        meta_area,
    );

    let mut display = editor.value.clone();
    display.insert(editor.cursor, '│');
    if editor.unique_values.is_empty() {
        frame.render_widget(
            Paragraph::new(display)
                .style(Style::default().fg(fg()))
                .wrap(Wrap { trim: false }),
            body_area,
        );
    } else {
        let [values_area, divider_area, input_area] = Layout::horizontal([
            Constraint::Length(24),
            Constraint::Length(1),
            Constraint::Min(12),
        ])
        .areas(body_area);
        let value_lines = editor
            .unique_values
            .iter()
            .enumerate()
            .map(|(index, option)| {
                let prefix = if index == editor.selected_value {
                    "›"
                } else {
                    " "
                };
                let style = if index == editor.selected_value && editor.focus == EditorFocus::Values
                {
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::Rgb(255, 210, 110))
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(fg())
                };
                let available = values_area.width as usize - prefix.len() - 1;
                let value = clip_with_ellipsis(&option.value, available.max(1));
                Line::from(Span::styled(format!("{prefix} {value}"), style))
            })
            .collect::<Vec<_>>();

        frame.render_widget(
            Paragraph::new(Text::from(value_lines))
                .style(Style::default().fg(fg()))
                .wrap(Wrap { trim: false }),
            values_area,
        );
        let divider_full_area = Rect::new(
            divider_area.x,
            divider_area.y,
            divider_area.width,
            divider_area.height + hint_area.height,
        );
        let divider_lines = (0..divider_full_area.height)
            .map(|_| Line::from(Span::styled("│", Style::default().fg(editor_border))))
            .collect::<Vec<_>>();
        frame.render_widget(
            Paragraph::new(Text::from(divider_lines)).wrap(Wrap { trim: false }),
            divider_full_area,
        );
        frame.render_widget(
            Paragraph::new(display)
                .style(if editor.focus == EditorFocus::Input {
                    Style::default().fg(fg()).bg(Color::Rgb(45, 49, 58))
                } else {
                    Style::default().fg(fg())
                })
                .wrap(Wrap { trim: false }),
            input_area,
        );
    }

    frame.render_widget(
        Paragraph::new(if editor.unique_values.is_empty() {
            "Enter save  Alt+Enter newline  Esc cancel  arrows stay in editor"
        } else {
            "Tab switch picker/input  Enter save  Alt+Enter newline  Esc cancel  arrows stay in popup"
        })
            .style(Style::default().fg(mutefg())),
        hint_area,
    );
}

fn render_date_picker_modal(frame: &mut Frame<'_>, picker: &DatePickerModal, area: Rect) {
    let popup_width = 30;
    let popup =
        centered_absolute_rect(popup_width, if picker.include_time { 13 } else { 11 }, area);
    frame.render_widget(Clear, popup);

    let title = format!(" 🗓 Pick {} ", picker.column_name);
    let block = modal_block(&title, Color::Rgb(255, 210, 110));
    let inner = block.inner(popup);
    frame.render_widget(block, popup);

    let [
        fields_area,
        month_divider_area,
        days_area,
        time_divider_area,
        time_area,
    ] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Length(7),
        Constraint::Length(if picker.include_time { 1 } else { 0 }),
        Constraint::Length(if picker.include_time { 1 } else { 0 }),
    ])
    .areas(inner);

    let field_style = |active: bool| {
        if active {
            Style::default()
                .fg(Color::Black)
                .bg(Color::Rgb(255, 210, 110))
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(fg())
        }
    };
    let fields = Line::from(vec![
        Span::styled("D ", Style::default().fg(mutefg())),
        Span::styled(
            format!("{:02} ", picker.selected.day()),
            field_style(picker.focus == DatePickerFocus::Day),
        ),
        Span::styled("M ", Style::default().fg(mutefg())),
        Span::styled(
            format!("{} ", picker.selected.format("%B")),
            field_style(picker.focus == DatePickerFocus::Month),
        ),
        Span::styled("Y ", Style::default().fg(mutefg())),
        Span::styled(
            format!("{}", picker.selected.year()),
            field_style(picker.focus == DatePickerFocus::Year),
        ),
    ]);
    frame.render_widget(Paragraph::new(fields), fields_area);
    frame.render_widget(
        Paragraph::new("─".repeat(month_divider_area.width as usize))
            .style(Style::default().fg(border())),
        month_divider_area,
    );

    let mut lines = Vec::new();
    lines.push(Line::from(vec![
        Span::styled(" Mo ", Style::default().fg(mutefg())),
        Span::styled(" Tu ", Style::default().fg(mutefg())),
        Span::styled(" We ", Style::default().fg(mutefg())),
        Span::styled(" Th ", Style::default().fg(mutefg())),
        Span::styled(" Fr ", Style::default().fg(mutefg())),
        Span::styled(" Sa ", Style::default().fg(mutefg())),
        Span::styled(" Su ", Style::default().fg(mutefg())),
    ]));

    for week in build_calendar_lines(picker.selected, picker.focus == DatePickerFocus::Grid) {
        lines.push(Line::from(week));
    }

    frame.render_widget(
        Paragraph::new(Text::from(lines)).style(Style::default().fg(fg())),
        days_area,
    );

    if picker.include_time {
        frame.render_widget(
            Paragraph::new("─".repeat(time_divider_area.width as usize))
                .style(Style::default().fg(border())),
            time_divider_area,
        );
        let time_fields = Line::from(vec![
            Span::styled("H ", Style::default().fg(mutefg())),
            Span::styled(
                format!("{:02} ", picker.hour),
                field_style(picker.focus == DatePickerFocus::Hour),
            ),
            Span::styled("M ", Style::default().fg(mutefg())),
            Span::styled(
                format!("{:02} ", picker.minute),
                field_style(picker.focus == DatePickerFocus::Minute),
            ),
            Span::styled("S ", Style::default().fg(mutefg())),
            Span::styled(
                format!("{:02}", picker.second),
                field_style(picker.focus == DatePickerFocus::Second),
            ),
        ]);
        frame.render_widget(Paragraph::new(time_fields), time_area);
    }
}

fn render_filter_modal(frame: &mut Frame<'_>, app: &App, filter: &FilterModal, area: Rect) {
    let popup = centered_absolute_rect(62, 18, area);
    frame.render_widget(Clear, popup);

    let block = modal_block(" Filter ", accent());
    let inner = block.inner(popup);
    frame.render_widget(block, popup);

    let [header_area, stack_area, input_area, values_area, hint_area] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(3),
        Constraint::Length(2),
        Constraint::Min(4),
        Constraint::Length(2),
    ])
    .areas(inner);

    let join = match filter.join {
        FilterJoin::And => "AND",
        FilterJoin::Or => "OR",
    };
    frame.render_widget(
        Paragraph::new(format!(
            "column: {}   join-next: {}   wildcards: * ?",
            app.state.table.columns[filter.column].name, join
        ))
        .style(Style::default().fg(fg())),
        header_area,
    );

    let stack = if app.state.active_filters.is_empty() {
        "pending clauses: none".to_owned()
    } else {
        app.state
            .active_filters
            .iter()
            .enumerate()
            .map(|(index, clause)| {
                let join = if index == 0 {
                    "WHERE"
                } else {
                    match clause.join {
                        FilterJoin::And => "AND",
                        FilterJoin::Or => "OR",
                    }
                };
                format!(
                    "{join} {} ~ {}",
                    app.state.table.columns[clause.column].name, clause.pattern
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    frame.render_widget(
        Paragraph::new(stack)
            .style(Style::default().fg(mutefg()))
            .wrap(Wrap { trim: false }),
        stack_area,
    );

    let mut draft = filter.draft_pattern.clone();
    draft.insert(filter.cursor, '│');
    frame.render_widget(
        Paragraph::new(format!("pattern: {draft}"))
            .style(Style::default().fg(fg()))
            .wrap(Wrap { trim: false }),
        input_area,
    );

    let value_lines = if filter.unique_values.is_empty() {
        vec![Line::from(Span::styled(
            "No low-cardinality set here. Type free text, * wildcard, or ? single-char wildcard.",
            Style::default().fg(mutefg()),
        ))]
    } else {
        filter
            .unique_values
            .iter()
            .enumerate()
            .map(|(index, option)| {
                let prefix = if index == filter.selected_value {
                    "›"
                } else {
                    " "
                };
                let style = if index == filter.selected_value {
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::Rgb(255, 210, 110))
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(fg())
                };
                Line::from(Span::styled(
                    format!("{prefix} {} ({})", option.value, option.count),
                    style,
                ))
            })
            .collect()
    };
    frame.render_widget(Paragraph::new(Text::from(value_lines)), values_area);

    frame.render_widget(
        Paragraph::new(
            "Enter apply+close  + add clause  Tab AND/OR  PgUp/PgDn column  Up/Down values  c clear",
        )
        .style(Style::default().fg(mutefg())),
        hint_area,
    );
}

fn render_help_modal(frame: &mut Frame<'_>, area: Rect) {
    let popup = centered_rect(68, 48, area);
    frame.render_widget(Clear, popup);

    let block = modal_block(" Help ", accent());
    let inner = block.inner(popup);
    frame.render_widget(block, popup);

    let lines = vec![
        Line::from("sqlight layout"),
        Line::from(""),
        Line::from("• left sidebar = table selector with scrollbar"),
        Line::from("• center pane = smart-width data grid with vertical/horizontal scrollbars"),
        Line::from("• preview bar = full selected-cell value"),
        Line::from("• help bar = always-visible shortcut legend"),
        Line::from(""),
        Line::from("mouse"),
        Line::from("• click table in sidebar"),
        Line::from("• click header to cycle sort"),
        Line::from("• click cell to select"),
        Line::from("• double click cell to edit"),
        Line::from("• wheel scrolls visible panel"),
        Line::from("• shift+wheel scrolls horizontally"),
        Line::from("• drag horizontal scrollbar"),
        Line::from(""),
        Line::from("editing"),
        Line::from("• bool values render as checkboxes and toggle on Enter"),
        Line::from("• date/date-time values open calendar popup"),
        Line::from("• text/numeric values open editor popup"),
        Line::from("• F opens filter dialog with wildcard search and unique-value picks"),
        Line::from("• c clears active filters immediately from grid view"),
        Line::from("• j jumps through foreign keys in demo tables"),
        Line::from(""),
        Line::from("Esc or ? closes help"),
    ];

    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .style(Style::default().fg(fg()))
            .wrap(Wrap { trim: false }),
        inner,
    );
}

fn build_calendar_lines(
    selected: chrono::NaiveDate,
    grid_focused: bool,
) -> Vec<Vec<Span<'static>>> {
    let first = chrono::NaiveDate::from_ymd_opt(selected.year(), selected.month(), 1)
        .expect("valid first day");
    let last = last_day_of_month(selected.year(), selected.month());
    let mut day = 1u32;
    let first_weekday = weekday_offset(first.weekday());
    let mut lines = Vec::new();

    for week_index in 0..6 {
        let mut spans = Vec::new();
        for weekday in 0..7 {
            let cell_index = week_index * 7 + weekday;
            if cell_index < first_weekday || day > last.day() {
                spans.push(Span::styled("    ", Style::default().fg(mutefg())));
                continue;
            }

            let current = chrono::NaiveDate::from_ymd_opt(selected.year(), selected.month(), day)
                .expect("valid calendar day");
            let text = format!("{day:>2}  ");
            let style = if current == selected && grid_focused {
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Rgb(255, 210, 110))
                    .add_modifier(Modifier::BOLD)
            } else if current == selected {
                Style::default()
                    .fg(Color::Rgb(255, 210, 110))
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(fg())
            };
            spans.push(Span::styled(text, style));
            day += 1;
        }
        lines.push(spans);
    }

    lines
}

fn weekday_offset(weekday: Weekday) -> usize {
    match weekday {
        Weekday::Mon => 0,
        Weekday::Tue => 1,
        Weekday::Wed => 2,
        Weekday::Thu => 3,
        Weekday::Fri => 4,
        Weekday::Sat => 5,
        Weekday::Sun => 6,
    }
}

fn last_day_of_month(year: i32, month: u32) -> chrono::NaiveDate {
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    chrono::NaiveDate::from_ymd_opt(next_year, next_month, 1)
        .expect("valid next month")
        .pred_opt()
        .expect("previous day exists")
}

fn panel_block(title: &str, border_color: Color) -> Block<'static> {
    Block::default()
        .borders(Borders::ALL)
        .title(title.to_owned())
        .border_style(Style::default().fg(border_color))
        .style(Style::default().bg(panel_bg()))
}

fn modal_block(title: &str, border_color: Color) -> Block<'static> {
    Block::default()
        .borders(Borders::ALL)
        .title(title.to_owned())
        .border_style(Style::default().fg(border_color))
        .style(Style::default().bg(bg()))
}

fn keycap(key: &str) -> Span<'static> {
    Span::styled(
        format!(" {key} "),
        Style::default()
            .fg(Color::Black)
            .bg(Color::Rgb(170, 200, 255))
            .add_modifier(Modifier::BOLD),
    )
}

fn label(text: &str) -> Span<'static> {
    Span::styled(format!(" {text}  "), Style::default().fg(fg()))
}

fn type_icon(kind: &ColumnKind) -> &'static str {
    match kind {
        ColumnKind::Boolean => "☑",
        ColumnKind::Integer => "№",
        ColumnKind::Float => "∿",
        ColumnKind::Date => "🗓",
        ColumnKind::DateTime => "◷",
        ColumnKind::ForeignKeyId => "↗",
        ColumnKind::ShortText | ColumnKind::LongText | ColumnKind::TextHeavy => "≣",
        ColumnKind::Unknown => "·",
    }
}

fn table_icon() -> &'static str {
    "▤"
}

fn kind_name(kind: &ColumnKind) -> &'static str {
    match kind {
        ColumnKind::Boolean => "boolean",
        ColumnKind::Integer => "integer",
        ColumnKind::Float => "float",
        ColumnKind::Date => "date",
        ColumnKind::DateTime => "datetime",
        ColumnKind::ForeignKeyId => "foreign key",
        ColumnKind::ShortText => "short text",
        ColumnKind::LongText => "long text",
        ColumnKind::TextHeavy => "text heavy",
        ColumnKind::Unknown => "unknown",
    }
}

fn display_cell(value: &str, kind: &ColumnKind) -> String {
    match kind {
        ColumnKind::Boolean => match parse_bool_text(value) {
            Some(true) => "▣".to_owned(),
            Some(false) => "▢".to_owned(),
            None => "·".to_owned(),
        },
        _ => value.to_owned(),
    }
}

fn sort_marker(sort: Option<crate::state::SortState>, column_index: usize) -> &'static str {
    match sort {
        Some(sort) if sort.column == column_index && sort.direction == SortDirection::Asc => "▲",
        Some(sort) if sort.column == column_index && sort.direction == SortDirection::Desc => "▼",
        _ => "◇",
    }
}

#[derive(Clone, Copy)]
enum Align {
    Left,
    Center,
    Right,
}

fn alignment_for(kind: &ColumnKind) -> Align {
    match kind {
        ColumnKind::Boolean => Align::Center,
        ColumnKind::Integer | ColumnKind::Float | ColumnKind::ForeignKeyId => Align::Right,
        _ => Align::Left,
    }
}

fn fit_cell(text: &str, width: u16, align: Align) -> String {
    if width == 0 {
        return String::new();
    }
    if width <= 2 {
        return clip_with_ellipsis(text, width as usize);
    }

    let inner_width = width as usize - 2;
    let clipped = clip_with_ellipsis(text, inner_width);
    let used = clipped.width();
    let remaining = inner_width.saturating_sub(used);
    let padded = match align {
        Align::Left => format!("{clipped}{}", " ".repeat(remaining)),
        Align::Right => format!("{}{clipped}", " ".repeat(remaining)),
        Align::Center => {
            let left = remaining / 2;
            let right = remaining - left;
            format!("{}{}{clipped}", " ".repeat(left), " ".repeat(right))
        }
    };
    format!(" {padded} ")
}

fn clip_with_ellipsis(text: &str, width: usize) -> String {
    if width == 0 {
        return String::new();
    }
    if text.width() <= width {
        return text.to_owned();
    }
    if width <= 3 {
        return ".".repeat(width);
    }

    let mut used = 0usize;
    let mut result = String::new();
    let limit = width - 3;
    for ch in text.chars() {
        let ch_width = ch.width().unwrap_or(0);
        if used + ch_width > limit {
            break;
        }
        used += ch_width;
        result.push(ch);
    }
    result.push_str("...");
    result
}

fn centered_rect(width_percent: u16, height_percent: u16, area: Rect) -> Rect {
    let [_, vertical, _] = Layout::vertical([
        Constraint::Percentage((100 - height_percent) / 2),
        Constraint::Percentage(height_percent),
        Constraint::Percentage((100 - height_percent) / 2),
    ])
    .areas(area);
    let [_, horizontal, _] = Layout::horizontal([
        Constraint::Percentage((100 - width_percent) / 2),
        Constraint::Percentage(width_percent),
        Constraint::Percentage((100 - width_percent) / 2),
    ])
    .areas(vertical);
    horizontal
}

fn centered_absolute_rect(width: u16, height: u16, area: Rect) -> Rect {
    let width = width.min(area.width.saturating_sub(2)).max(1);
    let height = height.min(area.height.saturating_sub(2)).max(1);
    Rect::new(
        area.x + area.width.saturating_sub(width) / 2,
        area.y + area.height.saturating_sub(height) / 2,
        width,
        height,
    )
}

fn bg() -> Color {
    Color::Rgb(28, 31, 38)
}

fn panel_bg() -> Color {
    Color::Rgb(37, 41, 49)
}

fn border() -> Color {
    Color::Rgb(88, 96, 110)
}

fn accent() -> Color {
    Color::Rgb(120, 188, 255)
}

fn accent_soft() -> Color {
    Color::Rgb(103, 119, 147)
}

fn fg() -> Color {
    Color::Rgb(232, 236, 241)
}

fn mutefg() -> Color {
    Color::Rgb(143, 151, 166)
}
