use std::collections::HashMap;

use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{block::BorderType, Block, Borders, Paragraph},
    Frame,
};
use unicode_width::UnicodeWidthChar;

use crate::{
    config::Config,
    db::{
        schema::Column,
        types::{affinity, ColAffinity, SqlValue},
    },
    grid::layout,
    theme::Theme,
};

use super::search_result_format::format_search_result_text;

const SELECT_COL_WIDTH: usize = 2;

pub struct FkPickerState {
    pub target_table: String,
    #[allow(dead_code)]
    pub target_col: String,
    pub display_cols: Vec<String>,
    pub rows: Vec<Vec<SqlValue>>,
    pub filter: String,
    pub selected: usize,
    pub source_table: String,
    pub source_col: String,
    pub source_rowid: i64,
    pub loading: bool,
    pub original: crate::db::types::SqlValue,
}

pub struct RowHit {
    pub row_index: usize,
    pub matched_ranges: Vec<Option<(usize, usize)>>,
}

#[derive(Clone, Copy)]
struct HighlightStyles {
    base: Style,
    matched: Style,
}

#[derive(Clone, Copy)]
struct TableRuleGlyphs {
    middle: char,
}

impl FkPickerState {
    pub fn new(
        target_table: String,
        target_col: String,
        display_cols: Vec<String>,
        source_table: String,
        source_col: String,
        source_rowid: i64,
        original: crate::db::types::SqlValue,
    ) -> Self {
        Self {
            target_table,
            target_col,
            display_cols,
            rows: Vec::new(),
            filter: String::new(),
            selected: 0,
            source_table,
            source_col,
            source_rowid,
            loading: true,
            original,
        }
    }

    pub fn move_up(&mut self) {
        self.selected = self.selected.saturating_sub(1);
    }

    pub fn move_down(&mut self) {
        let visible = self.visible_rows();
        if self.selected + 1 < visible.len() {
            self.selected += 1;
        }
    }

    pub fn push_filter_char(&mut self, ch: char) {
        self.filter.push(ch);
        self.selected = 0;
    }

    pub fn pop_filter_char(&mut self) {
        self.filter.pop();
        self.selected = 0;
    }

    pub fn selected_value(&self) -> Option<&SqlValue> {
        let hit = self.visible_rows().get(self.selected)?.row_index;
        self.rows.get(hit)?.first()
    }

    pub fn visible_rows(&self) -> Vec<RowHit> {
        if self.filter.trim().is_empty() {
            return self
                .rows
                .iter()
                .enumerate()
                .map(|(row_index, row)| RowHit {
                    row_index,
                    matched_ranges: vec![None; row.len()],
                })
                .collect();
        }

        let needle = self.filter.trim().to_lowercase();
        self.rows
            .iter()
            .enumerate()
            .filter_map(|(row_index, row)| {
                self.match_row(row, &needle).map(|matched_ranges| RowHit {
                    row_index,
                    matched_ranges,
                })
            })
            .collect()
    }

    fn visible_column_indices(&self, visible_rows: &[RowHit]) -> Vec<usize> {
        let total_columns = self.column_headers().len();
        let numeric_search = search_prefers_numeric(&self.filter);
        let allowed = |col_idx: usize| {
            col_idx == 0 || numeric_search || !is_numeric_or_temporal_column(self, col_idx)
        };

        if self.filter.trim().is_empty() || visible_rows.is_empty() {
            let columns: Vec<usize> = (0..total_columns)
                .filter(|&col_idx| allowed(col_idx))
                .collect();
            return if columns.is_empty() { vec![0] } else { columns };
        }

        let last_matched = visible_rows
            .iter()
            .flat_map(|row| {
                row.matched_ranges
                    .iter()
                    .enumerate()
                    .filter_map(|(col_idx, matched)| matched.map(|_| col_idx))
            })
            .max();

        if let Some(last_matched) = last_matched {
            let columns: Vec<usize> = (0..=last_matched)
                .filter(|&col_idx| allowed(col_idx))
                .collect();
            if columns.is_empty() {
                vec![0]
            } else {
                columns
            }
        } else {
            let columns: Vec<usize> = (0..total_columns)
                .filter(|&col_idx| allowed(col_idx))
                .collect();
            if columns.is_empty() {
                vec![0]
            } else {
                columns
            }
        }
    }

    fn column_headers(&self) -> Vec<String> {
        let mut headers = Vec::with_capacity(self.display_cols.len() + 1);
        headers.push(self.target_col.clone());
        headers.extend(self.display_cols.iter().cloned());
        headers
    }

    fn match_row(&self, row: &[SqlValue], needle: &str) -> Option<Vec<Option<(usize, usize)>>> {
        let needle_len = needle.chars().count();
        let mut matched_ranges = Vec::with_capacity(row.len());
        let mut found = false;

        for value in row {
            let display = value_to_display_string(value);
            let display_lower = display.to_lowercase();
            let matched = display_lower.find(needle).map(|byte_start| {
                let start = display_lower[..byte_start].chars().count();
                found = true;
                (start, start + needle_len)
            });
            matched_ranges.push(matched);
        }

        found.then_some(matched_ranges)
    }
}

pub fn render(
    frame: &mut Frame,
    area: Rect,
    state: &FkPickerState,
    theme: &Theme,
    _config: &Config,
) {
    let popup_width = ((area.width * 3) / 5).max(54).min(area.width);
    let popup_height = ((area.height * 3) / 5).max(12).min(area.height);
    let x = area.x + (area.width.saturating_sub(popup_width)) / 2;
    let y = area.y + (area.height.saturating_sub(popup_height)) / 2;
    let popup_area = Rect {
        x,
        y,
        width: popup_width,
        height: popup_height,
    };

    super::paint_popup_surface(frame, popup_area, theme);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(theme.accent))
        .title(format!(
            " FK: {} → {} ",
            state.source_col, state.target_table
        ))
        .style(Style::default().bg(theme.bg_raised));
    let inner = block.inner(popup_area);
    frame.render_widget(block, popup_area);

    if state.loading {
        frame.render_widget(
            Paragraph::new(" Loading…")
                .style(Style::default().fg(theme.fg_dim).bg(theme.bg_raised)),
            inner,
        );
        return;
    }

    if inner.height == 0 || inner.width == 0 {
        return;
    }

    let hint_area = Rect {
        x: inner.x,
        y: inner.y,
        width: inner.width,
        height: 1,
    };
    let input_area = Rect {
        x: inner.x,
        y: inner.y.saturating_add(1),
        width: inner.width,
        height: 1,
    };
    let footer_area = Rect {
        x: inner.x,
        y: inner.y + inner.height.saturating_sub(1),
        width: inner.width,
        height: 1,
    };

    frame.render_widget(
        Paragraph::new(Line::from(Span::styled(
            " Type to filter related rows. Matching columns stay in view.",
            Style::default().fg(theme.fg_faint),
        )))
        .style(Style::default().bg(theme.bg_raised)),
        hint_area,
    );

    let filter_line = Line::from(vec![
        Span::styled(" Search: ", Style::default().fg(theme.fg_dim)),
        Span::styled(
            format!(" {} ", state.filter),
            Style::default().fg(theme.fg).bg(theme.bg_soft),
        ),
        Span::styled("▌", Style::default().fg(theme.accent).bg(theme.bg_soft)),
    ]);
    frame.render_widget(
        Paragraph::new(filter_line).style(Style::default().bg(theme.bg_raised)),
        input_area,
    );

    let table_top_y = inner.y + 2;
    if footer_area.y > table_top_y {
        let visible_rows = state.visible_rows();
        let visible_columns = state.visible_column_indices(&visible_rows);
        render_result_table(
            frame.buffer_mut(),
            Rect {
                x: popup_area.x,
                y: table_top_y,
                width: popup_area.width,
                height: footer_area.y - table_top_y,
            },
            state,
            &visible_rows,
            &visible_columns,
            theme,
        );
    }

    frame.render_widget(
        Paragraph::new(Line::from(Span::styled(
            " ↵ select · esc cancel",
            Style::default().fg(theme.fg_faint),
        )))
        .style(Style::default().bg(theme.bg_raised)),
        footer_area,
    );
}

fn render_result_table(
    buf: &mut Buffer,
    table_area: Rect,
    state: &FkPickerState,
    visible_rows: &[RowHit],
    visible_columns: &[usize],
    theme: &Theme,
) {
    if table_area.width <= 2 || table_area.height < 3 || visible_columns.is_empty() {
        return;
    }

    let inner_available = table_area
        .width
        .saturating_sub(2)
        .saturating_sub(SELECT_COL_WIDTH as u16) as usize;
    if inner_available == 0 {
        return;
    }

    let headers = state.column_headers();
    let initial_data_width = inner_available.saturating_sub(visible_columns.len());
    let all_widths = compute_column_widths(
        &headers,
        visible_rows,
        visible_columns,
        state,
        initial_data_width,
    );
    let fit_count = clipped_visible_count(&all_widths, inner_available);
    let visible_columns = &visible_columns[..fit_count];
    let data_width = inner_available.saturating_sub(visible_columns.len());
    let column_widths =
        compute_column_widths(&headers, visible_rows, visible_columns, state, data_width);
    let separator_positions =
        compute_separator_positions(table_area.x + 1, &column_widths, SELECT_COL_WIDTH);

    render_table_rule(
        buf,
        table_area,
        table_area.y,
        &separator_positions,
        TableRuleGlyphs { middle: '┬' },
        theme,
    );

    let header_y = table_area.y + 1;
    fill_row(
        buf,
        Rect {
            x: table_area.x + 1,
            y: header_y,
            width: table_area.width.saturating_sub(2),
            height: 1,
        },
        Style::default().bg(theme.bg_raised),
    );
    render_vertical_separators(buf, header_y, &separator_positions, theme, theme.bg_raised);
    let mut cell_x = table_area.x + 1 + SELECT_COL_WIDTH as u16 + 1;
    for (&col_idx, &width) in visible_columns.iter().zip(&column_widths) {
        draw_truncated_text(
            buf,
            cell_x + 1,
            header_y,
            width.saturating_sub(1),
            &headers[col_idx],
            None,
            HighlightStyles {
                base: Style::default()
                    .fg(theme.fg)
                    .bg(theme.bg_raised)
                    .add_modifier(Modifier::BOLD),
                matched: Style::default()
                    .fg(theme.fg)
                    .bg(theme.bg_raised)
                    .add_modifier(Modifier::BOLD),
            },
        );
        cell_x += width as u16 + 1;
    }

    render_table_rule(
        buf,
        table_area,
        table_area.y + 2,
        &separator_positions,
        TableRuleGlyphs { middle: '┼' },
        theme,
    );

    let visible_result_rows = table_area.height.saturating_sub(3) as usize;
    if visible_result_rows == 0 {
        return;
    }

    let start = if state.selected >= visible_result_rows {
        state.selected - visible_result_rows + 1
    } else {
        0
    };

    if visible_rows.is_empty() {
        let row_y = table_area.y + 3;
        fill_row(
            buf,
            Rect {
                x: table_area.x + 1,
                y: row_y,
                width: table_area.width.saturating_sub(2),
                height: 1,
            },
            Style::default().bg(theme.bg_raised),
        );
        render_vertical_separators(buf, row_y, &separator_positions, theme, theme.bg_raised);
        draw_truncated_text(
            buf,
            table_area.x + 1 + SELECT_COL_WIDTH as u16 + 2,
            row_y,
            table_area
                .width
                .saturating_sub(SELECT_COL_WIDTH as u16)
                .saturating_sub(visible_columns.len() as u16)
                .saturating_sub(4) as usize,
            "No matches",
            None,
            HighlightStyles {
                base: Style::default().fg(theme.fg_faint).bg(theme.bg_raised),
                matched: Style::default().fg(theme.fg_faint).bg(theme.bg_raised),
            },
        );
        return;
    }

    for (view_idx, hit) in visible_rows
        .iter()
        .enumerate()
        .skip(start)
        .take(visible_result_rows)
    {
        let row_y = table_area.y + 3 + (view_idx - start) as u16;
        let is_selected = view_idx == state.selected;
        let bg = if is_selected {
            theme.bg_soft
        } else {
            theme.bg_raised
        };
        fill_row(
            buf,
            Rect {
                x: table_area.x + 1,
                y: row_y,
                width: table_area.width.saturating_sub(2),
                height: 1,
            },
            Style::default().bg(bg),
        );
        render_vertical_separators(buf, row_y, &separator_positions, theme, bg);
        if is_selected {
            buf.set_string(
                table_area.x + 1,
                row_y,
                "⏵",
                Style::default().fg(theme.accent).bg(bg),
            );
        }

        let row = &state.rows[hit.row_index];
        let mut cell_x = table_area.x + 1 + SELECT_COL_WIDTH as u16 + 1;
        for (&col_idx, &width) in visible_columns.iter().zip(&column_widths) {
            let display = row
                .get(col_idx)
                .map(value_to_display_string)
                .unwrap_or_else(|| "–".to_string());
            draw_truncated_text(
                buf,
                cell_x + 1,
                row_y,
                width.saturating_sub(1),
                &display,
                hit.matched_ranges.get(col_idx).copied().flatten(),
                HighlightStyles {
                    base: Style::default()
                        .fg(if is_selected { theme.fg } else { theme.fg_dim })
                        .bg(bg),
                    matched: Style::default()
                        .fg(theme.accent)
                        .bg(bg)
                        .add_modifier(Modifier::BOLD),
                },
            );
            cell_x += width as u16 + 1;
        }
    }
}

fn compute_column_widths(
    headers: &[String],
    visible_rows: &[RowHit],
    visible_columns: &[usize],
    state: &FkPickerState,
    available_width: usize,
) -> Vec<usize> {
    if visible_columns.is_empty() || available_width == 0 {
        return Vec::new();
    }
    if visible_columns.len() == 1 {
        return vec![available_width];
    }

    let columns: Vec<Column> = visible_columns
        .iter()
        .map(|&col_idx| synth_column(state, headers[col_idx].clone(), col_idx))
        .collect();
    let rows: Vec<Vec<SqlValue>> = visible_rows
        .iter()
        .map(|hit| {
            visible_columns
                .iter()
                .filter_map(|&col_idx| state.rows[hit.row_index].get(col_idx).cloned())
                .collect()
        })
        .collect();
    let fk_cols = vec![false; columns.len()];
    let widths = layout::compute_col_widths(
        &columns,
        &rows,
        available_width.min(u16::MAX as usize) as u16,
        &HashMap::new(),
        &fk_cols,
    );
    distribute_extra_width_like_grid(columns, widths, available_width)
}

fn clipped_visible_count(widths: &[usize], inner_available: usize) -> usize {
    let mut used = 0usize;
    let mut count = 0usize;
    for &width in widths {
        let needed = used + width + count + 1;
        if count == 0 || needed <= inner_available {
            used += width;
            count += 1;
        } else {
            break;
        }
    }
    count.max(1).min(widths.len())
}

fn compute_separator_positions(
    mut left: u16,
    column_widths: &[usize],
    select_width: usize,
) -> Vec<u16> {
    let mut separators = Vec::with_capacity(column_widths.len());
    left += select_width as u16;
    separators.push(left);
    left += 1;
    for (index, &width) in column_widths.iter().enumerate() {
        left += width as u16;
        if index + 1 < column_widths.len() {
            separators.push(left);
            left += 1;
        }
    }
    separators
}

fn render_table_rule(
    buf: &mut Buffer,
    table_area: Rect,
    y: u16,
    separator_positions: &[u16],
    glyphs: TableRuleGlyphs,
    theme: &Theme,
) {
    let left_x = table_area.x.saturating_add(1);
    let right_x = table_area.x + table_area.width.saturating_sub(2);
    if right_x < left_x {
        return;
    }

    for x in left_x..=right_x {
        let symbol = if separator_positions.contains(&x) {
            glyphs.middle
        } else {
            '─'
        };
        buf.set_string(
            x,
            y,
            symbol.to_string(),
            Style::default().fg(theme.line).bg(theme.bg_raised),
        );
    }
}

fn render_vertical_separators(
    buf: &mut Buffer,
    y: u16,
    separator_positions: &[u16],
    theme: &Theme,
    bg: ratatui::style::Color,
) {
    for &x in separator_positions {
        buf.set_string(x, y, "│", Style::default().fg(theme.line).bg(bg));
    }
}

fn fill_row(buf: &mut Buffer, area: Rect, style: Style) {
    if area.width == 0 || area.height == 0 {
        return;
    }
    buf.set_string(area.x, area.y, " ".repeat(area.width as usize), style);
}

fn truncated_spans<'a>(
    value: &'a str,
    matched_range: Option<(usize, usize)>,
    max_width: usize,
    base_style: Style,
    matched_style: Style,
) -> Vec<Span<'a>> {
    if max_width == 0 {
        return Vec::new();
    }

    let chars: Vec<(usize, char)> = value.chars().enumerate().collect();
    let total_width: usize = chars
        .iter()
        .map(|(_, ch)| UnicodeWidthChar::width(*ch).unwrap_or(1))
        .sum();
    let needs_ellipsis = total_width > max_width;
    let content_limit = if needs_ellipsis && max_width > 3 {
        max_width - 3
    } else {
        max_width
    };

    let mut spans = Vec::new();
    let mut used_width = 0usize;
    for (idx, ch) in chars {
        let ch_width = UnicodeWidthChar::width(ch).unwrap_or(1);
        if used_width + ch_width > content_limit {
            break;
        }
        let style = if matched_range.is_some_and(|(start, end)| idx >= start && idx < end) {
            matched_style
        } else {
            base_style
        };
        spans.push(Span::styled(ch.to_string(), style));
        used_width += ch_width;
    }

    if needs_ellipsis {
        spans.push(Span::styled("...".to_string(), base_style));
    }

    spans
}

fn draw_truncated_text(
    buf: &mut Buffer,
    x: u16,
    y: u16,
    max_width: usize,
    value: &str,
    matched_range: Option<(usize, usize)>,
    styles: HighlightStyles,
) {
    let mut cursor = x;
    for span in truncated_spans(value, matched_range, max_width, styles.base, styles.matched) {
        for ch in span.content.chars() {
            let ch_width = UnicodeWidthChar::width(ch).unwrap_or(1) as u16;
            buf.set_string(cursor, y, ch.to_string(), span.style);
            cursor += ch_width;
        }
    }
}

fn value_to_display_string(value: &SqlValue) -> String {
    match value {
        SqlValue::Null => "null".to_string(),
        SqlValue::Integer(n) => n.to_string(),
        SqlValue::Real(f) => f.to_string(),
        SqlValue::Text(s) => format_search_result_text(s),
        SqlValue::Blob(bytes) => format!("<blob {} bytes>", bytes.len()),
    }
}

fn search_prefers_numeric(filter: &str) -> bool {
    filter
        .trim_start()
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_digit())
}

fn is_numeric_or_temporal_column(state: &FkPickerState, col_idx: usize) -> bool {
    state
        .rows
        .iter()
        .filter_map(|row| row.get(col_idx))
        .any(is_number_or_temporal_value)
}

fn is_number_or_temporal_value(value: &SqlValue) -> bool {
    match value {
        SqlValue::Integer(_) | SqlValue::Real(_) => true,
        SqlValue::Text(text) => format_search_result_text(text) != *text,
        SqlValue::Null | SqlValue::Blob(_) => false,
    }
}

fn synth_column(state: &FkPickerState, header: String, col_idx: usize) -> Column {
    Column {
        cid: col_idx as i64,
        name: header,
        col_type: inferred_column_type(state, col_idx),
        not_null: false,
        default_value: None,
        is_pk: col_idx == 0,
    }
}

fn inferred_column_type(state: &FkPickerState, col_idx: usize) -> String {
    for value in state.rows.iter().filter_map(|row| row.get(col_idx)) {
        match value {
            SqlValue::Integer(_) => return "INTEGER".to_string(),
            SqlValue::Real(_) => return "REAL".to_string(),
            SqlValue::Text(_) | SqlValue::Null | SqlValue::Blob(_) => {}
        }
    }
    "TEXT".to_string()
}

fn distribute_extra_width_like_grid(
    columns: Vec<Column>,
    widths: Vec<u16>,
    available_width: usize,
) -> Vec<usize> {
    let mut widths: Vec<usize> = widths.into_iter().map(usize::from).collect();
    let total: usize = widths.iter().sum();
    if total >= available_width || widths.is_empty() {
        return widths;
    }

    let extra = available_width - total;
    let text_cols: Vec<usize> = columns
        .iter()
        .enumerate()
        .filter_map(|(idx, column)| {
            matches!(
                affinity(&column.col_type),
                ColAffinity::Text | ColAffinity::Blob
            )
            .then_some(idx)
        })
        .collect();

    let targets = if text_cols.is_empty() {
        vec![widths.len() - 1]
    } else {
        text_cols
    };

    let base = extra / targets.len();
    let remainder = extra % targets.len();
    for (idx, target) in targets.into_iter().enumerate() {
        widths[target] += base + usize::from(idx < remainder);
    }
    widths
}

#[cfg(test)]
mod tests {
    use super::{is_numeric_or_temporal_column, render_table_rule, FkPickerState, TableRuleGlyphs};
    use crate::{db::types::SqlValue, theme::Theme};
    use ratatui::{buffer::Buffer, layout::Rect};

    #[test]
    fn fk_picker_matches_full_substrings_only() {
        let mut state = FkPickerState::new(
            "users".to_string(),
            "id".to_string(),
            vec!["name".to_string()],
            "posts".to_string(),
            "user_id".to_string(),
            1,
            SqlValue::Integer(1),
        );
        state.loading = false;
        state.rows = vec![
            vec![SqlValue::Integer(1), SqlValue::Text("abc".to_string())],
            vec![SqlValue::Integer(2), SqlValue::Text("axbyc".to_string())],
            vec![SqlValue::Integer(3), SqlValue::Text("zabcx".to_string())],
        ];
        state.filter = "abc".to_string();

        let rows = state.visible_rows();
        let ids = rows
            .iter()
            .filter_map(|hit| match &state.rows[hit.row_index][0] {
                SqlValue::Integer(id) => Some(*id),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(ids, vec![1, 3]);
    }

    #[test]
    fn fk_picker_hides_non_matching_columns() {
        let mut state = FkPickerState::new(
            "users".to_string(),
            "id".to_string(),
            vec!["name".to_string(), "city".to_string()],
            "posts".to_string(),
            "user_id".to_string(),
            1,
            SqlValue::Integer(1),
        );
        state.loading = false;
        state.rows = vec![vec![
            SqlValue::Integer(1),
            SqlValue::Text("Alice".to_string()),
            SqlValue::Text("Berlin".to_string()),
        ]];
        state.filter = "ber".to_string();

        let rows = state.visible_rows();
        let visible = state.visible_column_indices(&rows);

        assert_eq!(visible, vec![0, 1, 2]);
    }

    #[test]
    fn fk_table_rules_stay_inside_popup_frame() {
        let mut buf = Buffer::empty(Rect::new(0, 0, 12, 4));
        let theme = Theme::default();

        render_table_rule(
            &mut buf,
            Rect::new(0, 0, 12, 4),
            0,
            &[3, 7],
            TableRuleGlyphs { middle: '┬' },
            &theme,
        );

        let left = buf.cell((0, 0)).unwrap();
        let middle = buf.cell((3, 0)).unwrap();
        let right = buf.cell((11, 0)).unwrap();
        let line = buf.cell((1, 0)).unwrap();

        assert_eq!(left.symbol(), " ");
        assert_eq!(middle.symbol(), "┬");
        assert_eq!(right.symbol(), " ");
        assert_eq!(middle.fg, theme.line);
        assert_eq!(line.fg, theme.line);
    }

    #[test]
    fn empty_match_set_falls_back_to_all_columns() {
        let state = FkPickerState::new(
            "users".to_string(),
            "id".to_string(),
            vec!["name".to_string(), "city".to_string()],
            "posts".to_string(),
            "user_id".to_string(),
            1,
            SqlValue::Integer(1),
        );

        let visible = state.visible_column_indices(&[]);

        assert_eq!(visible, vec![0, 1, 2]);
    }

    #[test]
    fn hides_numeric_and_temporal_columns_until_search_starts_with_number() {
        let mut state = FkPickerState::new(
            "users".to_string(),
            "id".to_string(),
            vec!["name".to_string(), "created_at".to_string()],
            "posts".to_string(),
            "user_id".to_string(),
            1,
            SqlValue::Integer(1),
        );
        state.rows = vec![vec![
            SqlValue::Integer(42),
            SqlValue::Text("Alice".to_string()),
            SqlValue::Text("2026-04-25".to_string()),
        ]];

        assert!(is_numeric_or_temporal_column(&state, 0));
        assert!(!is_numeric_or_temporal_column(&state, 1));
        assert!(is_numeric_or_temporal_column(&state, 2));
        assert_eq!(
            state.visible_column_indices(&state.visible_rows()),
            vec![0, 1]
        );

        state.filter = "2".to_string();

        assert_eq!(
            state.visible_column_indices(&state.visible_rows()),
            vec![0, 1, 2]
        );
    }
}
