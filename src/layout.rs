use crate::{
    db::{ColumnKind, LoadedTable},
    state::{GridLayout, HorizontalScrollState},
};
use unicode_width::UnicodeWidthStr;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnWidthHints {
    pub header_width: u16,
    pub min_width: u16,
    pub preferred_width: u16,
    pub max_reasonable_width: u16,
    pub shrink_priority: u8,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct SampleStats {
    p50: u16,
    p90: u16,
}

pub fn plan_widths(
    table: &LoadedTable,
    viewport_width: u16,
    row_offset: usize,
    visible_row_count: usize,
    scroll: HorizontalScrollState,
) -> GridLayout {
    let hints = collect_hints(table, row_offset, visible_row_count);
    let mut widths = hints
        .iter()
        .map(|hint| hint.preferred_width)
        .collect::<Vec<_>>();
    let min_total = total_width(&hints.iter().map(|hint| hint.min_width).collect::<Vec<_>>());
    let preferred_total = total_width(&widths);
    let viewport = u32::from(viewport_width);

    if preferred_total > viewport {
        compress_widths(&hints, &mut widths, viewport);
    } else if preferred_total < viewport {
        expand_widths(&hints, &mut widths, viewport);
    }

    let final_total = total_width(&widths);

    GridLayout {
        column_widths: widths,
        total_width: final_total,
        viewport_width,
        horizontal_overflow: min_total > viewport,
        horizontal_scroll: scroll,
    }
}

pub fn visible_columns(
    widths: &[u16],
    first_visible_col: usize,
    viewport_width: u16,
) -> Vec<usize> {
    let mut used = 0u32;
    let mut visible = Vec::new();

    for index in first_visible_col..widths.len() {
        let needed = u32::from(widths[index]) + u32::from(!visible.is_empty());
        if !visible.is_empty() && used + needed > u32::from(viewport_width) {
            break;
        }

        if visible.is_empty() && needed > u32::from(viewport_width) {
            visible.push(index);
            break;
        }

        used += needed;
        visible.push(index);
    }

    visible
}

pub fn min_first_visible_for_selection(
    widths: &[u16],
    selected_col: usize,
    current_first_visible_col: usize,
    viewport_width: u16,
) -> usize {
    if selected_col < current_first_visible_col {
        return selected_col;
    }

    let mut first_visible_col = current_first_visible_col;
    while !visible_columns(widths, first_visible_col, viewport_width).contains(&selected_col)
        && first_visible_col < selected_col
    {
        first_visible_col += 1;
    }
    first_visible_col
}

fn collect_hints(
    table: &LoadedTable,
    row_offset: usize,
    visible_row_count: usize,
) -> Vec<ColumnWidthHints> {
    let sample_end = (row_offset + visible_row_count + 120).min(table.rows.len());
    let sample_rows = &table.rows[row_offset.min(table.rows.len())..sample_end];

    table
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let header_width = text_width(&column.name).max(1) as u16;
            let stats = sample_stats(sample_rows.iter().map(|row| row.cells[index].as_str()));
            let min_width = min_width_for_kind(&column.kind).max(header_width.min(12));
            let max_reasonable_width = max_width_for_kind(&column.kind).max(min_width);
            let preferred_width = preferred_width_for_kind(
                &column.kind,
                header_width,
                stats,
                min_width,
                max_reasonable_width,
            );

            ColumnWidthHints {
                header_width,
                min_width,
                preferred_width,
                max_reasonable_width,
                shrink_priority: shrink_priority_for_kind(&column.kind),
            }
        })
        .collect()
}

fn sample_stats<'a>(samples: impl Iterator<Item = &'a str>) -> SampleStats {
    let mut lengths = samples
        .filter_map(|sample| {
            let trimmed = sample.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(text_width(trimmed) as u16)
            }
        })
        .collect::<Vec<_>>();

    if lengths.is_empty() {
        return SampleStats::default();
    }

    lengths.sort_unstable();
    let p50 = percentile(&lengths, 50);
    let p90 = percentile(&lengths, 90);

    SampleStats { p50, p90 }
}

fn preferred_width_for_kind(
    kind: &ColumnKind,
    header_width: u16,
    stats: SampleStats,
    min_width: u16,
    max_width: u16,
) -> u16 {
    let preferred = match kind {
        ColumnKind::Boolean => 5,
        ColumnKind::Integer => header_width.max(stats.p90).max(6) + 2,
        ColumnKind::Float => header_width.max(stats.p90).max(8) + 2,
        ColumnKind::Date => header_width.max(10),
        ColumnKind::DateTime => header_width.max(19),
        ColumnKind::ForeignKeyId => header_width.max(stats.p90).max(8) + 2,
        ColumnKind::ShortText => header_width.max(stats.p50).max(stats.p90.min(18)) + 2,
        ColumnKind::LongText => header_width.max(stats.p50.max(14)).max(stats.p90.min(28)) + 2,
        ColumnKind::TextHeavy => header_width.max(stats.p50.max(18)).max(stats.p90.min(46)) + 2,
        ColumnKind::Unknown => header_width.max(stats.p50.max(8)) + 2,
    };

    preferred.clamp(min_width, max_width)
}

fn min_width_for_kind(kind: &ColumnKind) -> u16 {
    match kind {
        ColumnKind::Boolean => 5,
        ColumnKind::Integer | ColumnKind::Float | ColumnKind::Date | ColumnKind::ForeignKeyId => 8,
        ColumnKind::DateTime => 16,
        ColumnKind::ShortText | ColumnKind::Unknown => 10,
        ColumnKind::LongText => 12,
        ColumnKind::TextHeavy => 14,
    }
}

fn max_width_for_kind(kind: &ColumnKind) -> u16 {
    match kind {
        ColumnKind::Boolean => 5,
        ColumnKind::Integer => 12,
        ColumnKind::Float => 14,
        ColumnKind::Date => 12,
        ColumnKind::DateTime => 21,
        ColumnKind::ForeignKeyId => 14,
        ColumnKind::ShortText => 20,
        ColumnKind::LongText => 32,
        ColumnKind::TextHeavy => 50,
        ColumnKind::Unknown => 24,
    }
}

fn shrink_priority_for_kind(kind: &ColumnKind) -> u8 {
    match kind {
        ColumnKind::Boolean => 0,
        ColumnKind::Integer | ColumnKind::Float => 1,
        ColumnKind::Date | ColumnKind::DateTime => 2,
        ColumnKind::ForeignKeyId => 3,
        ColumnKind::ShortText | ColumnKind::Unknown => 4,
        ColumnKind::LongText => 5,
        ColumnKind::TextHeavy => 6,
    }
}

fn compress_widths(hints: &[ColumnWidthHints], widths: &mut [u16], viewport: u32) {
    while total_width(widths) > viewport {
        let mut changed = false;

        for priority in 0..=6 {
            for (index, hint) in hints.iter().enumerate() {
                if hint.shrink_priority != priority || widths[index] <= hint.min_width {
                    continue;
                }

                widths[index] -= 1;
                changed = true;

                if total_width(widths) <= viewport {
                    return;
                }
            }
        }

        if !changed {
            return;
        }
    }
}

fn expand_widths(hints: &[ColumnWidthHints], widths: &mut [u16], viewport: u32) {
    let order = [6, 5, 4, 3, 2, 1, 0];
    while total_width(widths) < viewport {
        let mut changed = false;

        for priority in order {
            for (index, hint) in hints.iter().enumerate() {
                if hint.shrink_priority != priority || widths[index] >= hint.max_reasonable_width {
                    continue;
                }

                widths[index] += 1;
                changed = true;

                if total_width(widths) >= viewport {
                    return;
                }
            }
        }

        if !changed {
            return;
        }
    }
}

fn total_width(widths: &[u16]) -> u32 {
    widths.iter().map(|width| u32::from(*width)).sum::<u32>()
        + widths.len().saturating_sub(1) as u32
}

fn percentile(values: &[u16], pct: usize) -> u16 {
    let index = ((values.len() - 1) * pct) / 100;
    values[index]
}

fn text_width(text: &str) -> usize {
    UnicodeWidthStr::width(text)
}

#[cfg(test)]
mod tests {
    use crate::db::{ColumnKind, ColumnSpec, LoadedTable, RowRecord};
    use crate::state::HorizontalScrollState;

    use super::plan_widths;

    fn sample_table() -> LoadedTable {
        LoadedTable {
            name: "items".to_owned(),
            columns: vec![
                ColumnSpec {
                    name: "flag".to_owned(),
                    declared_type: Some("BOOLEAN".to_owned()),
                    kind: ColumnKind::Boolean,
                    is_foreign_key: false,
                    referenced_table: None,
                    referenced_column: None,
                },
                ColumnSpec {
                    name: "id".to_owned(),
                    declared_type: Some("INTEGER".to_owned()),
                    kind: ColumnKind::Integer,
                    is_foreign_key: false,
                    referenced_table: None,
                    referenced_column: None,
                },
                ColumnSpec {
                    name: "notes".to_owned(),
                    declared_type: Some("TEXT".to_owned()),
                    kind: ColumnKind::TextHeavy,
                    is_foreign_key: false,
                    referenced_table: None,
                    referenced_column: None,
                },
            ],
            rows: vec![
                RowRecord {
                    rowid: Some(1),
                    cells: vec![
                        "true".to_owned(),
                        "1".to_owned(),
                        "very long note field that should keep more width than tiny columns"
                            .to_owned(),
                    ],
                },
                RowRecord {
                    rowid: Some(2),
                    cells: vec![
                        "false".to_owned(),
                        "2".to_owned(),
                        "another long note field".to_owned(),
                    ],
                },
            ],
        }
    }

    #[test]
    fn shrinks_short_columns_before_text_columns() {
        let table = sample_table();
        let layout = plan_widths(&table, 28, 0, 10, HorizontalScrollState::default());

        assert!(layout.column_widths[0] <= 5);
        assert!(layout.column_widths[1] <= 8);
        assert!(layout.column_widths[2] >= 14);
    }

    #[test]
    fn enables_horizontal_overflow_only_after_min_widths_fail() {
        let table = sample_table();
        let layout = plan_widths(&table, 20, 0, 10, HorizontalScrollState::default());

        assert!(layout.horizontal_overflow);
        assert!(layout.total_width > u32::from(layout.viewport_width));
    }

    #[test]
    fn long_boolean_headers_never_panic_width_clamp() {
        let table = LoadedTable {
            name: "flags".to_owned(),
            columns: vec![ColumnSpec {
                name: "is_enabled".to_owned(),
                declared_type: Some("BOOLEAN".to_owned()),
                kind: ColumnKind::Boolean,
                is_foreign_key: false,
                referenced_table: None,
                referenced_column: None,
            }],
            rows: vec![RowRecord {
                rowid: Some(1),
                cells: vec!["true".to_owned()],
            }],
        };

        let layout = plan_widths(&table, 40, 0, 5, HorizontalScrollState::default());
        assert_eq!(layout.column_widths, vec![10]);
    }
}
