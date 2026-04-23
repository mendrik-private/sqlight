use std::time::Instant;

use chrono::NaiveDate;
use ratatui::layout::Rect;

use crate::db::{ColumnKind, LoadedTable, RowRecord};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SortState {
    pub column: usize,
    pub direction: SortDirection,
}

impl SortState {
    pub fn next_for(current: Option<Self>, column: usize) -> Option<Self> {
        match current {
            Some(state) if state.column == column && state.direction == SortDirection::Asc => {
                Some(Self {
                    column,
                    direction: SortDirection::Desc,
                })
            }
            Some(state) if state.column == column && state.direction == SortDirection::Desc => None,
            _ => Some(Self {
                column,
                direction: SortDirection::Asc,
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HorizontalScrollState {
    pub first_visible_col: usize,
    pub x_char_offset: u16,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GridLayout {
    pub column_widths: Vec<u16>,
    pub total_width: u32,
    pub viewport_width: u16,
    pub horizontal_overflow: bool,
    pub horizontal_scroll: HorizontalScrollState,
}

#[derive(Clone, Debug)]
pub struct EditorModal {
    pub column_name: String,
    pub column_kind: ColumnKind,
    pub value: String,
    pub cursor: usize,
    pub unique_values: Vec<FilterValueOption>,
    pub selected_value: usize,
    pub focus: EditorFocus,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DatePickerFocus {
    Month,
    Day,
    Year,
    Grid,
    Hour,
    Minute,
    Second,
}

#[derive(Clone, Debug)]
pub struct DatePickerModal {
    pub column_name: String,
    pub include_time: bool,
    pub hour: u32,
    pub minute: u32,
    pub second: u32,
    pub selected: NaiveDate,
    pub focus: DatePickerFocus,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EditorFocus {
    Values,
    Input,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FilterJoin {
    And,
    Or,
}

#[derive(Clone, Debug)]
pub struct FilterClause {
    pub column: usize,
    pub pattern: String,
    pub join: FilterJoin,
}

#[derive(Clone, Debug)]
pub struct FilterValueOption {
    pub value: String,
    pub count: usize,
}

#[derive(Clone, Debug)]
pub struct FilterModal {
    pub column: usize,
    pub draft_pattern: String,
    pub cursor: usize,
    pub join: FilterJoin,
    pub selected_value: usize,
    pub unique_values: Vec<FilterValueOption>,
}

#[derive(Clone, Debug)]
pub enum ModalState {
    Editor(EditorModal),
    DatePicker(DatePickerModal),
    Filter(FilterModal),
    Help,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UiRegion {
    Cell { row: usize, col: usize },
    Header { col: usize },
    Breadcrumb { index: usize },
    TableItem { index: usize },
    TablePanel,
    HorizontalScrollbarTrack,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DragTarget {
    HorizontalScrollbar,
}

#[derive(Clone, Debug)]
pub struct ClickInfo {
    pub at: Instant,
    pub region: UiRegion,
}

#[derive(Clone, Debug, Default)]
pub struct MouseState {
    pub last_click: Option<ClickInfo>,
    pub hover_region: Option<UiRegion>,
    pub drag_target: Option<DragTarget>,
}

#[derive(Clone, Debug)]
pub struct Hitbox {
    pub area: Rect,
    pub region: UiRegion,
}

impl Hitbox {
    pub fn contains(&self, column: u16, row: u16) -> bool {
        let x_end = self.area.x.saturating_add(self.area.width);
        let y_end = self.area.y.saturating_add(self.area.height);
        column >= self.area.x && column < x_end && row >= self.area.y && row < y_end
    }
}

pub struct AppState {
    pub db_label: String,
    pub tables: Vec<String>,
    pub table_row_counts: Vec<usize>,
    pub current_table: usize,
    pub table: LoadedTable,
    pub source_rows: Vec<RowRecord>,
    pub selected_row: usize,
    pub selected_col: usize,
    pub row_offset: usize,
    pub viewport_rows: usize,
    pub side_scroll: usize,
    pub side_viewport_rows: usize,
    pub grid_layout: GridLayout,
    pub mouse_state: MouseState,
    pub hitboxes: Vec<Hitbox>,
    pub sort: Option<SortState>,
    pub active_filters: Vec<FilterClause>,
    pub status: String,
    pub modal: Option<ModalState>,
    pub should_quit: bool,
}

impl AppState {
    pub fn current_value(&self) -> Option<&str> {
        self.table
            .rows
            .get(self.selected_row)
            .and_then(|row| row.cells.get(self.selected_col))
            .map(String::as_str)
    }
}
