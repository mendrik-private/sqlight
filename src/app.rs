use std::{cmp, collections::BTreeMap, error::Error};

use chrono::{Datelike, Duration, Local, NaiveDate};
use crossterm::event::{Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};

use crate::{
    db::{ColumnKind, Database, RowRecord, parse_bool_text},
    input::{InputCommand, InputContext, translate_event},
    layout,
    state::{
        AppState, DatePickerFocus, DatePickerModal, EditorFocus, EditorModal, FilterClause,
        FilterJoin, FilterModal, FilterValueOption, ModalState, SortDirection, SortState, UiRegion,
    },
};

pub struct App {
    pub db: Database,
    pub state: AppState,
}

impl App {
    pub fn new(db: Database, table_name: Option<&str>) -> Result<Self, Box<dyn Error>> {
        let current_table = match table_name {
            Some(table_name) => db
                .tables()
                .iter()
                .position(|name| name == table_name)
                .unwrap_or_default(),
            None => 0,
        };

        let table = if let Some(name) = db.tables().get(current_table) {
            db.load_table(name, None)?
        } else {
            return Err("database has no user tables".into());
        };

        let source_rows = table.rows.clone();
        let table_row_counts = db
            .tables()
            .iter()
            .map(|name| db.table_row_count(name).unwrap_or(0))
            .collect::<Vec<_>>();

        let state = AppState {
            db_label: db.label().to_owned(),
            tables: db.tables().to_vec(),
            table_row_counts,
            current_table,
            source_rows,
            table,
            selected_row: 0,
            selected_col: 0,
            row_offset: 0,
            viewport_rows: 0,
            side_scroll: 0,
            side_viewport_rows: 0,
            grid_layout: Default::default(),
            mouse_state: Default::default(),
            hitboxes: Vec::new(),
            sort: None,
            active_filters: Vec::new(),
            status: "Ready. Mouse, wheel, popups, filters, and FK jumps are active.".to_owned(),
            modal: None,
            should_quit: false,
        };

        Ok(Self { db, state })
    }

    pub fn handle_event(&mut self, event: Event) -> Result<(), Box<dyn Error>> {
        if self.handle_modal_event(&event)? {
            return Ok(());
        }

        let result = translate_event(
            event,
            InputContext {
                hitboxes: &self.state.hitboxes,
                mouse_state: &self.state.mouse_state,
                now: std::time::Instant::now(),
            },
        );
        self.state.mouse_state = result.mouse_state;

        for command in result.commands {
            self.apply_command(command)?;
        }

        Ok(())
    }

    pub fn refresh_layout(
        &mut self,
        viewport_width: u16,
        viewport_rows: usize,
        side_viewport_rows: usize,
    ) {
        self.state.viewport_rows = viewport_rows;
        self.state.side_viewport_rows = side_viewport_rows;
        self.state.grid_layout = layout::plan_widths(
            &self.state.table,
            viewport_width,
            self.state.row_offset,
            viewport_rows,
            self.state.grid_layout.horizontal_scroll,
        );
        self.ensure_row_visible();
        self.ensure_column_visible();
        self.ensure_table_visible();
    }

    pub fn visible_columns(&self) -> Vec<usize> {
        layout::visible_columns(
            &self.state.grid_layout.column_widths,
            self.state.grid_layout.horizontal_scroll.first_visible_col,
            self.state.grid_layout.viewport_width,
        )
    }

    pub fn horizontal_scroll_position(&self) -> usize {
        let hidden_cols = self.state.grid_layout.horizontal_scroll.first_visible_col;
        self.state.grid_layout.column_widths[..hidden_cols]
            .iter()
            .map(|width| *width as usize + 1)
            .sum()
    }

    pub fn filter_summary(&self) -> String {
        if self.state.active_filters.is_empty() {
            "filters: none".to_owned()
        } else {
            let joined = self
                .state
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
                        self.state.table.columns[clause.column].name, clause.pattern
                    )
                })
                .collect::<Vec<_>>()
                .join(" ");
            format!("filters: {joined}")
        }
    }

    fn apply_command(&mut self, command: InputCommand) -> Result<(), Box<dyn Error>> {
        match command {
            InputCommand::MoveUp => {
                self.state.selected_row = self.state.selected_row.saturating_sub(1);
                self.ensure_row_visible();
            }
            InputCommand::MoveDown => {
                if !self.state.table.rows.is_empty() {
                    self.state.selected_row = cmp::min(
                        self.state.selected_row + 1,
                        self.state.table.rows.len().saturating_sub(1),
                    );
                    self.ensure_row_visible();
                }
            }
            InputCommand::MoveLeft => {
                self.state.selected_col = self.state.selected_col.saturating_sub(1);
                self.ensure_column_visible();
            }
            InputCommand::MoveRight => {
                if !self.state.table.columns.is_empty() {
                    self.state.selected_col = cmp::min(
                        self.state.selected_col + 1,
                        self.state.table.columns.len().saturating_sub(1),
                    );
                    self.ensure_column_visible();
                }
            }
            InputCommand::NextTable => {
                if !self.state.tables.is_empty() {
                    let next = cmp::min(
                        self.state.current_table + 1,
                        self.state.tables.len().saturating_sub(1),
                    );
                    self.select_table(next)?;
                }
            }
            InputCommand::PreviousTable => {
                self.select_table(self.state.current_table.saturating_sub(1))?;
            }
            InputCommand::ScrollRows(delta) => self.scroll_rows(delta),
            InputCommand::ScrollColumns(delta) => self.scroll_columns(delta),
            InputCommand::ScrollTables(delta) => self.scroll_tables(delta),
            InputCommand::SelectCell { row, col } => {
                self.state.selected_row =
                    cmp::min(row, self.state.table.rows.len().saturating_sub(1));
                self.state.selected_col =
                    cmp::min(col, self.state.table.columns.len().saturating_sub(1));
                self.ensure_row_visible();
                self.ensure_column_visible();
            }
            InputCommand::SelectTable { index } => {
                self.select_table(index)?;
            }
            InputCommand::ToggleSort { col } => {
                let target_col = if col == usize::MAX {
                    self.state.selected_col
                } else {
                    col
                };
                self.state.sort = SortState::next_for(self.state.sort, target_col);
                self.reload_table()?;
            }
            InputCommand::EditSelected => self.start_edit_selected(),
            InputCommand::Search => self.start_filter_dialog(),
            InputCommand::Group => {
                self.state.status = "Grouping command routed. Aggregates popup next.".to_owned();
            }
            InputCommand::JumpForeignKey => self.jump_foreign_key()?,
            InputCommand::ClearFilters => {
                self.state.active_filters.clear();
                self.apply_filters();
                self.state.status = "Filters cleared.".to_owned();
            }
            InputCommand::ToggleHelp => {
                self.state.modal = match self.state.modal.take() {
                    Some(ModalState::Help) => None,
                    Some(other) => Some(other),
                    None => Some(ModalState::Help),
                };
            }
            InputCommand::Quit => self.state.should_quit = true,
            InputCommand::Breadcrumb { index } => {
                self.state.status = match index {
                    0 => format!(
                        "DB '{}': {} tables.",
                        self.state.db_label,
                        self.state.tables.len()
                    ),
                    1 => format!("Viewing table '{}'.", self.state.table.name),
                    _ => "Breadcrumb target not mapped.".to_owned(),
                };
            }
            InputCommand::BeginHorizontalDrag { column }
            | InputCommand::DragHorizontalDrag { column } => {
                self.position_horizontal_scroll(column);
            }
            InputCommand::EndHorizontalDrag => {}
        }

        Ok(())
    }

    fn handle_modal_event(&mut self, event: &Event) -> Result<bool, Box<dyn Error>> {
        let Some(modal) = self.state.modal.take() else {
            return Ok(false);
        };

        match modal {
            ModalState::Editor(editor) => {
                let (consumed, next_modal) = self.handle_editor_modal(event, editor);
                self.state.modal = next_modal;
                Ok(consumed)
            }
            ModalState::DatePicker(picker) => {
                let (consumed, next_modal) = self.handle_date_picker_modal(event, picker);
                self.state.modal = next_modal;
                Ok(consumed)
            }
            ModalState::Filter(filter) => {
                let (consumed, next_modal) = self.handle_filter_modal(event, filter);
                self.state.modal = next_modal;
                Ok(consumed)
            }
            ModalState::Help => {
                let consumed = matches!(
                    event,
                    Event::Key(KeyEvent {
                        code: KeyCode::Esc
                            | KeyCode::Char('?')
                            | KeyCode::Char('f')
                            | KeyCode::Char('F')
                            | KeyCode::F(1),
                        kind: KeyEventKind::Press | KeyEventKind::Repeat,
                        ..
                    })
                );
                if consumed {
                    self.state.status = "Help closed.".to_owned();
                    self.state.modal = None;
                } else {
                    self.state.modal = Some(ModalState::Help);
                }
                Ok(consumed)
            }
        }
    }

    fn handle_editor_modal(
        &mut self,
        event: &Event,
        mut editor: EditorModal,
    ) -> (bool, Option<ModalState>) {
        let Event::Key(key) = event else {
            return (false, Some(ModalState::Editor(editor)));
        };

        if !matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) {
            return (false, Some(ModalState::Editor(editor)));
        }

        match key.code {
            KeyCode::Esc => {
                self.state.status = format!("Canceled edit for '{}'.", editor.column_name);
                (true, None)
            }
            KeyCode::Tab if !editor.unique_values.is_empty() => {
                editor.focus = match editor.focus {
                    EditorFocus::Values => EditorFocus::Input,
                    EditorFocus::Input => EditorFocus::Values,
                };
                (true, Some(ModalState::Editor(editor)))
            }
            KeyCode::Enter if key.modifiers.contains(KeyModifiers::ALT) => {
                editor.value.insert(editor.cursor, '\n');
                editor.cursor += 1;
                (true, Some(ModalState::Editor(editor)))
            }
            KeyCode::Enter => {
                let value =
                    if editor.focus == EditorFocus::Values && !editor.unique_values.is_empty() {
                        editor.unique_values[editor.selected_value].value.clone()
                    } else {
                        editor.value.clone()
                    };
                self.set_current_value(value);
                self.state.status = format!("Updated '{}' locally.", editor.column_name);
                (true, None)
            }
            KeyCode::Left => {
                if editor.focus == EditorFocus::Input {
                    editor.cursor = prev_boundary(&editor.value, editor.cursor);
                }
                (true, Some(ModalState::Editor(editor)))
            }
            KeyCode::Right => {
                if editor.focus == EditorFocus::Input {
                    editor.cursor = next_boundary(&editor.value, editor.cursor);
                } else {
                    editor.focus = EditorFocus::Input;
                }
                (true, Some(ModalState::Editor(editor)))
            }
            KeyCode::Up => {
                if editor.focus == EditorFocus::Values && !editor.unique_values.is_empty() {
                    editor.selected_value = editor.selected_value.saturating_sub(1);
                } else {
                    editor.cursor = move_vertical(&editor.value, editor.cursor, -1);
                }
                (true, Some(ModalState::Editor(editor)))
            }
            KeyCode::Down => {
                if editor.focus == EditorFocus::Values && !editor.unique_values.is_empty() {
                    editor.selected_value = cmp::min(
                        editor.selected_value + 1,
                        editor.unique_values.len().saturating_sub(1),
                    );
                } else {
                    editor.cursor = move_vertical(&editor.value, editor.cursor, 1);
                }
                (true, Some(ModalState::Editor(editor)))
            }
            KeyCode::Home => {
                editor.cursor = line_start(&editor.value, editor.cursor);
                (true, Some(ModalState::Editor(editor)))
            }
            KeyCode::End => {
                editor.cursor = line_end(&editor.value, editor.cursor);
                (true, Some(ModalState::Editor(editor)))
            }
            KeyCode::Backspace => {
                if editor.focus == EditorFocus::Input && editor.cursor > 0 {
                    let start = prev_boundary(&editor.value, editor.cursor);
                    editor.value.replace_range(start..editor.cursor, "");
                    editor.cursor = start;
                }
                (true, Some(ModalState::Editor(editor)))
            }
            KeyCode::Delete => {
                if editor.focus == EditorFocus::Input && editor.cursor < editor.value.len() {
                    let end = next_boundary(&editor.value, editor.cursor);
                    editor.value.replace_range(editor.cursor..end, "");
                }
                (true, Some(ModalState::Editor(editor)))
            }
            KeyCode::Char(ch)
                if editor.focus == EditorFocus::Input
                    && !key.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                editor.value.insert(editor.cursor, ch);
                editor.cursor += ch.len_utf8();
                (true, Some(ModalState::Editor(editor)))
            }
            _ => (true, Some(ModalState::Editor(editor))),
        }
    }

    fn handle_date_picker_modal(
        &mut self,
        event: &Event,
        mut picker: DatePickerModal,
    ) -> (bool, Option<ModalState>) {
        let Event::Key(key) = event else {
            return (false, Some(ModalState::DatePicker(picker)));
        };

        if !matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) {
            return (false, Some(ModalState::DatePicker(picker)));
        }

        match key.code {
            KeyCode::Esc => {
                self.state.status = format!("Canceled date pick for '{}'.", picker.column_name);
                (true, None)
            }
            KeyCode::Tab => {
                picker.focus = next_datepicker_focus(picker.focus, picker.include_time);
                (true, Some(ModalState::DatePicker(picker)))
            }
            KeyCode::BackTab => {
                picker.focus = prev_datepicker_focus(picker.focus, picker.include_time);
                (true, Some(ModalState::DatePicker(picker)))
            }
            KeyCode::Up => {
                adjust_picker_focus(&mut picker, 1);
                (true, Some(ModalState::DatePicker(picker)))
            }
            KeyCode::Down => {
                adjust_picker_focus(&mut picker, -1);
                (true, Some(ModalState::DatePicker(picker)))
            }
            KeyCode::Left if picker.focus == DatePickerFocus::Grid => {
                picker.selected -= Duration::days(1);
                (true, Some(ModalState::DatePicker(picker)))
            }
            KeyCode::Right if picker.focus == DatePickerFocus::Grid => {
                picker.selected += Duration::days(1);
                (true, Some(ModalState::DatePicker(picker)))
            }
            KeyCode::PageUp => {
                picker.selected = shift_month(picker.selected, -1);
                (true, Some(ModalState::DatePicker(picker)))
            }
            KeyCode::PageDown => {
                picker.selected = shift_month(picker.selected, 1);
                (true, Some(ModalState::DatePicker(picker)))
            }
            KeyCode::Home => {
                picker.selected =
                    first_day_of_month(picker.selected.year(), picker.selected.month());
                (true, Some(ModalState::DatePicker(picker)))
            }
            KeyCode::End => {
                picker.selected =
                    last_day_of_month(picker.selected.year(), picker.selected.month());
                (true, Some(ModalState::DatePicker(picker)))
            }
            KeyCode::Enter => {
                let value = if picker.include_time {
                    format!(
                        "{} {:02}:{:02}:{:02}",
                        picker.selected.format("%Y-%m-%d"),
                        picker.hour,
                        picker.minute,
                        picker.second
                    )
                } else {
                    picker.selected.format("%Y-%m-%d").to_string()
                };
                self.set_current_value(value);
                self.state.status = format!("Updated '{}' locally.", picker.column_name);
                (true, None)
            }
            _ => (true, Some(ModalState::DatePicker(picker))),
        }
    }

    fn handle_filter_modal(
        &mut self,
        event: &Event,
        mut filter: FilterModal,
    ) -> (bool, Option<ModalState>) {
        let Event::Key(key) = event else {
            return (false, Some(ModalState::Filter(filter)));
        };

        if !matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) {
            return (false, Some(ModalState::Filter(filter)));
        }

        match key.code {
            KeyCode::Esc => {
                self.state.status = "Canceled filter dialog.".to_owned();
                (true, None)
            }
            KeyCode::Tab => {
                filter.join = toggle_join(filter.join);
                (true, Some(ModalState::Filter(filter)))
            }
            KeyCode::PageUp => {
                filter.column = filter.column.saturating_sub(1);
                self.refresh_filter_modal_options(&mut filter);
                (true, Some(ModalState::Filter(filter)))
            }
            KeyCode::PageDown => {
                if !self.state.table.columns.is_empty() {
                    filter.column = cmp::min(
                        filter.column + 1,
                        self.state.table.columns.len().saturating_sub(1),
                    );
                    self.refresh_filter_modal_options(&mut filter);
                }
                (true, Some(ModalState::Filter(filter)))
            }
            KeyCode::Up => {
                filter.selected_value = filter.selected_value.saturating_sub(1);
                (true, Some(ModalState::Filter(filter)))
            }
            KeyCode::Down => {
                if !filter.unique_values.is_empty() {
                    filter.selected_value = cmp::min(
                        filter.selected_value + 1,
                        filter.unique_values.len().saturating_sub(1),
                    );
                }
                (true, Some(ModalState::Filter(filter)))
            }
            KeyCode::Char('+') => {
                if let Some(clause) = self.build_filter_clause(&filter) {
                    self.state.active_filters.push(clause);
                    self.apply_filters();
                    filter.draft_pattern.clear();
                    filter.cursor = 0;
                    self.state.status = format!("Added filter clause. {}", self.filter_summary());
                }
                self.refresh_filter_modal_options(&mut filter);
                (true, Some(ModalState::Filter(filter)))
            }
            KeyCode::Char('c') | KeyCode::Char('C') => {
                self.state.active_filters.clear();
                self.apply_filters();
                self.state.status = "Cleared filters.".to_owned();
                (true, None)
            }
            KeyCode::Enter => {
                if let Some(clause) = self.build_filter_clause(&filter) {
                    self.state.active_filters.push(clause);
                }
                self.apply_filters();
                self.state.status = self.filter_summary();
                (true, None)
            }
            KeyCode::Left => {
                filter.cursor = prev_boundary(&filter.draft_pattern, filter.cursor);
                (true, Some(ModalState::Filter(filter)))
            }
            KeyCode::Right => {
                filter.cursor = next_boundary(&filter.draft_pattern, filter.cursor);
                (true, Some(ModalState::Filter(filter)))
            }
            KeyCode::Home => {
                filter.cursor = 0;
                (true, Some(ModalState::Filter(filter)))
            }
            KeyCode::End => {
                filter.cursor = filter.draft_pattern.len();
                (true, Some(ModalState::Filter(filter)))
            }
            KeyCode::Backspace => {
                if filter.cursor > 0 {
                    let start = prev_boundary(&filter.draft_pattern, filter.cursor);
                    filter.draft_pattern.replace_range(start..filter.cursor, "");
                    filter.cursor = start;
                }
                (true, Some(ModalState::Filter(filter)))
            }
            KeyCode::Delete => {
                if filter.cursor < filter.draft_pattern.len() {
                    let end = next_boundary(&filter.draft_pattern, filter.cursor);
                    filter.draft_pattern.replace_range(filter.cursor..end, "");
                }
                (true, Some(ModalState::Filter(filter)))
            }
            KeyCode::Char(ch) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                filter.draft_pattern.insert(filter.cursor, ch);
                filter.cursor += ch.len_utf8();
                (true, Some(ModalState::Filter(filter)))
            }
            _ => (false, Some(ModalState::Filter(filter))),
        }
    }

    fn reload_table(&mut self) -> Result<(), Box<dyn Error>> {
        let table_name = self.state.tables[self.state.current_table].clone();
        let sort = self
            .state
            .sort
            .map(|sort| (sort.column, sort.direction == SortDirection::Asc));
        self.state.table = self.db.load_table(&table_name, sort)?;
        self.state.source_rows = self.state.table.rows.clone();
        self.state.selected_row = cmp::min(
            self.state.selected_row,
            self.state.table.rows.len().saturating_sub(1),
        );
        self.state.selected_col = cmp::min(
            self.state.selected_col,
            self.state.table.columns.len().saturating_sub(1),
        );
        self.state
            .active_filters
            .retain(|clause| clause.column < self.state.table.columns.len());
        self.apply_filters();
        self.ensure_row_visible();
        self.ensure_column_visible();
        Ok(())
    }

    fn select_table(&mut self, index: usize) -> Result<(), Box<dyn Error>> {
        if index >= self.state.tables.len() || index == self.state.current_table {
            return Ok(());
        }

        self.state.current_table = index;
        self.state.selected_row = 0;
        self.state.selected_col = 0;
        self.state.row_offset = 0;
        self.state.sort = None;
        self.state.active_filters.clear();
        self.state.grid_layout.horizontal_scroll.first_visible_col = 0;
        self.reload_table()?;
        self.ensure_table_visible();
        self.state.status = format!("Selected table '{}'.", self.state.table.name);
        Ok(())
    }

    fn start_edit_selected(&mut self) {
        if self.state.table.rows.is_empty() || self.state.table.columns.is_empty() {
            return;
        }

        let column = self.state.table.columns[self.state.selected_col].clone();
        let value = self.state.current_value().unwrap_or("").to_owned();

        match column.kind {
            ColumnKind::Boolean => {
                let next = match parse_bool_text(&value) {
                    Some(true) => "false".to_owned(),
                    Some(false) => "true".to_owned(),
                    None => "true".to_owned(),
                };
                self.set_current_value(next);
                self.state.status = format!("Toggled '{}'.", column.name);
            }
            ColumnKind::Date | ColumnKind::DateTime => {
                let selected =
                    parse_date_value(&value).unwrap_or_else(|| Local::now().date_naive());
                let (hour, minute, second) = parse_time_value(&value).unwrap_or((0, 0, 0));
                self.state.modal = Some(ModalState::DatePicker(DatePickerModal {
                    column_name: column.name,
                    include_time: matches!(column.kind, ColumnKind::DateTime),
                    hour,
                    minute,
                    second,
                    selected,
                    focus: DatePickerFocus::Month,
                }));
            }
            _ => {
                let unique_values = if should_offer_value_picker(&column.kind) {
                    let collected =
                        collect_unique_values(&self.state.source_rows, self.state.selected_col, 10);
                    if collected
                        .iter()
                        .all(|option| option.value.chars().count() <= 32)
                    {
                        collected
                    } else {
                        Vec::new()
                    }
                } else {
                    Vec::new()
                };
                self.state.modal = Some(ModalState::Editor(EditorModal {
                    column_name: column.name,
                    column_kind: column.kind,
                    cursor: value.len(),
                    focus: if unique_values.is_empty() {
                        EditorFocus::Input
                    } else {
                        EditorFocus::Values
                    },
                    selected_value: 0,
                    unique_values,
                    value,
                }));
            }
        }
    }

    fn start_filter_dialog(&mut self) {
        let mut modal = FilterModal {
            column: self.state.selected_col,
            draft_pattern: String::new(),
            cursor: 0,
            join: FilterJoin::And,
            selected_value: 0,
            unique_values: Vec::new(),
        };
        self.refresh_filter_modal_options(&mut modal);
        self.state.modal = Some(ModalState::Filter(modal));
    }

    fn refresh_filter_modal_options(&self, modal: &mut FilterModal) {
        let unique_values = collect_unique_values(&self.state.source_rows, modal.column, 8);
        modal.unique_values = unique_values;
        if modal.selected_value >= modal.unique_values.len() {
            modal.selected_value = modal.unique_values.len().saturating_sub(1);
        }
    }

    fn build_filter_clause(&self, filter: &FilterModal) -> Option<FilterClause> {
        let pattern = if !filter.draft_pattern.trim().is_empty() {
            filter.draft_pattern.trim().to_owned()
        } else {
            filter
                .unique_values
                .get(filter.selected_value)
                .map(|value| value.value.clone())?
        };

        Some(FilterClause {
            column: filter.column,
            pattern,
            join: filter.join,
        })
    }

    fn apply_filters(&mut self) {
        if self.state.active_filters.is_empty() {
            self.state.table.rows = self.state.source_rows.clone();
        } else {
            self.state.table.rows = self
                .state
                .source_rows
                .iter()
                .filter(|row| row_matches_filters(row, &self.state.active_filters))
                .cloned()
                .collect();
        }

        self.state.selected_row = cmp::min(
            self.state.selected_row,
            self.state.table.rows.len().saturating_sub(1),
        );
        self.ensure_row_visible();
    }

    fn jump_foreign_key(&mut self) -> Result<(), Box<dyn Error>> {
        let Some(column) = self
            .state
            .table
            .columns
            .get(self.state.selected_col)
            .cloned()
        else {
            return Ok(());
        };
        let Some(target_table) = column.referenced_table.clone() else {
            self.state.status = "Current column has no foreign key target.".to_owned();
            return Ok(());
        };
        let target_column = column
            .referenced_column
            .clone()
            .unwrap_or_else(|| "id".to_owned());
        let Some(value) = self.state.current_value().map(str::to_owned) else {
            return Ok(());
        };

        let target_index = self
            .state
            .tables
            .iter()
            .position(|table| table == &target_table);
        let Some(target_index) = target_index else {
            self.state.status = format!("FK target table '{target_table}' not loaded.");
            return Ok(());
        };

        self.select_table(target_index)?;
        if let Some(column_index) = self
            .state
            .table
            .columns
            .iter()
            .position(|candidate| candidate.name == target_column)
        {
            if let Some(row_index) = self.state.source_rows.iter().position(|row| {
                row.cells
                    .get(column_index)
                    .is_some_and(|cell| cell == &value)
            }) {
                self.state.selected_row = row_index;
                self.state.selected_col = column_index;
                self.ensure_row_visible();
                self.ensure_column_visible();
            }
        }

        self.state.status = format!("Jumped via FK to '{}.{}'.", target_table, target_column);
        Ok(())
    }

    fn set_current_value(&mut self, value: String) {
        let selected_rowid = self
            .state
            .table
            .rows
            .get(self.state.selected_row)
            .and_then(|row| row.rowid);

        if let Some(row) = self.state.table.rows.get_mut(self.state.selected_row) {
            if let Some(cell) = row.cells.get_mut(self.state.selected_col) {
                *cell = value.clone();
            }
        }

        if let Some(rowid) = selected_rowid {
            if let Some(source_row) = self
                .state
                .source_rows
                .iter_mut()
                .find(|row| row.rowid == Some(rowid))
            {
                if let Some(cell) = source_row.cells.get_mut(self.state.selected_col) {
                    *cell = value;
                }
            }
        } else if let Some(source_row) = self.state.source_rows.get_mut(self.state.selected_row) {
            if let Some(cell) = source_row.cells.get_mut(self.state.selected_col) {
                *cell = value;
            }
        }

        self.apply_filters();
    }

    fn scroll_rows(&mut self, delta: i32) {
        let max_offset = self.max_row_offset();
        self.state.row_offset = bounded_offset(self.state.row_offset, delta, max_offset);
        self.state.status = format!(
            "Rows {}-{} of {}.",
            self.state.row_offset + 1,
            self.state.row_offset + self.state.viewport_rows.max(1),
            self.state.table.rows.len()
        );
    }

    fn scroll_columns(&mut self, delta: i32) {
        if self.state.table.columns.is_empty() {
            return;
        }

        let max_offset = self.state.table.columns.len().saturating_sub(1);
        self.state.grid_layout.horizontal_scroll.first_visible_col = bounded_offset(
            self.state.grid_layout.horizontal_scroll.first_visible_col,
            delta,
            max_offset,
        );
        self.state.status = format!(
            "Horizontal scroll at column {}.",
            self.state.grid_layout.horizontal_scroll.first_visible_col + 1
        );
    }

    fn scroll_tables(&mut self, delta: i32) {
        let max_offset = self
            .state
            .tables
            .len()
            .saturating_sub(self.state.side_viewport_rows.max(1));
        self.state.side_scroll = bounded_offset(self.state.side_scroll, delta, max_offset);
    }

    fn ensure_row_visible(&mut self) {
        if self.state.viewport_rows == 0 {
            return;
        }

        if self.state.selected_row < self.state.row_offset {
            self.state.row_offset = self.state.selected_row;
        } else if self.state.selected_row >= self.state.row_offset + self.state.viewport_rows {
            self.state.row_offset = self.state.selected_row + 1 - self.state.viewport_rows;
        }
        self.state.row_offset = cmp::min(self.state.row_offset, self.max_row_offset());
    }

    fn ensure_column_visible(&mut self) {
        let viewport_width = self.state.grid_layout.viewport_width;
        if viewport_width == 0 || self.state.table.columns.is_empty() {
            return;
        }

        self.state.grid_layout.horizontal_scroll.first_visible_col =
            layout::min_first_visible_for_selection(
                &self.state.grid_layout.column_widths,
                self.state.selected_col,
                self.state.grid_layout.horizontal_scroll.first_visible_col,
                viewport_width,
            );
    }

    fn ensure_table_visible(&mut self) {
        if self.state.side_viewport_rows == 0 {
            return;
        }

        if self.state.current_table < self.state.side_scroll {
            self.state.side_scroll = self.state.current_table;
        } else if self.state.current_table >= self.state.side_scroll + self.state.side_viewport_rows
        {
            self.state.side_scroll = self.state.current_table + 1 - self.state.side_viewport_rows;
        }
    }

    fn max_row_offset(&self) -> usize {
        self.state
            .table
            .rows
            .len()
            .saturating_sub(self.state.viewport_rows.max(1))
    }

    fn position_horizontal_scroll(&mut self, mouse_x: u16) {
        if !self.state.grid_layout.horizontal_overflow || self.state.table.columns.is_empty() {
            return;
        }

        let Some(track) = self
            .state
            .hitboxes
            .iter()
            .find(|hitbox| hitbox.region == UiRegion::HorizontalScrollbarTrack)
        else {
            return;
        };

        let track_width = track.area.width.max(1);
        let relative = mouse_x.saturating_sub(track.area.x).min(track_width - 1);
        let ratio = relative as f32 / track_width as f32;
        let max_first = self.state.table.columns.len().saturating_sub(1);
        self.state.grid_layout.horizontal_scroll.first_visible_col =
            (ratio * max_first as f32).round() as usize;
    }
}

fn collect_unique_values(
    rows: &[RowRecord],
    column: usize,
    max_unique: usize,
) -> Vec<FilterValueOption> {
    let mut counts = BTreeMap::<String, usize>::new();
    for row in rows {
        let Some(value) = row.cells.get(column) else {
            continue;
        };
        *counts.entry(value.clone()).or_default() += 1;
        if counts.len() > max_unique {
            return Vec::new();
        }
    }

    counts
        .into_iter()
        .map(|(value, count)| FilterValueOption { value, count })
        .collect()
}

fn row_matches_filters(row: &RowRecord, filters: &[FilterClause]) -> bool {
    let mut result = true;
    for (index, clause) in filters.iter().enumerate() {
        let matches = row
            .cells
            .get(clause.column)
            .is_some_and(|value| value_matches(&clause.pattern, value));
        if index == 0 {
            result = matches;
        } else {
            result = match clause.join {
                FilterJoin::And => result && matches,
                FilterJoin::Or => result || matches,
            };
        }
    }
    result
}

fn value_matches(pattern: &str, value: &str) -> bool {
    let pattern = pattern.trim().to_ascii_lowercase();
    let value = value.to_ascii_lowercase();
    if pattern.is_empty() {
        return true;
    }

    if pattern.contains('*') || pattern.contains('?') {
        wildcard_match(pattern.as_bytes(), value.as_bytes())
    } else {
        value.contains(&pattern)
    }
}

fn wildcard_match(pattern: &[u8], value: &[u8]) -> bool {
    let mut pattern_index = 0usize;
    let mut value_index = 0usize;
    let mut star_index = None;
    let mut match_index = 0usize;

    while value_index < value.len() {
        if pattern_index < pattern.len()
            && (pattern[pattern_index] == b'?' || pattern[pattern_index] == value[value_index])
        {
            pattern_index += 1;
            value_index += 1;
        } else if pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
            star_index = Some(pattern_index);
            pattern_index += 1;
            match_index = value_index;
        } else if let Some(star) = star_index {
            pattern_index = star + 1;
            match_index += 1;
            value_index = match_index;
        } else {
            return false;
        }
    }

    while pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
        pattern_index += 1;
    }

    pattern_index == pattern.len()
}

fn toggle_join(join: FilterJoin) -> FilterJoin {
    match join {
        FilterJoin::And => FilterJoin::Or,
        FilterJoin::Or => FilterJoin::And,
    }
}

fn should_offer_value_picker(kind: &ColumnKind) -> bool {
    matches!(kind, ColumnKind::ShortText | ColumnKind::Unknown)
}

fn prev_datepicker_focus(focus: DatePickerFocus, include_time: bool) -> DatePickerFocus {
    match focus {
        DatePickerFocus::Month => {
            if include_time {
                DatePickerFocus::Second
            } else {
                DatePickerFocus::Grid
            }
        }
        DatePickerFocus::Day => DatePickerFocus::Month,
        DatePickerFocus::Year => DatePickerFocus::Day,
        DatePickerFocus::Grid => DatePickerFocus::Year,
        DatePickerFocus::Hour => DatePickerFocus::Grid,
        DatePickerFocus::Minute => DatePickerFocus::Hour,
        DatePickerFocus::Second => DatePickerFocus::Minute,
    }
}

fn next_datepicker_focus(focus: DatePickerFocus, include_time: bool) -> DatePickerFocus {
    match focus {
        DatePickerFocus::Month => DatePickerFocus::Day,
        DatePickerFocus::Day => DatePickerFocus::Year,
        DatePickerFocus::Year => DatePickerFocus::Grid,
        DatePickerFocus::Grid => {
            if include_time {
                DatePickerFocus::Hour
            } else {
                DatePickerFocus::Month
            }
        }
        DatePickerFocus::Hour => DatePickerFocus::Minute,
        DatePickerFocus::Minute => DatePickerFocus::Second,
        DatePickerFocus::Second => DatePickerFocus::Month,
    }
}

fn adjust_picker_focus(picker: &mut DatePickerModal, delta: i32) {
    match picker.focus {
        DatePickerFocus::Month => {
            picker.selected = shift_month(picker.selected, delta);
        }
        DatePickerFocus::Day => {
            picker.selected += Duration::days(delta as i64);
        }
        DatePickerFocus::Year => {
            let year = picker.selected.year() + delta;
            let day = picker
                .selected
                .day()
                .min(last_day_of_month(year, picker.selected.month()).day());
            picker.selected = NaiveDate::from_ymd_opt(year, picker.selected.month(), day)
                .expect("valid adjusted year");
        }
        DatePickerFocus::Grid => {
            picker.selected += Duration::days((delta * 7) as i64);
        }
        DatePickerFocus::Hour => {
            picker.hour = wrap_component(picker.hour, delta, 24);
        }
        DatePickerFocus::Minute => {
            picker.minute = wrap_component(picker.minute, delta, 60);
        }
        DatePickerFocus::Second => {
            picker.second = wrap_component(picker.second, delta, 60);
        }
    }
}

fn wrap_component(current: u32, delta: i32, max: u32) -> u32 {
    let value = current as i32 + delta;
    value.rem_euclid(max as i32) as u32
}

fn bounded_offset(current: usize, delta: i32, max: usize) -> usize {
    if delta.is_negative() {
        current.saturating_sub(delta.unsigned_abs() as usize)
    } else {
        cmp::min(current.saturating_add(delta as usize), max)
    }
}

fn prev_boundary(value: &str, cursor: usize) -> usize {
    value[..cursor]
        .char_indices()
        .last()
        .map(|(index, _)| index)
        .unwrap_or(0)
}

fn next_boundary(value: &str, cursor: usize) -> usize {
    if cursor >= value.len() {
        return value.len();
    }

    value[cursor..]
        .char_indices()
        .nth(1)
        .map(|(offset, _)| cursor + offset)
        .unwrap_or(value.len())
}

fn line_start(value: &str, cursor: usize) -> usize {
    value[..cursor]
        .rfind('\n')
        .map(|index| index + 1)
        .unwrap_or(0)
}

fn line_end(value: &str, cursor: usize) -> usize {
    value[cursor..]
        .find('\n')
        .map(|offset| cursor + offset)
        .unwrap_or(value.len())
}

fn move_vertical(value: &str, cursor: usize, delta: i32) -> usize {
    let starts = line_starts(value);
    let current_line = starts
        .iter()
        .rposition(|start| *start <= cursor)
        .unwrap_or(0);
    let current_start = starts[current_line];
    let current_col = value[current_start..cursor].chars().count();
    let target_line = if delta.is_negative() {
        current_line.saturating_sub(delta.unsigned_abs() as usize)
    } else {
        cmp::min(
            current_line + delta as usize,
            starts.len().saturating_sub(1),
        )
    };
    let target_start = starts[target_line];
    let target_end = if target_line + 1 < starts.len() {
        starts[target_line + 1].saturating_sub(1)
    } else {
        value.len()
    };
    nth_char_boundary(value, target_start, current_col).min(target_end)
}

fn line_starts(value: &str) -> Vec<usize> {
    let mut starts = vec![0];
    for (index, ch) in value.char_indices() {
        if ch == '\n' {
            starts.push(index + 1);
        }
    }
    starts
}

fn nth_char_boundary(value: &str, start: usize, chars: usize) -> usize {
    value[start..]
        .char_indices()
        .nth(chars)
        .map(|(offset, _)| start + offset)
        .unwrap_or(value.len())
}

fn parse_date_value(value: &str) -> Option<NaiveDate> {
    let trimmed = value.trim();
    let date_part = trimmed.get(0..10).unwrap_or(trimmed);
    NaiveDate::parse_from_str(date_part, "%Y-%m-%d").ok()
}

fn parse_time_value(value: &str) -> Option<(u32, u32, u32)> {
    let time = value.trim().split_once(' ')?.1.trim();
    let mut parts = time.split(':');
    let hour = parts.next()?.parse().ok()?;
    let minute = parts.next()?.parse().ok()?;
    let second = parts.next()?.parse().ok()?;
    Some((hour, minute, second))
}

fn shift_month(date: NaiveDate, delta: i32) -> NaiveDate {
    let month_index = date.year() * 12 + date.month0() as i32 + delta;
    let year = month_index.div_euclid(12);
    let month = month_index.rem_euclid(12) as u32 + 1;
    let day = date.day().min(last_day_of_month(year, month).day());
    NaiveDate::from_ymd_opt(year, month, day).expect("valid shifted month date")
}

fn first_day_of_month(year: i32, month: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(year, month, 1).expect("valid first day")
}

fn last_day_of_month(year: i32, month: u32) -> NaiveDate {
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    NaiveDate::from_ymd_opt(next_year, next_month, 1)
        .expect("valid next month")
        .pred_opt()
        .expect("month has previous day")
}

#[cfg(test)]
mod tests {
    use crate::{
        db::RowRecord,
        state::{FilterClause, FilterJoin},
    };

    use super::{row_matches_filters, value_matches};

    #[test]
    fn wildcard_and_substring_matching_work() {
        assert!(value_matches("Ada", "Ada Lovelace"));
        assert!(value_matches("A*a", "Ada"));
        assert!(value_matches("2???-05-*", "2024-05-02"));
        assert!(!value_matches("Grace*", "Ada Lovelace"));
    }

    #[test]
    fn filters_support_and_or_chains() {
        let row = RowRecord {
            rowid: Some(1),
            cells: vec![
                "Ada Lovelace".to_owned(),
                "Compiler".to_owned(),
                "true".to_owned(),
            ],
        };

        let filters = vec![
            FilterClause {
                column: 0,
                pattern: "Ada*".to_owned(),
                join: FilterJoin::And,
            },
            FilterClause {
                column: 1,
                pattern: "Compiler".to_owned(),
                join: FilterJoin::And,
            },
        ];
        assert!(row_matches_filters(&row, &filters));

        let filters = vec![
            FilterClause {
                column: 0,
                pattern: "Grace*".to_owned(),
                join: FilterJoin::And,
            },
            FilterClause {
                column: 1,
                pattern: "Compiler".to_owned(),
                join: FilterJoin::Or,
            },
        ];
        assert!(row_matches_filters(&row, &filters));
    }
}
