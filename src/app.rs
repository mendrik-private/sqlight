use std::sync::Arc;

use tokio::sync::mpsc::UnboundedSender;

use crate::{
    config::Config,
    db::{self, schema::Column, schema::Schema, types::SqlValue, DbPool},
    theme::Theme,
    ui::{
        popup::{
            DatePickerState, DatetimePickerState, PopupKind, TextEditorState,
        },
        sidebar::{SidebarAction, SidebarState},
        toast::{ToastKind, ToastState},
    },
};

#[derive(Debug, Clone, PartialEq)]
pub enum AppMode {
    Browse,
    Edit,
}

pub enum FocusPane {
    Sidebar,
    Grid,
}

pub struct TableTab {
    pub table_name: String,
    pub row_count: Option<i64>,
}

pub struct App {
    pub schema: Schema,
    pub sidebar: SidebarState,
    pub should_quit: bool,
    pub dirty: bool,
    pub theme: Theme,
    pub config: Config,
    pub focus: FocusPane,
    pub open_tabs: Vec<TableTab>,
    pub active_tab: Option<usize>,
    pub sidebar_visible: bool,
    pub grid: Option<crate::grid::GridState>,
    pub mode: AppMode,
    pub popup: Option<PopupKind>,
    pub toast: ToastState,
    pub readonly: bool,
    pool: Arc<DbPool>,
    tx: UnboundedSender<Message>,
}

#[allow(dead_code)]
pub enum Message {
    Quit,
    Key(crossterm::event::KeyEvent),
    Mouse(crossterm::event::MouseEvent),
    Resize(u16, u16),
    Tick,
    OpenTable(String),
    RowCountReady {
        table: String,
        count: i64,
    },
    CloseTab(usize),
    ActivateTab(usize),
    NextTab,
    PrevTab,
    GridDataReady {
        table: String,
        columns: Vec<Column>,
        fk_cols: Vec<bool>,
        rows: Vec<Vec<SqlValue>>,
        total_rows: i64,
    },
    WindowReady {
        table: String,
        offset: i64,
        rows: Vec<Vec<SqlValue>>,
        total_rows: i64,
    },
    ScrollDown(usize),
    ScrollUp(usize),
    ScrollToRow(i64),
    ScrollToEnd,
    MoveRight,
    MoveLeft,
    MoveDown,
    MoveUp,
    MoveColFirst,
    MoveColLast,
    MoveFirstCell,
    MoveLastCell,
    OpenPopup,
    ClosePopup,
    CommitEdit,
    EditCommitted {
        rowid: i64,
    },
    EditFailed(String),
    DistinctCountReady {
        col: String,
        count: i64,
        values: Vec<String>,
    },
}

impl App {
    pub fn new(
        schema: Schema,
        config: Config,
        pool: Arc<DbPool>,
        tx: UnboundedSender<Message>,
        readonly: bool,
    ) -> Self {
        Self {
            schema,
            sidebar: SidebarState::default(),
            should_quit: false,
            dirty: true,
            theme: Theme::default(),
            config,
            focus: FocusPane::Sidebar,
            open_tabs: Vec::new(),
            active_tab: None,
            sidebar_visible: true,
            grid: None,
            mode: AppMode::Browse,
            popup: None,
            toast: ToastState::new(),
            readonly,
            pool,
            tx,
        }
    }

    pub fn update(&mut self, msg: Message) {
        match msg {
            Message::Quit => self.should_quit = true,
            Message::Resize(_w, _h) => self.dirty = true,
            Message::Key(key) => {
                self.dirty = true;
                self.handle_key(key);
            }
            Message::Mouse(_ev) => {}
            Message::Tick => {
                self.toast.tick();
                let maybe_fetch = if let Some(ref mut grid) = self.grid {
                    grid.window.tick_count = grid.window.tick_count.wrapping_add(1);
                    if grid.needs_fetch && !grid.window.fetch_in_flight {
                        grid.window.fetch_in_flight = true;
                        grid.needs_fetch = false;
                        let (off, lim) = grid.window.fetch_params(grid.focused_row as i64);
                        Some((grid.table_name.clone(), grid.columns.clone(), off, lim))
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some((table, cols, off, lim)) = maybe_fetch {
                    self.spawn_window_fetch(&table, &cols, off, lim);
                    self.dirty = true;
                }
                if self.grid.as_ref().is_some_and(|g| g.window.fetch_in_flight) {
                    self.dirty = true;
                }
            }
            Message::OpenTable(name) => self.open_table(name),
            Message::RowCountReady { table, count } => self.update_row_count(table, count),
            Message::CloseTab(idx) => self.close_tab(idx),
            Message::ActivateTab(idx) => self.activate_tab(idx),
            Message::NextTab => self.next_tab(),
            Message::PrevTab => self.prev_tab(),
            Message::GridDataReady {
                table,
                columns,
                fk_cols,
                rows,
                total_rows,
            } => self.on_grid_data_ready(table, columns, fk_cols, rows, total_rows),
            Message::WindowReady {
                table,
                offset,
                rows,
                total_rows,
            } => {
                if let Some(ref mut grid) = self.grid {
                    if grid.table_name == table {
                        grid.window.offset = offset;
                        grid.window.rows = rows;
                        grid.window.total_rows = total_rows;
                        grid.window.fetch_in_flight = false;
                        grid.needs_fetch = false;
                        if total_rows > 0 {
                            let max_row = (total_rows - 1) as usize;
                            if grid.focused_row > max_row {
                                grid.focused_row = max_row;
                            }
                        } else {
                            grid.focused_row = 0;
                        }
                        let vp = grid.window.viewport_rows as i64;
                        let max_start = (total_rows - vp).max(0);
                        if grid.viewport_start > max_start {
                            grid.viewport_start = max_start;
                        }
                    }
                }
                self.dirty = true;
            }
            Message::ScrollDown(n) => {
                let maybe_fetch = if let Some(ref mut grid) = self.grid {
                    grid.scroll_down(n);
                    if grid.needs_fetch && !grid.window.fetch_in_flight {
                        grid.window.fetch_in_flight = true;
                        grid.needs_fetch = false;
                        let (off, lim) = grid.window.fetch_params(grid.focused_row as i64);
                        Some((grid.table_name.clone(), grid.columns.clone(), off, lim))
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some((table, cols, off, lim)) = maybe_fetch {
                    self.spawn_window_fetch(&table, &cols, off, lim);
                }
                self.dirty = true;
            }
            Message::ScrollUp(n) => {
                let maybe_fetch = if let Some(ref mut grid) = self.grid {
                    grid.scroll_up(n);
                    if grid.needs_fetch && !grid.window.fetch_in_flight {
                        grid.window.fetch_in_flight = true;
                        grid.needs_fetch = false;
                        let (off, lim) = grid.window.fetch_params(grid.focused_row as i64);
                        Some((grid.table_name.clone(), grid.columns.clone(), off, lim))
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some((table, cols, off, lim)) = maybe_fetch {
                    self.spawn_window_fetch(&table, &cols, off, lim);
                }
                self.dirty = true;
            }
            Message::ScrollToRow(i) => {
                let maybe_fetch = if let Some(ref mut grid) = self.grid {
                    grid.scroll_to_row(i);
                    if grid.needs_fetch && !grid.window.fetch_in_flight {
                        grid.window.fetch_in_flight = true;
                        grid.needs_fetch = false;
                        let (off, lim) = grid.window.fetch_params(grid.focused_row as i64);
                        Some((grid.table_name.clone(), grid.columns.clone(), off, lim))
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some((table, cols, off, lim)) = maybe_fetch {
                    self.spawn_window_fetch(&table, &cols, off, lim);
                }
                self.dirty = true;
            }
            Message::ScrollToEnd => {
                let maybe_fetch = if let Some(ref mut grid) = self.grid {
                    grid.scroll_to_end();
                    if grid.needs_fetch && !grid.window.fetch_in_flight {
                        grid.window.fetch_in_flight = true;
                        grid.needs_fetch = false;
                        let (off, lim) = grid.window.fetch_params(grid.focused_row as i64);
                        Some((grid.table_name.clone(), grid.columns.clone(), off, lim))
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some((table, cols, off, lim)) = maybe_fetch {
                    self.spawn_window_fetch(&table, &cols, off, lim);
                }
                self.dirty = true;
            }
            Message::MoveDown => {
                let maybe_fetch = if let Some(ref mut grid) = self.grid {
                    grid.scroll_down(1);
                    if grid.needs_fetch && !grid.window.fetch_in_flight {
                        grid.window.fetch_in_flight = true;
                        grid.needs_fetch = false;
                        let (off, lim) = grid.window.fetch_params(grid.focused_row as i64);
                        Some((grid.table_name.clone(), grid.columns.clone(), off, lim))
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some((table, cols, off, lim)) = maybe_fetch {
                    self.spawn_window_fetch(&table, &cols, off, lim);
                }
                self.dirty = true;
            }
            Message::MoveUp => {
                let maybe_fetch = if let Some(ref mut grid) = self.grid {
                    grid.scroll_up(1);
                    if grid.needs_fetch && !grid.window.fetch_in_flight {
                        grid.window.fetch_in_flight = true;
                        grid.needs_fetch = false;
                        let (off, lim) = grid.window.fetch_params(grid.focused_row as i64);
                        Some((grid.table_name.clone(), grid.columns.clone(), off, lim))
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some((table, cols, off, lim)) = maybe_fetch {
                    self.spawn_window_fetch(&table, &cols, off, lim);
                }
                self.dirty = true;
            }
            Message::MoveRight => {
                if let Some(ref mut grid) = self.grid {
                    grid.move_col_right();
                }
                self.dirty = true;
            }
            Message::MoveLeft => {
                if let Some(ref mut grid) = self.grid {
                    grid.move_col_left();
                }
                self.dirty = true;
            }
            Message::MoveColFirst => {
                if let Some(ref mut grid) = self.grid {
                    grid.move_col_first();
                }
                self.dirty = true;
            }
            Message::MoveColLast => {
                if let Some(ref mut grid) = self.grid {
                    grid.move_col_last();
                }
                self.dirty = true;
            }
            Message::MoveFirstCell => {
                let maybe_fetch = if let Some(ref mut grid) = self.grid {
                    grid.focused_col = 0;
                    grid.h_scroll = 0;
                    grid.scroll_to_row(0);
                    if grid.needs_fetch && !grid.window.fetch_in_flight {
                        grid.window.fetch_in_flight = true;
                        grid.needs_fetch = false;
                        let (off, lim) = grid.window.fetch_params(grid.focused_row as i64);
                        Some((grid.table_name.clone(), grid.columns.clone(), off, lim))
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some((table, cols, off, lim)) = maybe_fetch {
                    self.spawn_window_fetch(&table, &cols, off, lim);
                }
                self.dirty = true;
            }
            Message::MoveLastCell => {
                let maybe_fetch = if let Some(ref mut grid) = self.grid {
                    grid.move_col_last();
                    grid.scroll_to_end();
                    if grid.needs_fetch && !grid.window.fetch_in_flight {
                        grid.window.fetch_in_flight = true;
                        grid.needs_fetch = false;
                        let (off, lim) = grid.window.fetch_params(grid.focused_row as i64);
                        Some((grid.table_name.clone(), grid.columns.clone(), off, lim))
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some((table, cols, off, lim)) = maybe_fetch {
                    self.spawn_window_fetch(&table, &cols, off, lim);
                }
                self.dirty = true;
            }
            Message::OpenPopup => {
                if self.readonly {
                    self.toast.push("Read-only database", ToastKind::Error);
                    return;
                }
                if let Some(ref grid) = self.grid {
                    let col = grid.columns.get(grid.focused_col).cloned();
                    let table = grid.table_name.clone();
                    let cell_value = grid
                        .window
                        .get_row(grid.viewport_start + grid.focused_row as i64)
                        .and_then(|r| r.get(grid.focused_col))
                        .cloned()
                        .unwrap_or(SqlValue::Null);
                    let actual_rowid = grid.viewport_start + grid.focused_row as i64 + 1;

                    if let Some(col) = col {
                        let upper = col.col_type.to_uppercase();
                        let original = cell_value;
                        if upper.contains("DATE") && upper.contains("TIME") {
                            self.popup = Some(PopupKind::DatetimePicker(DatetimePickerState::new(
                                table,
                                actual_rowid,
                                col.name,
                                original,
                            )));
                        } else if upper.contains("DATE") {
                            self.popup = Some(PopupKind::DatePicker(DatePickerState::new(
                                table,
                                actual_rowid,
                                col.name,
                                original,
                            )));
                        } else {
                            self.popup = Some(PopupKind::TextEditor(TextEditorState::new(
                                table,
                                actual_rowid,
                                col.name.clone(),
                                col.col_type.clone(),
                                original,
                                self.readonly,
                            )));
                        }
                        self.mode = AppMode::Edit;
                    }
                }
                self.dirty = true;
            }
            Message::ClosePopup => {
                self.popup = None;
                self.mode = AppMode::Browse;
                self.dirty = true;
            }
            Message::CommitEdit => {
                let write_info = self.popup.as_ref().and_then(|p| match p {
                    PopupKind::TextEditor(s) => Some((
                        s.table.clone(),
                        s.col_name.clone(),
                        s.rowid,
                        s.as_sql_value(),
                    )),
                    PopupKind::ValuePicker(s) => s.selected_value().map(|v| {
                        (
                            s.table.clone(),
                            s.col_name.clone(),
                            s.rowid,
                            SqlValue::Text(v.to_string()),
                        )
                    }),
                    PopupKind::DatePicker(s) => Some((
                        s.table.clone(),
                        s.col_name.clone(),
                        s.rowid,
                        s.as_sql_value(),
                    )),
                    PopupKind::DatetimePicker(s) => Some((
                        s.table.clone(),
                        s.col_name.clone(),
                        s.rowid,
                        s.as_sql_value(),
                    )),
                });
                if let Some((table, col, rowid, value)) = write_info {
                    let pool = Arc::clone(&self.pool);
                    let tx = self.tx.clone();
                    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
                        let conn = pool.get()?;
                        crate::db::write::commit_cell_edit(&conn, &table, &col, rowid, &value)?;
                        let _ = tx.send(Message::EditCommitted { rowid });
                        Ok(())
                    });
                }
                self.popup = None;
                self.mode = AppMode::Browse;
                self.dirty = true;
            }
            Message::EditCommitted { rowid: _ } => {
                self.toast
                    .push("Cell updated successfully", ToastKind::Success);
                self.dirty = true;
            }
            Message::EditFailed(err) => {
                self.toast.push(format!("Error: {}", err), ToastKind::Error);
                self.dirty = true;
            }
            Message::DistinctCountReady { .. } => {}
        }
    }

    pub fn view(&mut self, frame: &mut ratatui::Frame) {
        crate::ui::render(frame, self);
    }

    fn handle_key(&mut self, key: crossterm::event::KeyEvent) {
        use crossterm::event::{KeyCode, KeyModifiers};

        match self.mode {
            AppMode::Edit => {
                self.handle_edit_key(key);
                return;
            }
            AppMode::Browse => {}
        }

        match (key.code, key.modifiers) {
            (KeyCode::Char('b'), KeyModifiers::CONTROL) => {
                self.sidebar_visible = !self.sidebar_visible;
            }
            (KeyCode::Char('w'), KeyModifiers::CONTROL) => {
                if let Some(idx) = self.active_tab {
                    let _ = self.tx.send(Message::CloseTab(idx));
                }
            }
            (KeyCode::Char(c), KeyModifiers::CONTROL) if c.is_ascii_digit() && c != '0' => {
                let idx = (c as usize) - ('1' as usize);
                let _ = self.tx.send(Message::ActivateTab(idx));
            }
            (KeyCode::Tab, KeyModifiers::NONE) => {
                self.focus = match self.focus {
                    FocusPane::Sidebar => FocusPane::Grid,
                    FocusPane::Grid => FocusPane::Sidebar,
                };
            }
            (KeyCode::Tab, KeyModifiers::CONTROL) => {
                let _ = self.tx.send(Message::NextTab);
            }
            (KeyCode::BackTab, _) => {
                let _ = self.tx.send(Message::PrevTab);
            }
            _ => match self.focus {
                FocusPane::Sidebar => self.handle_sidebar_key(key),
                FocusPane::Grid => self.handle_grid_key(key),
            },
        }
    }

    fn handle_edit_key(&mut self, key: crossterm::event::KeyEvent) {
        use crossterm::event::{KeyCode, KeyModifiers};
        match &mut self.popup {
            None => {
                self.mode = AppMode::Browse;
            }
            Some(PopupKind::TextEditor(state)) => match key.code {
                KeyCode::Esc => {
                    let _ = self.tx.send(Message::ClosePopup);
                }
                KeyCode::Enter => {
                    let _ = self.tx.send(Message::CommitEdit);
                }
                KeyCode::Char(c)
                    if key.modifiers == KeyModifiers::NONE
                        || key.modifiers == KeyModifiers::SHIFT =>
                {
                    state.insert_char(c);
                    self.dirty = true;
                }
                KeyCode::Backspace => {
                    state.delete_backward();
                    self.dirty = true;
                }
                KeyCode::Left => {
                    state.move_cursor_left();
                    self.dirty = true;
                }
                KeyCode::Right => {
                    state.move_cursor_right();
                    self.dirty = true;
                }
                _ => {}
            },
            Some(PopupKind::ValuePicker(state)) => match key.code {
                KeyCode::Esc => {
                    let _ = self.tx.send(Message::ClosePopup);
                }
                KeyCode::Enter => {
                    let _ = self.tx.send(Message::CommitEdit);
                }
                KeyCode::Up => {
                    state.move_up();
                    self.dirty = true;
                }
                KeyCode::Down => {
                    state.move_down();
                    self.dirty = true;
                }
                KeyCode::Char(c)
                    if key.modifiers == KeyModifiers::NONE
                        || key.modifiers == KeyModifiers::SHIFT =>
                {
                    state.push_filter_char(c);
                    self.dirty = true;
                }
                KeyCode::Backspace => {
                    state.pop_filter_char();
                    self.dirty = true;
                }
                _ => {}
            },
            Some(PopupKind::DatePicker(state)) => match key.code {
                KeyCode::Esc => {
                    let _ = self.tx.send(Message::ClosePopup);
                }
                KeyCode::Enter => {
                    let _ = self.tx.send(Message::CommitEdit);
                }
                KeyCode::PageUp => {
                    state.prev_month();
                    self.dirty = true;
                }
                KeyCode::PageDown => {
                    state.next_month();
                    self.dirty = true;
                }
                KeyCode::Left => {
                    state.move_day(-1);
                    self.dirty = true;
                }
                KeyCode::Right => {
                    state.move_day(1);
                    self.dirty = true;
                }
                KeyCode::Up => {
                    state.move_day(-7);
                    self.dirty = true;
                }
                KeyCode::Down => {
                    state.move_day(7);
                    self.dirty = true;
                }
                KeyCode::Delete => {
                    state.clear();
                    self.dirty = true;
                }
                _ => {}
            },
            Some(PopupKind::DatetimePicker(state)) => match key.code {
                KeyCode::Esc => {
                    let _ = self.tx.send(Message::ClosePopup);
                }
                KeyCode::Enter => {
                    let _ = self.tx.send(Message::CommitEdit);
                }
                KeyCode::PageUp => {
                    state.prev_month();
                    self.dirty = true;
                }
                KeyCode::PageDown => {
                    state.next_month();
                    self.dirty = true;
                }
                _ => {}
            },
        }
    }

    fn handle_grid_key(&mut self, key: crossterm::event::KeyEvent) {
        use crossterm::event::{KeyCode, KeyModifiers};
        let vp = self.grid.as_ref().map_or(20, |g| g.window.viewport_rows);
        match (key.code, key.modifiers) {
            (KeyCode::Down, _) | (KeyCode::Char('j'), KeyModifiers::NONE) => {
                let _ = self.tx.send(Message::MoveDown);
            }
            (KeyCode::Up, _) | (KeyCode::Char('k'), KeyModifiers::NONE) => {
                let _ = self.tx.send(Message::MoveUp);
            }
            (KeyCode::Right, _) | (KeyCode::Char('l'), KeyModifiers::NONE) => {
                let _ = self.tx.send(Message::MoveRight);
            }
            (KeyCode::Left, _) | (KeyCode::Char('h'), KeyModifiers::NONE) => {
                let _ = self.tx.send(Message::MoveLeft);
            }
            (KeyCode::Home, KeyModifiers::CONTROL) => {
                let _ = self.tx.send(Message::MoveFirstCell);
            }
            (KeyCode::End, KeyModifiers::CONTROL) => {
                let _ = self.tx.send(Message::MoveLastCell);
            }
            (KeyCode::Home, _) => {
                let _ = self.tx.send(Message::MoveColFirst);
            }
            (KeyCode::End, _) => {
                let _ = self.tx.send(Message::MoveColLast);
            }
            (KeyCode::PageDown, _) => {
                let _ = self.tx.send(Message::ScrollDown(vp.saturating_sub(1)));
            }
            (KeyCode::PageUp, _) => {
                let _ = self.tx.send(Message::ScrollUp(vp.saturating_sub(1)));
            }
            (KeyCode::Enter, KeyModifiers::NONE) => {
                let _ = self.tx.send(Message::OpenPopup);
            }
            (KeyCode::Esc, _) => {
                let _ = self.tx.send(Message::ClosePopup);
            }
            _ => {}
        }
    }

    fn handle_sidebar_key(&mut self, key: crossterm::event::KeyEvent) {
        use crossterm::event::KeyCode;

        match key.code {
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Up => self.sidebar.move_up(&self.schema),
            KeyCode::Down => self.sidebar.move_down(&self.schema),
            KeyCode::Enter => {
                if let Some(SidebarAction::OpenTable(name)) = self.sidebar.enter(&self.schema) {
                    let _ = self.tx.send(Message::OpenTable(name));
                }
            }
            _ => {}
        }
    }

    fn open_table(&mut self, name: String) {
        if let Some(idx) = self.open_tabs.iter().position(|t| t.table_name == name) {
            self.active_tab = Some(idx);
        } else {
            self.open_tabs.push(TableTab {
                table_name: name.clone(),
                row_count: None,
            });
            self.active_tab = Some(self.open_tabs.len() - 1);
            self.spawn_row_count(name.clone());

            let cols_and_fks = self
                .schema
                .tables
                .iter()
                .find(|t| t.name == name)
                .map(|tm| {
                    let columns = tm.columns.clone();
                    let fk_names: Vec<String> = tm
                        .foreign_keys
                        .iter()
                        .map(|fk| fk.from_col.clone())
                        .collect();
                    let fk_cols: Vec<bool> =
                        columns.iter().map(|c| fk_names.contains(&c.name)).collect();
                    (columns, fk_cols)
                });

            if let Some((columns, fk_cols)) = cols_and_fks {
                self.spawn_grid_fetch(name, columns, fk_cols);
            }
        }
        self.dirty = true;
    }

    fn spawn_row_count(&self, table: String) {
        let tx = self.tx.clone();
        let pool = Arc::clone(&self.pool);
        tokio::task::spawn(async move {
            let table_inner = table.clone();
            let result = tokio::task::spawn_blocking(move || -> anyhow::Result<i64> {
                let conn = pool.get()?;
                let count: i64 = conn.query_row(
                    &format!("SELECT COUNT(*) FROM \"{}\"", table_inner),
                    [],
                    |row| row.get(0),
                )?;
                Ok(count)
            })
            .await;

            if let Ok(Ok(count)) = result {
                let _ = tx.send(Message::RowCountReady { table, count });
            }
        });
    }

    fn spawn_grid_fetch(&self, table: String, columns: Vec<Column>, fk_cols: Vec<bool>) {
        let tx = self.tx.clone();
        let pool = Arc::clone(&self.pool);
        tokio::task::spawn(async move {
            let table_c = table.clone();
            let cols_c = columns.clone();
            let result = tokio::task::spawn_blocking(
                move || -> anyhow::Result<(Vec<Vec<SqlValue>>, i64)> {
                    let conn = pool.get()?;
                    let total = db::count_rows(&conn, &table_c)?;
                    let rows = db::fetch_rows(&conn, &table_c, &cols_c, 0, 50)?;
                    Ok((rows, total))
                },
            )
            .await;
            if let Ok(Ok((rows, total_rows))) = result {
                let _ = tx.send(Message::GridDataReady {
                    table,
                    columns,
                    fk_cols,
                    rows,
                    total_rows,
                });
            }
        });
    }

    fn spawn_window_fetch(&self, table: &str, columns: &[Column], offset: i64, limit: i64) {
        let pool = Arc::clone(&self.pool);
        let tx = self.tx.clone();
        let table = table.to_string();
        let columns = columns.to_vec();
        tokio::task::spawn(async move {
            let table_c = table.clone();
            let result = tokio::task::spawn_blocking(
                move || -> anyhow::Result<(Vec<Vec<SqlValue>>, i64)> {
                    let conn = pool.get()?;
                    let total = db::count_rows(&conn, &table_c)?;
                    let rows = db::fetch_rows(&conn, &table_c, &columns, offset, limit)?;
                    Ok((rows, total))
                },
            )
            .await;
            if let Ok(Ok((rows, total_rows))) = result {
                let _ = tx.send(Message::WindowReady {
                    table,
                    offset,
                    rows,
                    total_rows,
                });
            }
        });
    }

    fn on_grid_data_ready(
        &mut self,
        table: String,
        columns: Vec<Column>,
        fk_cols: Vec<bool>,
        rows: Vec<Vec<SqlValue>>,
        total_rows: i64,
    ) {
        let is_active = self
            .active_tab
            .and_then(|i| self.open_tabs.get(i))
            .is_some_and(|t| t.table_name == table);
        if is_active {
            let grid_width = 180u16;
            self.grid = Some(crate::grid::GridState::new(
                table, columns, fk_cols, rows, total_rows, grid_width,
            ));
        }
        self.dirty = true;
    }

    fn update_row_count(&mut self, table: String, count: i64) {
        for tab in &mut self.open_tabs {
            if tab.table_name == table {
                tab.row_count = Some(count);
            }
        }
        self.dirty = true;
    }

    fn close_tab(&mut self, idx: usize) {
        if idx < self.open_tabs.len() {
            self.open_tabs.remove(idx);
            self.active_tab = if self.open_tabs.is_empty() {
                None
            } else {
                Some(idx.saturating_sub(1).min(self.open_tabs.len() - 1))
            };
            self.dirty = true;
        }
    }

    fn activate_tab(&mut self, idx: usize) {
        if idx < self.open_tabs.len() {
            self.active_tab = Some(idx);
            self.dirty = true;
        }
    }

    fn next_tab(&mut self) {
        if self.open_tabs.is_empty() {
            return;
        }
        let current = self.active_tab.unwrap_or(0);
        self.active_tab = Some((current + 1) % self.open_tabs.len());
        self.dirty = true;
    }

    fn prev_tab(&mut self) {
        if self.open_tabs.is_empty() {
            return;
        }
        let current = self.active_tab.unwrap_or(0);
        self.active_tab = Some(current.checked_sub(1).unwrap_or(self.open_tabs.len() - 1));
        self.dirty = true;
    }
}
