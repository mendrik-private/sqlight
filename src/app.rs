use std::sync::Arc;

use ratatui::layout::Rect;
use rusqlite::OptionalExtension;
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    config::Config,
    db::{
        self,
        schema::Column,
        schema::Schema,
        types::{affinity, ColAffinity, SqlValue},
        DbPool,
    },
    filter::predicate::filter_to_sql,
    grid::{SortDir, SortSpec},
    theme::Theme,
    ui::{
        popup::{
            CommandPaletteState, DatePickerState, DatetimePickerState, FilterPopupState,
            FkPickerState, PaletteCommand, PopupKind, TextEditorState,
        },
        sidebar::{SidebarAction, SidebarState},
        tabbar::TabMouseAction,
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

pub struct JumpFrame {
    pub table: String,
    pub row: i64,
    #[allow(dead_code)]
    pub col: usize,
}

pub struct TableTab {
    pub table_name: String,
    pub row_count: Option<i64>,
}

#[derive(Debug, Clone)]
pub enum UndoOp {
    Update,
    Insert,
    Delete,
}

#[derive(Debug, Clone)]
pub struct UndoFrame {
    pub op: UndoOp,
    pub table: String,
    pub rowid: i64,
    pub cols: Vec<(String, SqlValue)>,
}

#[derive(Debug, Clone)]
pub enum ConfirmKind {
    DeleteRow { table: String, rowid: i64 },
}

pub struct PendingConfirm {
    pub message: String,
    pub kind: ConfirmKind,
    pub created: std::time::Instant,
    pub timeout_secs: u64,
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
    pub jump_stack: Vec<JumpFrame>,
    pub db_path: String,
    pub undo_stack: Vec<UndoFrame>,
    pub pending_confirm: Option<PendingConfirm>,
    pub tabbar_area: Rect,
    pub sidebar_area: Option<Rect>,
    pub grid_outer_area: Option<Rect>,
    pub grid_inner_area: Option<Rect>,
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
        table: String,
        col: String,
        original: SqlValue,
    },
    EditFailed(String),
    DistinctCountReady {
        col: String,
        count: i64,
        values: Vec<String>,
    },
    JumpToFk,
    FkRowsReady {
        target_table: String,
        rows: Vec<Vec<SqlValue>>,
    },
    JumpBack,
    JumpToTargetRow {
        table: String,
        rowid: i64,
    },
    CycleSort,
    JumpToLetter(char),
    JumpToSortedOffset {
        table: String,
        offset: i64,
    },
    OpenFilterPopup,
    ApplyFilter,
    ClearFilters,
    InsertRow,
    DeleteRow,
    ConfirmDelete,
    CancelConfirm,
    UndoAction,
    RowInserted {
        table: String,
        rowid: i64,
    },
    RowDeleted {
        table: String,
        rowid: i64,
    },
    OpenCommandPalette,
    ExecuteCommand(PaletteCommand),
    ExportDone {
        format: String,
        path: String,
        count: u64,
    },
    ReloadSchema,
    SchemaReady(Schema),
    CopyCell,
}

impl App {
    pub fn new(
        schema: Schema,
        config: Config,
        pool: Arc<DbPool>,
        tx: UnboundedSender<Message>,
        readonly: bool,
        db_path: String,
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
            jump_stack: Vec::new(),
            db_path,
            undo_stack: Vec::new(),
            pending_confirm: None,
            tabbar_area: Rect::default(),
            sidebar_area: None,
            grid_outer_area: None,
            grid_inner_area: None,
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
            Message::Mouse(ev) => {
                self.dirty = true;
                self.handle_mouse(ev);
            }
            Message::Tick => {
                self.toast.tick();
                let maybe_fetch = if let Some(ref mut grid) = self.grid {
                    grid.window.tick_count = grid.window.tick_count.wrapping_add(1);
                    if grid.needs_fetch && !grid.window.fetch_in_flight {
                        grid.window.fetch_in_flight = true;
                        grid.needs_fetch = false;
                        let (off, lim) = grid.window.fetch_params(grid.focused_row as i64);
                        let sort = grid.sort.as_ref().and_then(|s| {
                            grid.columns
                                .get(s.col_idx)
                                .map(|c| (c.name.clone(), s.direction == SortDir::Asc))
                        });
                        Some((
                            grid.table_name.clone(),
                            grid.columns.clone(),
                            sort,
                            off,
                            lim,
                        ))
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some((table, cols, sort, off, lim)) = maybe_fetch {
                    self.spawn_window_fetch(&table, &cols, sort, off, lim);
                    self.dirty = true;
                }
                if self.grid.as_ref().is_some_and(|g| g.window.fetch_in_flight) {
                    self.dirty = true;
                }
                let expired = self
                    .pending_confirm
                    .as_ref()
                    .is_some_and(|c| c.created.elapsed().as_secs() >= c.timeout_secs);
                if expired {
                    self.pending_confirm = None;
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
                        let sort = grid.sort.as_ref().and_then(|s| {
                            grid.columns
                                .get(s.col_idx)
                                .map(|c| (c.name.clone(), s.direction == SortDir::Asc))
                        });
                        Some((
                            grid.table_name.clone(),
                            grid.columns.clone(),
                            sort,
                            off,
                            lim,
                        ))
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some((table, cols, sort, off, lim)) = maybe_fetch {
                    self.spawn_window_fetch(&table, &cols, sort, off, lim);
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
                        let sort = grid.sort.as_ref().and_then(|s| {
                            grid.columns
                                .get(s.col_idx)
                                .map(|c| (c.name.clone(), s.direction == SortDir::Asc))
                        });
                        Some((
                            grid.table_name.clone(),
                            grid.columns.clone(),
                            sort,
                            off,
                            lim,
                        ))
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some((table, cols, sort, off, lim)) = maybe_fetch {
                    self.spawn_window_fetch(&table, &cols, sort, off, lim);
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
                        let sort = grid.sort.as_ref().and_then(|s| {
                            grid.columns
                                .get(s.col_idx)
                                .map(|c| (c.name.clone(), s.direction == SortDir::Asc))
                        });
                        Some((
                            grid.table_name.clone(),
                            grid.columns.clone(),
                            sort,
                            off,
                            lim,
                        ))
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some((table, cols, sort, off, lim)) = maybe_fetch {
                    self.spawn_window_fetch(&table, &cols, sort, off, lim);
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
                        let sort = grid.sort.as_ref().and_then(|s| {
                            grid.columns
                                .get(s.col_idx)
                                .map(|c| (c.name.clone(), s.direction == SortDir::Asc))
                        });
                        Some((
                            grid.table_name.clone(),
                            grid.columns.clone(),
                            sort,
                            off,
                            lim,
                        ))
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some((table, cols, sort, off, lim)) = maybe_fetch {
                    self.spawn_window_fetch(&table, &cols, sort, off, lim);
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
                        let sort = grid.sort.as_ref().and_then(|s| {
                            grid.columns
                                .get(s.col_idx)
                                .map(|c| (c.name.clone(), s.direction == SortDir::Asc))
                        });
                        Some((
                            grid.table_name.clone(),
                            grid.columns.clone(),
                            sort,
                            off,
                            lim,
                        ))
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some((table, cols, sort, off, lim)) = maybe_fetch {
                    self.spawn_window_fetch(&table, &cols, sort, off, lim);
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
                        let sort = grid.sort.as_ref().and_then(|s| {
                            grid.columns
                                .get(s.col_idx)
                                .map(|c| (c.name.clone(), s.direction == SortDir::Asc))
                        });
                        Some((
                            grid.table_name.clone(),
                            grid.columns.clone(),
                            sort,
                            off,
                            lim,
                        ))
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some((table, cols, sort, off, lim)) = maybe_fetch {
                    self.spawn_window_fetch(&table, &cols, sort, off, lim);
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
                        let sort = grid.sort.as_ref().and_then(|s| {
                            grid.columns
                                .get(s.col_idx)
                                .map(|c| (c.name.clone(), s.direction == SortDir::Asc))
                        });
                        Some((
                            grid.table_name.clone(),
                            grid.columns.clone(),
                            sort,
                            off,
                            lim,
                        ))
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some((table, cols, sort, off, lim)) = maybe_fetch {
                    self.spawn_window_fetch(&table, &cols, sort, off, lim);
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
                        let sort = grid.sort.as_ref().and_then(|s| {
                            grid.columns
                                .get(s.col_idx)
                                .map(|c| (c.name.clone(), s.direction == SortDir::Asc))
                        });
                        Some((
                            grid.table_name.clone(),
                            grid.columns.clone(),
                            sort,
                            off,
                            lim,
                        ))
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some((table, cols, sort, off, lim)) = maybe_fetch {
                    self.spawn_window_fetch(&table, &cols, sort, off, lim);
                }
                self.dirty = true;
            }
            Message::OpenPopup => {
                if self.readonly {
                    self.toast.push("Read-only database", ToastKind::Error);
                    return;
                }
                if let Some(ref grid) = self.grid {
                    let col_idx = grid.focused_col;
                    let col = grid.columns.get(col_idx).cloned();
                    let table_name = grid.table_name.clone();
                    let abs_row = grid.viewport_start + grid.focused_row as i64;
                    let cell_value = grid
                        .window
                        .get_row(abs_row)
                        .and_then(|r| r.get(col_idx))
                        .cloned()
                        .unwrap_or(SqlValue::Null);
                    let actual_rowid = abs_row + 1;
                    let is_fk = grid.fk_cols.get(col_idx).copied().unwrap_or(false);

                    if let Some(col) = col {
                        if is_fk {
                            let table_meta =
                                self.schema.tables.iter().find(|t| t.name == table_name);
                            let fk_opt = table_meta.and_then(|tm| {
                                tm.foreign_keys
                                    .iter()
                                    .find(|fk| fk.from_col == col.name)
                                    .cloned()
                            });
                            if let Some(fk) = fk_opt {
                                let target_meta =
                                    self.schema.tables.iter().find(|t| t.name == fk.to_table);
                                let display_cols = target_meta
                                    .map(|tm| {
                                        let preferred = ["name", "title", "label", "description"];
                                        let mut cols: Vec<String> = preferred
                                            .iter()
                                            .filter_map(|&p| {
                                                tm.columns
                                                    .iter()
                                                    .find(|c| c.name.to_lowercase() == p)
                                                    .map(|c| c.name.clone())
                                            })
                                            .take(3)
                                            .collect();
                                        if cols.is_empty() {
                                            cols = tm
                                                .columns
                                                .iter()
                                                .filter(|c| {
                                                    let up = c.col_type.to_uppercase();
                                                    up.contains("TEXT") || up.contains("CHAR")
                                                })
                                                .take(3)
                                                .map(|c| c.name.clone())
                                                .collect();
                                        }
                                        if cols.is_empty() {
                                            cols = tm
                                                .columns
                                                .iter()
                                                .take(3)
                                                .map(|c| c.name.clone())
                                                .collect();
                                        }
                                        cols
                                    })
                                    .unwrap_or_default();

                                let picker_state = FkPickerState::new(
                                    fk.to_table.clone(),
                                    fk.to_col.clone(),
                                    display_cols.clone(),
                                    table_name.clone(),
                                    col.name.clone(),
                                    actual_rowid,
                                    cell_value.clone(),
                                );
                                self.popup = Some(PopupKind::FkPicker(picker_state));
                                self.mode = AppMode::Edit;

                                let pool = Arc::clone(&self.pool);
                                let tx = self.tx.clone();
                                let to_table = fk.to_table.clone();
                                let to_col = fk.to_col.clone();
                                let disp_cols = display_cols;
                                tokio::task::spawn(async move {
                                    let to_table_c = to_table.clone();
                                    let result = tokio::task::spawn_blocking(
                                        move || -> anyhow::Result<Vec<Vec<SqlValue>>> {
                                            let conn = pool.get()?;
                                            let col_list = std::iter::once(&to_col)
                                                .chain(disp_cols.iter())
                                                .map(|c| format!("\"{}\"", c))
                                                .collect::<Vec<_>>()
                                                .join(", ");
                                            let sql = format!(
                                                "SELECT {} FROM \"{}\" LIMIT 200",
                                                col_list, to_table_c
                                            );
                                            let mut stmt = conn.prepare(&sql)?;
                                            let col_count = 1 + disp_cols.len();
                                            let rows = stmt
                                                .query_map([], |row| {
                                                    let mut vals = Vec::new();
                                                    for i in 0..col_count {
                                                        let v = match row.get_ref(i)? {
                                                            rusqlite::types::ValueRef::Null => {
                                                                SqlValue::Null
                                                            }
                                                            rusqlite::types::ValueRef::Integer(
                                                                n,
                                                            ) => SqlValue::Integer(n),
                                                            rusqlite::types::ValueRef::Real(f) => {
                                                                SqlValue::Real(f)
                                                            }
                                                            rusqlite::types::ValueRef::Text(b) => {
                                                                SqlValue::Text(
                                                                    String::from_utf8_lossy(b)
                                                                        .into_owned(),
                                                                )
                                                            }
                                                            rusqlite::types::ValueRef::Blob(b) => {
                                                                SqlValue::Blob(b.to_vec())
                                                            }
                                                        };
                                                        vals.push(v);
                                                    }
                                                    Ok(vals)
                                                })?
                                                .collect::<Result<Vec<_>, _>>()?;
                                            Ok(rows)
                                        },
                                    )
                                    .await;
                                    if let Ok(Ok(rows)) = result {
                                        let _ = tx.send(Message::FkRowsReady {
                                            target_table: to_table,
                                            rows,
                                        });
                                    }
                                });

                                self.dirty = true;
                                return;
                            }
                        }

                        let upper = col.col_type.to_uppercase();
                        let original = cell_value;
                        if upper.contains("TIMESTAMP")
                            || upper.contains("DATETIME")
                            || (upper.contains("DATE") && upper.contains("TIME"))
                        {
                            self.popup = Some(PopupKind::DatetimePicker(DatetimePickerState::new(
                                table_name,
                                actual_rowid,
                                col.name,
                                original,
                            )));
                        } else if upper.contains("DATE") {
                            self.popup = Some(PopupKind::DatePicker(DatePickerState::new(
                                table_name,
                                actual_rowid,
                                col.name,
                                original,
                            )));
                        } else {
                            self.popup = Some(PopupKind::TextEditor(TextEditorState::new(
                                table_name,
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
                if self.readonly {
                    self.toast.push("Read-only database", ToastKind::Error);
                    self.popup = None;
                    self.mode = AppMode::Browse;
                    self.dirty = true;
                    return;
                }
                let write_info = self.popup.as_ref().and_then(|p| match p {
                    PopupKind::TextEditor(s) => Some((
                        s.table.clone(),
                        s.col_name.clone(),
                        s.rowid,
                        s.as_sql_value(),
                        s.original.clone(),
                    )),
                    PopupKind::ValuePicker(s) => s.selected_value().map(|v| {
                        (
                            s.table.clone(),
                            s.col_name.clone(),
                            s.rowid,
                            SqlValue::Text(v.to_string()),
                            s.original.clone(),
                        )
                    }),
                    PopupKind::DatePicker(s) => Some((
                        s.table.clone(),
                        s.col_name.clone(),
                        s.rowid,
                        s.as_sql_value(),
                        s.original.clone(),
                    )),
                    PopupKind::DatetimePicker(s) => Some((
                        s.table.clone(),
                        s.col_name.clone(),
                        s.rowid,
                        s.as_sql_value(),
                        s.original.clone(),
                    )),
                    PopupKind::FkPicker(s) => s.selected_value().cloned().map(|v| {
                        (
                            s.source_table.clone(),
                            s.source_col.clone(),
                            s.source_rowid,
                            v,
                            s.original.clone(),
                        )
                    }),
                    PopupKind::FilterPopup(_) => None,
                    PopupKind::CommandPalette(_) => None,
                });
                if let Some((table, col, rowid, value, original)) = write_info {
                    let pool = Arc::clone(&self.pool);
                    let tx = self.tx.clone();
                    let table_c = table.clone();
                    let col_c = col.clone();
                    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
                        let conn = pool.get()?;
                        crate::db::write::commit_cell_edit(&conn, &table_c, &col_c, rowid, &value)?;
                        let _ = tx.send(Message::EditCommitted {
                            rowid,
                            table: table_c,
                            col: col_c,
                            original,
                        });
                        Ok(())
                    });
                }
                self.popup = None;
                self.mode = AppMode::Browse;
                self.dirty = true;
            }
            Message::EditCommitted {
                rowid: _,
                table: _,
                col,
                original,
            } => {
                self.undo_stack.push(UndoFrame {
                    op: UndoOp::Update,
                    table: self
                        .grid
                        .as_ref()
                        .map(|g| g.table_name.clone())
                        .unwrap_or_default(),
                    rowid: self
                        .grid
                        .as_ref()
                        .map(|g| g.viewport_start + g.focused_row as i64 + 1)
                        .unwrap_or(0),
                    cols: vec![(col, original)],
                });
                if self.undo_stack.len() > 100 {
                    self.undo_stack.remove(0);
                }
                if let Some(ref mut grid) = self.grid {
                    grid.window.rows.clear();
                    grid.needs_fetch = true;
                }
                self.toast.push("Cell updated", ToastKind::Success);
                self.dirty = true;
            }
            Message::EditFailed(err) => {
                self.toast.push(format!("Error: {}", err), ToastKind::Error);
                self.dirty = true;
            }
            Message::DistinctCountReady { .. } => {}
            Message::FkRowsReady { target_table, rows } => {
                if let Some(PopupKind::FkPicker(ref mut state)) = self.popup {
                    if state.target_table == target_table {
                        state.rows = rows;
                        state.loading = false;
                    }
                }
                self.dirty = true;
            }
            Message::JumpToFk => {
                let jump_info = self.grid.as_ref().and_then(|g| {
                    let col_idx = g.focused_col;
                    if !g.fk_cols.get(col_idx).copied().unwrap_or(false) {
                        return None;
                    }
                    let table_meta = self.schema.tables.iter().find(|t| t.name == g.table_name)?;
                    let col = g.columns.get(col_idx)?;
                    let fk = table_meta
                        .foreign_keys
                        .iter()
                        .find(|fk| fk.from_col == col.name)?;
                    let abs_row = g.viewport_start + g.focused_row as i64;
                    let cell_val = g.window.get_row(abs_row)?.get(col_idx)?.clone();
                    let frame = JumpFrame {
                        table: g.table_name.clone(),
                        row: g.focused_row as i64,
                        col: col_idx,
                    };
                    Some((fk.to_table.clone(), fk.to_col.clone(), cell_val, frame))
                });

                if let Some((to_table, to_col, cell_val, frame)) = jump_info {
                    self.jump_stack.push(frame);
                    let _ = self.tx.send(Message::OpenTable(to_table.clone()));
                    let pool = Arc::clone(&self.pool);
                    let tx = self.tx.clone();
                    tokio::task::spawn(async move {
                        let to_table_c = to_table.clone();
                        let to_col_c = to_col.clone();
                        let result =
                            tokio::task::spawn_blocking(move || -> anyhow::Result<Option<i64>> {
                                let conn = pool.get()?;
                                let val = match &cell_val {
                                    SqlValue::Integer(n) => rusqlite::types::Value::Integer(*n),
                                    SqlValue::Text(s) => rusqlite::types::Value::Text(s.clone()),
                                    SqlValue::Real(f) => rusqlite::types::Value::Real(*f),
                                    _ => rusqlite::types::Value::Null,
                                };
                                let rowid: Option<i64> = conn
                                    .query_row(
                                        &format!(
                                            "SELECT rowid FROM \"{}\" WHERE \"{}\" = ?1 LIMIT 1",
                                            to_table_c, to_col_c
                                        ),
                                        rusqlite::params![val],
                                        |row| row.get(0),
                                    )
                                    .optional()?;
                                Ok(rowid)
                            })
                            .await;
                        if let Ok(Ok(Some(rowid))) = result {
                            let _ = tx.send(Message::JumpToTargetRow {
                                table: to_table,
                                rowid,
                            });
                        }
                    });
                }
                self.dirty = true;
            }
            Message::JumpToTargetRow { table, rowid } => {
                let maybe_fetch = if let Some(ref mut grid) = self.grid {
                    if grid.table_name == table {
                        grid.scroll_to_row(rowid - 1);
                        if grid.needs_fetch && !grid.window.fetch_in_flight {
                            grid.window.fetch_in_flight = true;
                            grid.needs_fetch = false;
                            let (off, lim) = grid.window.fetch_params(grid.focused_row as i64);
                            let sort = grid.sort.as_ref().and_then(|s| {
                                grid.columns
                                    .get(s.col_idx)
                                    .map(|c| (c.name.clone(), s.direction == SortDir::Asc))
                            });
                            Some((
                                grid.table_name.clone(),
                                grid.columns.clone(),
                                sort,
                                off,
                                lim,
                            ))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some((t, cols, sort, off, lim)) = maybe_fetch {
                    self.spawn_window_fetch(&t, &cols, sort, off, lim);
                }
                self.dirty = true;
            }
            Message::CycleSort => {
                let maybe_fetch = if let Some(ref mut grid) = self.grid {
                    let col_idx = grid.focused_col;
                    grid.sort = match &grid.sort {
                        None => Some(SortSpec {
                            col_idx,
                            direction: SortDir::Asc,
                        }),
                        Some(s) if s.col_idx == col_idx => match s.direction {
                            SortDir::Asc => Some(SortSpec {
                                col_idx,
                                direction: SortDir::Desc,
                            }),
                            SortDir::Desc => None,
                        },
                        Some(_) => Some(SortSpec {
                            col_idx,
                            direction: SortDir::Asc,
                        }),
                    };
                    grid.viewport_start = 0;
                    grid.focused_row = 0;
                    grid.window.rows.clear();
                    grid.window.offset = 0;
                    if !grid.window.fetch_in_flight {
                        grid.window.fetch_in_flight = true;
                        let (off, lim) = grid.window.fetch_params(0);
                        let sort = grid.sort.as_ref().and_then(|s| {
                            grid.columns
                                .get(s.col_idx)
                                .map(|c| (c.name.clone(), s.direction == SortDir::Asc))
                        });
                        Some((
                            grid.table_name.clone(),
                            grid.columns.clone(),
                            sort,
                            off,
                            lim,
                        ))
                    } else {
                        grid.needs_fetch = true;
                        None
                    }
                } else {
                    None
                };
                if let Some((table, cols, sort, off, lim)) = maybe_fetch {
                    self.spawn_window_fetch(&table, &cols, sort, off, lim);
                }
                self.dirty = true;
            }
            Message::JumpToSortedOffset { table, offset } => {
                let maybe_fetch = if let Some(ref mut grid) = self.grid {
                    if grid.table_name == table {
                        grid.scroll_to_row(offset);
                        if grid.needs_fetch && !grid.window.fetch_in_flight {
                            grid.window.fetch_in_flight = true;
                            grid.needs_fetch = false;
                            let (off, lim) = grid.window.fetch_params(grid.focused_row as i64);
                            let sort = grid.sort.as_ref().and_then(|s| {
                                grid.columns
                                    .get(s.col_idx)
                                    .map(|c| (c.name.clone(), s.direction == SortDir::Asc))
                            });
                            Some((
                                grid.table_name.clone(),
                                grid.columns.clone(),
                                sort,
                                off,
                                lim,
                            ))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some((t, cols, sort, off, lim)) = maybe_fetch {
                    self.spawn_window_fetch(&t, &cols, sort, off, lim);
                }
                self.dirty = true;
            }
            Message::JumpToLetter(letter) => {
                if let Some(ref grid) = self.grid {
                    let sort = grid.sort.as_ref().cloned();
                    if let Some(sort) = sort {
                        if let Some(col) = grid.columns.get(sort.col_idx).cloned() {
                            let pool = Arc::clone(&self.pool);
                            let tx = self.tx.clone();
                            let table = grid.table_name.clone();
                            let col_name = col.name.clone();
                            let dir_asc = sort.direction == SortDir::Asc;
                            let letter_uc = letter.to_uppercase().next().unwrap_or(letter);
                            let table_inner = table.clone();
                            tokio::task::spawn(async move {
                                let result = tokio::task::spawn_blocking(move || -> anyhow::Result<i64> {
                                    let conn = pool.get()?;
                                    let offset: i64 = if dir_asc {
                                        if letter == '#' {
                                            conn.query_row(
                                                &format!(
                                                    "SELECT COUNT(*) FROM \"{}\" WHERE \"{}\" IS NULL",
                                                    table_inner, col_name
                                                ),
                                                [],
                                                |row| row.get(0),
                                            )?
                                        } else {
                                            conn.query_row(
                                                &format!(
                                                    "SELECT COUNT(*) FROM \"{}\" WHERE \"{}\" IS NULL OR \"{}\" < ?1",
                                                    table_inner, col_name, col_name
                                                ),
                                                rusqlite::params![letter_uc.to_string()],
                                                |row| row.get(0),
                                            )?
                                        }
                                    } else if letter == '#' {
                                        conn.query_row(
                                            &format!(
                                                "SELECT COUNT(*) FROM \"{}\" WHERE \"{}\" IS NOT NULL AND \"{}\" NOT GLOB '[0-9]*'",
                                                table_inner, col_name, col_name
                                            ),
                                            [],
                                            |row| row.get(0),
                                        )?
                                    } else {
                                        let pattern = format!("{}%", letter_uc);
                                        conn.query_row(
                                            &format!(
                                                "SELECT COUNT(*) FROM \"{}\" WHERE \"{}\" > ?1 AND \"{}\" NOT LIKE ?2",
                                                table_inner, col_name, col_name
                                            ),
                                            rusqlite::params![letter_uc.to_string(), pattern],
                                            |row| row.get(0),
                                        )?
                                    };
                                    Ok(offset)
                                })
                                .await;
                                if let Ok(Ok(offset)) = result {
                                    let _ = tx.send(Message::JumpToSortedOffset { table, offset });
                                }
                            });
                        }
                    }
                }
                self.dirty = true;
            }
            Message::JumpBack => {
                if let Some(frame) = self.jump_stack.pop() {
                    let _ = self.tx.send(Message::OpenTable(frame.table.clone()));
                    let _ = self.tx.send(Message::JumpToTargetRow {
                        table: frame.table,
                        rowid: frame.row + 1,
                    });
                }
                self.dirty = true;
            }
            Message::OpenFilterPopup => {
                if let Some(ref grid) = self.grid {
                    let col_idx = grid.focused_col;
                    if let Some(col) = grid.columns.get(col_idx) {
                        let col_name = col.name.clone();
                        let col_filter = grid
                            .filter
                            .columns
                            .get(&col_name)
                            .cloned()
                            .unwrap_or_default();
                        self.popup = Some(PopupKind::FilterPopup(FilterPopupState::new(
                            col_name, col_filter,
                        )));
                        self.mode = AppMode::Edit;
                    }
                }
                self.dirty = true;
            }
            Message::ApplyFilter => {
                if let Some(PopupKind::FilterPopup(state)) = self.popup.take() {
                    if let Some(ref mut grid) = self.grid {
                        grid.filter
                            .columns
                            .insert(state.col_name.clone(), state.col_filter);
                        grid.viewport_start = 0;
                        grid.focused_row = 0;
                        grid.window.rows.clear();
                        grid.window.offset = 0;
                        let table = grid.table_name.clone();
                        let cols = grid.columns.clone();
                        let sort = grid.sort.as_ref().and_then(|s| {
                            grid.columns
                                .get(s.col_idx)
                                .map(|c| (c.name.clone(), s.direction == SortDir::Asc))
                        });
                        let (off, lim) = grid.window.fetch_params(0);
                        grid.window.fetch_in_flight = true;
                        let filter = grid.filter.clone();
                        let db_path = self.db_path.clone();
                        let _ = crate::filter::save_filter(&filter, &db_path, &table);
                        self.spawn_window_fetch_with_filter(&table, &cols, sort, off, lim, filter);
                    }
                    self.mode = AppMode::Browse;
                }
                self.dirty = true;
            }
            Message::ClearFilters => {
                if let Some(ref mut grid) = self.grid {
                    grid.filter = crate::filter::FilterSet::default();
                    grid.viewport_start = 0;
                    grid.focused_row = 0;
                    grid.window.rows.clear();
                    grid.window.offset = 0;
                    let table = grid.table_name.clone();
                    let cols = grid.columns.clone();
                    let sort = grid.sort.as_ref().and_then(|s| {
                        grid.columns
                            .get(s.col_idx)
                            .map(|c| (c.name.clone(), s.direction == SortDir::Asc))
                    });
                    let (off, lim) = grid.window.fetch_params(0);
                    grid.window.fetch_in_flight = true;
                    let db_path = self.db_path.clone();
                    let empty_filter = crate::filter::FilterSet::default();
                    let _ = crate::filter::save_filter(&empty_filter, &db_path, &table);
                    self.spawn_window_fetch_with_filter(
                        &table,
                        &cols,
                        sort,
                        off,
                        lim,
                        empty_filter,
                    );
                }
                self.popup = None;
                self.mode = AppMode::Browse;
                self.dirty = true;
            }
            Message::InsertRow => {
                if self.readonly {
                    self.toast.push("Read-only database", ToastKind::Error);
                    return;
                }
                let table = self.grid.as_ref().map(|g| g.table_name.clone());
                if let Some(table) = table {
                    let pool = Arc::clone(&self.pool);
                    let tx = self.tx.clone();
                    let table_c = table.clone();
                    tokio::task::spawn(async move {
                        let result = tokio::task::spawn_blocking(move || {
                            let conn = pool.get()?;
                            let rowid = crate::db::write::insert_default_row(&conn, &table_c)?;
                            Ok::<i64, anyhow::Error>(rowid)
                        })
                        .await;
                        match result {
                            Ok(Ok(rowid)) => {
                                let _ = tx.send(Message::RowInserted { table, rowid });
                            }
                            Ok(Err(e)) => {
                                let _ = tx.send(Message::EditFailed(e.to_string()));
                            }
                            Err(_) => {}
                        }
                    });
                }
                self.dirty = true;
            }
            Message::RowInserted { table, rowid } => {
                if let Some(ref mut grid) = self.grid {
                    if grid.table_name == table {
                        self.undo_stack.push(UndoFrame {
                            op: UndoOp::Insert,
                            table: table.clone(),
                            rowid,
                            cols: Vec::new(),
                        });
                        if self.undo_stack.len() > 100 {
                            self.undo_stack.remove(0);
                        }
                        grid.window.rows.clear();
                        grid.window.total_rows += 1;
                        grid.needs_fetch = true;
                        if let Some(idx) = self.active_tab {
                            if let Some(tab) = self.open_tabs.get_mut(idx) {
                                tab.row_count = tab.row_count.map(|n| n + 1);
                            }
                        }
                    }
                }
                self.toast.push("Row inserted", ToastKind::Success);
                self.dirty = true;
            }
            Message::DeleteRow => {
                if self.readonly {
                    self.toast.push("Read-only database", ToastKind::Error);
                    return;
                }
                if let Some(ref grid) = self.grid {
                    let row_num = grid.focused_row + 1;
                    let table = grid.table_name.clone();
                    let approx_rowid = grid.viewport_start + grid.focused_row as i64 + 1;
                    let msg = format!("Delete row #{}? [y/n]", row_num);
                    self.pending_confirm = Some(PendingConfirm {
                        message: msg,
                        kind: ConfirmKind::DeleteRow {
                            table,
                            rowid: approx_rowid,
                        },
                        created: std::time::Instant::now(),
                        timeout_secs: 5,
                    });
                }
                self.dirty = true;
            }
            Message::ConfirmDelete => {
                if let Some(confirm) = self.pending_confirm.take() {
                    match confirm.kind {
                        ConfirmKind::DeleteRow { table, rowid } => {
                            let pool = Arc::clone(&self.pool);
                            let tx_ch = self.tx.clone();
                            let columns = self
                                .grid
                                .as_ref()
                                .map(|g| g.columns.clone())
                                .unwrap_or_default();
                            let table_c = table.clone();
                            tokio::task::spawn(async move {
                                drop(columns);
                                let tx_err = tx_ch.clone();
                                let result =
                                    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
                                        let conn = pool.get()?;
                                        crate::db::write::delete_row(&conn, &table_c, rowid)?;
                                        let _ = tx_ch.send(Message::RowDeleted {
                                            table: table_c,
                                            rowid,
                                        });
                                        Ok(())
                                    })
                                    .await;
                                if let Ok(Err(e)) = result {
                                    let _ = tx_err.send(Message::EditFailed(e.to_string()));
                                }
                            });
                        }
                    }
                }
                self.dirty = true;
            }
            Message::RowDeleted { table, rowid } => {
                self.undo_stack.push(UndoFrame {
                    op: UndoOp::Delete,
                    table: table.clone(),
                    rowid,
                    cols: Vec::new(),
                });
                if self.undo_stack.len() > 100 {
                    self.undo_stack.remove(0);
                }
                if let Some(ref mut grid) = self.grid {
                    if grid.table_name == table {
                        grid.window.rows.clear();
                        grid.window.total_rows = grid.window.total_rows.saturating_sub(1);
                        grid.needs_fetch = true;
                        if let Some(idx) = self.active_tab {
                            if let Some(tab) = self.open_tabs.get_mut(idx) {
                                tab.row_count = tab.row_count.map(|n| n.saturating_sub(1));
                            }
                        }
                    }
                }
                self.toast.push("Row deleted", ToastKind::Success);
                self.dirty = true;
            }
            Message::CancelConfirm => {
                self.pending_confirm = None;
                self.dirty = true;
            }
            Message::UndoAction => {
                if let Some(frame) = self.undo_stack.pop() {
                    match frame.op {
                        UndoOp::Update => {
                            if self.readonly {
                                self.toast.push("Read-only: cannot undo", ToastKind::Error);
                                return;
                            }
                            let pool = Arc::clone(&self.pool);
                            let tx = self.tx.clone();
                            let table = frame.table.clone();
                            let rowid = frame.rowid;
                            let cols = frame.cols.clone();
                            tokio::task::spawn(async move {
                                let result =
                                    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
                                        let conn = pool.get()?;
                                        for (col_name, value) in &cols {
                                            crate::db::write::commit_cell_edit(
                                                &conn, &table, col_name, rowid, value,
                                            )?;
                                        }
                                        Ok(())
                                    })
                                    .await;
                                if let Ok(Err(e)) = result {
                                    let _ = tx.send(Message::EditFailed(e.to_string()));
                                }
                            });
                            let toast_msg = if frame.cols.len() == 1 {
                                format!(
                                    "Undo: reverted col \"{}\" on row {}",
                                    frame.cols[0].0, frame.rowid
                                )
                            } else {
                                format!(
                                    "Undo: reverted {} cols on row {}",
                                    frame.cols.len(),
                                    frame.rowid
                                )
                            };
                            self.toast.push(toast_msg, ToastKind::Info);
                        }
                        UndoOp::Insert => {
                            let pool = Arc::clone(&self.pool);
                            let tx = self.tx.clone();
                            let table = frame.table.clone();
                            let rowid = frame.rowid;
                            tokio::task::spawn(async move {
                                let result = tokio::task::spawn_blocking(move || {
                                    let conn = pool.get()?;
                                    crate::db::write::delete_row(&conn, &table, rowid)?;
                                    Ok::<_, anyhow::Error>(())
                                })
                                .await;
                                if let Ok(Err(e)) = result {
                                    let _ = tx.send(Message::EditFailed(e.to_string()));
                                }
                            });
                            self.toast.push(
                                format!("Undo: deleted inserted row {}", frame.rowid),
                                ToastKind::Info,
                            );
                        }
                        UndoOp::Delete => {
                            if frame.cols.is_empty() {
                                self.toast.push(
                                    "Undo: cannot restore deleted row (no backup)",
                                    ToastKind::Error,
                                );
                                return;
                            }
                            let pool = Arc::clone(&self.pool);
                            let tx = self.tx.clone();
                            let table = frame.table.clone();
                            let rowid = frame.rowid;
                            let cols = frame.cols.clone();
                            tokio::task::spawn(async move {
                                let result =
                                    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
                                        let conn = pool.get()?;
                                        crate::db::write::reinsert_row(
                                            &conn, &table, rowid, &cols,
                                        )?;
                                        Ok(())
                                    })
                                    .await;
                                if let Ok(Err(e)) = result {
                                    let _ = tx.send(Message::EditFailed(e.to_string()));
                                }
                            });
                            self.toast.push(
                                format!("Undo: restored deleted row {}", rowid),
                                ToastKind::Info,
                            );
                        }
                    }
                    if let Some(ref mut grid) = self.grid {
                        grid.window.rows.clear();
                        grid.needs_fetch = true;
                    }
                    self.dirty = true;
                } else {
                    self.toast.push("Nothing to undo", ToastKind::Info);
                    self.dirty = true;
                }
            }
            Message::OpenCommandPalette => {
                let table_names = self.schema.tables.iter().map(|t| t.name.clone()).collect();
                let state = CommandPaletteState::new(table_names);
                self.popup = Some(PopupKind::CommandPalette(state));
                self.mode = AppMode::Edit;
                self.dirty = true;
            }
            Message::ExecuteCommand(cmd) => {
                self.execute_palette_command(cmd);
                self.dirty = true;
            }
            Message::ExportDone {
                format: _,
                path,
                count,
            } => {
                self.toast.push(
                    format!("Exported {} rows to {}", count, path),
                    ToastKind::Success,
                );
                self.dirty = true;
            }
            Message::ReloadSchema => {
                let pool = Arc::clone(&self.pool);
                let tx = self.tx.clone();
                tokio::task::spawn(async move {
                    let result = tokio::task::spawn_blocking(move || {
                        let conn = pool.get()?;
                        crate::db::load_schema(&conn)
                    })
                    .await;
                    if let Ok(Ok(schema)) = result {
                        let _ = tx.send(Message::SchemaReady(schema));
                    }
                });
                self.dirty = true;
            }
            Message::SchemaReady(schema) => {
                self.schema = schema;
                self.sidebar.tables_expanded = true;
                self.toast.push("Schema reloaded", ToastKind::Success);
                self.dirty = true;
            }
            Message::CopyCell => {
                let text = self.grid.as_ref().and_then(|g| {
                    let abs_row = g.focused_row as i64;
                    let col_idx = g.focused_col;
                    g.window.get_row(abs_row)?.get(col_idx).map(|v| match v {
                        SqlValue::Null => "NULL".to_string(),
                        SqlValue::Integer(n) => n.to_string(),
                        SqlValue::Real(f) => f.to_string(),
                        SqlValue::Text(s) => s.clone(),
                        SqlValue::Blob(b) => format!("<blob {} bytes>", b.len()),
                    })
                });
                if let Some(text) = text {
                    use std::io::Write;
                    let encoded = base64_encode(text.as_bytes());
                    let osc52 = format!("\x1b]52;c;{}\x07", encoded);
                    let _ = std::io::stdout().write_all(osc52.as_bytes());
                    let _ = std::io::stdout().flush();
                    self.toast.push("Copied to clipboard", ToastKind::Success);
                }
                self.dirty = true;
            }
        }
    }

    pub fn view(&mut self, frame: &mut ratatui::Frame) {
        crate::ui::render(frame, self);
    }

    fn execute_palette_command(&mut self, cmd: PaletteCommand) {
        match cmd {
            PaletteCommand::ExportCsv | PaletteCommand::ExportJson | PaletteCommand::ExportSql => {
                if let Some(ref grid) = self.grid {
                    let table = grid.table_name.clone();
                    let columns = grid.columns.clone();
                    let sort = grid.sort.clone();
                    let filter = grid.filter.clone();
                    let pool = Arc::clone(&self.pool);
                    let tx = self.tx.clone();
                    let format = match &cmd {
                        PaletteCommand::ExportCsv => "csv",
                        PaletteCommand::ExportJson => "json",
                        PaletteCommand::ExportSql => "sql",
                        _ => "csv",
                    }
                    .to_string();
                    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
                    let export_path = format!("{}/sqv_export.{}", home, format);
                    let export_path_clone = export_path.clone();
                    tokio::task::spawn(async move {
                        let format_c = format.clone();
                        let result = tokio::task::spawn_blocking(move || -> anyhow::Result<u64> {
                            let conn = pool.get()?;
                            let path = std::path::Path::new(&export_path);
                            match format_c.as_str() {
                                "csv" => crate::export::export_csv(
                                    &conn, &table, &columns, &filter, &sort, path,
                                ),
                                "json" => crate::export::export_json(
                                    &conn, &table, &columns, &filter, &sort, path,
                                ),
                                "sql" => crate::export::export_sql(
                                    &conn, &table, &columns, &filter, &sort, path,
                                ),
                                _ => Err(anyhow::anyhow!("unknown format")),
                            }
                        })
                        .await;
                        if let Ok(Ok(count)) = result {
                            let _ = tx.send(Message::ExportDone {
                                format,
                                path: export_path_clone,
                                count,
                            });
                        } else if let Ok(Err(e)) = result {
                            let _ = tx.send(Message::EditFailed(e.to_string()));
                        }
                    });
                }
            }
            PaletteCommand::SwitchTable(name) => {
                let _ = self.tx.send(Message::OpenTable(name));
            }
            PaletteCommand::ToggleSidebar => {
                self.sidebar_visible = !self.sidebar_visible;
            }
            PaletteCommand::ToggleReadonly => {
                self.readonly = !self.readonly;
                let msg = if self.readonly {
                    "Read-only mode enabled"
                } else {
                    "Read-only mode disabled"
                };
                self.toast.push(msg, ToastKind::Info);
            }
            PaletteCommand::ResetColumnWidths => {
                if let Some(ref mut grid) = self.grid {
                    grid.manual_widths.clear();
                    let avail = grid.avail_col_width;
                    let sample_rows: Vec<Vec<SqlValue>> =
                        grid.window.rows.iter().take(50).cloned().collect();
                    grid.col_widths = crate::grid::layout::compute_col_widths(
                        &grid.columns,
                        &sample_rows,
                        avail,
                        &grid.manual_widths,
                        &grid.fk_cols,
                    );
                }
                self.toast.push("Column widths reset", ToastKind::Info);
            }
            PaletteCommand::ClearFilters => {
                let _ = self.tx.send(Message::ClearFilters);
                self.toast.push("Filters cleared", ToastKind::Info);
            }
            PaletteCommand::ReloadSchema => {
                let _ = self.tx.send(Message::ReloadSchema);
            }
            PaletteCommand::CopyCell => {
                let _ = self.tx.send(Message::CopyCell);
            }
            PaletteCommand::CopyRowJson => {
                self.copy_row_as_json();
            }
            PaletteCommand::Quit => {
                self.should_quit = true;
            }
            PaletteCommand::OpenDb => {
                self.toast.push(
                    "Use --path argument to open a different DB",
                    ToastKind::Info,
                );
            }
        }
    }

    fn copy_row_as_json(&self) {
        let json = self.grid.as_ref().and_then(|g| {
            let abs_row = g.focused_row as i64;
            let row = g.window.get_row(abs_row)?;
            let fields: Vec<String> = g
                .columns
                .iter()
                .zip(row)
                .map(|(col, val)| {
                    let v_json = match val {
                        SqlValue::Null => "null".to_string(),
                        SqlValue::Integer(n) => n.to_string(),
                        SqlValue::Real(f) => f.to_string(),
                        SqlValue::Text(s) => format!("\"{}\"", s.replace('"', "\\\"")),
                        SqlValue::Blob(_) => "null".to_string(),
                    };
                    format!("\"{}\": {}", col.name.replace('"', "\\\""), v_json)
                })
                .collect();
            Some(format!("{{{}}}", fields.join(", ")))
        });
        if let Some(text) = json {
            use std::io::Write;
            let encoded = base64_encode(text.as_bytes());
            let osc52 = format!("\x1b]52;c;{}\x07", encoded);
            let _ = std::io::stdout().write_all(osc52.as_bytes());
            let _ = std::io::stdout().flush();
        }
    }

    fn handle_mouse(&mut self, mouse: crossterm::event::MouseEvent) {
        use crossterm::event::{MouseButton, MouseEventKind};

        if self.popup.is_some() || matches!(self.mode, AppMode::Edit) {
            return;
        }

        let middle_click = matches!(mouse.kind, MouseEventKind::Down(MouseButton::Middle));
        let left_click = matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left));
        if !left_click && !middle_click {
            return;
        }

        let x = mouse.column;
        let y = mouse.row;

        if let Some(action) =
            crate::ui::tabbar::hit_test(self.tabbar_area, self, x, y, middle_click)
        {
            match action {
                TabMouseAction::Activate(idx) => self.activate_tab(idx),
                TabMouseAction::Close(idx) => self.close_tab(idx),
            }
            return;
        }

        if let Some(area) = self.sidebar_area {
            if self.sidebar_visible
                && x >= area.x
                && x < area.x + area.width
                && y >= area.y
                && y < area.y + area.height
            {
                self.focus = FocusPane::Sidebar;
                if left_click {
                    if let Some(action) = self.sidebar.click_at(area, &self.schema, x, y) {
                        match action {
                            SidebarAction::OpenTable(name) => self.open_table(name),
                            SidebarAction::Toggle => {}
                        }
                    }
                }
                return;
            }
        }

        if let Some(area) = self.grid_inner_area {
            if x >= area.x && x < area.x + area.width && y >= area.y && y < area.y + area.height {
                self.focus = FocusPane::Grid;
                if !left_click {
                    return;
                }
                let mut cycle_sort = false;
                if let Some(grid) = self.grid.as_mut() {
                    if let Some(hit) = crate::grid::hit_test(area, grid, x, y) {
                        match hit {
                            crate::grid::GridHit::Header(col) => {
                                grid.focus_cell(grid.focused_row, col);
                                cycle_sort = true;
                            }
                            crate::grid::GridHit::RowGutter(row) => {
                                grid.focus_cell(row, grid.focused_col);
                            }
                            crate::grid::GridHit::Cell { row, col } => {
                                grid.focus_cell(row, col);
                            }
                            crate::grid::GridHit::Scrollbar => {}
                        }
                    }
                }
                if cycle_sort {
                    let _ = self.tx.send(Message::CycleSort);
                }
            }
        }
    }

    fn handle_key(&mut self, key: crossterm::event::KeyEvent) {
        use crossterm::event::{KeyCode, KeyModifiers};

        // Handle pending confirmation dialog first
        if self.pending_confirm.is_some() {
            match key.code {
                KeyCode::Char('y') | KeyCode::Char('Y') => {
                    let _ = self.tx.send(Message::ConfirmDelete);
                    return;
                }
                KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => {
                    let _ = self.tx.send(Message::CancelConfirm);
                    return;
                }
                _ => {}
            }
        }

        match self.mode {
            AppMode::Edit => {
                self.handle_edit_key(key);
                return;
            }
            AppMode::Browse => {}
        }

        // Ctrl-P / Ctrl-Shift-P opens command palette (checked after edit handling)
        if (key.code == KeyCode::Char('p') || key.code == KeyCode::Char('P'))
            && key.modifiers.contains(KeyModifiers::CONTROL)
        {
            let _ = self.tx.send(Message::OpenCommandPalette);
            return;
        }

        match (key.code, key.modifiers) {
            (KeyCode::Char('z'), KeyModifiers::CONTROL) => {
                let _ = self.tx.send(Message::UndoAction);
            }
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
            Some(PopupKind::FkPicker(state)) => match key.code {
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
            Some(PopupKind::FilterPopup(state)) => match key.code {
                KeyCode::Esc => {
                    self.popup = None;
                    self.mode = AppMode::Browse;
                    self.dirty = true;
                }
                KeyCode::Enter => {
                    let _ = self.tx.send(Message::ApplyFilter);
                }
                KeyCode::Char('n') if key.modifiers == KeyModifiers::NONE => {
                    state.editing = true;
                    state.edit_value.clear();
                    self.dirty = true;
                }
                KeyCode::Delete => {
                    if state.selected_rule < state.col_filter.rules.len() {
                        state.col_filter.rules.remove(state.selected_rule);
                        if state.selected_rule > 0
                            && state.selected_rule >= state.col_filter.rules.len()
                        {
                            state.selected_rule -= 1;
                        }
                    }
                    self.dirty = true;
                }
                KeyCode::Up => {
                    state.selected_rule = state.selected_rule.saturating_sub(1);
                    self.dirty = true;
                }
                KeyCode::Down => {
                    if !state.col_filter.rules.is_empty() {
                        state.selected_rule =
                            (state.selected_rule + 1).min(state.col_filter.rules.len() - 1);
                    }
                    self.dirty = true;
                }
                KeyCode::Char(' ') => {
                    if let Some(rule) = state.col_filter.rules.get_mut(state.selected_rule) {
                        rule.enabled = !rule.enabled;
                    }
                    self.dirty = true;
                }
                KeyCode::Char(c)
                    if state.editing
                        && (key.modifiers == KeyModifiers::NONE
                            || key.modifiers == KeyModifiers::SHIFT) =>
                {
                    state.edit_value.push(c);
                    self.dirty = true;
                }
                KeyCode::Backspace if state.editing => {
                    state.edit_value.pop();
                    self.dirty = true;
                }
                _ => {}
            },
            Some(PopupKind::CommandPalette(state)) => match key.code {
                KeyCode::Esc => {
                    let _ = self.tx.send(Message::ClosePopup);
                }
                KeyCode::Enter => {
                    if let Some(cmd) = state.selected_command() {
                        let _ = self.tx.send(Message::ClosePopup);
                        let _ = self.tx.send(Message::ExecuteCommand(cmd));
                    }
                }
                KeyCode::Up => {
                    state.move_up();
                    self.dirty = true;
                }
                KeyCode::Down => {
                    state.move_down();
                    self.dirty = true;
                }
                KeyCode::Char(c) => {
                    state.push_char(c);
                    self.dirty = true;
                }
                KeyCode::Backspace => {
                    state.pop_char();
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
            (KeyCode::Down, _) => {
                let _ = self.tx.send(Message::MoveDown);
            }
            (KeyCode::Char('j'), KeyModifiers::NONE) => {
                let is_fk = self
                    .grid
                    .as_ref()
                    .and_then(|g| g.fk_cols.get(g.focused_col).copied())
                    .unwrap_or(false);
                if is_fk {
                    let _ = self.tx.send(Message::JumpToFk);
                } else {
                    let _ = self.tx.send(Message::MoveDown);
                }
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
            (KeyCode::Backspace, _) => {
                if !self.jump_stack.is_empty() {
                    let _ = self.tx.send(Message::JumpBack);
                }
            }
            (KeyCode::Char('s'), KeyModifiers::NONE) => {
                let _ = self.tx.send(Message::CycleSort);
            }
            (KeyCode::Char('f'), KeyModifiers::NONE) => {
                let _ = self.tx.send(Message::OpenFilterPopup);
            }
            (KeyCode::Char('F'), KeyModifiers::SHIFT) => {
                let _ = self.tx.send(Message::ClearFilters);
            }
            (KeyCode::Char('i'), KeyModifiers::NONE) => {
                let _ = self.tx.send(Message::InsertRow);
            }
            (KeyCode::Char('d'), KeyModifiers::NONE) => {
                let _ = self.tx.send(Message::DeleteRow);
            }
            (KeyCode::Char(c), KeyModifiers::NONE)
                if c.is_alphabetic()
                    && !matches!(c, 'j' | 'k' | 'h' | 'l' | 's' | 'f' | 'i' | 'd') =>
            {
                let is_text_sort = self.grid.as_ref().is_some_and(|g| {
                    if let Some(sort) = &g.sort {
                        if let Some(col) = g.columns.get(sort.col_idx) {
                            return matches!(affinity(&col.col_type), ColAffinity::Text);
                        }
                    }
                    false
                });
                if is_text_sort {
                    let _ = self.tx.send(Message::JumpToLetter(c));
                }
            }
            (KeyCode::Char('#'), KeyModifiers::NONE) => {
                let is_text_sort = self.grid.as_ref().is_some_and(|g| {
                    if let Some(sort) = &g.sort {
                        if let Some(col) = g.columns.get(sort.col_idx) {
                            return matches!(affinity(&col.col_type), ColAffinity::Text);
                        }
                    }
                    false
                });
                if is_text_sort {
                    let _ = self.tx.send(Message::JumpToLetter('#'));
                }
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
                    let total = db::count_rows(&conn, &table_c, "", &[])?;
                    let rows = db::fetch_rows(&conn, &table_c, &cols_c, 0, 50, None, "", &[])?;
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

    fn spawn_window_fetch(
        &self,
        table: &str,
        columns: &[Column],
        sort: Option<(String, bool)>,
        offset: i64,
        limit: i64,
    ) {
        let filter = self
            .grid
            .as_ref()
            .map(|g| g.filter.clone())
            .unwrap_or_default();
        self.spawn_window_fetch_with_filter(table, columns, sort, offset, limit, filter);
    }

    fn spawn_window_fetch_with_filter(
        &self,
        table: &str,
        columns: &[Column],
        sort: Option<(String, bool)>,
        offset: i64,
        limit: i64,
        filter: crate::filter::FilterSet,
    ) {
        let pool = Arc::clone(&self.pool);
        let tx = self.tx.clone();
        let table = table.to_string();
        let columns = columns.to_vec();
        tokio::task::spawn(async move {
            let table_c = table.clone();
            let result = tokio::task::spawn_blocking(
                move || -> anyhow::Result<(Vec<Vec<SqlValue>>, i64)> {
                    let conn = pool.get()?;
                    let (where_clause, where_params) = filter_to_sql(&filter);
                    let total = db::count_rows(&conn, &table_c, &where_clause, &where_params)?;
                    let order_by = sort.as_ref().map(|(s, b)| (s.as_str(), *b));
                    let rows = db::fetch_rows(
                        &conn,
                        &table_c,
                        &columns,
                        offset,
                        limit,
                        order_by,
                        &where_clause,
                        &where_params,
                    )?;
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
            let mut grid = crate::grid::GridState::new(
                table.clone(),
                columns,
                fk_cols,
                rows,
                total_rows,
                grid_width,
            );
            if let Ok(saved_filter) = crate::filter::load_filter(&self.db_path, &table) {
                if !saved_filter.is_empty() {
                    grid.filter = saved_filter.clone();
                    let cols = grid.columns.clone();
                    let (off, lim) = grid.window.fetch_params(0);
                    grid.window.fetch_in_flight = true;
                    self.spawn_window_fetch_with_filter(
                        &table,
                        &cols,
                        None,
                        off,
                        lim,
                        saved_filter,
                    );
                }
            }
            self.grid = Some(grid);
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

fn base64_encode(data: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in data.chunks(3) {
        let b0 = chunk[0];
        let b1 = chunk.get(1).copied().unwrap_or(0);
        let b2 = chunk.get(2).copied().unwrap_or(0);
        out.push(CHARS[(b0 >> 2) as usize] as char);
        out.push(CHARS[((b0 & 0x3) << 4 | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            out.push(CHARS[((b1 & 0xf) << 2 | (b2 >> 6)) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(CHARS[(b2 & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}
