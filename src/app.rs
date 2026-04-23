use std::sync::Arc;

use tokio::sync::mpsc::UnboundedSender;

use crate::{
    config::Config,
    db::{schema::Schema, DbPool},
    theme::Theme,
    ui::sidebar::{SidebarAction, SidebarState},
};

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
    pool: Arc<DbPool>,
    tx: UnboundedSender<Message>,
}

pub enum Message {
    Quit,
    Key(crossterm::event::KeyEvent),
    Mouse(crossterm::event::MouseEvent),
    Resize(u16, u16),
    Tick,
    OpenTable(String),
    RowCountReady { table: String, count: i64 },
    CloseTab(usize),
    ActivateTab(usize),
    NextTab,
    PrevTab,
}

impl App {
    pub fn new(
        schema: Schema,
        config: Config,
        pool: Arc<DbPool>,
        tx: UnboundedSender<Message>,
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
            Message::Tick => {}
            Message::OpenTable(name) => self.open_table(name),
            Message::RowCountReady { table, count } => self.update_row_count(table, count),
            Message::CloseTab(idx) => self.close_tab(idx),
            Message::ActivateTab(idx) => self.activate_tab(idx),
            Message::NextTab => self.next_tab(),
            Message::PrevTab => self.prev_tab(),
        }
    }

    pub fn view(&mut self, frame: &mut ratatui::Frame) {
        crate::ui::render(frame, self);
    }

    fn handle_key(&mut self, key: crossterm::event::KeyEvent) {
        use crossterm::event::{KeyCode, KeyModifiers};

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
                FocusPane::Grid => {}
            },
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
            self.spawn_row_count(name);
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
