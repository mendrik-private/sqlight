use crate::{config::Config, db::schema::Schema, theme::Theme, ui::sidebar::SidebarState};

pub struct App {
    pub schema: Schema,
    pub sidebar: SidebarState,
    pub should_quit: bool,
    pub dirty: bool,
    pub theme: Theme,
    pub config: Config,
}

pub enum Message {
    Quit,
    Key(#[allow(dead_code)] crossterm::event::KeyEvent),
    Mouse(#[allow(dead_code)] crossterm::event::MouseEvent),
    Resize(#[allow(dead_code)] u16, #[allow(dead_code)] u16),
    Tick,
}

impl App {
    pub fn new(schema: Schema, config: Config) -> Self {
        Self {
            schema,
            sidebar: SidebarState::default(),
            should_quit: false,
            dirty: true,
            theme: Theme::default(),
            config,
        }
    }

    pub fn update(&mut self, msg: Message) {
        match msg {
            Message::Quit => self.should_quit = true,
            Message::Resize(_, _) => self.dirty = true,
            Message::Key(_) => self.dirty = true,
            Message::Mouse(_) => {}
            Message::Tick => {}
        }
    }

    pub fn view(&mut self, frame: &mut ratatui::Frame) {
        crate::ui::render(frame, self);
    }
}
