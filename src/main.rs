mod app;
mod db;
mod input;
mod layout;
mod state;
mod ui;

use std::{env, error::Error, io, time::Duration};

use app::App;
use crossterm::{
    cursor::Show,
    event::{self, DisableMouseCapture, EnableMouseCapture},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use db::Database;
use ratatui::{Terminal, backend::CrosstermBackend};

struct TerminalGuard;

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let mut stdout = io::stdout();
        let _ = execute!(stdout, LeaveAlternateScreen, DisableMouseCapture, Show);
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    let db_path = args.get(1).map(String::as_str);
    let table_name = args.get(2).map(String::as_str);

    let db = Database::open(db_path)?;
    let mut app = App::new(db, table_name)?;

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    let _guard = TerminalGuard;

    run(&mut terminal, &mut app)
}

fn run(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut App,
) -> Result<(), Box<dyn Error>> {
    loop {
        terminal.draw(|frame| ui::render(frame, app))?;
        if app.state.should_quit {
            return Ok(());
        }

        if event::poll(Duration::from_millis(250))? {
            let event = event::read()?;
            app.handle_event(event)?;
        }
    }
}
