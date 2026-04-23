mod app;
mod config;
mod db;
mod event;
mod theme;
mod ui;

use anyhow::Context;
use clap::Parser;
use ratatui::{backend::CrosstermBackend, Terminal};

use app::{App, Message};
use config::Config;

#[derive(Parser, Debug)]
#[command(name = "sqv", about = "Terminal SQLite viewer")]
struct Args {
    /// Path to SQLite database file, or :memory:
    path: String,
    /// Open database in read-only mode
    #[arg(long)]
    readonly: bool,
}

struct TerminalGuard;

impl TerminalGuard {
    fn new() -> anyhow::Result<Self> {
        crossterm::terminal::enable_raw_mode()?;
        crossterm::execute!(
            std::io::stdout(),
            crossterm::terminal::EnterAlternateScreen,
            crossterm::event::EnableMouseCapture,
            crossterm::cursor::Hide,
        )?;
        Ok(Self)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = crossterm::terminal::disable_raw_mode();
        let _ = crossterm::execute!(
            std::io::stdout(),
            crossterm::event::DisableMouseCapture,
            crossterm::terminal::LeaveAlternateScreen,
            crossterm::cursor::Show,
        );
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    if args.path != ":memory:" && !std::path::Path::new(&args.path).exists() {
        eprintln!("Error: database file '{}' does not exist", args.path);
        std::process::exit(1);
    }

    let orig_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = crossterm::terminal::disable_raw_mode();
        let _ = crossterm::execute!(
            std::io::stdout(),
            crossterm::event::DisableMouseCapture,
            crossterm::terminal::LeaveAlternateScreen,
            crossterm::cursor::Show,
        );
        orig_hook(info);
    }));

    let config = Config::load().unwrap_or_default();

    let pool = db::open_pool(&args.path, args.readonly)
        .with_context(|| format!("Failed to open database '{}'", args.path))?;

    let conn = pool.get().context("Failed to get DB connection")?;
    let schema = db::load_schema(&conn).context("Failed to load schema")?;
    drop(conn);

    let _guard = TerminalGuard::new()?;

    let mut terminal = Terminal::new(CrosstermBackend::new(std::io::stdout()))?;
    let mut app = App::new(schema, config);

    run_event_loop(&mut terminal, &mut app).await?;

    Ok(())
}

async fn run_event_loop(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    app: &mut App,
) -> anyhow::Result<()> {
    use futures::StreamExt;

    let mut events = crossterm::event::EventStream::new();
    let mut tick_interval = tokio::time::interval(std::time::Duration::from_millis(33));

    terminal.draw(|f| app.view(f))?;
    app.dirty = false;

    loop {
        tokio::select! {
            _ = tick_interval.tick() => {
                app.update(Message::Tick);
                if app.should_quit {
                    break;
                }
                if app.dirty {
                    terminal.draw(|f| app.view(f))?;
                    app.dirty = false;
                }
            }
            maybe_event = events.next() => {
                match maybe_event {
                    Some(Ok(ev)) => {
                        if let Some(msg) = event::translate_event(ev) {
                            app.update(msg);
                        }
                    }
                    _ => break,
                }
            }
        }
    }

    Ok(())
}
