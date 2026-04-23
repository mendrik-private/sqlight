mod app;
mod config;
mod db;
mod event;
mod export;
mod filter;
mod grid;
mod theme;
mod ui;

use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use ratatui::{backend::CrosstermBackend, Terminal};

use app::{App, Message};
use config::Config;

#[derive(Parser, Debug)]
#[command(name = "sqv", about = "Terminal SQLite viewer")]
struct Args {
    /// Path to SQLite database file, or :memory:
    #[arg(required_unless_present = "subcommand")]
    path: Option<String>,
    /// Open database in read-only mode
    #[arg(long)]
    readonly: bool,
    #[command(subcommand)]
    subcommand: Option<SubCmd>,
}

#[derive(clap::Subcommand, Debug)]
enum SubCmd {
    /// Check terminal capabilities
    CheckTerminal,
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

    if let Some(SubCmd::CheckTerminal) = &args.subcommand {
        check_terminal();
        return Ok(());
    }

    let path = args.path.as_deref().unwrap_or(":memory:");

    if path != ":memory:" && !std::path::Path::new(path).exists() {
        eprintln!("Error: database file '{}' does not exist", path);
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

    let pool = Arc::new(
        db::open_pool(path, args.readonly)
            .with_context(|| format!("Failed to open database '{}'", path))?,
    );

    let conn = pool.get().context("Failed to get DB connection")?;
    let schema = db::load_schema(&conn).context("Failed to load schema")?;
    drop(conn);

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Message>();

    let _guard = TerminalGuard::new()?;

    let mut terminal = Terminal::new(CrosstermBackend::new(std::io::stdout()))?;
    let mut app = App::new(schema, config, pool, tx, args.readonly, path.to_string());

    run_event_loop(&mut terminal, &mut app, &mut rx).await?;

    Ok(())
}

async fn run_event_loop(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    app: &mut App,
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<Message>,
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
            Some(msg) = rx.recv() => {
                app.update(msg);
            }
        }
    }

    Ok(())
}

fn check_terminal() {
    let colorterm = std::env::var("COLORTERM").unwrap_or_else(|_| "unknown".to_string());
    let color_support = match colorterm.to_lowercase().as_str() {
        "truecolor" | "24bit" => "truecolor ✓",
        "256color" => "256color",
        _ => "unknown",
    };

    let term_program = std::env::var("TERM_PROGRAM").unwrap_or_else(|_| "unknown".to_string());
    let term = std::env::var("TERM").unwrap_or_default();
    let mouse = if term.contains("xterm") || !term_program.is_empty() {
        "yes"
    } else {
        "unknown"
    };

    println!("Terminal capabilities:");
    println!("  COLORTERM: {}", color_support);
    println!(
        "  TERM_PROGRAM: {}",
        if term_program.is_empty() {
            "unknown".to_string()
        } else {
            term_program
        }
    );
    println!("  Mouse support: {} (from TERM)", mouse);
    println!("  Unicode: yes");
    println!(
        "  Nerd fonts: not detectable (set ui.nerd_font = false in config if icons look wrong)"
    );
}
