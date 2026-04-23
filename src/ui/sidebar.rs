use ratatui::{
    layout::Rect,
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, ListState},
    Frame,
};

use crate::{config::Config, db::schema::Schema, theme::Theme};

pub struct SidebarState {
    pub list_state: ListState,
}

impl Default for SidebarState {
    fn default() -> Self {
        let mut list_state = ListState::default();
        list_state.select(Some(0));
        Self { list_state }
    }
}

pub fn render_sidebar(
    frame: &mut Frame,
    area: Rect,
    schema: &Schema,
    state: &mut SidebarState,
    theme: &Theme,
    config: &Config,
) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(theme.line))
        .title(Span::styled("▌ SCHEMA", Style::default().fg(theme.fg_mute)));

    let inner = block.inner(area);
    frame.render_widget(block, area);

    let (table_icon, view_icon, index_icon) = if config.nerd_font {
        ("󰓫", "󰈈", "󰓹")
    } else {
        ("[T]", "[V]", "[I]")
    };

    let header_style = Style::default()
        .fg(theme.fg_mute)
        .add_modifier(Modifier::BOLD);
    let name_style = Style::default().fg(theme.fg_dim);
    let accent_style = Style::default().fg(theme.accent);

    let mut items: Vec<ListItem> = Vec::new();

    // Tables section
    items.push(ListItem::new(Line::from(Span::styled(
        format!("TABLES ({})", schema.tables.len()),
        header_style,
    ))));
    for table in &schema.tables {
        let icon_span = Span::styled(format!(" {} ", table_icon), Style::default().fg(theme.teal));
        let name_span = Span::styled(table.name.clone(), name_style);
        items.push(ListItem::new(Line::from(vec![icon_span, name_span])));
    }

    // Views section
    items.push(ListItem::new(Line::from(Span::styled(
        format!("VIEWS ({})", schema.views.len()),
        header_style,
    ))));
    for view in &schema.views {
        let icon_span = Span::styled(
            format!(" {} ", view_icon),
            Style::default().fg(theme.purple),
        );
        let name_span = Span::styled(view.name.clone(), name_style);
        items.push(ListItem::new(Line::from(vec![icon_span, name_span])));
    }

    // Indexes section
    items.push(ListItem::new(Line::from(Span::styled(
        format!("INDEXES ({})", schema.indexes.len()),
        header_style,
    ))));
    for index in &schema.indexes {
        let icon_span = Span::styled(
            format!(" {} ", index_icon),
            Style::default().fg(theme.yellow),
        );
        let name_span = Span::styled(index.name.clone(), name_style);
        items.push(ListItem::new(Line::from(vec![icon_span, name_span])));
    }

    let list = List::new(items)
        .highlight_style(accent_style)
        .highlight_symbol("▶ ");

    frame.render_stateful_widget(list, inner, &mut state.list_state);
}
