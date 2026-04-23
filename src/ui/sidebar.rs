use ratatui::{
    layout::Rect,
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, ListState},
    Frame,
};

use crate::{config::Config, db::schema::Schema, theme::Theme};

pub enum SidebarAction {
    OpenTable(String),
    Toggle,
}

pub struct SidebarState {
    pub selected: usize,
    pub tables_expanded: bool,
    pub views_expanded: bool,
    pub indexes_expanded: bool,
    list_state: ListState,
}

impl Default for SidebarState {
    fn default() -> Self {
        let mut list_state = ListState::default();
        list_state.select(Some(0));
        Self {
            selected: 0,
            tables_expanded: true,
            views_expanded: true,
            indexes_expanded: true,
            list_state,
        }
    }
}

impl SidebarState {
    fn visible_count(&self, schema: &Schema) -> usize {
        let mut count = 3; // three section headers always visible
        if self.tables_expanded {
            count += schema.tables.len();
        }
        if self.views_expanded {
            count += schema.views.len();
        }
        if self.indexes_expanded {
            count += schema.indexes.len();
        }
        count
    }

    fn views_header_idx(&self, schema: &Schema) -> usize {
        1 + if self.tables_expanded {
            schema.tables.len()
        } else {
            0
        }
    }

    fn indexes_header_idx(&self, schema: &Schema) -> usize {
        self.views_header_idx(schema)
            + 1
            + if self.views_expanded {
                schema.views.len()
            } else {
                0
            }
    }

    pub fn move_down(&mut self, schema: &Schema) {
        let total = self.visible_count(schema);
        self.selected = (self.selected + 1) % total;
        self.list_state.select(Some(self.selected));
    }

    pub fn move_up(&mut self, schema: &Schema) {
        let total = self.visible_count(schema);
        self.selected = self.selected.checked_sub(1).unwrap_or(total - 1);
        self.list_state.select(Some(self.selected));
    }

    pub fn enter(&mut self, schema: &Schema) -> Option<SidebarAction> {
        let views_header = self.views_header_idx(schema);
        let indexes_header = self.indexes_header_idx(schema);

        if self.selected == 0 {
            self.tables_expanded = !self.tables_expanded;
            self.clamp_selection(schema);
            return Some(SidebarAction::Toggle);
        }

        if self.tables_expanded && self.selected > 0 && self.selected < views_header {
            let idx = self.selected - 1;
            return schema
                .tables
                .get(idx)
                .map(|t| SidebarAction::OpenTable(t.name.clone()));
        }

        if self.selected == views_header {
            self.views_expanded = !self.views_expanded;
            self.clamp_selection(schema);
            return Some(SidebarAction::Toggle);
        }

        if self.selected == indexes_header {
            self.indexes_expanded = !self.indexes_expanded;
            self.clamp_selection(schema);
            return Some(SidebarAction::Toggle);
        }

        None
    }

    fn clamp_selection(&mut self, schema: &Schema) {
        let total = self.visible_count(schema);
        if self.selected >= total {
            self.selected = total.saturating_sub(1);
        }
        self.list_state.select(Some(self.selected));
    }
}

pub fn render_sidebar(
    frame: &mut Frame,
    area: Rect,
    schema: &Schema,
    state: &mut SidebarState,
    theme: &Theme,
    config: &Config,
    focused: bool,
) {
    let border_color = if focused { theme.accent } else { theme.line };
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(border_color))
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
    let accent_style = Style::default()
        .fg(theme.accent)
        .add_modifier(Modifier::BOLD);

    let mut items: Vec<ListItem> = Vec::new();

    let tables_arrow = if state.tables_expanded { "▼" } else { "▶" };
    items.push(ListItem::new(Line::from(Span::styled(
        format!("{} TABLES ({})", tables_arrow, schema.tables.len()),
        header_style,
    ))));
    if state.tables_expanded {
        for table in &schema.tables {
            let icon_span =
                Span::styled(format!(" {} ", table_icon), Style::default().fg(theme.teal));
            let name_span = Span::styled(table.name.clone(), name_style);
            items.push(ListItem::new(Line::from(vec![icon_span, name_span])));
        }
    }

    let views_arrow = if state.views_expanded { "▼" } else { "▶" };
    items.push(ListItem::new(Line::from(Span::styled(
        format!("{} VIEWS ({})", views_arrow, schema.views.len()),
        header_style,
    ))));
    if state.views_expanded {
        for view in &schema.views {
            let icon_span = Span::styled(
                format!(" {} ", view_icon),
                Style::default().fg(theme.purple),
            );
            let name_span = Span::styled(view.name.clone(), name_style);
            items.push(ListItem::new(Line::from(vec![icon_span, name_span])));
        }
    }

    let indexes_arrow = if state.indexes_expanded { "▼" } else { "▶" };
    items.push(ListItem::new(Line::from(Span::styled(
        format!("{} INDEXES ({})", indexes_arrow, schema.indexes.len()),
        header_style,
    ))));
    if state.indexes_expanded {
        for index in &schema.indexes {
            let icon_span = Span::styled(
                format!(" {} ", index_icon),
                Style::default().fg(theme.yellow),
            );
            let name_span = Span::styled(index.name.clone(), name_style);
            items.push(ListItem::new(Line::from(vec![icon_span, name_span])));
        }
    }

    let list = List::new(items)
        .highlight_style(accent_style)
        .highlight_symbol("▶ ");

    frame.render_stateful_widget(list, inner, &mut state.list_state);
}
