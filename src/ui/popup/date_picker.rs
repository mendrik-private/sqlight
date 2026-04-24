use chrono::{Datelike, Duration, NaiveDate};
use ratatui::{
    layout::Rect,
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{block::BorderType, Block, Borders, Paragraph},
    Frame,
};

use crate::{db::types::SqlValue, theme::Theme};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DateFocus {
    Day,
    Month,
    Year,
    Calendar,
}

#[allow(dead_code)]
pub struct DatePickerState {
    pub table: String,
    pub rowid: i64,
    pub col_name: String,
    pub current: Option<NaiveDate>,
    pub view_month: NaiveDate,
    pub original: SqlValue,
    pub focus: DateFocus,
}

impl DatePickerState {
    pub fn new(table: String, rowid: i64, col_name: String, original: SqlValue) -> Self {
        let current = match &original {
            SqlValue::Text(s) => NaiveDate::parse_from_str(s, "%Y-%m-%d").ok(),
            _ => None,
        };
        let today = chrono::Local::now().date_naive();
        let base = current.unwrap_or(today);
        let view_month = first_of_month(base).unwrap_or(today);
        Self {
            table,
            rowid,
            col_name,
            current,
            view_month,
            original,
            focus: DateFocus::Day,
        }
    }

    pub fn prev_month(&mut self) {
        self.view_month = shift_month(self.view_month, -1);
        self.sync_current_into_month();
    }

    pub fn next_month(&mut self) {
        self.view_month = shift_month(self.view_month, 1);
        self.sync_current_into_month();
    }

    pub fn move_day(&mut self, delta: i64) {
        let current = self.selected_date();
        let next = current + Duration::days(delta);
        self.current = Some(next);
        self.view_month = first_of_month(next).unwrap_or(self.view_month);
    }

    pub fn clear(&mut self) {
        self.current = None;
    }

    pub fn focus_next(&mut self) {
        self.focus = match self.focus {
            DateFocus::Day => DateFocus::Month,
            DateFocus::Month => DateFocus::Year,
            DateFocus::Year => DateFocus::Calendar,
            DateFocus::Calendar => DateFocus::Day,
        };
    }

    pub fn focus_prev(&mut self) {
        self.focus = match self.focus {
            DateFocus::Day => DateFocus::Calendar,
            DateFocus::Month => DateFocus::Day,
            DateFocus::Year => DateFocus::Month,
            DateFocus::Calendar => DateFocus::Year,
        };
    }

    pub fn adjust_focused(&mut self, delta: i32) {
        match self.focus {
            DateFocus::Day => self.adjust_day(delta),
            DateFocus::Month => self.adjust_month(delta),
            DateFocus::Year => self.adjust_year(delta),
            DateFocus::Calendar => self.move_day(delta as i64),
        }
    }

    pub fn calendar_left(&mut self) {
        if self.focus == DateFocus::Calendar {
            self.move_day(-1);
        }
    }

    pub fn calendar_right(&mut self) {
        if self.focus == DateFocus::Calendar {
            self.move_day(1);
        }
    }

    pub fn as_sql_value(&self) -> SqlValue {
        match self.current {
            Some(d) => SqlValue::Text(d.format("%Y-%m-%d").to_string()),
            None => SqlValue::Null,
        }
    }

    fn selected_date(&self) -> NaiveDate {
        self.current.unwrap_or(self.view_month)
    }

    fn adjust_day(&mut self, delta: i32) {
        let date = self.selected_date();
        let max_day = days_in_month(date.year(), date.month());
        let day = (date.day() as i32 + delta).clamp(1, max_day as i32) as u32;
        self.current = NaiveDate::from_ymd_opt(date.year(), date.month(), day);
        self.sync_current_into_month();
    }

    fn adjust_month(&mut self, delta: i32) {
        let date = self.selected_date();
        let shifted = shift_month(date, delta);
        self.current = NaiveDate::from_ymd_opt(
            shifted.year(),
            shifted.month(),
            date.day()
                .min(days_in_month(shifted.year(), shifted.month())),
        );
        self.sync_current_into_month();
    }

    fn adjust_year(&mut self, delta: i32) {
        let date = self.selected_date();
        let year = date.year().saturating_add(delta);
        self.current = NaiveDate::from_ymd_opt(
            year,
            date.month(),
            date.day().min(days_in_month(year, date.month())),
        );
        self.sync_current_into_month();
    }

    fn sync_current_into_month(&mut self) {
        if let Some(current) = self.current {
            self.view_month = first_of_month(current).unwrap_or(self.view_month);
        }
    }
}

pub fn render(frame: &mut Frame, area: Rect, state: &DatePickerState, theme: &Theme) {
    let popup_width = 34u16.min(area.width);
    let popup_height = 15u16.min(area.height);
    let x = area.x + (area.width.saturating_sub(popup_width)) / 2;
    let y = area.y + (area.height.saturating_sub(popup_height)) / 2;
    let popup_area = Rect {
        x,
        y,
        width: popup_width,
        height: popup_height,
    };

    super::paint_popup_surface(frame, popup_area, theme);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(theme.accent))
        .title(format!(" Date: {} ", state.col_name))
        .style(Style::default().bg(theme.bg_raised));

    let inner = block.inner(popup_area);
    frame.render_widget(block, popup_area);

    let selected = state.selected_date();
    let mut lines = vec![
        render_date_inputs(state, selected, theme),
        divider(inner.width, theme),
        Line::from(Span::styled(
            format!(" {:^28} ", state.view_month.format("%B %Y")),
            Style::default().fg(theme.accent).bg(theme.bg_raised),
        )),
        Line::from(Span::styled(
            " Mo Tue Wed Thu Fri Sat Sun",
            Style::default().fg(theme.fg_dim).bg(theme.bg_raised),
        )),
    ];
    lines.extend(render_calendar_lines(state, theme));
    lines.push(divider(inner.width, theme));
    lines.push(Line::from(Span::styled(
        " Tab next · Shift-Tab prev · PgUp/PgDn month · Enter ok",
        Style::default().fg(theme.fg_faint).bg(theme.bg_raised),
    )));

    frame.render_widget(
        Paragraph::new(lines).style(Style::default().bg(theme.bg_raised)),
        inner,
    );
}

fn render_date_inputs(
    state: &DatePickerState,
    selected: NaiveDate,
    theme: &Theme,
) -> Line<'static> {
    Line::from(vec![
        field_span(
            "D",
            &format!("{:02}", selected.day()),
            state.focus == DateFocus::Day,
            theme,
        ),
        Span::raw("  "),
        field_span(
            "M",
            &format!("{:02}", selected.month()),
            state.focus == DateFocus::Month,
            theme,
        ),
        Span::raw("  "),
        field_span(
            "Y",
            &format!("{:04}", selected.year()),
            state.focus == DateFocus::Year,
            theme,
        ),
    ])
}

fn field_span(label: &str, value: &str, focused: bool, theme: &Theme) -> Span<'static> {
    let style = if focused {
        Style::default()
            .fg(theme.bg)
            .bg(theme.accent)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(theme.fg).bg(theme.bg_soft)
    };
    Span::styled(format!("{label}:{value}"), style)
}

fn render_calendar_lines(state: &DatePickerState, theme: &Theme) -> Vec<Line<'static>> {
    let first_dow = state.view_month.weekday().num_days_from_monday();
    let days = days_in_month(state.view_month.year(), state.view_month.month());
    let today = chrono::Local::now().date_naive();

    let mut day = 1u32;
    let mut col = first_dow;
    let mut out = Vec::new();
    while day <= days {
        let mut spans = Vec::new();
        for week_col in 0..7 {
            if (day == 1 && week_col < col) || day > days {
                spans.push(Span::styled("    ", Style::default().bg(theme.bg_raised)));
            } else {
                let date =
                    NaiveDate::from_ymd_opt(state.view_month.year(), state.view_month.month(), day);
                let style = if state.current == date && state.focus == DateFocus::Calendar {
                    Style::default()
                        .fg(theme.bg)
                        .bg(theme.accent)
                        .add_modifier(Modifier::BOLD)
                } else if state.current == date {
                    Style::default().fg(theme.accent).bg(theme.bg_soft)
                } else if date == Some(today) {
                    Style::default().fg(theme.accent).bg(theme.bg_raised)
                } else {
                    Style::default().fg(theme.fg).bg(theme.bg_raised)
                };
                spans.push(Span::styled(format!("{:>3} ", day), style));
                day += 1;
            }
        }
        col = 0;
        out.push(Line::from(spans));
    }
    out
}

fn divider(width: u16, theme: &Theme) -> Line<'static> {
    Line::from(Span::styled(
        "─".repeat(width as usize),
        Style::default().fg(theme.line).bg(theme.bg_raised),
    ))
}

fn first_of_month(date: NaiveDate) -> Option<NaiveDate> {
    NaiveDate::from_ymd_opt(date.year(), date.month(), 1)
}

fn shift_month(date: NaiveDate, delta: i32) -> NaiveDate {
    let base_month = date.month0() as i32 + delta;
    let year = date.year() + base_month.div_euclid(12);
    let month0 = base_month.rem_euclid(12) as u32;
    let month = month0 + 1;
    let day = date.day().min(days_in_month(year, month));
    NaiveDate::from_ymd_opt(year, month, day).unwrap_or(date)
}

fn days_in_month(year: i32, month: u32) -> u32 {
    let next_month = if month == 12 { 1 } else { month + 1 };
    let next_year = if month == 12 { year + 1 } else { year };
    NaiveDate::from_ymd_opt(next_year, next_month, 1)
        .and_then(|d| d.pred_opt())
        .map_or(28, |d| d.day())
}
