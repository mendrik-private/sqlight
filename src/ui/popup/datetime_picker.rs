use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};
use ratatui::{
    layout::Rect,
    style::Style,
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
    Frame,
};

use crate::{db::types::SqlValue, theme::Theme};

#[allow(dead_code)]
pub struct DatetimePickerState {
    pub table: String,
    pub rowid: i64,
    pub col_name: String,
    pub date: Option<NaiveDate>,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub view_month: NaiveDate,
    pub original: SqlValue,
    pub focus_date: bool,
}

impl DatetimePickerState {
    pub fn new(table: String, rowid: i64, col_name: String, original: SqlValue) -> Self {
        let dt = match &original {
            SqlValue::Text(s) => NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").ok(),
            _ => None,
        };
        let today = chrono::Local::now().naive_local();
        let (date, hour, minute, second) = if let Some(d) = dt {
            (
                Some(d.date()),
                d.hour() as u8,
                d.minute() as u8,
                d.second() as u8,
            )
        } else {
            (None, 0u8, 0u8, 0u8)
        };
        let base = date.unwrap_or_else(|| today.date());
        let view_month = NaiveDate::from_ymd_opt(base.year(), base.month(), 1).unwrap_or(base);
        Self {
            table,
            rowid,
            col_name,
            date,
            hour,
            minute,
            second,
            view_month,
            original,
            focus_date: true,
        }
    }

    pub fn prev_month(&mut self) {
        let m = self.view_month.month();
        let y = self.view_month.year();
        self.view_month = if m == 1 {
            NaiveDate::from_ymd_opt(y - 1, 12, 1).unwrap_or(self.view_month)
        } else {
            NaiveDate::from_ymd_opt(y, m - 1, 1).unwrap_or(self.view_month)
        };
    }

    pub fn next_month(&mut self) {
        let m = self.view_month.month();
        let y = self.view_month.year();
        self.view_month = if m == 12 {
            NaiveDate::from_ymd_opt(y + 1, 1, 1).unwrap_or(self.view_month)
        } else {
            NaiveDate::from_ymd_opt(y, m + 1, 1).unwrap_or(self.view_month)
        };
    }

    pub fn as_sql_value(&self) -> SqlValue {
        match self.date {
            Some(d) => {
                let dt = d
                    .and_hms_opt(self.hour as u32, self.minute as u32, self.second as u32)
                    .unwrap_or_else(|| {
                        d.and_hms_opt(0, 0, 0).unwrap_or_else(|| {
                            NaiveDate::from_ymd_opt(1970, 1, 1)
                                .and_then(|d| d.and_hms_opt(0, 0, 0))
                                .unwrap_or_default()
                        })
                    });
                SqlValue::Text(dt.format("%Y-%m-%d %H:%M:%S").to_string())
            }
            None => SqlValue::Null,
        }
    }
}

pub fn render(frame: &mut Frame, area: Rect, state: &DatetimePickerState, theme: &Theme) {
    let popup_width = 28u16.min(area.width);
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
        .border_style(Style::default().fg(theme.accent))
        .title(format!(" DateTime: {} ", state.col_name))
        .style(Style::default().bg(theme.bg_raised));

    let inner = block.inner(popup_area);
    frame.render_widget(block, popup_area);

    let header = format!(" {:^24} ", state.view_month.format("%B %Y").to_string());
    let mut lines = vec![
        Line::from(Span::styled(header, Style::default().fg(theme.accent))),
        Line::from(Span::styled(
            " Mo Tu We Th Fr Sa Su",
            Style::default().fg(theme.fg_dim),
        )),
    ];

    let first_dow = state.view_month.weekday().num_days_from_monday();
    let days_in_month = days_in_month(state.view_month.year(), state.view_month.month());
    let today = chrono::Local::now().date_naive();

    let mut day = 1u32;
    let mut col = first_dow;
    while day <= days_in_month {
        let mut spans = Vec::new();
        for c in 0..7 {
            if (day == 1 && c < col) || day > days_in_month {
                spans.push(Span::raw("   "));
            } else {
                let date =
                    NaiveDate::from_ymd_opt(state.view_month.year(), state.view_month.month(), day);
                let is_selected = state.date == date;
                let is_today = date == Some(today);
                let style = if is_selected {
                    Style::default().fg(theme.bg).bg(theme.accent)
                } else if is_today {
                    Style::default().fg(theme.accent)
                } else {
                    Style::default().fg(theme.fg)
                };
                spans.push(Span::styled(format!("{:>2} ", day), style));
                day += 1;
            }
        }
        col = 0;
        lines.push(Line::from(spans));
    }

    lines.push(Line::from(Span::styled(
        format!(
            " Time: {:02}:{:02}:{:02}",
            state.hour, state.minute, state.second
        ),
        Style::default().fg(theme.fg),
    )));

    lines.push(Line::from(Span::styled(
        " ↵ ok · Esc cancel",
        Style::default().fg(theme.fg_faint),
    )));

    frame.render_widget(
        Paragraph::new(lines).style(Style::default().bg(theme.bg_raised)),
        inner,
    );
}

fn days_in_month(year: i32, month: u32) -> u32 {
    let next_month = if month == 12 { 1 } else { month + 1 };
    let next_year = if month == 12 { year + 1 } else { year };
    NaiveDate::from_ymd_opt(next_year, next_month, 1)
        .and_then(|d| d.pred_opt())
        .map_or(28, |d| d.day())
}
