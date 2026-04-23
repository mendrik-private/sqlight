use std::collections::VecDeque;
use std::time::Instant;

use ratatui::{
    layout::Rect,
    style::Style,
    text::{Line, Span},
    widgets::Paragraph,
    Frame,
};

use crate::theme::Theme;

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum ToastKind {
    Success,
    Error,
    Info,
}

pub struct Toast {
    pub message: String,
    pub kind: ToastKind,
    pub created: Instant,
}

pub struct ToastState {
    pub toasts: VecDeque<Toast>,
}

impl ToastState {
    pub fn new() -> Self {
        Self {
            toasts: VecDeque::new(),
        }
    }

    pub fn push(&mut self, message: impl Into<String>, kind: ToastKind) {
        self.toasts.push_back(Toast {
            message: message.into(),
            kind,
            created: Instant::now(),
        });
        while self.toasts.len() > 5 {
            self.toasts.pop_front();
        }
    }

    pub fn tick(&mut self) {
        let now = Instant::now();
        self.toasts.retain(|t| {
            let duration = match t.kind {
                ToastKind::Success | ToastKind::Info => std::time::Duration::from_secs(3),
                ToastKind::Error => std::time::Duration::from_secs(5),
            };
            now.duration_since(t.created) < duration
        });
    }
}

impl Default for ToastState {
    fn default() -> Self {
        Self::new()
    }
}

pub fn render_toasts(frame: &mut Frame, area: Rect, state: &ToastState, theme: &Theme) {
    for (i, toast) in state.toasts.iter().enumerate() {
        let msg = format!("  {}  ", toast.message);
        let w = (msg.len() as u16).min(60).min(area.width);
        let x = area.x + area.width.saturating_sub(w);
        let y = area.y + i as u16;
        if y >= area.y + area.height {
            break;
        }
        let bg = match toast.kind {
            ToastKind::Success => theme.green,
            ToastKind::Error => theme.red,
            ToastKind::Info => theme.fg_mute,
        };
        let toast_area = Rect {
            x,
            y,
            width: w,
            height: 1,
        };
        let para = Paragraph::new(Line::from(Span::styled(
            &msg,
            Style::default().fg(theme.bg).bg(bg),
        )));
        frame.render_widget(para, toast_area);
    }
}

pub fn render_confirm(frame: &mut Frame, area: Rect, message: &str, theme: &Theme) {
    let msg = format!("  {}  ", message);
    let w = (msg.len() as u16).min(60).min(area.width);
    let x = area.x + area.width.saturating_sub(w);
    let y = area.y + area.height.saturating_sub(3);
    let confirm_area = Rect {
        x,
        y,
        width: w,
        height: 1,
    };
    let para = Paragraph::new(Line::from(Span::styled(
        &msg,
        Style::default().fg(theme.bg).bg(theme.yellow),
    )));
    frame.render_widget(para, confirm_area);
}
