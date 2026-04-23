use crossterm::event::{Event, KeyCode, KeyModifiers};

use crate::app::Message;

pub fn translate_event(event: Event) -> Option<Message> {
    match event {
        Event::Key(key) => {
            if key.code == KeyCode::Char('q') && key.modifiers.contains(KeyModifiers::CONTROL) {
                Some(Message::Quit)
            } else {
                Some(Message::Key(key))
            }
        }
        Event::Mouse(mouse) => {
            use crossterm::event::MouseEventKind;
            match mouse.kind {
                MouseEventKind::ScrollDown => Some(Message::ScrollDown(3)),
                MouseEventKind::ScrollUp => Some(Message::ScrollUp(3)),
                _ => Some(Message::Mouse(mouse)),
            }
        }
        Event::Resize(w, h) => Some(Message::Resize(w, h)),
        _ => None,
    }
}
