pub mod date_picker;
pub mod datetime_picker;
pub mod filter;
pub mod fk_picker;
pub mod text_editor;
pub mod value_picker;

use ratatui::{layout::Rect, Frame};

use crate::{config::Config, theme::Theme};

pub use date_picker::DatePickerState;
pub use datetime_picker::DatetimePickerState;
pub use filter::FilterPopupState;
pub use fk_picker::FkPickerState;
pub use text_editor::TextEditorState;
pub use value_picker::ValuePickerState;

#[allow(dead_code)]
pub enum PopupKind {
    TextEditor(TextEditorState),
    ValuePicker(ValuePickerState),
    DatePicker(DatePickerState),
    DatetimePicker(DatetimePickerState),
    FkPicker(FkPickerState),
    FilterPopup(FilterPopupState),
}

pub fn render_popup(
    frame: &mut Frame,
    area: Rect,
    popup: &mut PopupKind,
    theme: &Theme,
    config: &Config,
) {
    match popup {
        PopupKind::TextEditor(state) => text_editor::render(frame, area, state, theme, config),
        PopupKind::ValuePicker(state) => value_picker::render(frame, area, state, theme, config),
        PopupKind::DatePicker(state) => date_picker::render(frame, area, state, theme),
        PopupKind::DatetimePicker(state) => datetime_picker::render(frame, area, state, theme),
        PopupKind::FkPicker(state) => fk_picker::render(frame, area, state, theme, config),
        PopupKind::FilterPopup(state) => filter::render(frame, area, state, theme, config),
    }
}
