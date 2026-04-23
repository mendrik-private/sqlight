use std::time::{Duration, Instant};

use crossterm::event::{Event, KeyCode, KeyEventKind, KeyModifiers, MouseButton, MouseEventKind};

use crate::state::{ClickInfo, DragTarget, Hitbox, MouseState, UiRegion};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InputCommand {
    MoveUp,
    MoveDown,
    MoveLeft,
    MoveRight,
    NextTable,
    PreviousTable,
    ScrollRows(i32),
    ScrollColumns(i32),
    ScrollTables(i32),
    SelectCell { row: usize, col: usize },
    SelectTable { index: usize },
    ToggleSort { col: usize },
    EditSelected,
    Search,
    Group,
    JumpForeignKey,
    ClearFilters,
    ToggleHelp,
    Quit,
    Breadcrumb { index: usize },
    BeginHorizontalDrag { column: u16 },
    DragHorizontalDrag { column: u16 },
    EndHorizontalDrag,
}

pub struct InputContext<'a> {
    pub hitboxes: &'a [Hitbox],
    pub mouse_state: &'a MouseState,
    pub now: Instant,
}

pub struct InputResult {
    pub commands: Vec<InputCommand>,
    pub mouse_state: MouseState,
}

pub fn translate_event(event: Event, context: InputContext<'_>) -> InputResult {
    let mut commands = Vec::new();
    let mut mouse_state = context.mouse_state.clone();

    match event {
        Event::Key(key) if matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) => {
            match key.code {
                KeyCode::Up => commands.push(InputCommand::MoveUp),
                KeyCode::Down => commands.push(InputCommand::MoveDown),
                KeyCode::Left => commands.push(InputCommand::MoveLeft),
                KeyCode::Right => commands.push(InputCommand::MoveRight),
                KeyCode::Tab => commands.push(InputCommand::NextTable),
                KeyCode::BackTab => commands.push(InputCommand::PreviousTable),
                KeyCode::Enter => commands.push(InputCommand::EditSelected),
                KeyCode::Char('/') | KeyCode::Char('f') | KeyCode::Char('F') => {
                    commands.push(InputCommand::Search)
                }
                KeyCode::Char('s') => commands.push(InputCommand::ToggleSort { col: usize::MAX }),
                KeyCode::Char('g') => commands.push(InputCommand::Group),
                KeyCode::Char('j') => commands.push(InputCommand::JumpForeignKey),
                KeyCode::Char('c') | KeyCode::Char('C') => {
                    commands.push(InputCommand::ClearFilters)
                }
                KeyCode::Char('?') | KeyCode::F(1) => commands.push(InputCommand::ToggleHelp),
                KeyCode::Char('q') => commands.push(InputCommand::Quit),
                _ => {}
            }
        }
        Event::Mouse(mouse) => {
            let hovered_region = hit_test(context.hitboxes, mouse.column, mouse.row);
            mouse_state.hover_region = hovered_region.clone();

            match mouse.kind {
                MouseEventKind::Down(MouseButton::Left) => {
                    if let Some(region) = hovered_region {
                        match &region {
                            UiRegion::Cell { row, col } => {
                                commands.push(InputCommand::SelectCell {
                                    row: *row,
                                    col: *col,
                                });
                                if is_double_click(&mouse_state, &region, context.now) {
                                    commands.push(InputCommand::EditSelected);
                                }
                            }
                            UiRegion::Header { col } => {
                                commands.push(InputCommand::ToggleSort { col: *col });
                            }
                            UiRegion::TableItem { index } => {
                                commands.push(InputCommand::SelectTable { index: *index });
                            }
                            UiRegion::Breadcrumb { index } => {
                                commands.push(InputCommand::Breadcrumb { index: *index });
                            }
                            UiRegion::HorizontalScrollbarTrack => {
                                commands.push(InputCommand::BeginHorizontalDrag {
                                    column: mouse.column,
                                });
                                mouse_state.drag_target = Some(DragTarget::HorizontalScrollbar);
                            }
                            UiRegion::TablePanel => {}
                        }

                        mouse_state.last_click = Some(ClickInfo {
                            at: context.now,
                            region,
                        });
                    }
                }
                MouseEventKind::Drag(MouseButton::Left) => {
                    if mouse_state.drag_target == Some(DragTarget::HorizontalScrollbar) {
                        commands.push(InputCommand::DragHorizontalDrag {
                            column: mouse.column,
                        });
                    }
                }
                MouseEventKind::Up(MouseButton::Left) => {
                    if mouse_state.drag_target.take().is_some() {
                        commands.push(InputCommand::EndHorizontalDrag);
                    }
                }
                MouseEventKind::ScrollUp => {
                    if mouse.modifiers.contains(KeyModifiers::SHIFT) {
                        commands.push(InputCommand::ScrollColumns(-1));
                    } else if is_table_region(mouse_state.hover_region.as_ref()) {
                        commands.push(InputCommand::ScrollTables(-2));
                    } else {
                        commands.push(InputCommand::ScrollRows(-3));
                    }
                }
                MouseEventKind::ScrollDown => {
                    if mouse.modifiers.contains(KeyModifiers::SHIFT) {
                        commands.push(InputCommand::ScrollColumns(1));
                    } else if is_table_region(mouse_state.hover_region.as_ref()) {
                        commands.push(InputCommand::ScrollTables(2));
                    } else {
                        commands.push(InputCommand::ScrollRows(3));
                    }
                }
                _ => {}
            }
        }
        _ => {}
    }

    InputResult {
        commands,
        mouse_state,
    }
}

fn hit_test(hitboxes: &[Hitbox], column: u16, row: u16) -> Option<UiRegion> {
    hitboxes
        .iter()
        .rev()
        .find(|hitbox| hitbox.contains(column, row))
        .map(|hitbox| hitbox.region.clone())
}

fn is_double_click(mouse_state: &MouseState, region: &UiRegion, now: Instant) -> bool {
    mouse_state.last_click.as_ref().is_some_and(|click| {
        click.region == *region && now.duration_since(click.at) <= Duration::from_millis(450)
    })
}

fn is_table_region(region: Option<&UiRegion>) -> bool {
    matches!(
        region,
        Some(UiRegion::TableItem { .. } | UiRegion::TablePanel)
    )
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers, MouseEvent, MouseEventKind};
    use ratatui::layout::Rect;

    use crate::state::{Hitbox, MouseState, UiRegion};

    use super::{InputCommand, InputContext, translate_event};

    #[test]
    fn maps_shift_wheel_to_horizontal_scroll() {
        let event = Event::Mouse(MouseEvent {
            kind: MouseEventKind::ScrollDown,
            column: 5,
            row: 2,
            modifiers: KeyModifiers::SHIFT,
        });

        let result = translate_event(
            event,
            InputContext {
                hitboxes: &[],
                mouse_state: &MouseState::default(),
                now: Instant::now(),
            },
        );

        assert_eq!(result.commands, vec![InputCommand::ScrollColumns(1)]);
    }

    #[test]
    fn wheel_over_table_panel_scrolls_sidebar() {
        let hitboxes = vec![Hitbox {
            area: Rect::new(0, 0, 20, 4),
            region: UiRegion::TablePanel,
        }];

        let result = translate_event(
            Event::Mouse(MouseEvent {
                kind: MouseEventKind::ScrollDown,
                column: 3,
                row: 1,
                modifiers: KeyModifiers::NONE,
            }),
            InputContext {
                hitboxes: &hitboxes,
                mouse_state: &MouseState::default(),
                now: Instant::now(),
            },
        );

        assert_eq!(result.commands, vec![InputCommand::ScrollTables(2)]);
    }

    #[test]
    fn header_click_becomes_sort_command() {
        let hitboxes = vec![Hitbox {
            area: Rect::new(0, 0, 8, 1),
            region: UiRegion::Header { col: 2 },
        }];

        let event = Event::Mouse(MouseEvent {
            kind: MouseEventKind::Down(crossterm::event::MouseButton::Left),
            column: 2,
            row: 0,
            modifiers: KeyModifiers::NONE,
        });

        let result = translate_event(
            event,
            InputContext {
                hitboxes: &hitboxes,
                mouse_state: &MouseState::default(),
                now: Instant::now(),
            },
        );

        assert_eq!(result.commands, vec![InputCommand::ToggleSort { col: 2 }]);
    }

    #[test]
    fn second_cell_click_triggers_edit() {
        let now = Instant::now();
        let hitboxes = vec![Hitbox {
            area: Rect::new(0, 1, 12, 1),
            region: UiRegion::Cell { row: 4, col: 1 },
        }];

        let first = translate_event(
            Event::Mouse(MouseEvent {
                kind: MouseEventKind::Down(crossterm::event::MouseButton::Left),
                column: 2,
                row: 1,
                modifiers: KeyModifiers::NONE,
            }),
            InputContext {
                hitboxes: &hitboxes,
                mouse_state: &MouseState::default(),
                now,
            },
        );

        let second = translate_event(
            Event::Mouse(MouseEvent {
                kind: MouseEventKind::Down(crossterm::event::MouseButton::Left),
                column: 2,
                row: 1,
                modifiers: KeyModifiers::NONE,
            }),
            InputContext {
                hitboxes: &hitboxes,
                mouse_state: &first.mouse_state,
                now: now + Duration::from_millis(200),
            },
        );

        assert_eq!(
            second.commands,
            vec![
                InputCommand::SelectCell { row: 4, col: 1 },
                InputCommand::EditSelected
            ]
        );
    }

    #[test]
    fn question_mark_opens_help() {
        let result = translate_event(
            Event::Key(KeyEvent::new(KeyCode::Char('?'), KeyModifiers::NONE)),
            InputContext {
                hitboxes: &[],
                mouse_state: &MouseState::default(),
                now: Instant::now(),
            },
        );

        assert_eq!(result.commands, vec![InputCommand::ToggleHelp]);
    }

    #[test]
    fn c_clears_filters() {
        let result = translate_event(
            Event::Key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::NONE)),
            InputContext {
                hitboxes: &[],
                mouse_state: &MouseState::default(),
                now: Instant::now(),
            },
        );

        assert_eq!(result.commands, vec![InputCommand::ClearFilters]);
    }
}
