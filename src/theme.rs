use ratatui::style::Color;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Theme {
    pub bg: Color,
    pub bg_soft: Color,
    pub bg_raised: Color,
    pub line: Color,
    pub line_soft: Color,
    pub fg: Color,
    pub fg_dim: Color,
    pub fg_mute: Color,
    pub fg_faint: Color,
    pub accent: Color,
    pub red: Color,
    pub yellow: Color,
    pub green: Color,
    pub teal: Color,
    pub blue: Color,
    pub purple: Color,
    pub pink: Color,
}

impl Default for Theme {
    fn default() -> Self {
        Self {
            bg: Color::Rgb(0x1d, 0x1b, 0x1a),
            bg_soft: Color::Rgb(0x24, 0x21, 0x1f),
            bg_raised: Color::Rgb(0x2a, 0x26, 0x24),
            line: Color::Rgb(0x3a, 0x33, 0x2f),
            line_soft: Color::Rgb(0x2f, 0x2a, 0x27),
            fg: Color::Rgb(0xe8, 0xdf, 0xd3),
            fg_dim: Color::Rgb(0xa8, 0x9c, 0x8a),
            fg_mute: Color::Rgb(0x6b, 0x64, 0x59),
            fg_faint: Color::Rgb(0x4a, 0x45, 0x3e),
            accent: Color::Rgb(0xd9, 0x9a, 0x5e),
            red: Color::Rgb(0xe0, 0x6c, 0x75),
            yellow: Color::Rgb(0xe5, 0xc0, 0x7b),
            green: Color::Rgb(0xa3, 0xb5, 0x65),
            teal: Color::Rgb(0x7c, 0xb7, 0xa8),
            blue: Color::Rgb(0x82, 0xaa, 0xdc),
            purple: Color::Rgb(0xc0, 0x8b, 0xc0),
            pink: Color::Rgb(0xd8, 0x8a, 0xa0),
        }
    }
}
