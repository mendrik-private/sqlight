use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub nerd_font: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self { nerd_font: true }
    }
}

impl Config {
    pub fn load() -> anyhow::Result<Self> {
        if let Some(path) = config_path() {
            if path.exists() {
                let content = std::fs::read_to_string(path)?;
                return Ok(toml::from_str(&content)?);
            }
        }
        Ok(Self::default())
    }
}

fn config_path() -> Option<PathBuf> {
    crate::app_dirs::config_dir().map(|dir| dir.join("config.toml"))
}
