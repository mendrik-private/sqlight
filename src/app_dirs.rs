use directories::ProjectDirs;
use std::path::PathBuf;

const APP_NAME: &str = "sqview";
const LEGACY_APP_NAME: &str = "sqv";

pub fn config_dir() -> Option<PathBuf> {
    let current = ProjectDirs::from("", "", APP_NAME)?;
    let legacy = ProjectDirs::from("", "", LEGACY_APP_NAME)?;

    if current.config_dir().exists() {
        Some(current.config_dir().to_path_buf())
    } else if legacy.config_dir().exists() {
        Some(legacy.config_dir().to_path_buf())
    } else {
        Some(current.config_dir().to_path_buf())
    }
}

pub fn data_local_dir() -> Option<PathBuf> {
    let current = ProjectDirs::from("", "", APP_NAME)?;
    let legacy = ProjectDirs::from("", "", LEGACY_APP_NAME)?;

    if current.data_local_dir().exists() {
        Some(current.data_local_dir().to_path_buf())
    } else if legacy.data_local_dir().exists() {
        Some(legacy.data_local_dir().to_path_buf())
    } else {
        Some(current.data_local_dir().to_path_buf())
    }
}
