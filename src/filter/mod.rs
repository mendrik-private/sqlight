pub mod predicate;
pub mod rule;

pub use rule::{ColumnFilter, FilterOp, FilterSet, FilterValue};

use std::path::PathBuf;

pub fn filter_path(db_path: &str, table_name: &str) -> Option<PathBuf> {
    let db_basename = std::path::Path::new(db_path)
        .file_stem()?
        .to_str()?
        .to_string();
    let dirs = directories::ProjectDirs::from("", "", "sqv")?;
    let state_dir = dirs.data_local_dir().join("filters").join(&db_basename);
    Some(state_dir.join(format!("{}.toml", table_name)))
}

pub fn save_filter(filter: &FilterSet, db_path: &str, table_name: &str) -> anyhow::Result<()> {
    let path = filter_path(db_path, table_name)
        .ok_or_else(|| anyhow::anyhow!("could not determine filter path"))?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let content = toml::to_string(filter)?;
    std::fs::write(&path, content)?;
    Ok(())
}

pub fn load_filter(db_path: &str, table_name: &str) -> anyhow::Result<FilterSet> {
    let path = filter_path(db_path, table_name)
        .ok_or_else(|| anyhow::anyhow!("could not determine filter path"))?;
    let content = std::fs::read_to_string(&path)?;
    let filter: FilterSet = toml::from_str(&content)?;
    Ok(filter)
}
