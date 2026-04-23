use serde::{Deserialize, Serialize};

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColAffinity {
    Integer,
    Real,
    Numeric,
    Text,
    Blob,
}

pub fn affinity(col_type: &str) -> ColAffinity {
    let upper = col_type.to_uppercase();
    if upper.contains("INT") {
        return ColAffinity::Integer;
    }
    if upper.contains("CHAR") || upper.contains("CLOB") || upper.contains("TEXT") {
        return ColAffinity::Text;
    }
    if upper.is_empty() || upper.contains("BLOB") {
        return ColAffinity::Blob;
    }
    if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
        return ColAffinity::Real;
    }
    ColAffinity::Numeric
}
