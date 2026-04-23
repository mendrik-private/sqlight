use crate::db::types::SqlValue;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FilterSet {
    pub columns: HashMap<String, ColumnFilter>,
}

impl FilterSet {
    pub fn is_empty(&self) -> bool {
        self.columns
            .values()
            .all(|cf| cf.rules.is_empty() || cf.rules.iter().all(|r| !r.enabled))
    }

    #[allow(dead_code)]
    pub fn active_count(&self) -> usize {
        self.columns
            .values()
            .flat_map(|cf| cf.rules.iter())
            .filter(|r| r.enabled)
            .count()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ColumnFilter {
    pub rules: Vec<FilterRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterRule {
    pub op: FilterOp,
    pub value: FilterValue,
    pub enabled: bool,
    pub label: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FilterOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Contains,
    NotContains,
    StartsWith,
    EndsWith,
    Like,
    Regex,
    IsNull,
    IsNotNull,
    Between,
    In,
    Today,
    ThisWeek,
    ThisMonth,
    ThisYear,
    LastNDays,
    Formula,
}

impl FilterOp {
    pub fn label(&self) -> &'static str {
        match self {
            FilterOp::Eq => "= (equals)",
            FilterOp::Ne => "≠ (not equals)",
            FilterOp::Lt => "< (less than)",
            FilterOp::Le => "≤ (less or equal)",
            FilterOp::Gt => "> (greater than)",
            FilterOp::Ge => "≥ (greater or equal)",
            FilterOp::Contains => "contains",
            FilterOp::NotContains => "not contains",
            FilterOp::StartsWith => "starts with",
            FilterOp::EndsWith => "ends with",
            FilterOp::Like => "LIKE",
            FilterOp::Regex => "regex",
            FilterOp::IsNull => "is null",
            FilterOp::IsNotNull => "is not null",
            FilterOp::Between => "between",
            FilterOp::In => "in",
            FilterOp::Today => "today",
            FilterOp::ThisWeek => "this week",
            FilterOp::ThisMonth => "this month",
            FilterOp::ThisYear => "this year",
            FilterOp::LastNDays => "last N days",
            FilterOp::Formula => "formula",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterValue {
    Literal(SqlValue),
    Range(SqlValue, SqlValue),
    List(Vec<SqlValue>),
    Pattern(String),
    Regex(String),
    Formula(String),
    N(i64),
}

impl Default for FilterValue {
    fn default() -> Self {
        FilterValue::Literal(SqlValue::Null)
    }
}
