use super::rule::{FilterOp, FilterSet, FilterValue};
use crate::db::types::SqlValue;
use rusqlite::types::Value as RusqliteValue;

fn sql_val(v: &SqlValue) -> RusqliteValue {
    match v {
        SqlValue::Null => RusqliteValue::Null,
        SqlValue::Integer(n) => RusqliteValue::Integer(*n),
        SqlValue::Real(f) => RusqliteValue::Real(*f),
        SqlValue::Text(s) => RusqliteValue::Text(s.clone()),
        SqlValue::Blob(b) => RusqliteValue::Blob(b.clone()),
    }
}

/// Returns (WHERE clause without "WHERE", params vec).
/// The WHERE clause uses ?1, ?2, ... positional params.
pub fn filter_to_sql(filter: &FilterSet) -> (String, Vec<RusqliteValue>) {
    let mut parts: Vec<String> = Vec::new();
    let mut params: Vec<RusqliteValue> = Vec::new();
    let mut param_idx = 1usize;

    for (col_name, col_filter) in &filter.columns {
        let enabled_rules: Vec<_> = col_filter.rules.iter().filter(|r| r.enabled).collect();
        if enabled_rules.is_empty() {
            continue;
        }

        let col_parts: Vec<String> = enabled_rules
            .iter()
            .map(|rule| {
                let col = format!("\"{}\"", col_name);
                match &rule.op {
                    FilterOp::Eq => {
                        params.push(sql_val(if let FilterValue::Literal(v) = &rule.value {
                            v
                        } else {
                            &SqlValue::Null
                        }));
                        let p = format!("?{}", param_idx);
                        param_idx += 1;
                        format!("{} = {}", col, p)
                    }
                    FilterOp::Ne => {
                        params.push(sql_val(if let FilterValue::Literal(v) = &rule.value {
                            v
                        } else {
                            &SqlValue::Null
                        }));
                        let p = format!("?{}", param_idx);
                        param_idx += 1;
                        format!("{} != {}", col, p)
                    }
                    FilterOp::Lt => {
                        params.push(sql_val(if let FilterValue::Literal(v) = &rule.value {
                            v
                        } else {
                            &SqlValue::Null
                        }));
                        let p = format!("?{}", param_idx);
                        param_idx += 1;
                        format!("{} < {}", col, p)
                    }
                    FilterOp::Le => {
                        params.push(sql_val(if let FilterValue::Literal(v) = &rule.value {
                            v
                        } else {
                            &SqlValue::Null
                        }));
                        let p = format!("?{}", param_idx);
                        param_idx += 1;
                        format!("{} <= {}", col, p)
                    }
                    FilterOp::Gt => {
                        params.push(sql_val(if let FilterValue::Literal(v) = &rule.value {
                            v
                        } else {
                            &SqlValue::Null
                        }));
                        let p = format!("?{}", param_idx);
                        param_idx += 1;
                        format!("{} > {}", col, p)
                    }
                    FilterOp::Ge => {
                        params.push(sql_val(if let FilterValue::Literal(v) = &rule.value {
                            v
                        } else {
                            &SqlValue::Null
                        }));
                        let p = format!("?{}", param_idx);
                        param_idx += 1;
                        format!("{} >= {}", col, p)
                    }
                    FilterOp::Contains => {
                        let pattern = if let FilterValue::Pattern(s) = &rule.value {
                            format!("%{}%", s)
                        } else {
                            "%".to_string()
                        };
                        params.push(RusqliteValue::Text(pattern));
                        let p = format!("?{}", param_idx);
                        param_idx += 1;
                        format!("{} LIKE {}", col, p)
                    }
                    FilterOp::NotContains => {
                        let pattern = if let FilterValue::Pattern(s) = &rule.value {
                            format!("%{}%", s)
                        } else {
                            "%".to_string()
                        };
                        params.push(RusqliteValue::Text(pattern));
                        let p = format!("?{}", param_idx);
                        param_idx += 1;
                        format!("{} NOT LIKE {}", col, p)
                    }
                    FilterOp::StartsWith => {
                        let pattern = if let FilterValue::Pattern(s) = &rule.value {
                            format!("{}%", s)
                        } else {
                            "%".to_string()
                        };
                        params.push(RusqliteValue::Text(pattern));
                        let p = format!("?{}", param_idx);
                        param_idx += 1;
                        format!("{} LIKE {}", col, p)
                    }
                    FilterOp::EndsWith => {
                        let pattern = if let FilterValue::Pattern(s) = &rule.value {
                            format!("%{}", s)
                        } else {
                            "%".to_string()
                        };
                        params.push(RusqliteValue::Text(pattern));
                        let p = format!("?{}", param_idx);
                        param_idx += 1;
                        format!("{} LIKE {}", col, p)
                    }
                    FilterOp::Like => {
                        let pattern = if let FilterValue::Pattern(s) = &rule.value {
                            s.clone()
                        } else {
                            "%".to_string()
                        };
                        params.push(RusqliteValue::Text(pattern));
                        let p = format!("?{}", param_idx);
                        param_idx += 1;
                        format!("{} LIKE {}", col, p)
                    }
                    FilterOp::Regex => {
                        let pattern = if let FilterValue::Regex(s) = &rule.value {
                            s.clone()
                        } else {
                            String::new()
                        };
                        params.push(RusqliteValue::Text(pattern));
                        let p = format!("?{}", param_idx);
                        param_idx += 1;
                        format!("regexp({}, {})", p, col)
                    }
                    FilterOp::IsNull => format!("{} IS NULL", col),
                    FilterOp::IsNotNull => format!("{} IS NOT NULL", col),
                    FilterOp::Between => {
                        if let FilterValue::Range(lo, hi) = &rule.value {
                            params.push(sql_val(lo));
                            let p1 = format!("?{}", param_idx);
                            param_idx += 1;
                            params.push(sql_val(hi));
                            let p2 = format!("?{}", param_idx);
                            param_idx += 1;
                            format!("{} BETWEEN {} AND {}", col, p1, p2)
                        } else {
                            "1=1".to_string()
                        }
                    }
                    FilterOp::In => {
                        if let FilterValue::List(vals) = &rule.value {
                            let placeholders: Vec<String> = vals
                                .iter()
                                .map(|v| {
                                    params.push(sql_val(v));
                                    let p = format!("?{}", param_idx);
                                    param_idx += 1;
                                    p
                                })
                                .collect();
                            format!("{} IN ({})", col, placeholders.join(", "))
                        } else {
                            "1=1".to_string()
                        }
                    }
                    FilterOp::Today => format!("date({}) = date('now')", col),
                    FilterOp::ThisWeek => {
                        format!("date({}) >= date('now', 'weekday 0', '-6 days')", col)
                    }
                    FilterOp::ThisMonth => {
                        format!("strftime('%Y-%m', {}) = strftime('%Y-%m', 'now')", col)
                    }
                    FilterOp::ThisYear => {
                        format!("strftime('%Y', {}) = strftime('%Y', 'now')", col)
                    }
                    FilterOp::LastNDays => {
                        let n = if let FilterValue::N(n) = &rule.value {
                            *n
                        } else {
                            7
                        };
                        format!("date({}) >= date('now', '-{} days')", col, n)
                    }
                    FilterOp::Formula => {
                        if let FilterValue::Formula(formula) = &rule.value {
                            sanitize_formula(formula, col_name)
                        } else {
                            "1=1".to_string()
                        }
                    }
                }
            })
            .collect();

        if col_parts.len() == 1 {
            parts.push(col_parts.into_iter().next().unwrap_or_default());
        } else {
            parts.push(format!("({})", col_parts.join(" OR ")));
        }
    }

    let where_clause = parts.join(" AND ");
    (where_clause, params)
}

fn sanitize_formula(formula: &str, col_name: &str) -> String {
    let quoted_col = format!("\"{}\"", col_name);
    let mut result = String::new();
    let mut i = 0;
    let bytes = formula.as_bytes();
    while i < bytes.len() {
        if bytes[i..].starts_with(b"col") {
            let after = i + 3;
            let before_ok = i == 0 || !bytes[i - 1].is_ascii_alphanumeric();
            let after_ok = after >= bytes.len() || !bytes[after].is_ascii_alphanumeric();
            if before_ok && after_ok {
                result.push_str(&quoted_col);
                i += 3;
                continue;
            }
        }
        let c = bytes[i] as char;
        match c {
            '0'..='9'
            | '+'
            | '-'
            | '*'
            | '/'
            | '('
            | ')'
            | '>'
            | '<'
            | '='
            | '!'
            | '.'
            | ' '
            | '\t' => {
                result.push(c);
            }
            _ => return "1=1".to_string(),
        }
        i += 1;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::types::SqlValue;
    use crate::filter::rule::{ColumnFilter, FilterOp, FilterRule, FilterSet, FilterValue};

    fn make_set(col: &str, op: FilterOp, val: FilterValue) -> FilterSet {
        let mut fs = FilterSet::default();
        fs.columns.insert(
            col.to_string(),
            ColumnFilter {
                rules: vec![FilterRule {
                    op,
                    value: val,
                    enabled: true,
                    label: None,
                }],
            },
        );
        fs
    }

    #[test]
    fn test_simple_equality() {
        let fs = make_set(
            "id",
            FilterOp::Eq,
            FilterValue::Literal(SqlValue::Integer(42)),
        );
        let (clause, params) = filter_to_sql(&fs);
        assert!(clause.contains("\"id\" = ?1"), "got: {}", clause);
        assert_eq!(params.len(), 1);
        assert_eq!(params[0], RusqliteValue::Integer(42));
    }

    #[test]
    fn test_text_contains() {
        let fs = make_set(
            "name",
            FilterOp::Contains,
            FilterValue::Pattern("foo".to_string()),
        );
        let (clause, params) = filter_to_sql(&fs);
        assert!(clause.contains("LIKE"), "got: {}", clause);
        match params.first() {
            Some(RusqliteValue::Text(p)) => assert_eq!(p, "%foo%"),
            _ => panic!("expected Text param"),
        }
    }

    #[test]
    fn test_between() {
        let fs = make_set(
            "age",
            FilterOp::Between,
            FilterValue::Range(SqlValue::Integer(18), SqlValue::Integer(65)),
        );
        let (clause, params) = filter_to_sql(&fs);
        assert!(clause.contains("BETWEEN"), "got: {}", clause);
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_or_within_column() {
        let mut fs = FilterSet::default();
        fs.columns.insert(
            "status".to_string(),
            ColumnFilter {
                rules: vec![
                    FilterRule {
                        op: FilterOp::Eq,
                        value: FilterValue::Literal(SqlValue::Text("active".to_string())),
                        enabled: true,
                        label: None,
                    },
                    FilterRule {
                        op: FilterOp::Eq,
                        value: FilterValue::Literal(SqlValue::Text("pending".to_string())),
                        enabled: true,
                        label: None,
                    },
                ],
            },
        );
        let (clause, _) = filter_to_sql(&fs);
        assert!(clause.contains(" OR "), "got: {}", clause);
    }

    #[test]
    fn test_and_across_columns() {
        let mut fs = FilterSet::default();
        fs.columns.insert(
            "age".to_string(),
            ColumnFilter {
                rules: vec![FilterRule {
                    op: FilterOp::Gt,
                    value: FilterValue::Literal(SqlValue::Integer(18)),
                    enabled: true,
                    label: None,
                }],
            },
        );
        fs.columns.insert(
            "name".to_string(),
            ColumnFilter {
                rules: vec![FilterRule {
                    op: FilterOp::Contains,
                    value: FilterValue::Pattern("foo".to_string()),
                    enabled: true,
                    label: None,
                }],
            },
        );
        let (clause, _) = filter_to_sql(&fs);
        assert!(
            clause.contains("\"age\"") && clause.contains("\"name\""),
            "got: {}",
            clause
        );
    }

    #[test]
    fn test_null_checks() {
        let fs_null = make_set(
            "email",
            FilterOp::IsNull,
            FilterValue::Literal(SqlValue::Null),
        );
        let (clause, params) = filter_to_sql(&fs_null);
        assert!(clause.contains("IS NULL"), "got: {}", clause);
        assert_eq!(params.len(), 0);

        let fs_notnull = make_set(
            "email",
            FilterOp::IsNotNull,
            FilterValue::Literal(SqlValue::Null),
        );
        let (clause2, _) = filter_to_sql(&fs_notnull);
        assert!(clause2.contains("IS NOT NULL"), "got: {}", clause2);
    }

    #[test]
    fn test_disabled_rule_excluded() {
        let mut fs = FilterSet::default();
        fs.columns.insert(
            "id".to_string(),
            ColumnFilter {
                rules: vec![FilterRule {
                    op: FilterOp::Eq,
                    value: FilterValue::Literal(SqlValue::Integer(42)),
                    enabled: false,
                    label: None,
                }],
            },
        );
        let (clause, params) = filter_to_sql(&fs);
        assert!(clause.is_empty(), "got: {}", clause);
        assert_eq!(params.len(), 0);
    }

    #[test]
    fn test_empty_filter_set() {
        let fs = FilterSet::default();
        let (clause, params) = filter_to_sql(&fs);
        assert!(clause.is_empty());
        assert!(params.is_empty());
    }

    #[test]
    fn test_starts_with() {
        let fs = make_set(
            "title",
            FilterOp::StartsWith,
            FilterValue::Pattern("foo".to_string()),
        );
        let (clause, params) = filter_to_sql(&fs);
        assert!(clause.contains("LIKE"), "got: {}", clause);
        match params.first() {
            Some(RusqliteValue::Text(p)) => assert_eq!(p, "foo%"),
            _ => panic!("expected Text param"),
        }
    }
}
