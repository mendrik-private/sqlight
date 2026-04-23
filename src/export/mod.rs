use std::path::Path;

use rusqlite::Connection;

use crate::{
    db::{schema::Column, types::SqlValue},
    filter::{predicate::filter_to_sql, FilterSet},
    grid::{SortDir, SortSpec},
};

pub fn export_csv(
    conn: &Connection,
    table: &str,
    columns: &[Column],
    filter: &FilterSet,
    sort: &Option<SortSpec>,
    path: &Path,
) -> anyhow::Result<u64> {
    let (where_clause, where_params) = filter_to_sql(filter);
    let order = sort.as_ref().and_then(|s| {
        columns
            .get(s.col_idx)
            .map(|c| (c.name.clone(), s.direction == SortDir::Asc))
    });
    let order_ref = order.as_ref().map(|(s, b)| (s.as_str(), *b));

    let rows = crate::db::fetch_rows(
        conn,
        table,
        columns,
        0,
        i64::MAX,
        order_ref,
        &where_clause,
        &where_params,
    )?;

    let mut out = std::fs::File::create(path)?;
    use std::io::Write;

    let headers: Vec<String> = columns.iter().map(|c| csv_escape(&c.name)).collect();
    writeln!(out, "{}", headers.join(","))?;

    let mut count = 0u64;
    for row in &rows {
        let cells: Vec<String> = row.iter().map(|v| csv_escape(&val_to_str(v))).collect();
        writeln!(out, "{}", cells.join(","))?;
        count += 1;
    }
    Ok(count)
}

fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

pub fn export_json(
    conn: &Connection,
    table: &str,
    columns: &[Column],
    filter: &FilterSet,
    sort: &Option<SortSpec>,
    path: &Path,
) -> anyhow::Result<u64> {
    let (where_clause, where_params) = filter_to_sql(filter);
    let order = sort.as_ref().and_then(|s| {
        columns
            .get(s.col_idx)
            .map(|c| (c.name.clone(), s.direction == SortDir::Asc))
    });
    let order_ref = order.as_ref().map(|(s, b)| (s.as_str(), *b));

    let rows = crate::db::fetch_rows(
        conn,
        table,
        columns,
        0,
        i64::MAX,
        order_ref,
        &where_clause,
        &where_params,
    )?;

    let mut out = std::fs::File::create(path)?;
    use std::io::Write;
    writeln!(out, "[")?;
    let count = rows.len() as u64;
    for (i, row) in rows.iter().enumerate() {
        let pairs: Vec<String> = columns
            .iter()
            .zip(row)
            .map(|(col, val)| {
                format!(
                    "  \"{}\": {}",
                    col.name.replace('"', "\\\""),
                    val_to_json(val)
                )
            })
            .collect();
        let sep = if i + 1 < rows.len() { "," } else { "" };
        writeln!(out, "{{{}}}{}", pairs.join(", "), sep)?;
    }
    writeln!(out, "]")?;
    Ok(count)
}

pub fn export_sql(
    conn: &Connection,
    table: &str,
    columns: &[Column],
    filter: &FilterSet,
    sort: &Option<SortSpec>,
    path: &Path,
) -> anyhow::Result<u64> {
    let (where_clause, where_params) = filter_to_sql(filter);
    let order = sort.as_ref().and_then(|s| {
        columns
            .get(s.col_idx)
            .map(|c| (c.name.clone(), s.direction == SortDir::Asc))
    });
    let order_ref = order.as_ref().map(|(s, b)| (s.as_str(), *b));

    let rows = crate::db::fetch_rows(
        conn,
        table,
        columns,
        0,
        i64::MAX,
        order_ref,
        &where_clause,
        &where_params,
    )?;

    let mut out = std::fs::File::create(path)?;
    use std::io::Write;

    let col_names: String = columns
        .iter()
        .map(|c| format!("\"{}\"", c.name))
        .collect::<Vec<_>>()
        .join(", ");

    let mut count = 0u64;
    for row in &rows {
        let vals: Vec<String> = row.iter().map(val_to_sql_literal).collect();
        writeln!(
            out,
            "INSERT INTO \"{}\" ({}) VALUES ({});",
            table,
            col_names,
            vals.join(", ")
        )?;
        count += 1;
    }
    Ok(count)
}

fn val_to_str(v: &SqlValue) -> String {
    match v {
        SqlValue::Null => String::new(),
        SqlValue::Integer(n) => n.to_string(),
        SqlValue::Real(f) => f.to_string(),
        SqlValue::Text(s) => s.clone(),
        SqlValue::Blob(b) => format!("<blob {} bytes>", b.len()),
    }
}

fn val_to_json(v: &SqlValue) -> String {
    match v {
        SqlValue::Null => "null".to_string(),
        SqlValue::Integer(n) => n.to_string(),
        SqlValue::Real(f) => f.to_string(),
        SqlValue::Text(s) => {
            format!(
                "\"{}\"",
                s.replace('"', "\\\"")
                    .replace('\n', "\\n")
                    .replace('\r', "\\r")
            )
        }
        SqlValue::Blob(_) => "null".to_string(),
    }
}

fn val_to_sql_literal(v: &SqlValue) -> String {
    match v {
        SqlValue::Null => "NULL".to_string(),
        SqlValue::Integer(n) => n.to_string(),
        SqlValue::Real(f) => f.to_string(),
        SqlValue::Text(s) => format!("'{}'", s.replace('\'', "''")),
        SqlValue::Blob(b) => format!("X'{}'", to_hex(b)),
    }
}

fn to_hex(b: &[u8]) -> String {
    b.iter().map(|byte| format!("{:02X}", byte)).collect()
}
