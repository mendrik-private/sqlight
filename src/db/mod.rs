pub mod query;
pub mod schema;
pub mod types;
pub mod write;

use anyhow::Context;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::functions::FunctionFlags;
use rusqlite::{Connection, OptionalExtension, Row};

use schema::{Column, ForeignKey, IndexMeta, Schema, TableMeta, ViewMeta};

pub type DbPool = r2d2::Pool<SqliteConnectionManager>;

pub struct RowFetch<'a> {
    pub table: &'a str,
    pub columns: &'a [Column],
    pub offset: i64,
    pub limit: i64,
    pub order_by: Option<(&'a str, bool)>,
    pub where_clause: &'a str,
    pub where_params: &'a [rusqlite::types::Value],
}

fn register_functions(conn: &Connection) -> rusqlite::Result<()> {
    conn.create_scalar_function(
        "regexp",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let pattern: String = ctx.get(0)?;
            let text: String = ctx.get(1)?;
            let re = regex::Regex::new(&pattern)
                .map_err(|e| rusqlite::Error::UserFunctionError(Box::new(e)))?;
            Ok(re.is_match(&text))
        },
    )?;
    Ok(())
}

pub fn open_pool(path: &str, readonly: bool) -> anyhow::Result<DbPool> {
    let manager = if path == ":memory:" {
        SqliteConnectionManager::memory().with_init(|conn| {
            conn.execute_batch("PRAGMA foreign_keys = ON;")?;
            register_functions(conn)?;
            Ok(())
        })
    } else {
        let flags = if readonly {
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI
        } else {
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE
                | rusqlite::OpenFlags::SQLITE_OPEN_URI
        };
        SqliteConnectionManager::file(path)
            .with_flags(flags)
            .with_init(|conn| {
                conn.execute_batch("PRAGMA foreign_keys = ON;")?;
                register_functions(conn)?;
                Ok(())
            })
    };
    Ok(r2d2::Pool::new(manager)?)
}

pub fn load_schema(conn: &Connection) -> anyhow::Result<Schema> {
    let table_names = load_object_names(conn, "table")?;
    let view_names_sql = load_views_with_sql(conn)?;

    let mut tables = Vec::new();
    for name in &table_names {
        let columns = load_columns(conn, name)?;
        let foreign_keys = load_foreign_keys(conn, name)?;
        let index_names = load_index_names_for_table(conn, name)?;
        tables.push(TableMeta {
            name: name.clone(),
            columns,
            foreign_keys,
            indexes: index_names,
        });
    }

    let views = view_names_sql
        .into_iter()
        .map(|(name, sql)| ViewMeta { name, sql })
        .collect();

    let indexes = load_all_indexes(conn, &table_names)?;

    Ok(Schema {
        tables,
        views,
        indexes,
    })
}

fn load_object_names(conn: &Connection, obj_type: &str) -> anyhow::Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master WHERE type = ?1 AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )?;
    let rows = stmt.query_map([obj_type], |row| row.get::<_, String>(0))?;
    rows.collect::<Result<Vec<_>, _>>()
        .context("loading object names")
}

fn load_views_with_sql(conn: &Connection) -> anyhow::Result<Vec<(String, Option<String>)>> {
    let mut stmt = conn.prepare(
        "SELECT name, sql FROM sqlite_master WHERE type = 'view' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
    })?;
    rows.collect::<Result<Vec<_>, _>>().context("loading views")
}

fn load_columns(conn: &Connection, table: &str) -> anyhow::Result<Vec<Column>> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info(\"{}\")", table))?;
    let rows = stmt.query_map([], |row| {
        Ok(Column {
            cid: row.get::<_, i64>(0)?,
            name: row.get::<_, String>(1)?,
            col_type: row.get::<_, String>(2)?,
            not_null: row.get::<_, i64>(3)? != 0,
            default_value: row.get::<_, Option<String>>(4)?,
            is_pk: row.get::<_, i64>(5)? != 0,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>()
        .context("loading columns")
}

fn load_foreign_keys(conn: &Connection, table: &str) -> anyhow::Result<Vec<ForeignKey>> {
    let mut stmt = conn.prepare(&format!("PRAGMA foreign_key_list(\"{}\")", table))?;
    let rows = stmt.query_map([], |row| {
        Ok(ForeignKey {
            to_table: row.get::<_, String>(2)?,
            from_col: row.get::<_, String>(3)?,
            to_col: row.get::<_, String>(4)?,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>()
        .context("loading foreign keys")
}

fn load_index_names_for_table(conn: &Connection, table: &str) -> anyhow::Result<Vec<String>> {
    let mut stmt = conn.prepare(&format!("PRAGMA index_list(\"{}\")", table))?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
    let names: Vec<String> = rows
        .collect::<Result<Vec<_>, _>>()
        .context("loading index names")?
        .into_iter()
        .filter(|n| !n.starts_with("sqlite_"))
        .collect();
    Ok(names)
}

fn load_all_indexes(conn: &Connection, tables: &[String]) -> anyhow::Result<Vec<IndexMeta>> {
    let mut indexes = Vec::new();
    for table in tables {
        let mut stmt = conn.prepare(&format!("PRAGMA index_list(\"{}\")", table))?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(1)?, // name
                row.get::<_, i64>(2)?,    // unique
            ))
        })?;
        for row in rows {
            let (name, unique) = row?;
            if !name.starts_with("sqlite_") {
                indexes.push(IndexMeta {
                    name,
                    table: table.clone(),
                    unique: unique != 0,
                });
            }
        }
    }
    Ok(indexes)
}

pub fn count_rows(
    conn: &Connection,
    table: &str,
    where_clause: &str,
    where_params: &[rusqlite::types::Value],
) -> anyhow::Result<i64> {
    let where_part = build_where_part(where_clause);
    let sql = format!("SELECT COUNT(*) FROM \"{}\"{}", table, where_part);
    let count: i64 = conn.query_row(
        &sql,
        rusqlite::params_from_iter(where_params.iter()),
        |row| row.get(0),
    )?;
    Ok(count)
}

fn build_order_terms(order_by: Option<(&str, bool)>) -> String {
    match order_by {
        Some((col, asc)) => format!(
            "\"{}\" {}, rowid ASC",
            col,
            if asc { "ASC" } else { "DESC" }
        ),
        None => "rowid ASC".to_string(),
    }
}

fn build_where_part(where_clause: &str) -> String {
    if where_clause.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", where_clause)
    }
}

fn decode_sql_value(value: rusqlite::types::ValueRef<'_>) -> types::SqlValue {
    use rusqlite::types::ValueRef;
    use types::SqlValue;

    match value {
        ValueRef::Null => SqlValue::Null,
        ValueRef::Integer(n) => SqlValue::Integer(n),
        ValueRef::Real(f) => SqlValue::Real(f),
        ValueRef::Text(bytes) => SqlValue::Text(String::from_utf8_lossy(bytes).into_owned()),
        ValueRef::Blob(bytes) => SqlValue::Blob(bytes.to_vec()),
    }
}

fn decode_row_values(row: &Row<'_>, col_count: usize) -> rusqlite::Result<Vec<types::SqlValue>> {
    (0..col_count)
        .map(|index| row.get_ref(index).map(decode_sql_value))
        .collect()
}

pub fn fetch_rows(
    conn: &Connection,
    request: RowFetch<'_>,
) -> anyhow::Result<Vec<Vec<types::SqlValue>>> {
    if request.columns.is_empty() {
        return Ok(Vec::new());
    }

    let col_names: Vec<String> = request
        .columns
        .iter()
        .map(|column| format!("\"{}\"", column.name))
        .collect();
    let order_clause = format!(" ORDER BY {}", build_order_terms(request.order_by));
    let where_part = build_where_part(request.where_clause);
    let query = format!(
        "SELECT {} FROM \"{}\"{}{}  LIMIT {} OFFSET {}",
        col_names.join(", "),
        request.table,
        where_part,
        order_clause,
        request.limit,
        request.offset
    );

    let mut stmt = conn.prepare(&query)?;
    let col_count = request.columns.len();
    let rows = stmt.query_map(
        rusqlite::params_from_iter(request.where_params.iter()),
        |row| decode_row_values(row, col_count),
    )?;

    rows.collect::<Result<Vec<_>, _>>().context("fetching rows")
}

pub fn fetch_random_rows(
    conn: &Connection,
    table: &str,
    columns: &[Column],
    limit: usize,
) -> anyhow::Result<Vec<Vec<types::SqlValue>>> {
    if columns.is_empty() || limit == 0 {
        return Ok(Vec::new());
    }

    let col_names: Vec<String> = columns.iter().map(|c| format!("\"{}\"", c.name)).collect();
    let query = format!(
        "SELECT {} FROM \"{}\" ORDER BY RANDOM() LIMIT {}",
        col_names.join(", "),
        table,
        limit
    );

    let mut stmt = conn.prepare(&query)?;
    let col_count = columns.len();
    let rows = stmt.query_map([], |row| decode_row_values(row, col_count))?;

    rows.collect::<Result<Vec<_>, _>>()
        .context("fetching random sample rows")
}

pub fn fetch_rowid_at_offset(
    conn: &Connection,
    table: &str,
    offset: i64,
    order_by: Option<(&str, bool)>,
    where_clause: &str,
    where_params: &[rusqlite::types::Value],
) -> anyhow::Result<Option<i64>> {
    let order_clause = format!(" ORDER BY {}", build_order_terms(order_by));
    let where_part = build_where_part(where_clause);
    let query = format!(
        "SELECT rowid FROM \"{}\"{}{} LIMIT 1 OFFSET {}",
        table, where_part, order_clause, offset
    );
    conn.query_row(
        &query,
        rusqlite::params_from_iter(where_params.iter()),
        |row| row.get(0),
    )
    .optional()
    .context("fetching rowid at offset")
}

pub fn fetch_offset_for_rowid(
    conn: &Connection,
    table: &str,
    rowid: i64,
    order_by: Option<(&str, bool)>,
    where_clause: &str,
    where_params: &[rusqlite::types::Value],
) -> anyhow::Result<Option<i64>> {
    let order_terms = build_order_terms(order_by);
    let where_part = build_where_part(where_clause);
    let rowid_param = where_params.len() + 1;
    let query = format!(
        "SELECT visible_offset FROM (
            SELECT rowid, ROW_NUMBER() OVER (ORDER BY {}) - 1 AS visible_offset
            FROM \"{}\"{}
        ) WHERE rowid = ?{} LIMIT 1",
        order_terms, table, where_part, rowid_param
    );
    let mut params = where_params.to_vec();
    params.push(rusqlite::types::Value::Integer(rowid));
    conn.query_row(&query, rusqlite::params_from_iter(params.iter()), |row| {
        row.get(0)
    })
    .optional()
    .context("fetching offset for rowid")
}

pub fn load_distinct_values(
    conn: &Connection,
    table: &str,
    column: &str,
    limit: usize,
) -> anyhow::Result<Vec<String>> {
    use rusqlite::types::ValueRef;

    let sql = format!(
        "SELECT DISTINCT \"{column}\" FROM \"{table}\" WHERE \"{column}\" IS NOT NULL ORDER BY 1 LIMIT {limit}"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |row| {
        let value = match row.get_ref(0)? {
            ValueRef::Null => String::new(),
            ValueRef::Integer(n) => n.to_string(),
            ValueRef::Real(f) => f.to_string(),
            ValueRef::Text(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            ValueRef::Blob(bytes) => format!("<blob {} bytes>", bytes.len()),
        };
        Ok(value)
    })?;

    rows.collect::<Result<Vec<_>, _>>()
        .context("loading distinct values")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text_column(cid: i64, name: &str) -> Column {
        Column {
            cid,
            name: name.to_string(),
            col_type: "TEXT".to_string(),
            not_null: false,
            default_value: None,
            is_pk: false,
        }
    }

    #[test]
    fn fetch_rows_respects_sort_and_filter() {
        let conn = Connection::open_in_memory().expect("in-memory db");
        conn.execute_batch(
            r#"
            CREATE TABLE items (name TEXT, created_at TEXT);
            INSERT INTO items (rowid, name, created_at) VALUES
              (11, 'a', '2024-01-01'),
              (22, 'b', '2024-01-02'),
              (33, 'c', '2024-01-03');
            "#,
        )
        .expect("seed items");

        let columns = vec![text_column(0, "name"), text_column(1, "created_at")];
        let where_params = [rusqlite::types::Value::Text("a".to_string())];
        let rows = fetch_rows(
            &conn,
            RowFetch {
                table: "items",
                columns: &columns,
                offset: 0,
                limit: 2,
                order_by: Some(("created_at", false)),
                where_clause: "\"name\" != ?1",
                where_params: &where_params,
            },
        )
        .expect("row fetch");

        assert_eq!(
            rows,
            vec![
                vec![
                    types::SqlValue::Text("c".to_string()),
                    types::SqlValue::Text("2024-01-03".to_string()),
                ],
                vec![
                    types::SqlValue::Text("b".to_string()),
                    types::SqlValue::Text("2024-01-02".to_string()),
                ],
            ]
        );
    }

    #[test]
    fn fetch_rowid_at_offset_respects_sort_and_filter() {
        let conn = Connection::open_in_memory().expect("in-memory db");
        conn.execute_batch(
            r#"
            CREATE TABLE items (name TEXT, created_at TEXT);
            INSERT INTO items (rowid, name, created_at) VALUES
              (11, 'a', '2024-01-01'),
              (22, 'b', '2024-01-02'),
              (33, 'c', '2024-01-03');
            "#,
        )
        .expect("seed items");

        let rowid = fetch_rowid_at_offset(
            &conn,
            "items",
            0,
            Some(("created_at", false)),
            "\"name\" != ?1",
            &[rusqlite::types::Value::Text("c".to_string())],
        )
        .expect("rowid lookup");

        assert_eq!(rowid, Some(22));
    }

    #[test]
    fn fetch_offset_for_rowid_respects_sort_and_filter() {
        let conn = Connection::open_in_memory().expect("in-memory db");
        conn.execute_batch(
            r#"
            CREATE TABLE items (name TEXT, created_at TEXT);
            INSERT INTO items (rowid, name, created_at) VALUES
              (11, 'a', '2024-01-01'),
              (22, 'b', '2024-01-02'),
              (33, 'c', '2024-01-03');
            "#,
        )
        .expect("seed items");

        let offset = fetch_offset_for_rowid(
            &conn,
            "items",
            22,
            Some(("created_at", false)),
            "\"name\" != ?1",
            &[rusqlite::types::Value::Text("a".to_string())],
        )
        .expect("offset lookup");

        assert_eq!(offset, Some(1));
    }

    #[test]
    fn sorted_row_lookup_is_stable_for_duplicate_values() {
        let conn = Connection::open_in_memory().expect("in-memory db");
        conn.execute_batch(
            r#"
            CREATE TABLE items (name TEXT);
            INSERT INTO items (rowid, name) VALUES
              (20, 'same'),
              (10, 'same'),
              (30, 'z');
            "#,
        )
        .expect("seed items");

        let first_rowid = fetch_rowid_at_offset(&conn, "items", 0, Some(("name", true)), "", &[])
            .expect("rowid lookup");
        let second_offset =
            fetch_offset_for_rowid(&conn, "items", 20, Some(("name", true)), "", &[])
                .expect("offset lookup");

        assert_eq!(first_rowid, Some(10));
        assert_eq!(second_offset, Some(1));
    }
}
