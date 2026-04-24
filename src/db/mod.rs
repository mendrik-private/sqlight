pub mod query;
pub mod schema;
pub mod types;
pub mod write;

use anyhow::Context;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::functions::FunctionFlags;
use rusqlite::Connection;

use schema::{Column, ForeignKey, IndexMeta, Schema, TableMeta, ViewMeta};

pub type DbPool = r2d2::Pool<SqliteConnectionManager>;

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
    let where_part = if where_clause.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", where_clause)
    };
    let sql = format!("SELECT COUNT(*) FROM \"{}\"{}", table, where_part);
    let count: i64 = conn.query_row(
        &sql,
        rusqlite::params_from_iter(where_params.iter()),
        |row| row.get(0),
    )?;
    Ok(count)
}

#[allow(clippy::too_many_arguments)]
pub fn fetch_rows(
    conn: &Connection,
    table: &str,
    columns: &[Column],
    offset: i64,
    limit: i64,
    order_by: Option<(&str, bool)>,
    where_clause: &str,
    where_params: &[rusqlite::types::Value],
) -> anyhow::Result<Vec<Vec<types::SqlValue>>> {
    use rusqlite::types::ValueRef;
    use types::SqlValue;

    if columns.is_empty() {
        return Ok(Vec::new());
    }

    let col_names: Vec<String> = columns.iter().map(|c| format!("\"{}\"", c.name)).collect();
    let order_clause = match order_by {
        Some((col, asc)) => format!(" ORDER BY \"{}\" {}", col, if asc { "ASC" } else { "DESC" }),
        None => String::new(),
    };
    let where_part = if where_clause.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", where_clause)
    };
    let query = format!(
        "SELECT {} FROM \"{}\"{}{}  LIMIT {} OFFSET {}",
        col_names.join(", "),
        table,
        where_part,
        order_clause,
        limit,
        offset
    );

    let mut stmt = conn.prepare(&query)?;
    let col_count = columns.len();
    let rows = stmt.query_map(rusqlite::params_from_iter(where_params.iter()), |row| {
        let mut values = Vec::with_capacity(col_count);
        for i in 0..col_count {
            let val = match row.get_ref(i)? {
                ValueRef::Null => SqlValue::Null,
                ValueRef::Integer(n) => SqlValue::Integer(n),
                ValueRef::Real(f) => SqlValue::Real(f),
                ValueRef::Text(bytes) => {
                    SqlValue::Text(String::from_utf8_lossy(bytes).into_owned())
                }
                ValueRef::Blob(bytes) => SqlValue::Blob(bytes.to_vec()),
            };
            values.push(val);
        }
        Ok(values)
    })?;

    rows.collect::<Result<Vec<_>, _>>().context("fetching rows")
}

pub fn fetch_random_rows(
    conn: &Connection,
    table: &str,
    columns: &[Column],
    limit: usize,
) -> anyhow::Result<Vec<Vec<types::SqlValue>>> {
    use rusqlite::types::ValueRef;
    use types::SqlValue;

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
    let rows = stmt.query_map([], |row| {
        let mut values = Vec::with_capacity(col_count);
        for i in 0..col_count {
            let val = match row.get_ref(i)? {
                ValueRef::Null => SqlValue::Null,
                ValueRef::Integer(n) => SqlValue::Integer(n),
                ValueRef::Real(f) => SqlValue::Real(f),
                ValueRef::Text(bytes) => {
                    SqlValue::Text(String::from_utf8_lossy(bytes).into_owned())
                }
                ValueRef::Blob(bytes) => SqlValue::Blob(bytes.to_vec()),
            };
            values.push(val);
        }
        Ok(values)
    })?;

    rows.collect::<Result<Vec<_>, _>>()
        .context("fetching random sample rows")
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
