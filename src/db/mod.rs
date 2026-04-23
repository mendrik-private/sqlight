pub mod query;
pub mod schema;
pub mod types;
pub mod write;

use anyhow::Context;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::Connection;

use schema::{Column, ForeignKey, IndexMeta, Schema, TableMeta, ViewMeta};

pub type DbPool = r2d2::Pool<SqliteConnectionManager>;

pub fn open_pool(path: &str, readonly: bool) -> anyhow::Result<DbPool> {
    let manager = if path == ":memory:" {
        SqliteConnectionManager::memory()
            .with_init(|conn| conn.execute_batch("PRAGMA foreign_keys = ON;"))
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
            .with_init(|conn| conn.execute_batch("PRAGMA foreign_keys = ON;"))
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

pub fn count_rows(conn: &Connection, table: &str) -> anyhow::Result<i64> {
    let count: i64 = conn.query_row(&format!("SELECT COUNT(*) FROM \"{}\"", table), [], |row| {
        row.get(0)
    })?;
    Ok(count)
}

pub fn fetch_rows(
    conn: &Connection,
    table: &str,
    columns: &[Column],
    offset: i64,
    limit: i64,
) -> anyhow::Result<Vec<Vec<types::SqlValue>>> {
    use rusqlite::types::ValueRef;
    use types::SqlValue;

    if columns.is_empty() {
        return Ok(Vec::new());
    }

    let col_names: Vec<String> = columns.iter().map(|c| format!("\"{}\"", c.name)).collect();
    let query = format!(
        "SELECT {} FROM \"{}\" LIMIT ? OFFSET ?",
        col_names.join(", "),
        table
    );

    let mut stmt = conn.prepare(&query)?;
    let col_count = columns.len();
    let rows = stmt.query_map([limit, offset], |row| {
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
