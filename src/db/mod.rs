pub mod query;
pub mod schema;
pub mod types;

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
