use rusqlite::Connection;

use crate::db::schema::Column;
use crate::db::types::SqlValue;

pub fn commit_cell_edit(
    conn: &Connection,
    table: &str,
    col: &str,
    rowid: i64,
    value: &SqlValue,
) -> anyhow::Result<()> {
    let tx = conn.unchecked_transaction()?;
    let query = format!("UPDATE \"{}\" SET \"{}\" = ?1 WHERE rowid = ?2", table, col);
    let result = match value {
        SqlValue::Null => tx.execute(&query, rusqlite::params![rusqlite::types::Null, rowid]),
        SqlValue::Integer(n) => tx.execute(&query, rusqlite::params![n, rowid]),
        SqlValue::Real(f) => tx.execute(&query, rusqlite::params![f, rowid]),
        SqlValue::Text(s) => tx.execute(&query, rusqlite::params![s, rowid]),
        SqlValue::Blob(b) => tx.execute(&query, rusqlite::params![b, rowid]),
    };
    match result {
        Ok(1) => {
            tx.commit()?;
            Ok(())
        }
        Ok(0) => {
            let _ = tx.rollback();
            Err(anyhow::anyhow!("no row matched rowid {}", rowid))
        }
        Ok(n) => {
            let _ = tx.rollback();
            Err(anyhow::anyhow!("unexpected update count: {}", n))
        }
        Err(e) => {
            let _ = tx.rollback();
            Err(anyhow::anyhow!("{}", e))
        }
    }
}

pub fn insert_default_row(conn: &Connection, table: &str) -> anyhow::Result<i64> {
    let tx = conn.unchecked_transaction()?;
    let result = tx.execute(&format!("INSERT INTO \"{}\" DEFAULT VALUES", table), []);
    match result {
        Ok(_) => {
            let rowid = tx.last_insert_rowid();
            tx.commit()?;
            Ok(rowid)
        }
        Err(e) => {
            let _ = tx.rollback();
            Err(anyhow::anyhow!("{}", e))
        }
    }
}

pub fn delete_row(conn: &Connection, table: &str, rowid: i64) -> anyhow::Result<()> {
    let tx = conn.unchecked_transaction()?;
    let result = tx.execute(
        &format!("DELETE FROM \"{}\" WHERE rowid = ?1", table),
        rusqlite::params![rowid],
    );
    match result {
        Ok(_) => {
            tx.commit()?;
            Ok(())
        }
        Err(e) => {
            let _ = tx.rollback();
            Err(anyhow::anyhow!("{}", e))
        }
    }
}

#[allow(dead_code)]
pub fn fetch_row_by_rowid(
    conn: &Connection,
    table: &str,
    columns: &[Column],
    rowid: i64,
) -> anyhow::Result<Option<Vec<SqlValue>>> {
    let col_list: String = columns
        .iter()
        .map(|c| format!("\"{}\"", c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT {} FROM \"{}\" WHERE rowid = ?1 LIMIT 1",
        col_list, table
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query_map(rusqlite::params![rowid], |row| {
        let mut vals = Vec::new();
        for i in 0..columns.len() {
            let v = match row.get_ref(i)? {
                rusqlite::types::ValueRef::Null => SqlValue::Null,
                rusqlite::types::ValueRef::Integer(n) => SqlValue::Integer(n),
                rusqlite::types::ValueRef::Real(f) => SqlValue::Real(f),
                rusqlite::types::ValueRef::Text(b) => {
                    SqlValue::Text(String::from_utf8_lossy(b).into_owned())
                }
                rusqlite::types::ValueRef::Blob(b) => SqlValue::Blob(b.to_vec()),
            };
            vals.push(v);
        }
        Ok(vals)
    })?;
    Ok(rows.next().transpose()?)
}

pub fn reinsert_row(
    conn: &Connection,
    table: &str,
    rowid: i64,
    cols: &[(String, SqlValue)],
) -> anyhow::Result<()> {
    if cols.is_empty() {
        return Ok(());
    }
    let tx = conn.unchecked_transaction()?;
    let col_names = cols
        .iter()
        .map(|(n, _)| format!("\"{}\"", n))
        .collect::<Vec<_>>()
        .join(", ");
    let placeholders = (2..=cols.len() + 1)
        .map(|i| format!("?{}", i))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "INSERT OR REPLACE INTO \"{}\" (rowid, {}) VALUES (?1, {})",
        table, col_names, placeholders
    );
    let mut all_params: Vec<rusqlite::types::Value> = vec![rusqlite::types::Value::Integer(rowid)];
    for (_, v) in cols {
        all_params.push(match v {
            SqlValue::Null => rusqlite::types::Value::Null,
            SqlValue::Integer(n) => rusqlite::types::Value::Integer(*n),
            SqlValue::Real(f) => rusqlite::types::Value::Real(*f),
            SqlValue::Text(s) => rusqlite::types::Value::Text(s.clone()),
            SqlValue::Blob(b) => rusqlite::types::Value::Blob(b.clone()),
        });
    }
    let result = tx.execute(&sql, rusqlite::params_from_iter(all_params.iter()));
    match result {
        Ok(_) => {
            tx.commit()?;
            Ok(())
        }
        Err(e) => {
            let _ = tx.rollback();
            Err(anyhow::anyhow!("{}", e))
        }
    }
}
