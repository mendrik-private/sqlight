use rusqlite::Connection;

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
