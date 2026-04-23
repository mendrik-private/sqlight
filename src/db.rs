use std::{collections::HashMap, path::Path};

use rusqlite::{Connection, Result, types::ValueRef};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColumnKind {
    Boolean,
    Integer,
    Float,
    Date,
    DateTime,
    ForeignKeyId,
    ShortText,
    LongText,
    TextHeavy,
    Unknown,
}

#[derive(Clone, Debug)]
pub struct ColumnSpec {
    pub name: String,
    pub declared_type: Option<String>,
    pub kind: ColumnKind,
    pub is_foreign_key: bool,
    pub referenced_table: Option<String>,
    pub referenced_column: Option<String>,
}

#[derive(Clone, Debug)]
pub struct RowRecord {
    pub rowid: Option<i64>,
    pub cells: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct LoadedTable {
    pub name: String,
    pub columns: Vec<ColumnSpec>,
    pub rows: Vec<RowRecord>,
}

pub struct Database {
    conn: Connection,
    label: String,
    tables: Vec<String>,
}

impl Database {
    pub fn open(path: Option<&str>) -> Result<Self> {
        let (conn, label) = match path {
            Some(path) => (Connection::open(Path::new(path))?, path.to_owned()),
            None => {
                let conn = Connection::open_in_memory()?;
                seed_demo_data(&conn)?;
                (conn, "demo".to_owned())
            }
        };

        let tables = load_table_names(&conn)?;

        Ok(Self {
            conn,
            label,
            tables,
        })
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    pub fn tables(&self) -> &[String] {
        &self.tables
    }

    pub fn table_row_count(&self, table_name: &str) -> Result<usize> {
        let sql = format!("SELECT COUNT(*) FROM {}", quoted_ident(table_name));
        self.conn.query_row(&sql, [], |row| row.get(0))
    }

    pub fn load_table(&self, table_name: &str, sort: Option<(usize, bool)>) -> Result<LoadedTable> {
        let mut columns = self.load_columns(table_name)?;
        let rows = self.load_rows(table_name, &columns, sort)?;

        for (index, column) in columns.iter_mut().enumerate() {
            let samples = rows.iter().map(|row| row.cells[index].as_str());
            column.kind = infer_kind(
                column.declared_type.as_deref(),
                &column.name,
                column.is_foreign_key,
                samples,
            );
        }

        Ok(LoadedTable {
            name: table_name.to_owned(),
            columns,
            rows,
        })
    }

    fn load_columns(&self, table_name: &str) -> Result<Vec<ColumnSpec>> {
        let pragma = format!("PRAGMA table_info({})", quoted_ident(table_name));
        let mut stmt = self.conn.prepare(&pragma)?;
        let foreign_keys = self.load_foreign_keys(table_name)?;
        let columns = stmt
            .query_map([], |row| {
                let name: String = row.get(1)?;
                let declared_type: Option<String> = row.get(2)?;
                let foreign_key = foreign_keys.get(&name);
                Ok(ColumnSpec {
                    is_foreign_key: foreign_key.is_some(),
                    kind: ColumnKind::Unknown,
                    name,
                    declared_type,
                    referenced_table: foreign_key.map(|fk| fk.table.clone()),
                    referenced_column: foreign_key.map(|fk| fk.column.clone()),
                })
            })?
            .collect::<Result<Vec<_>>>()?;

        Ok(columns)
    }

    fn load_foreign_keys(&self, table_name: &str) -> Result<HashMap<String, ForeignKeyRef>> {
        let pragma = format!("PRAGMA foreign_key_list({})", quoted_ident(table_name));
        let mut stmt = self.conn.prepare(&pragma)?;
        let keys = stmt
            .query_map([], |row| {
                Ok(ForeignKeyRef {
                    column: row.get(4)?,
                    from: row.get(3)?,
                    table: row.get(2)?,
                })
            })?
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(|fk| (fk.from.clone(), fk))
            .collect::<HashMap<_, _>>();
        Ok(keys)
    }

    fn load_rows(
        &self,
        table_name: &str,
        columns: &[ColumnSpec],
        sort: Option<(usize, bool)>,
    ) -> Result<Vec<RowRecord>> {
        let order_by = sort
            .filter(|(index, _)| *index < columns.len())
            .map(|(index, asc)| {
                format!(
                    " ORDER BY {} {}",
                    quoted_ident(&columns[index].name),
                    if asc { "ASC" } else { "DESC" }
                )
            })
            .unwrap_or_default();

        match self.try_load_rows(table_name, columns, &order_by, true) {
            Ok(rows) => Ok(rows),
            Err(_) => self.try_load_rows(table_name, columns, &order_by, false),
        }
    }

    fn try_load_rows(
        &self,
        table_name: &str,
        columns: &[ColumnSpec],
        order_by: &str,
        include_rowid: bool,
    ) -> Result<Vec<RowRecord>> {
        let selected_columns = columns
            .iter()
            .map(|column| quoted_ident(&column.name))
            .collect::<Vec<_>>()
            .join(", ");

        let select_list = if include_rowid {
            format!("rowid, {selected_columns}")
        } else {
            selected_columns
        };

        let sql = format!(
            "SELECT {select_list} FROM {}{order_by} LIMIT 500",
            quoted_ident(table_name)
        );

        let mut stmt = self.conn.prepare(&sql)?;
        let mut rows = stmt.query([])?;
        let mut result = Vec::new();

        while let Some(row) = rows.next()? {
            let offset = usize::from(include_rowid);
            let rowid = if include_rowid {
                row.get::<_, i64>(0).ok()
            } else {
                None
            };

            let mut cells = Vec::with_capacity(columns.len());
            for column_index in 0..columns.len() {
                let value = row.get_ref(column_index + offset)?;
                cells.push(render_value(value));
            }

            result.push(RowRecord { rowid, cells });
        }

        Ok(result)
    }
}

#[derive(Clone, Debug)]
struct ForeignKeyRef {
    from: String,
    table: String,
    column: String,
}

fn load_table_names(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master \
         WHERE type = 'table' AND name NOT LIKE 'sqlite_%' \
         ORDER BY name",
    )?;
    stmt.query_map([], |row| row.get(0))?
        .collect::<Result<Vec<_>>>()
}

fn quoted_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn render_value(value: ValueRef<'_>) -> String {
    match value {
        ValueRef::Null => "NULL".to_owned(),
        ValueRef::Integer(value) => value.to_string(),
        ValueRef::Real(value) => {
            if value.fract() == 0.0 {
                format!("{value:.1}")
            } else {
                value.to_string()
            }
        }
        ValueRef::Text(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        ValueRef::Blob(bytes) => format!("<{} bytes>", bytes.len()),
    }
}

fn infer_kind<'a>(
    declared_type: Option<&str>,
    name: &str,
    is_foreign_key: bool,
    samples: impl Iterator<Item = &'a str>,
) -> ColumnKind {
    if is_foreign_key || name.ends_with("_id") {
        return ColumnKind::ForeignKeyId;
    }

    let materialized = samples
        .filter(|value| !is_null_like(value))
        .collect::<Vec<_>>();
    let declared = declared_type.unwrap_or_default().to_ascii_uppercase();
    if declared.contains("BOOL") {
        return ColumnKind::Boolean;
    }
    if declared.contains("INT") {
        return ColumnKind::Integer;
    }
    if declared.contains("REAL")
        || declared.contains("FLOA")
        || declared.contains("DOUB")
        || declared.contains("NUMERIC")
        || declared.contains("DECIMAL")
    {
        return ColumnKind::Float;
    }
    if declared.contains("DATE") && declared.contains("TIME") {
        return ColumnKind::DateTime;
    }
    if declared.contains("DATE") || declared.contains("TIME") {
        return ColumnKind::Date;
    }

    if !materialized.is_empty()
        && materialized
            .iter()
            .all(|value| parse_bool_text(value).is_some())
    {
        return ColumnKind::Boolean;
    }
    if !materialized.is_empty()
        && materialized
            .iter()
            .all(|value| value.parse::<i64>().is_ok())
    {
        return ColumnKind::Integer;
    }
    if !materialized.is_empty()
        && materialized
            .iter()
            .all(|value| value.parse::<f64>().is_ok())
    {
        return ColumnKind::Float;
    }
    if !materialized.is_empty() && materialized.iter().all(|value| looks_like_datetime(value)) {
        return ColumnKind::DateTime;
    }
    if !materialized.is_empty() && materialized.iter().all(|value| looks_like_date(value)) {
        return ColumnKind::Date;
    }
    if declared.contains("CHAR") || declared.contains("CLOB") || declared.contains("TEXT") {
        return infer_text_kind(materialized.into_iter());
    }

    infer_text_kind(materialized.into_iter())
}

fn infer_text_kind<'a>(samples: impl Iterator<Item = &'a str>) -> ColumnKind {
    let mut max_len = 0usize;
    let mut long_count = 0usize;

    for sample in samples {
        let trimmed = sample.trim();
        if is_null_like(trimmed) {
            continue;
        }

        let len = trimmed.chars().count();
        max_len = max_len.max(len);
        if len > 40 {
            long_count += 1;
        }
    }

    if long_count >= 2 || max_len > 80 {
        ColumnKind::TextHeavy
    } else if max_len > 24 {
        ColumnKind::LongText
    } else if max_len > 0 {
        ColumnKind::ShortText
    } else {
        ColumnKind::Unknown
    }
}

pub fn parse_bool_text(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" | "t" => Some(true),
        "0" | "false" | "no" | "n" | "f" => Some(false),
        _ => None,
    }
}

fn is_null_like(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed.is_empty() || trimmed.eq_ignore_ascii_case("null")
}

fn looks_like_date(value: &str) -> bool {
    let value = value.trim();
    value.len() == 10 && value.chars().nth(4) == Some('-') && value.chars().nth(7) == Some('-')
}

fn looks_like_datetime(value: &str) -> bool {
    let value = value.trim();
    (value.len() >= 19
        && value.chars().nth(4) == Some('-')
        && value.chars().nth(7) == Some('-')
        && value
            .chars()
            .nth(10)
            .is_some_and(|ch| ch == ' ' || ch == 'T'))
        || value.contains('T')
}

fn seed_demo_data(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE teams (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            region TEXT NOT NULL,
            archived VARCHAR(5) NOT NULL
        );

        CREATE TABLE contacts (
            id INTEGER PRIMARY KEY,
            team_id INTEGER NOT NULL REFERENCES teams(id),
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            active BOOLEAN NOT NULL,
            joined_at TEXT NOT NULL,
            last_review_on TEXT NOT NULL,
            notes TEXT,
            FOREIGN KEY(team_id) REFERENCES teams(id)
        );

        CREATE TABLE projects (
            id INTEGER PRIMARY KEY,
            owner_contact_id INTEGER NOT NULL REFERENCES contacts(id),
            name TEXT NOT NULL,
            status VARCHAR(8) NOT NULL,
            due_on TEXT NOT NULL,
            FOREIGN KEY(owner_contact_id) REFERENCES contacts(id)
        );

        INSERT INTO teams (name, region, archived) VALUES
            ('Compiler', 'EU', 'false'),
            ('Flight Control', 'US', 'false'),
            ('Research', 'Remote', 'true');

        INSERT INTO contacts (team_id, name, email, active, joined_at, last_review_on, notes) VALUES
            (1, 'Ada Lovelace', 'ada@example.com', 1, '2024-01-10 09:00:00', '2024-04-15', 'Prefers short weekly syncs and keeps detailed design notes.'),
            (1, 'Grace Hopper', 'grace@example.com', 1, '2024-02-20 14:30:00', '2024-04-22', 'Looking after release checklist and compiler migration plan.'),
            (2, 'Margaret Hamilton', 'margaret@example.com', 0, '2024-03-18 08:15:00', '2024-05-02', 'Longer note column to exercise content-aware compression and make text-heavy columns fight for width late.'),
            (2, 'Katherine Johnson', 'katherine@example.com', 1, '2024-04-05 11:45:00', '2024-04-30', 'Focuses on validation runs.'),
            (3, 'Dorothy Vaughan', 'dorothy@example.com', 1, '2024-04-12 16:20:00', '2024-05-05', 'Helps with planning, handoff, and analytics.');

        INSERT INTO projects (owner_contact_id, name, status, due_on) VALUES
            (1, 'Analytical Engine QA', 'open', '2024-06-15'),
            (2, 'Compiler Modernization', 'open', '2024-07-01'),
            (3, 'Apollo Replay', 'paused', '2024-07-20'),
            (4, 'Orbital Metrics', 'open', '2024-06-05'),
            (5, 'Team Dashboard', 'done', '2024-05-28');
        ",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ColumnKind, infer_kind, parse_bool_text};

    #[test]
    fn parses_text_booleans() {
        assert_eq!(parse_bool_text("true"), Some(true));
        assert_eq!(parse_bool_text("FALSE"), Some(false));
        assert_eq!(parse_bool_text("maybe"), None);
    }

    #[test]
    fn infers_boolean_from_varchar_samples() {
        let samples = ["true", "false", "TRUE", "false"];
        let kind = infer_kind(Some("VARCHAR"), "active_flag", false, samples.into_iter());
        assert_eq!(kind, ColumnKind::Boolean);
    }
}
