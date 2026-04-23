#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Schema {
    pub tables: Vec<TableMeta>,
    pub views: Vec<ViewMeta>,
    pub indexes: Vec<IndexMeta>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct TableMeta {
    pub name: String,
    pub columns: Vec<Column>,
    pub foreign_keys: Vec<ForeignKey>,
    pub indexes: Vec<String>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ViewMeta {
    pub name: String,
    pub sql: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct IndexMeta {
    pub name: String,
    pub table: String,
    pub unique: bool,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Column {
    pub cid: i64,
    pub name: String,
    pub col_type: String,
    pub not_null: bool,
    pub default_value: Option<String>,
    pub is_pk: bool,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ForeignKey {
    pub from_col: String,
    pub to_table: String,
    pub to_col: String,
}
