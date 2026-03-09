use std::str::FromStr;
use std::sync::OnceLock;

use anyhow::{Result, bail};
use sqlparser::{
    ast::{ColumnOption, DataType, Expr, Statement},
    dialect::SQLiteDialect,
    parser::Parser,
};
use strum::EnumString;

use crate::error::SqliteParseError;
use super::record::Record;

const SQLITE_INTERNAL_PREFIX: &str = "sqlite_";
const SCHEMA_TABLE_NAME: &str = "sqlite_schema";

#[derive(Debug)]
pub struct SchemaTable {
    entries: Vec<SchemaTableEntry>,
}

impl SchemaTable {
    pub fn from_entries(entries: Vec<SchemaTableEntry>) -> Self {
        Self { entries }
    }

    pub fn entries(&self) -> &[SchemaTableEntry] {
        &self.entries
    }

    pub fn table_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.object_type == SchemaObjectType::Table)
            .count()
    }

    pub fn user_table_names(&self) -> Vec<&str> {
        self.entries
            .iter()
            .filter(|entry| entry.is_user_table())
            .map(|entry| entry.table_name.as_str())
            .collect()
    }

    pub fn find_table(&self, name: &str) -> Option<&SchemaTableEntry> {
        self.entries.iter().find(|entry| {
            entry.object_type == SchemaObjectType::Table
                && entry.table_name.eq_ignore_ascii_case(name)
        })
    }

    pub fn indexes_for_table(&self, table_name: &str) -> impl Iterator<Item = &SchemaTableEntry> {
        self.entries.iter().filter(|entry| {
            entry.object_type == SchemaObjectType::Index
                && entry.table_name.eq_ignore_ascii_case(table_name)
                && entry.indexed_column_names().ok().flatten().is_some()
        })
    }

    pub fn find_index_for_column(
        &self,
        table_name: &str,
        column_name: &str,
    ) -> Option<&SchemaTableEntry> {
        self.entries.iter().find(|entry| {
            if entry.object_type != SchemaObjectType::Index
                || !entry.table_name.eq_ignore_ascii_case(table_name)
            {
                return false;
            }
            entry
                .indexed_column_names()
                .ok()
                .flatten()
                .and_then(|columns| columns.first())
                .is_some_and(|indexed_column| indexed_column.eq_ignore_ascii_case(column_name))
        })
    }
}

#[derive(Debug)]
pub struct SchemaTableEntry {
    pub name: String,
    pub rootpage: Option<u32>,
    object_type: SchemaObjectType,
    table_name: String,
    sql: Option<String>,
    metadata: OnceLock<SchemaMetadata>,
}

impl SchemaTableEntry {
    pub fn is_user_table(&self) -> bool {
        self.object_type == SchemaObjectType::Table
            && !self.table_name.starts_with(SQLITE_INTERNAL_PREFIX)
    }

    pub fn column_names(&self) -> Result<&[String]> {
        match self.metadata()? {
            SchemaMetadata::Table(table) => Ok(table.column_names.as_slice()),
            _ => bail!(SqliteParseError::MalformedSchema {
                object_name: self.table_name.clone(),
            }),
        }
    }

    pub fn rowid_alias_column_name(&self) -> Result<Option<&str>> {
        match self.metadata()? {
            SchemaMetadata::Table(table) => Ok(table
                .rowid_alias_index
                .map(|index| table.column_names[index].as_str())),
            _ => bail!(SqliteParseError::MalformedSchema {
                object_name: self.table_name.clone(),
            }),
        }
    }

    pub fn indexed_column_names(&self) -> Result<Option<&[String]>> {
        if self.object_type != SchemaObjectType::Index {
            return Ok(None);
        }

        match self.metadata()? {
            SchemaMetadata::Index(index) => Ok(index.indexed_column_names.as_deref()),
            _ => bail!(SqliteParseError::MalformedSchema {
                object_name: self.name.clone(),
            }),
        }
    }

    pub fn column_index(&self, column_name: &str) -> Result<usize> {
        self.column_names()?
            .iter()
            .position(|name| name.eq_ignore_ascii_case(column_name))
            .ok_or_else(|| SqliteParseError::ColumnNotFound {
                table_name: self.table_name.clone(),
                column_name: column_name.to_owned(),
            })
            .map_err(Into::into)
    }

    fn metadata(&self) -> Result<&SchemaMetadata> {
        if let Some(metadata) = self.metadata.get() {
            return Ok(metadata);
        }

        let metadata = match self.object_type {
            SchemaObjectType::Table => {
                SchemaMetadata::Table(TableMetadata::from_create_table(self.parse_create_table()?))
            }
            SchemaObjectType::Index => {
                SchemaMetadata::Index(IndexMetadata::from_create_index(self.parse_create_index()?))
            }
            _ => SchemaMetadata::Other,
        };
        let _ = self.metadata.set(metadata);

        Ok(self
            .metadata
            .get()
            .expect("schema metadata should be initialized"))
    }

    fn parse_create_table(&self) -> Result<sqlparser::ast::CreateTable> {
        let sql = self
            .sql
            .as_ref()
            .ok_or_else(|| SqliteParseError::MalformedSchema {
                object_name: self.table_name.clone(),
            })?;

        let dialect = SQLiteDialect {};
        let mut statements =
            Parser::parse_sql(&dialect, sql).map_err(|_| SqliteParseError::MalformedSchema {
                object_name: self.table_name.clone(),
            })?;

        if statements.len() != 1 {
            bail!(SqliteParseError::MalformedSchema {
                object_name: self.table_name.clone(),
            });
        }

        let statement = statements.pop().expect("single statement must exist");
        let Statement::CreateTable(create_table) = statement else {
            bail!(SqliteParseError::MalformedSchema {
                object_name: self.table_name.clone(),
            });
        };

        Ok(create_table)
    }

    fn parse_create_index(&self) -> Result<sqlparser::ast::CreateIndex> {
        let sql = self
            .sql
            .as_ref()
            .ok_or_else(|| SqliteParseError::MalformedSchema {
                object_name: self.name.clone(),
            })?;

        let dialect = SQLiteDialect {};
        let mut statements =
            Parser::parse_sql(&dialect, sql).map_err(|_| SqliteParseError::MalformedSchema {
                object_name: self.name.clone(),
            })?;

        if statements.len() != 1 {
            bail!(SqliteParseError::MalformedSchema {
                object_name: self.name.clone(),
            });
        }

        let statement = statements.pop().expect("single statement must exist");
        let Statement::CreateIndex(create_index) = statement else {
            bail!(SqliteParseError::MalformedSchema {
                object_name: self.name.clone(),
            });
        };

        Ok(create_index)
    }

    pub(crate) fn parse_record_payload(payload: &[u8]) -> Result<Self> {
        let record = Record::parse(payload)?;
        let columns = record.columns();
        if columns.len() != 5 {
            bail!(SqliteParseError::InvalidRecordHeaderSize(
                columns.len() as u64
            ));
        }

        let type_str = columns[0].decode_text(format!("{SCHEMA_TABLE_NAME}.type"))?;
        let object_type = SchemaObjectType::from_str(type_str)
            .map_err(|_| SqliteParseError::InvalidSchemaObjectType(type_str.to_owned()))?;
        let name = columns[1].decode_text(format!("{SCHEMA_TABLE_NAME}.name"))?;
        let table_name = columns[2].decode_text(format!("{SCHEMA_TABLE_NAME}.tbl_name"))?;
        let rootpage_integer =
            columns[3].decode_optional_integer(format!("{SCHEMA_TABLE_NAME}.rootpage"))?;
        let rootpage = rootpage_integer
            .map(|integer| {
                u32::try_from(integer).map_err(|_| SqliteParseError::InvalidRootPage(integer))
            })
            .transpose()?;
        let sql = columns[4].decode_nullable_text(format!("{SCHEMA_TABLE_NAME}.sql"))?;

        Ok(Self {
            object_type,
            name: name.to_owned(),
            table_name: table_name.to_owned(),
            rootpage,
            sql: sql.map(str::to_owned),
            metadata: OnceLock::new(),
        })
    }
}

#[derive(Debug)]
enum SchemaMetadata {
    Table(TableMetadata),
    Index(IndexMetadata),
    Other,
}

#[derive(Debug)]
struct TableMetadata {
    column_names: Vec<String>,
    rowid_alias_index: Option<usize>,
}

impl TableMetadata {
    fn from_create_table(create_table: sqlparser::ast::CreateTable) -> Self {
        let rowid_alias_index = create_table.columns.iter().position(|column| {
            matches!(column.data_type, DataType::Integer(_))
                && column
                    .options
                    .iter()
                    .any(|option| matches!(option.option, ColumnOption::PrimaryKey(_)))
        });
        let column_names = create_table
            .columns
            .into_iter()
            .map(|column| column.name.value)
            .collect();

        Self {
            column_names,
            rowid_alias_index,
        }
    }
}

#[derive(Debug)]
struct IndexMetadata {
    indexed_column_names: Option<Vec<String>>,
}

impl IndexMetadata {
    fn from_create_index(create_index: sqlparser::ast::CreateIndex) -> Self {
        let indexed_column_names = if create_index.concurrently
            || !create_index.include.is_empty()
            || create_index.nulls_distinct.is_some()
            || !create_index.with.is_empty()
            || create_index.predicate.is_some()
            || !create_index.index_options.is_empty()
            || !create_index.alter_options.is_empty()
            || create_index.using.is_some()
            || create_index.columns.is_empty()
        {
            None
        } else {
            create_index
                .columns
                .iter()
                .map(|index_column| {
                    if index_column.operator_class.is_some()
                        || index_column.column.options.asc.is_some()
                        || index_column.column.options.nulls_first.is_some()
                        || index_column.column.with_fill.is_some()
                    {
                        return None;
                    }
                    match &index_column.column.expr {
                        Expr::Identifier(identifier) => Some(identifier.value.clone()),
                        _ => None,
                    }
                })
                .collect()
        };

        Self {
            indexed_column_names,
        }
    }
}

#[derive(Clone, Debug, PartialEq, EnumString)]
#[strum(serialize_all = "lowercase")]
pub enum SchemaObjectType {
    Table,
    Index,
    View,
    Trigger,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_column_names_from_create_table_sql() {
        let entry = SchemaTableEntry {
            object_type: SchemaObjectType::Table,
            name: "apples".to_owned(),
            table_name: "apples".to_owned(),
            rootpage: Some(2),
            sql: Some(
                "CREATE TABLE apples (id integer primary key autoincrement, name text, color text)"
                    .to_owned(),
            ),
            metadata: OnceLock::new(),
        };

        assert_eq!(
            entry.column_names().unwrap(),
            vec!["id".to_owned(), "name".to_owned(), "color".to_owned()]
        );
        assert_eq!(entry.column_index("name").unwrap(), 1);
        assert_eq!(entry.rowid_alias_column_name().unwrap(), Some("id"));
    }

    #[test]
    fn parses_indexed_column_name_from_create_index_sql() {
        let entry = SchemaTableEntry {
            object_type: SchemaObjectType::Index,
            name: "idx_companies_country".to_owned(),
            table_name: "companies".to_owned(),
            rootpage: Some(4),
            sql: Some("CREATE INDEX idx_companies_country ON companies (country)".to_owned()),
            metadata: OnceLock::new(),
        };

        assert_eq!(
            entry.indexed_column_names().unwrap(),
            Some(&["country".to_owned()][..])
        );
    }

    #[test]
    fn parses_multi_column_index_metadata() {
        let entry = SchemaTableEntry {
            object_type: SchemaObjectType::Index,
            name: "idx_apples_color_name".to_owned(),
            table_name: "apples".to_owned(),
            rootpage: Some(4),
            sql: Some("CREATE INDEX idx_apples_color_name ON apples (color, name)".to_owned()),
            metadata: OnceLock::new(),
        };

        assert_eq!(
            entry.indexed_column_names().unwrap(),
            Some(&["color".to_owned(), "name".to_owned()][..])
        );
    }

    #[test]
    fn counts_only_schema_tables() {
        let schema = SchemaTable::from_entries(vec![
            SchemaTableEntry {
                object_type: SchemaObjectType::Table,
                name: "apples".to_owned(),
                table_name: "apples".to_owned(),
                rootpage: Some(2),
                sql: Some("CREATE TABLE apples (id integer)".to_owned()),
                metadata: OnceLock::new(),
            },
            SchemaTableEntry {
                object_type: SchemaObjectType::Index,
                name: "idx_apples_id".to_owned(),
                table_name: "apples".to_owned(),
                rootpage: Some(3),
                sql: Some("CREATE INDEX idx_apples_id ON apples (id)".to_owned()),
                metadata: OnceLock::new(),
            },
        ]);

        assert_eq!(schema.table_count(), 1);
    }
}
