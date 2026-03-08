use std::str::FromStr;

use anyhow::{Result, bail};
use sqlparser::{ast::Statement, dialect::SQLiteDialect, parser::Parser};
use strum::EnumString;

use crate::error::SqliteParseError;
use crate::page::{BTreeCell, BTreePage, BTreePageKind, TableLeafCell};
use crate::record::Record;

const SQLITE_INTERNAL_PREFIX: &str = "sqlite_";
const SCHEMA_TABLE_NAME: &str = "sqlite_schema";

#[derive(Clone, Debug, PartialEq)]
pub struct SchemaTable {
    entries: Vec<SchemaTableEntry>,
}

impl SchemaTable {
    pub fn parse(page: &[u8], btree_page: &BTreePage) -> Result<Self> {
        if btree_page.kind != BTreePageKind::TableLeaf {
            bail!(SqliteParseError::UnsupportedPageType(
                page[btree_page.header_offset]
            ));
        }

        let entries = btree_page
            .cells(page)?
            .into_iter()
            .map(|cell| match cell {
                BTreeCell::TableLeaf(cell) => SchemaTableEntry::parse_from_table_leaf_cell(cell),
                _ => unreachable!("schema table must be stored on a table leaf b-tree page"),
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self { entries })
    }

    pub fn entries(&self) -> &[SchemaTableEntry] {
        &self.entries
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
}

#[derive(Clone, Debug, PartialEq)]
pub struct SchemaTableEntry {
    pub object_type: SchemaObjectType,
    pub name: String,
    pub table_name: String,
    pub rootpage: Option<u32>,
    pub sql: Option<String>,
}

impl SchemaTableEntry {
    pub fn is_user_table(&self) -> bool {
        self.object_type == SchemaObjectType::Table
            && !self.table_name.starts_with(SQLITE_INTERNAL_PREFIX)
    }

    pub fn column_names(&self) -> Result<Vec<String>> {
        let sql = self
            .sql
            .as_ref()
            .ok_or_else(|| SqliteParseError::MissingCreateTableSql {
                table_name: self.table_name.clone(),
            })?;

        let dialect = SQLiteDialect {};
        let mut statements = Parser::parse_sql(&dialect, sql).map_err(|_| {
            SqliteParseError::UnsupportedCreateTableSql {
                table_name: self.table_name.clone(),
            }
        })?;

        if statements.len() != 1 {
            bail!(SqliteParseError::UnsupportedCreateTableSql {
                table_name: self.table_name.clone(),
            });
        }

        let statement = statements.pop().expect("single statement must exist");
        let Statement::CreateTable(create_table) = statement else {
            bail!(SqliteParseError::UnsupportedCreateTableSql {
                table_name: self.table_name.clone(),
            });
        };

        Ok(create_table
            .columns
            .into_iter()
            .map(|column| column.name.value)
            .collect())
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

    fn parse_from_table_leaf_cell(cell: TableLeafCell<'_>) -> Result<Self> {
        Self::parse_record(cell.payload)
    }

    fn parse_record(payload: &[u8]) -> Result<Self> {
        let record = Record::parse(payload)?;
        let columns = record.columns();
        if columns.len() != 5 {
            bail!(SqliteParseError::InvalidRecordHeaderSize(
                columns.len() as u64
            ));
        }

        let type_str = columns[0].decode_text(format!("{SCHEMA_TABLE_NAME}.type"))?;
        let object_type = SchemaObjectType::from_str(&type_str)
            .map_err(|_| SqliteParseError::InvalidSchemaObjectType(type_str))?;
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
            name,
            table_name,
            rootpage,
            sql,
        })
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
        };

        assert_eq!(
            entry.column_names().unwrap(),
            vec!["id".to_owned(), "name".to_owned(), "color".to_owned()]
        );
        assert_eq!(entry.column_index("name").unwrap(), 1);
    }
}
