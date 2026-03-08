use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

use crate::error::SqliteParseError;
use crate::page::{BTreeCell, BTreePage, BTreePageKind};
use crate::query::WhereClause;
use crate::record::Record;
use crate::schema_table::{SchemaTable, SchemaTableEntry};

const SQLITE_HEADER_LEN: usize = 100;
const SQLITE_MAGIC_HEADER: &[u8; 16] = b"SQLite format 3\0";
const SQLITE_MAX_PAGE_SIZE: u32 = 65_536;
const SQLITE_MAX_PAGE_SIZE_SENTINEL: u16 = 1;

#[derive(Debug)]
pub struct SqliteDB {
    path: PathBuf,
    header: DatabaseHeader,
    schema_page: BTreePage,
    schema_table: SchemaTable,
}

impl SqliteDB {
    pub fn open(path: &Path) -> Result<Self> {
        let mut file =
            File::open(path).with_context(|| format!("failed to open {}", path.display()))?;

        let mut header_bytes = [0_u8; SQLITE_HEADER_LEN];
        file.read_exact(&mut header_bytes)
            .context("failed to read SQLite database header")?;

        let header = DatabaseHeader::parse(&header_bytes)?;

        let mut page_one = vec![0_u8; header.page_size as usize];
        page_one[..SQLITE_HEADER_LEN].copy_from_slice(&header_bytes);
        file.read_exact(&mut page_one[SQLITE_HEADER_LEN..])
            .context("failed to read SQLite page 1")?;

        let schema_page = BTreePage::parse_page_one(&page_one)?;
        if schema_page.kind != BTreePageKind::TableLeaf {
            bail!(SqliteParseError::UnsupportedPageType(
                page_one[SQLITE_HEADER_LEN]
            ));
        }

        let schema_table = SchemaTable::parse(&page_one, &schema_page)?;

        Ok(Self {
            path: path.to_path_buf(),
            header,
            schema_page,
            schema_table,
        })
    }

    pub fn db_info(&self) -> DbInfo {
        DbInfo {
            page_size: self.header.page_size,
            table_count: self.schema_page.cell_count,
        }
    }

    pub fn table_names(&self) -> Vec<&str> {
        self.schema_table.user_table_names()
    }

    pub fn read_page(&self, page_number: u32) -> Result<Vec<u8>> {
        if page_number == 0 {
            bail!(SqliteParseError::InvalidPageNumber);
        }

        let mut file = File::open(&self.path)
            .with_context(|| format!("failed to open {}", self.path.display()))?;
        let page_size = self.header.page_size as usize;
        let file_offset = u64::from(page_number - 1) * u64::from(self.header.page_size);

        file.seek(SeekFrom::Start(file_offset))
            .with_context(|| format!("failed to seek to page {page_number}"))?;

        let mut page = vec![0_u8; page_size];
        file.read_exact(&mut page)
            .with_context(|| format!("failed to read page {page_number}"))?;

        Ok(page)
    }

    pub fn count_rows(&self, table_name: &str) -> Result<usize> {
        let (_, _page, btree_page) = self.read_table_root_page(table_name)?;
        Ok(usize::from(btree_page.cell_count))
    }

    pub fn select_rows(
        &self,
        table_name: &str,
        column_names: &[String],
        where_clause: Option<&WhereClause>,
    ) -> Result<Vec<String>> {
        let (entry, page, btree_page) = self.read_table_root_page(table_name)?;
        let column_indices = column_names
            .iter()
            .map(|column_name| entry.column_index(column_name))
            .collect::<Result<Vec<_>>>()?;
        let predicate = match where_clause {
            Some(WhereClause::EqualsText { column_name, value }) => Some((
                entry.column_index(column_name)?,
                column_name.clone(),
                value.clone(),
            )),
            None => None,
        };

        let rows = btree_page
            .cells(&page)?
            .into_iter()
            .map(|cell| match cell {
                BTreeCell::TableLeaf(cell) => {
                    let record = Record::parse(cell.payload)?;
                    if let Some((predicate_index, predicate_column_name, predicate_value)) =
                        predicate.as_ref()
                    {
                        let predicate_column = record.column(*predicate_index).ok_or(
                            SqliteParseError::RecordColumnOutOfBounds {
                                column_index: *predicate_index,
                            },
                        )?;
                        let actual_value = predicate_column
                            .decode_text(format!("{table_name}.{predicate_column_name}"))?;
                        if actual_value != *predicate_value {
                            return Ok(None);
                        }
                    }

                    let values = column_names
                        .iter()
                        .zip(&column_indices)
                        .map(|(column_name, &column_index)| {
                            let column = record.column(column_index).ok_or(
                                SqliteParseError::RecordColumnOutOfBounds { column_index },
                            )?;
                            column.decode_text(format!("{table_name}.{column_name}"))
                        })
                        .collect::<Result<Vec<_>>>()?;

                    Ok(Some(values.join("|")))
                }
                _ => unreachable!("table root page must be a table leaf page"),
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(rows.into_iter().flatten().collect())
    }

    fn read_table_root_page(
        &self,
        table_name: &str,
    ) -> Result<(SchemaTableEntry, Vec<u8>, BTreePage)> {
        let entry = self
            .schema_table
            .find_table(table_name)
            .cloned()
            .ok_or_else(|| SqliteParseError::TableNotFound(table_name.to_owned()))?;
        let rootpage = entry
            .rootpage
            .ok_or_else(|| SqliteParseError::MissingTableRootPage {
                table_name: table_name.to_owned(),
            })?;

        let page = self.read_page(rootpage)?;
        let header_offset = if rootpage == 1 { SQLITE_HEADER_LEN } else { 0 };
        let btree_page = BTreePage::parse(&page, header_offset)?;

        if btree_page.kind != BTreePageKind::TableLeaf {
            let page_type = page.get(header_offset).copied().unwrap_or_default();
            bail!(SqliteParseError::UnsupportedTablePageType {
                table_name: table_name.to_owned(),
                page_type,
            });
        }

        Ok((entry, page, btree_page))
    }
}

#[derive(Debug)]
pub struct DbInfo {
    pub page_size: u32,
    pub table_count: u16,
}

#[derive(Debug)]
struct DatabaseHeader {
    page_size: u32,
}

impl DatabaseHeader {
    fn parse(bytes: &[u8; SQLITE_HEADER_LEN]) -> Result<Self> {
        if &bytes[..SQLITE_MAGIC_HEADER.len()] != SQLITE_MAGIC_HEADER {
            bail!(SqliteParseError::InvalidFileHeader);
        }

        let raw_page_size = u16::from_be_bytes([bytes[16], bytes[17]]);
        let page_size = match raw_page_size {
            SQLITE_MAX_PAGE_SIZE_SENTINEL => SQLITE_MAX_PAGE_SIZE,
            512..=32_768 if raw_page_size.is_power_of_two() => raw_page_size as u32,
            other => bail!(SqliteParseError::InvalidPageSize(other)),
        };

        Ok(Self { page_size })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_db_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("sample.db")
    }

    #[test]
    fn parses_database_page_size() {
        let mut header = [0_u8; SQLITE_HEADER_LEN];
        header[..SQLITE_MAGIC_HEADER.len()].copy_from_slice(SQLITE_MAGIC_HEADER);
        header[16..18].copy_from_slice(&4096_u16.to_be_bytes());

        let parsed = DatabaseHeader::parse(&header).expect("header should parse");

        assert_eq!(parsed.page_size, 4096);
    }

    #[test]
    fn parses_special_case_page_size_65536() {
        let mut header = [0_u8; SQLITE_HEADER_LEN];
        header[..SQLITE_MAGIC_HEADER.len()].copy_from_slice(SQLITE_MAGIC_HEADER);
        header[16..18].copy_from_slice(&SQLITE_MAX_PAGE_SIZE_SENTINEL.to_be_bytes());

        let parsed = DatabaseHeader::parse(&header).expect("header should parse");

        assert_eq!(parsed.page_size, SQLITE_MAX_PAGE_SIZE);
    }

    #[test]
    fn parses_page_one_cell_count() {
        let mut page = vec![0_u8; SQLITE_HEADER_LEN + 8 + (3 * 2)];
        page[SQLITE_HEADER_LEN] = 13;
        page[SQLITE_HEADER_LEN + 3..SQLITE_HEADER_LEN + 5].copy_from_slice(&3_u16.to_be_bytes());

        let parsed = BTreePage::parse_page_one(&page).expect("page header should parse");

        assert_eq!(parsed.kind, BTreePageKind::TableLeaf);
        assert_eq!(parsed.cell_count, 3);
    }

    #[test]
    fn reads_non_root_page() {
        let database = SqliteDB::open(&sample_db_path()).expect("sample db should open");

        let page = database.read_page(2).expect("page 2 should be readable");

        assert_eq!(page.len(), database.db_info().page_size as usize);
    }

    #[test]
    fn finds_apples_rootpage() {
        let database = SqliteDB::open(&sample_db_path()).expect("sample db should open");

        let apples = database
            .schema_table
            .find_table("apples")
            .expect("apples table should exist");

        assert_eq!(apples.rootpage, Some(2));
    }

    #[test]
    fn counts_rows_in_apples_table() {
        let database = SqliteDB::open(&sample_db_path()).expect("sample db should open");

        let row_count = database
            .count_rows("apples")
            .expect("apples row count should parse");

        assert_eq!(row_count, 4);
    }

    #[test]
    fn selects_name_values_from_apples_table() {
        let database = SqliteDB::open(&sample_db_path()).expect("sample db should open");

        let mut values = database
            .select_rows("apples", &["name".to_owned()], None)
            .expect("apples names should parse");
        values.sort();

        assert_eq!(
            values,
            vec![
                "Fuji".to_owned(),
                "Golden Delicious".to_owned(),
                "Granny Smith".to_owned(),
                "Honeycrisp".to_owned(),
            ]
        );
    }

    #[test]
    fn missing_table_error_matches_sqlite_shape() {
        let database = SqliteDB::open(&sample_db_path()).expect("sample db should open");

        let error = database.count_rows("missing_table").unwrap_err();
        let error = error
            .downcast_ref::<SqliteParseError>()
            .expect("error should downcast to SqliteParseError");

        assert!(matches!(
            error,
            SqliteParseError::TableNotFound(table_name) if table_name == "missing_table"
        ));
    }

    #[test]
    fn missing_column_error_matches_sqlite_shape() {
        let database = SqliteDB::open(&sample_db_path()).expect("sample db should open");

        let error = database
            .select_rows("apples", &["missing_col".to_owned()], None)
            .unwrap_err();
        let error = error
            .downcast_ref::<SqliteParseError>()
            .expect("error should downcast to SqliteParseError");

        assert!(matches!(
            error,
            SqliteParseError::ColumnNotFound {
                table_name,
                column_name,
            } if table_name == "apples" && column_name == "missing_col"
        ));
    }

    #[test]
    fn selects_multi_column_rows_from_apples_table() {
        let database = SqliteDB::open(&sample_db_path()).expect("sample db should open");

        let mut rows = database
            .select_rows("apples", &["name".to_owned(), "color".to_owned()], None)
            .expect("apples rows should parse");
        rows.sort();

        assert_eq!(
            rows,
            vec![
                "Fuji|Red".to_owned(),
                "Golden Delicious|Yellow".to_owned(),
                "Granny Smith|Light Green".to_owned(),
                "Honeycrisp|Blush Red".to_owned(),
            ]
        );
    }

    #[test]
    fn preserves_projection_order_in_multi_column_rows() {
        let database = SqliteDB::open(&sample_db_path()).expect("sample db should open");

        let mut rows = database
            .select_rows("apples", &["color".to_owned(), "name".to_owned()], None)
            .expect("apples rows should parse");
        rows.sort();

        assert_eq!(
            rows,
            vec![
                "Blush Red|Honeycrisp".to_owned(),
                "Light Green|Granny Smith".to_owned(),
                "Red|Fuji".to_owned(),
                "Yellow|Golden Delicious".to_owned(),
            ]
        );
    }

    #[test]
    fn missing_one_of_multiple_columns_returns_column_not_found() {
        let database = SqliteDB::open(&sample_db_path()).expect("sample db should open");

        let error = database
            .select_rows(
                "apples",
                &["name".to_owned(), "missing_col".to_owned()],
                None,
            )
            .unwrap_err();
        let error = error
            .downcast_ref::<SqliteParseError>()
            .expect("error should downcast to SqliteParseError");

        assert!(matches!(
            error,
            SqliteParseError::ColumnNotFound {
                table_name,
                column_name,
            } if table_name == "apples" && column_name == "missing_col"
        ));
    }

    #[test]
    fn filters_rows_by_text_equality() {
        let database = SqliteDB::open(&sample_db_path()).expect("sample db should open");

        let rows = database
            .select_rows(
                "apples",
                &["name".to_owned(), "color".to_owned()],
                Some(&WhereClause::EqualsText {
                    column_name: "color".to_owned(),
                    value: "Yellow".to_owned(),
                }),
            )
            .expect("filtered rows should parse");

        assert_eq!(rows, vec!["Golden Delicious|Yellow".to_owned()]);
    }

    #[test]
    fn filters_on_non_projected_column() {
        let database = SqliteDB::open(&sample_db_path()).expect("sample db should open");

        let rows = database
            .select_rows(
                "apples",
                &["name".to_owned()],
                Some(&WhereClause::EqualsText {
                    column_name: "color".to_owned(),
                    value: "Yellow".to_owned(),
                }),
            )
            .expect("filtered rows should parse");

        assert_eq!(rows, vec!["Golden Delicious".to_owned()]);
    }

    #[test]
    fn missing_predicate_column_returns_column_not_found() {
        let database = SqliteDB::open(&sample_db_path()).expect("sample db should open");

        let error = database
            .select_rows(
                "apples",
                &["name".to_owned()],
                Some(&WhereClause::EqualsText {
                    column_name: "missing_col".to_owned(),
                    value: "Yellow".to_owned(),
                }),
            )
            .unwrap_err();
        let error = error
            .downcast_ref::<SqliteParseError>()
            .expect("error should downcast to SqliteParseError");

        assert!(matches!(
            error,
            SqliteParseError::ColumnNotFound {
                table_name,
                column_name,
            } if table_name == "apples" && column_name == "missing_col"
        ));
    }
}
