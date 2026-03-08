use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

use crate::error::SqliteParseError;
use crate::index::IndexScanner;
use crate::page::{BTreePage, BTreePageKind};
use crate::query::WhereClause;
use crate::record::Record;
use crate::schema::{SchemaTable, SchemaTableEntry};
use crate::table::TableScanner;

const SQLITE_HEADER_LEN: usize = 100;
const SQLITE_MAGIC_HEADER: &[u8; 16] = b"SQLite format 3\0";
const SQLITE_MAX_PAGE_SIZE: u32 = 65_536;
const SQLITE_MAX_PAGE_SIZE_SENTINEL: u16 = 1;

#[derive(Clone, Copy, Debug)]
enum ResolvedColumn<'a> {
    RowIdAlias,
    RecordColumn { column_name: &'a str, index: usize },
}

impl<'a> ResolvedColumn<'a> {
    fn resolve(entry: &SchemaTableEntry, column_name: &'a str) -> Result<Self> {
        let rowid_alias = entry.rowid_alias_column_name()?;
        if rowid_alias
            .as_deref()
            .is_some_and(|alias| alias.eq_ignore_ascii_case(column_name))
        {
            return Ok(Self::RowIdAlias);
        }

        Ok(Self::RecordColumn {
            column_name,
            index: entry.column_index(column_name)?,
        })
    }

    fn decode_output(&self, table_name: &str, rowid: u64, record: &Record) -> Result<String> {
        match self {
            Self::RowIdAlias => Ok(rowid.to_string()),
            Self::RecordColumn { column_name, index } => {
                let column =
                    record
                        .column(*index)
                        .ok_or(SqliteParseError::RecordColumnOutOfBounds {
                            column_index: *index,
                        })?;
                column.decode_output_value(format!("{table_name}.{column_name}"))
            }
        }
    }

    fn matches_text_literal(
        &self,
        table_name: &str,
        rowid: u64,
        record: &Record,
        expected: &str,
    ) -> Result<bool> {
        match self {
            Self::RowIdAlias => Ok(rowid.to_string() == expected),
            Self::RecordColumn { column_name, index } => {
                let column =
                    record
                        .column(*index)
                        .ok_or(SqliteParseError::RecordColumnOutOfBounds {
                            column_index: *index,
                        })?;
                let actual = column.decode_nullable_text(format!("{table_name}.{column_name}"))?;
                Ok(actual == Some(expected))
            }
        }
    }
}

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
        let (_, rootpage) = self.resolve_table_root(table_name)?;
        let scanner = TableScanner::new(self);
        let mut count = 0;
        scanner.visit_records(table_name, rootpage, |_, _| {
            count += 1;
            Ok(())
        })?;
        Ok(count)
    }

    pub fn select_rows(
        &self,
        table_name: &str,
        column_names: &[String],
        where_clause: Option<&WhereClause>,
    ) -> Result<Vec<String>> {
        let (entry, rootpage) = self.resolve_table_root(table_name)?;
        let resolved_columns = Self::resolve_columns(entry, column_names)?;
        let predicate = Self::resolve_predicate(entry, where_clause)?;

        if let Some(WhereClause::EqualsText { column_name, value }) = where_clause
            && let Some(index_entry) = self
                .schema_table
                .find_index_for_column(table_name, column_name)
        {
            return self.select_rows_via_index_scan(
                table_name,
                rootpage,
                &resolved_columns,
                index_entry,
                value,
            );
        }

        self.select_rows_via_table_scan(table_name, rootpage, &resolved_columns, predicate)
    }

    fn select_rows_via_table_scan<'a>(
        &self,
        table_name: &str,
        rootpage: u32,
        resolved_columns: &[ResolvedColumn<'a>],
        predicate: Option<(ResolvedColumn<'a>, &'a str)>,
    ) -> Result<Vec<String>> {
        let scanner = TableScanner::new(self);
        let mut rows = Vec::new();

        scanner.visit_records(table_name, rootpage, |rowid, record| {
            if let Some((predicate_column, predicate_value)) = predicate
                && !predicate_column.matches_text_literal(
                    table_name,
                    rowid,
                    &record,
                    predicate_value,
                )?
            {
                return Ok(());
            }

            rows.push(Self::project_row(
                table_name,
                rowid,
                &record,
                resolved_columns,
            )?);
            Ok(())
        })?;

        Ok(rows)
    }

    fn select_rows_via_index_scan(
        &self,
        table_name: &str,
        table_rootpage: u32,
        resolved_columns: &[ResolvedColumn],
        index_entry: &SchemaTableEntry,
        predicate_value: &str,
    ) -> Result<Vec<String>> {
        let index_rootpage =
            index_entry
                .rootpage
                .ok_or_else(|| SqliteParseError::MissingRootPage {
                    object_type: "index",
                    object_name: index_entry.name.clone(),
                })?;
        let index_scanner = IndexScanner::new(self);
        let table_scanner = TableScanner::new(self);
        let mut rows = Vec::new();

        index_scanner.visit_matching_rowids(
            &index_entry.name,
            index_rootpage,
            predicate_value,
            |rowid| {
                table_scanner.with_record_by_rowid(
                    table_name,
                    table_rootpage,
                    rowid,
                    |rowid, record| {
                        rows.push(Self::project_row(
                            table_name,
                            rowid,
                            &record,
                            resolved_columns,
                        )?);
                        Ok(())
                    },
                )?;
                Ok(())
            },
        )?;

        Ok(rows)
    }

    fn project_row(
        table_name: &str,
        rowid: u64,
        record: &Record,
        resolved_columns: &[ResolvedColumn],
    ) -> Result<String> {
        Ok(resolved_columns
            .iter()
            .map(|column| column.decode_output(table_name, rowid, record))
            .collect::<Result<Vec<_>>>()?
            .join("|"))
    }

    fn resolve_columns<'a>(
        entry: &SchemaTableEntry,
        column_names: &'a [String],
    ) -> Result<Vec<ResolvedColumn<'a>>> {
        column_names
            .iter()
            .map(|column_name| ResolvedColumn::resolve(entry, column_name))
            .collect()
    }

    fn resolve_predicate<'a>(
        entry: &SchemaTableEntry,
        where_clause: Option<&'a WhereClause>,
    ) -> Result<Option<(ResolvedColumn<'a>, &'a str)>> {
        match where_clause {
            Some(WhereClause::EqualsText { column_name, value }) => {
                Ok(Some((ResolvedColumn::resolve(entry, column_name)?, value)))
            }
            None => Ok(None),
        }
    }

    pub(crate) fn read_btree_page(&self, page_number: u32) -> Result<(Vec<u8>, BTreePage)> {
        let page = self.read_page(page_number)?;
        let header_offset = if page_number == 1 {
            SQLITE_HEADER_LEN
        } else {
            0
        };
        let btree_page = BTreePage::parse(&page, header_offset)?;
        Ok((page, btree_page))
    }

    fn resolve_table_root(&self, table_name: &str) -> Result<(&SchemaTableEntry, u32)> {
        let entry = self
            .schema_table
            .find_table(table_name)
            .ok_or_else(|| SqliteParseError::TableNotFound(table_name.to_owned()))?;
        let rootpage = entry
            .rootpage
            .ok_or_else(|| SqliteParseError::MissingRootPage {
                object_type: "table",
                object_name: table_name.to_owned(),
            })?;

        Ok((entry, rootpage))
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
        Path::new(env!("CARGO_MANIFEST_DIR")).join("sample_clone.db")
    }

    fn superheroes_db_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("superheroes.db")
    }

    fn companies_db_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("companies.db")
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

    #[test]
    fn superheroes_root_page_is_table_interior() {
        let database = SqliteDB::open(&superheroes_db_path()).expect("superheroes db should open");

        let superheroes = database
            .schema_table
            .find_table("superheroes")
            .expect("superheroes table should exist");
        let rootpage = superheroes
            .rootpage
            .expect("superheroes should have root page");
        let (_, btree_page) = database
            .read_btree_page(rootpage)
            .expect("superheroes root page should be readable");

        assert_eq!(btree_page.kind, BTreePageKind::TableInterior);
    }

    #[test]
    fn counts_rows_in_superheroes_table() {
        let database = SqliteDB::open(&superheroes_db_path()).expect("superheroes db should open");

        let row_count = database
            .count_rows("superheroes")
            .expect("superheroes row count should parse");

        assert_eq!(row_count, 6_895);
    }

    #[test]
    fn selects_filtered_rows_from_multi_page_superheroes_table() {
        let database = SqliteDB::open(&superheroes_db_path()).expect("superheroes db should open");

        let mut rows = database
            .select_rows(
                "superheroes",
                &["id".to_owned(), "name".to_owned()],
                Some(&WhereClause::EqualsText {
                    column_name: "eye_color".to_owned(),
                    value: "Pink Eyes".to_owned(),
                }),
            )
            .expect("filtered superheroes rows should parse");
        rows.sort();

        assert_eq!(
            rows,
            vec![
                "1085|Felicity (New Earth)".to_owned(),
                "2729|Thrust (New Earth)".to_owned(),
                "297|Stealth (New Earth)".to_owned(),
                "3289|Angora Lapin (New Earth)".to_owned(),
                "3913|Matris Ater Clementia (New Earth)".to_owned(),
                "790|Tobias Whale (New Earth)".to_owned(),
            ]
        );
    }

    #[test]
    fn finds_companies_country_index() {
        let database = SqliteDB::open(&companies_db_path()).expect("companies db should open");

        let index = database
            .schema_table
            .find_index_for_column("companies", "country")
            .expect("country index should exist");

        assert_eq!(index.name, "idx_companies_country");
        assert_eq!(index.rootpage, Some(4));
    }

    #[test]
    fn companies_index_root_page_is_index_interior() {
        let database = SqliteDB::open(&companies_db_path()).expect("companies db should open");
        let index = database
            .schema_table
            .find_index_for_column("companies", "country")
            .expect("country index should exist");
        let rootpage = index.rootpage.expect("index should have root page");
        let (_, btree_page) = database
            .read_btree_page(rootpage)
            .expect("index root page should be readable");

        assert_eq!(btree_page.kind, BTreePageKind::IndexInterior);
    }

    #[test]
    fn finds_company_row_by_rowid() {
        let database = SqliteDB::open(&companies_db_path()).expect("companies db should open");
        let companies = database
            .schema_table
            .find_table("companies")
            .expect("companies table should exist");
        let rootpage = companies.rootpage.expect("companies should have root page");
        let scanner = TableScanner::new(&database);
        let mut company_name = None;

        let found = scanner
            .with_record_by_rowid("companies", rootpage, 121_311, |_, record| {
                company_name = Some(
                    record
                        .column(companies.column_index("name").unwrap())
                        .unwrap()
                        .decode_text("companies.name")
                        .unwrap()
                        .to_owned(),
                );
                Ok(())
            })
            .expect("row lookup should succeed");

        assert!(found);
        assert_eq!(company_name.as_deref(), Some("unilink s.c."));
    }

    #[test]
    fn selects_rows_from_companies_via_index_scan() {
        let database = SqliteDB::open(&companies_db_path()).expect("companies db should open");

        let mut rows = database
            .select_rows(
                "companies",
                &["id".to_owned(), "name".to_owned()],
                Some(&WhereClause::EqualsText {
                    column_name: "country".to_owned(),
                    value: "eritrea".to_owned(),
                }),
            )
            .expect("indexed companies rows should parse");
        rows.sort();

        assert_eq!(
            rows,
            vec![
                "121311|unilink s.c.".to_owned(),
                "2102438|orange asmara it solutions".to_owned(),
                "5729848|zara mining share company".to_owned(),
                "6634629|asmara rental".to_owned(),
            ]
        );
    }
}
