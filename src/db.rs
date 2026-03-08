use std::fs::File;
use std::io::Read;
use std::path::Path;

use anyhow::{Context, Result, bail};

use crate::error::SqliteParseError;
use crate::page::{BTreePage, BTreePageKind};
use crate::schema_table::SchemaTable;

const SQLITE_HEADER_LEN: usize = 100;
const SQLITE_MAGIC_HEADER: &[u8; 16] = b"SQLite format 3\0";
const SQLITE_MAX_PAGE_SIZE: u32 = 65_536;
const SQLITE_MAX_PAGE_SIZE_SENTINEL: u16 = 1;

#[derive(Debug)]
pub struct SqliteDB {
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

    pub fn schema_table(&self) -> &SchemaTable {
        &self.schema_table
    }

    pub fn table_names(&self) -> Result<Vec<String>> {
        Ok(self
            .schema_table
            .user_table_names()
            .into_iter()
            .map(str::to_owned)
            .collect())
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
}
