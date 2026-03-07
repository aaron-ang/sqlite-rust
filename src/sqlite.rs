use std::fs::File;
use std::io::Read;
use std::path::Path;

use anyhow::{Context, Result};

use crate::error::SqliteParseError;

const SQLITE_HEADER_LEN: usize = 100;
const SQLITE_MAGIC_HEADER: &[u8; 16] = b"SQLite format 3\0";
const TABLE_LEAF_PAGE_TYPE: u8 = 0x0d;

#[derive(Debug)]
pub struct SqliteDatabase {
    header: DatabaseHeader,
    schema_page_header: BTreePageHeader,
}

impl SqliteDatabase {
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

        let schema_page_header = BTreePageHeader::parse_page_one(&page_one)?;

        Ok(Self {
            header,
            schema_page_header,
        })
    }

    pub fn db_info(&self) -> DbInfo {
        DbInfo {
            page_size: self.header.page_size,
            table_count: self.schema_page_header.cell_count,
        }
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
            return Err(SqliteParseError::InvalidFileHeader.into());
        }

        let raw_page_size = u16::from_be_bytes([bytes[16], bytes[17]]);
        let page_size = match raw_page_size {
            1 => 65_536,
            512..=32_768 if raw_page_size.is_power_of_two() => raw_page_size as u32,
            other => return Err(SqliteParseError::InvalidPageSize(other).into()),
        };

        Ok(Self { page_size })
    }
}

#[derive(Debug)]
struct BTreePageHeader {
    cell_count: u16,
}

impl BTreePageHeader {
    fn parse_page_one(page: &[u8]) -> Result<Self> {
        if page.len() < SQLITE_HEADER_LEN + 8 {
            return Err(SqliteParseError::PageTooShort.into());
        }

        let page_type = page[SQLITE_HEADER_LEN];
        if page_type != TABLE_LEAF_PAGE_TYPE {
            return Err(SqliteParseError::UnsupportedPageType(page_type).into());
        }

        let cell_count_offset = SQLITE_HEADER_LEN + 3;
        let cell_count = u16::from_be_bytes([page[cell_count_offset], page[cell_count_offset + 1]]);

        Ok(Self { cell_count })
    }
}

#[cfg(test)]
mod tests {
    use super::{BTreePageHeader, DatabaseHeader, SQLITE_HEADER_LEN, SQLITE_MAGIC_HEADER};

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
        header[16..18].copy_from_slice(&1_u16.to_be_bytes());

        let parsed = DatabaseHeader::parse(&header).expect("header should parse");

        assert_eq!(parsed.page_size, 65_536);
    }

    #[test]
    fn parses_page_one_cell_count() {
        let mut page = vec![0_u8; SQLITE_HEADER_LEN + 8];
        page[SQLITE_HEADER_LEN] = 0x0d;
        page[SQLITE_HEADER_LEN + 3..SQLITE_HEADER_LEN + 5].copy_from_slice(&3_u16.to_be_bytes());

        let parsed = BTreePageHeader::parse_page_one(&page).expect("page header should parse");

        assert_eq!(parsed.cell_count, 3);
    }
}
