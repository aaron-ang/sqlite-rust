use std::str::{self, FromStr};

use anyhow::{Result, bail};
use strum::EnumString;

use crate::error::SqliteParseError;
use crate::page::{BTreeCell, BTreePage, BTreePageKind, TableLeafCell};
use crate::varint::SqliteVarint;

const SCHEMA_COLUMN_COUNT: usize = 5;
const SQLITE_INTERNAL_PREFIX: &str = "sqlite_";

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

    fn parse_from_table_leaf_cell(cell: TableLeafCell<'_>) -> Result<Self> {
        Self::parse_record(cell.payload)
    }

    fn parse_record(payload: &[u8]) -> Result<Self> {
        let columns = parse_record_columns(payload)?;

        let type_str = decode_required_text("type", columns[0].0, columns[0].1)?;
        let object_type = SchemaObjectType::from_str(&type_str)
            .map_err(|_| SqliteParseError::InvalidSchemaObjectType(type_str))?;
        let name = decode_required_text("name", columns[1].0, columns[1].1)?;
        let table_name = decode_required_text("tbl_name", columns[2].0, columns[2].1)?;
        let rootpage = decode_rootpage(columns[3].0, columns[3].1)?;
        let sql = decode_nullable_text("sql", columns[4].0, columns[4].1)?;

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

fn parse_record_columns(payload: &[u8]) -> Result<Vec<(u64, &[u8])>> {
    let header_size_varint = SqliteVarint::parse(payload)?;
    let header_size = usize::try_from(header_size_varint.value())
        .map_err(|_| SqliteParseError::InvalidRecordHeaderSize(header_size_varint.value()))?;

    if header_size < header_size_varint.len() || header_size > payload.len() {
        bail!(SqliteParseError::InvalidRecordHeaderSize(
            header_size as u64
        ));
    }

    let mut serial_types = Vec::with_capacity(SCHEMA_COLUMN_COUNT);
    let mut header_cursor = header_size_varint.len();
    while header_cursor < header_size {
        let serial_type = SqliteVarint::parse(&payload[header_cursor..header_size])?;
        serial_types.push(serial_type.value());
        header_cursor += serial_type.len();
    }

    if header_cursor != header_size || serial_types.len() != SCHEMA_COLUMN_COUNT {
        bail!(SqliteParseError::InvalidRecordHeaderSize(
            header_size as u64
        ));
    }

    let mut body_cursor = header_size;
    let mut columns = Vec::with_capacity(SCHEMA_COLUMN_COUNT);
    for serial_type in serial_types {
        let value_size = serial_type_content_size(serial_type)?;
        let value_end = body_cursor.checked_add(value_size).ok_or(
            SqliteParseError::CellPayloadOutOfBounds {
                offset: body_cursor,
            },
        )?;
        let value = payload.get(body_cursor..value_end).ok_or(
            SqliteParseError::CellPayloadOutOfBounds {
                offset: body_cursor,
            },
        )?;
        columns.push((serial_type, value));
        body_cursor = value_end;
    }

    Ok(columns)
}

fn serial_type_content_size(serial_type: u64) -> Result<usize> {
    let size = match serial_type {
        0 => 0,
        1 => 1,
        2 => 2,
        3 => 3,
        4 => 4,
        5 => 6,
        6 => 8,
        7 => 8,
        8 => 0,
        9 => 0,
        10 | 11 => bail!(SqliteParseError::UnsupportedSerialType(serial_type)),
        n if n >= 12 => ((n - 12) / 2) as usize,
        _ => bail!(SqliteParseError::UnsupportedSerialType(serial_type)),
    };

    Ok(size)
}

fn decode_required_text(column: &'static str, serial_type: u64, value: &[u8]) -> Result<String> {
    if serial_type < 13 || serial_type % 2 == 0 {
        bail!(SqliteParseError::UnexpectedTextSerialType {
            column,
            serial_type,
        });
    }

    let text = str::from_utf8(value).map_err(|_| SqliteParseError::InvalidUtf8 { column })?;
    Ok(text.to_owned())
}

fn decode_nullable_text(
    column: &'static str,
    serial_type: u64,
    value: &[u8],
) -> Result<Option<String>> {
    if serial_type == 0 {
        return Ok(None);
    }

    Ok(Some(decode_required_text(column, serial_type, value)?))
}

fn decode_rootpage(serial_type: u64, value: &[u8]) -> Result<Option<u32>> {
    let integer = match serial_type {
        0 => return Ok(None),
        1..=6 => decode_signed_integer(value),
        8 => 0,
        9 => 1,
        _ => {
            bail!(SqliteParseError::UnexpectedIntegerSerialType {
                column: "rootpage",
                serial_type,
            });
        }
    };

    let rootpage =
        u32::try_from(integer).map_err(|_| SqliteParseError::InvalidRootPage(integer))?;
    Ok(Some(rootpage))
}

fn decode_signed_integer(bytes: &[u8]) -> i64 {
    let sign_byte = if bytes.first().is_some_and(|byte| byte & 0x80 != 0) {
        0xff
    } else {
        0x00
    };

    let mut widened = [sign_byte; 8];
    let start = widened.len() - bytes.len();
    widened[start..].copy_from_slice(bytes);
    i64::from_be_bytes(widened)
}
