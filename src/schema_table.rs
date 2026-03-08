use std::str::{self, FromStr};

use anyhow::{Result, bail};
use num_enum::{IntoPrimitive, TryFromPrimitive};
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SerialType {
    Fixed(FixedSerialType),
    Blob(usize),
    Text(usize),
}

impl SerialType {
    fn content_size(self) -> usize {
        match self {
            Self::Fixed(fixed) => fixed.content_size(),
            Self::Blob(len) | Self::Text(len) => len,
        }
    }

    fn is_text(self) -> bool {
        matches!(self, Self::Text(_))
    }

    fn is_integer(self) -> bool {
        matches!(self, Self::Fixed(fixed) if fixed.is_integer())
    }

    fn code(self) -> u64 {
        match self {
            Self::Fixed(fixed) => fixed.into(),
            Self::Blob(len) => (len as u64 * 2) + 12,
            Self::Text(len) => (len as u64 * 2) + 13,
        }
    }
}

impl TryFrom<u64> for SerialType {
    type Error = anyhow::Error;

    fn try_from(value: u64) -> Result<Self> {
        match value {
            0..=9 => Ok(Self::Fixed(
                FixedSerialType::try_from(value)
                    .map_err(|_| SqliteParseError::UnsupportedSerialType(value))?,
            )),
            10 | 11 => bail!(SqliteParseError::UnsupportedSerialType(value)),
            n if n >= 12 && n % 2 == 0 => Ok(Self::Blob(((n - 12) / 2) as usize)),
            n if n >= 13 && n % 2 == 1 => Ok(Self::Text(((n - 13) / 2) as usize)),
            _ => bail!(SqliteParseError::UnsupportedSerialType(value)),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, TryFromPrimitive, IntoPrimitive)]
#[repr(u64)]
enum FixedSerialType {
    Null = 0,
    Int8 = 1,
    Int16 = 2,
    Int24 = 3,
    Int32 = 4,
    Int48 = 5,
    Int64 = 6,
    Float64 = 7,
    Zero = 8,
    One = 9,
}

impl FixedSerialType {
    fn content_size(self) -> usize {
        match self {
            Self::Null | Self::Zero | Self::One => 0,
            Self::Int8 => 1,
            Self::Int16 => 2,
            Self::Int24 => 3,
            Self::Int32 => 4,
            Self::Int48 => 6,
            Self::Int64 | Self::Float64 => 8,
        }
    }

    fn is_integer(self) -> bool {
        matches!(
            self,
            Self::Int8
                | Self::Int16
                | Self::Int24
                | Self::Int32
                | Self::Int48
                | Self::Int64
                | Self::Zero
                | Self::One
        )
    }
}

fn parse_record_columns(payload: &[u8]) -> Result<Vec<(SerialType, &[u8])>> {
    let mut record_cursor = payload;
    let header_size_varint = SqliteVarint::parse(&mut record_cursor)?;
    let header_size = usize::try_from(header_size_varint.value())
        .map_err(|_| SqliteParseError::InvalidRecordHeaderSize(header_size_varint.value()))?;
    let header_prefix_len = payload.len() - record_cursor.len();

    if header_size < header_prefix_len || header_size > payload.len() {
        bail!(SqliteParseError::InvalidRecordHeaderSize(
            header_size as u64
        ));
    }

    let mut serial_types = Vec::with_capacity(SCHEMA_COLUMN_COUNT);
    let mut header_bytes = &payload[header_prefix_len..header_size];
    while !header_bytes.is_empty() {
        let serial_type = SqliteVarint::parse(&mut header_bytes)?;
        serial_types.push(SerialType::try_from(serial_type.value())?);
    }

    if serial_types.len() != SCHEMA_COLUMN_COUNT {
        bail!(SqliteParseError::InvalidRecordHeaderSize(
            header_size as u64
        ));
    }

    let mut body_cursor = header_size;
    let mut columns = Vec::with_capacity(SCHEMA_COLUMN_COUNT);
    for serial_type in serial_types {
        let value_size = serial_type.content_size();
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

fn decode_required_text(
    column: &'static str,
    serial_type: SerialType,
    value: &[u8],
) -> Result<String> {
    if !serial_type.is_text() {
        bail!(SqliteParseError::UnexpectedTextSerialType {
            column,
            serial_type: serial_type.code(),
        });
    }

    let text = str::from_utf8(value).map_err(|_| SqliteParseError::InvalidUtf8 { column })?;
    Ok(text.to_owned())
}

fn decode_nullable_text(
    column: &'static str,
    serial_type: SerialType,
    value: &[u8],
) -> Result<Option<String>> {
    if serial_type == SerialType::Fixed(FixedSerialType::Null) {
        return Ok(None);
    }

    Ok(Some(decode_required_text(column, serial_type, value)?))
}

fn decode_rootpage(serial_type: SerialType, value: &[u8]) -> Result<Option<u32>> {
    let integer = match serial_type {
        SerialType::Fixed(FixedSerialType::Null) => return Ok(None),
        serial_type if serial_type.is_integer() => match serial_type {
            SerialType::Fixed(FixedSerialType::Zero) => 0,
            SerialType::Fixed(FixedSerialType::One) => 1,
            _ => decode_signed_integer(value),
        },
        _ => {
            bail!(SqliteParseError::UnexpectedIntegerSerialType {
                column: "rootpage",
                serial_type: serial_type.code(),
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
