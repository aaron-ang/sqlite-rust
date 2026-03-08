use std::str;

use anyhow::{Result, bail};
use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::error::SqliteParseError;
use crate::varint::SqliteVarint;

#[derive(Debug, PartialEq)]
pub struct Record<'a> {
    columns: Vec<RecordColumn<'a>>,
}

impl<'a> Record<'a> {
    pub fn parse(payload: &'a [u8]) -> Result<Self> {
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

        let mut serial_types = Vec::new();
        let mut header_bytes = &payload[header_prefix_len..header_size];
        while !header_bytes.is_empty() {
            let serial_type = SqliteVarint::parse(&mut header_bytes)?;
            serial_types.push(SerialType::try_from(serial_type.value())?);
        }

        let mut body_cursor = header_size;
        let mut columns = Vec::with_capacity(serial_types.len());
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
            columns.push(RecordColumn { serial_type, value });
            body_cursor = value_end;
        }

        Ok(Self { columns })
    }

    pub fn columns(&self) -> &[RecordColumn<'a>] {
        &self.columns
    }

    pub fn column(&self, index: usize) -> Option<RecordColumn<'a>> {
        self.columns.get(index).copied()
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RecordColumn<'a> {
    serial_type: SerialType,
    value: &'a [u8],
}

impl<'a> RecordColumn<'a> {
    pub fn value(self) -> &'a [u8] {
        self.value
    }

    pub fn decode_text(self, column: impl Into<String>) -> Result<String> {
        let column = column.into();
        if !self.serial_type.is_text() {
            bail!(SqliteParseError::UnexpectedSerialType {
                column,
                expected: "text",
                serial_type: self.serial_type.code(),
            });
        }

        let text =
            str::from_utf8(self.value).map_err(|_| SqliteParseError::InvalidUtf8 { column })?;
        Ok(text.to_owned())
    }

    pub fn decode_nullable_text(self, column: impl Into<String>) -> Result<Option<String>> {
        if self.serial_type.is_null() {
            return Ok(None);
        }

        self.decode_text(column).map(Some)
    }

    pub fn decode_output_value(self, column: impl Into<String>) -> Result<String> {
        let column = column.into();

        if self.serial_type.is_null() {
            return Ok(String::new());
        }

        if self.serial_type.is_text() {
            return self.decode_text(column);
        }

        if self.serial_type.is_integer() {
            let value = self
                .decode_optional_integer(column)?
                .expect("non-null integer serial type should decode to a value");
            return Ok(value.to_string());
        }

        bail!(SqliteParseError::UnexpectedSerialType {
            column,
            expected: "text, integer, or null",
            serial_type: self.serial_type.code(),
        })
    }

    pub fn decode_optional_integer(self, column: impl Into<String>) -> Result<Option<i64>> {
        let column = column.into();
        match self.serial_type {
            SerialType::Fixed(FixedSerialType::Null) => Ok(None),
            serial_type if serial_type.is_integer() => Ok(Some(match serial_type {
                SerialType::Fixed(FixedSerialType::Zero) => 0,
                SerialType::Fixed(FixedSerialType::One) => 1,
                _ => decode_signed_integer(self.value),
            })),
            _ => bail!(SqliteParseError::UnexpectedSerialType {
                column,
                expected: "integer",
                serial_type: self.serial_type.code(),
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum SerialType {
    Fixed(FixedSerialType),
    Blob(usize),
    Text(usize),
}

impl SerialType {
    pub fn content_size(self) -> usize {
        match self {
            Self::Fixed(fixed) => fixed.content_size(),
            Self::Blob(len) | Self::Text(len) => len,
        }
    }

    pub fn is_text(self) -> bool {
        matches!(self, Self::Text(_))
    }

    pub fn is_integer(self) -> bool {
        matches!(self, Self::Fixed(fixed) if fixed.is_integer())
    }

    pub fn is_null(self) -> bool {
        self == Self::Fixed(FixedSerialType::Null)
    }

    pub fn code(self) -> u64 {
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

#[derive(Clone, Copy, Debug, PartialEq, TryFromPrimitive, IntoPrimitive)]
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

#[cfg(test)]
mod tests {
    use super::*;

    fn build_record_payload() -> Vec<u8> {
        let mut payload = vec![4, 0, 23, 9];
        payload.extend_from_slice(b"apple");
        payload
    }

    #[test]
    fn parses_record_columns() {
        let payload = build_record_payload();
        let record = Record::parse(&payload).expect("record should parse");

        assert_eq!(record.columns().len(), 3);
        assert_eq!(
            record.column(1).unwrap().decode_text("name").unwrap(),
            "apple"
        );
    }

    #[test]
    fn decodes_optional_integer() {
        let payload = build_record_payload();
        let record = Record::parse(&payload).expect("record should parse");

        assert_eq!(
            record
                .column(0)
                .unwrap()
                .decode_optional_integer("id")
                .unwrap(),
            None
        );
        assert_eq!(record.column(1).unwrap().serial_type, SerialType::Text(5));
    }

    #[test]
    fn decodes_integer_output_value() {
        let payload = build_record_payload();
        let record = Record::parse(&payload).expect("record should parse");

        assert_eq!(record.column(2).unwrap().decode_output_value("flag").unwrap(), "1");
    }
}
