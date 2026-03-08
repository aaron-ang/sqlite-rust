use anyhow::{Result, bail};

use crate::error::SqliteParseError;

const SQLITE_VARINT_MAX_BYTES: usize = 9;
const SQLITE_VARINT_CONTINUATION_BIT: u8 = 0x80;
const SQLITE_VARINT_DATA_MASK: u8 = 0x7f;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SqliteVarint {
    value: u64,
    len: usize,
}

impl SqliteVarint {
    pub fn parse(input: &[u8]) -> Result<Self> {
        if input.is_empty() {
            bail!(SqliteParseError::InvalidVarint);
        }

        let mut value = 0_u64;
        for (index, byte) in input
            .iter()
            .copied()
            .take(SQLITE_VARINT_MAX_BYTES)
            .enumerate()
        {
            if index == SQLITE_VARINT_MAX_BYTES - 1 {
                value = (value << 8) | u64::from(byte);
                return Ok(Self {
                    value,
                    len: SQLITE_VARINT_MAX_BYTES,
                });
            }

            value = (value << 7) | u64::from(byte & SQLITE_VARINT_DATA_MASK);
            if byte & SQLITE_VARINT_CONTINUATION_BIT == 0 {
                return Ok(Self {
                    value,
                    len: index + 1,
                });
            }
        }

        bail!(SqliteParseError::InvalidVarint);
    }

    pub fn value(self) -> u64 {
        self.value
    }

    pub fn len(self) -> usize {
        self.len
    }
}

#[cfg(test)]
mod tests {
    use super::SqliteVarint;

    #[test]
    fn parses_single_byte_varint() {
        let parsed = SqliteVarint::parse(&[120]).expect("varint should parse");

        assert_eq!(parsed.value, 120);
        assert_eq!(parsed.len, 1);
    }

    #[test]
    fn parses_multi_byte_varint() {
        let parsed = SqliteVarint::parse(&[129, 0]).expect("varint should parse");

        assert_eq!(parsed.value, 128);
        assert_eq!(parsed.len, 2);
    }
}
