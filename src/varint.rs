use anyhow::{Result, bail};
use bytes::Buf;

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
    pub fn parse<B: Buf>(input: &mut B) -> Result<Self> {
        if !input.has_remaining() {
            bail!(SqliteParseError::InvalidVarint);
        }

        let mut value = 0_u64;
        for (index, byte) in input
            .chunk()
            .iter()
            .copied()
            .take(SQLITE_VARINT_MAX_BYTES)
            .enumerate()
        {
            if index == SQLITE_VARINT_MAX_BYTES - 1 {
                input.advance(SQLITE_VARINT_MAX_BYTES);
                value = (value << 8) | u64::from(byte);
                return Ok(Self {
                    value,
                    len: SQLITE_VARINT_MAX_BYTES,
                });
            }

            value = (value << 7) | u64::from(byte & SQLITE_VARINT_DATA_MASK);
            if byte & SQLITE_VARINT_CONTINUATION_BIT == 0 {
                input.advance(index + 1);
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
    use super::*;

    #[test]
    fn parses_single_byte_varint() {
        let mut input: &[u8] = &[120];
        let parsed = SqliteVarint::parse(&mut input).expect("varint should parse");

        assert_eq!(parsed.value, 120);
        assert_eq!(parsed.len, 1);
        assert_eq!(input.remaining(), 0);
    }

    #[test]
    fn parses_multi_byte_varint() {
        let mut input: &[u8] = &[129, 0, 7];
        let parsed = SqliteVarint::parse(&mut input).expect("varint should parse");

        assert_eq!(parsed.value, 128);
        assert_eq!(parsed.len, 2);
        assert_eq!(input, &[7]);
    }
}
