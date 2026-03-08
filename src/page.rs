use anyhow::{Result, bail};
use bytes::Buf;
use num_enum::TryFromPrimitive;

use crate::error::SqliteParseError;
use crate::varint::SqliteVarint;

const PAGE_ONE_HEADER_OFFSET: usize = 100;
const LEAF_BTREE_HEADER_SIZE: usize = 8;
const INTERIOR_BTREE_HEADER_SIZE: usize = 12;
const U16_BYTE_LEN: usize = size_of::<u16>();
const U32_BYTE_LEN: usize = size_of::<u32>();

#[derive(Debug, PartialEq)]
pub enum Page {
    BTree(BTreePage),
    Freelist(FreelistPageKind),
    PayloadOverflow,
    PointerMap,
    LockByte,
}

#[derive(Debug, PartialEq)]
pub struct BTreePage {
    pub kind: BTreePageKind,
    pub header_offset: usize,
    pub first_freeblock_offset: u16,
    pub cell_count: u16,
    pub cell_content_area_offset: u16,
    pub fragmented_free_bytes: u8,
    pub right_most_ptr: Option<u32>,
    pub cell_pointers: Vec<u16>,
}

impl BTreePage {
    pub fn parse(page: &[u8], header_offset: usize) -> Result<Self> {
        let page_type = *page
            .get(header_offset)
            .ok_or(SqliteParseError::PageTooShort)?;
        let kind = BTreePageKind::try_from(page_type)
            .map_err(|_| SqliteParseError::UnsupportedPageType(page_type))?;
        let header_size = kind.header_size();

        let mut header = page
            .get(header_offset..header_offset + header_size)
            .ok_or(SqliteParseError::PageTooShort)?;

        let _page_type = header.get_u8();
        let first_freeblock_offset = header.get_u16();
        let cell_count = header.get_u16();
        let cell_content_area_offset = header.get_u16();
        let fragmented_free_bytes = header.get_u8();
        let right_most_ptr = kind.is_interior().then(|| header.get_u32());

        let ptr_array_start = header_offset + header_size;
        let ptr_array_size = usize::from(cell_count) * U16_BYTE_LEN;
        let ptr_array_end = ptr_array_start + ptr_array_size;
        let mut ptr_array = page
            .get(ptr_array_start..ptr_array_end)
            .ok_or(SqliteParseError::PageTooShort)?;

        let mut cell_pointers = Vec::with_capacity(usize::from(cell_count));
        while ptr_array.remaining() >= U16_BYTE_LEN {
            let cell_ptr = ptr_array.get_u16();
            let cell_offset = usize::from(cell_ptr);
            if cell_offset >= page.len() {
                bail!(SqliteParseError::CellPointerOutOfBounds(cell_offset));
            }
            cell_pointers.push(cell_ptr);
        }

        Ok(Self {
            kind,
            header_offset,
            first_freeblock_offset,
            cell_count,
            cell_content_area_offset,
            fragmented_free_bytes,
            right_most_ptr,
            cell_pointers,
        })
    }

    pub fn parse_page_one(page: &[u8]) -> Result<Self> {
        Self::parse(page, PAGE_ONE_HEADER_OFFSET)
    }

    pub fn cells<'a>(&self, page: &'a [u8]) -> Result<Vec<BTreeCell<'a>>> {
        self.cell_pointers
            .iter()
            .map(|&cell_ptr| self.parse_cell(page, usize::from(cell_ptr)))
            .collect()
    }

    pub fn parse_cell<'a>(&self, page: &'a [u8], cell_offset: usize) -> Result<BTreeCell<'a>> {
        let cell = page
            .get(cell_offset..)
            .ok_or(SqliteParseError::CellPointerOutOfBounds(cell_offset))?;
        let mut cursor = CellCursor::new(cell, cell_offset);

        match self.kind {
            BTreePageKind::TableLeaf => {
                let payload_size = cursor.read_varint()?;
                let rowid = cursor.read_varint()?;
                let payload = cursor.remaining_payload(payload_size.value() as usize)?;

                Ok(BTreeCell::TableLeaf(TableLeafCell {
                    payload_size,
                    rowid,
                    payload,
                    overflow_page: None,
                }))
            }
            BTreePageKind::TableInterior => {
                let left_child_ptr = cursor.read_u32_be()?;
                let key = cursor.read_varint()?;

                Ok(BTreeCell::TableInterior(TableInteriorCell {
                    left_child_ptr,
                    key,
                }))
            }
            BTreePageKind::IndexLeaf => {
                let payload_size = cursor.read_varint()?;
                let payload = cursor.remaining_payload(payload_size.value() as usize)?;

                Ok(BTreeCell::IndexLeaf(IndexLeafCell {
                    payload_size,
                    payload,
                    overflow_page: None,
                }))
            }
            BTreePageKind::IndexInterior => {
                let left_child_ptr = cursor.read_u32_be()?;
                let payload_size = cursor.read_varint()?;
                let payload = cursor.remaining_payload(payload_size.value() as usize)?;

                Ok(BTreeCell::IndexInterior(IndexInteriorCell {
                    left_child_ptr,
                    payload_size,
                    payload,
                    overflow_page: None,
                }))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum BTreeCell<'a> {
    TableLeaf(TableLeafCell<'a>),
    TableInterior(TableInteriorCell),
    IndexLeaf(IndexLeafCell<'a>),
    IndexInterior(IndexInteriorCell<'a>),
}

#[derive(Debug, PartialEq)]
pub struct TableLeafCell<'a> {
    pub payload_size: SqliteVarint,
    pub rowid: SqliteVarint,
    pub payload: &'a [u8],
    pub overflow_page: Option<u32>,
}

#[derive(Debug, PartialEq)]
pub struct TableInteriorCell {
    pub left_child_ptr: u32,
    pub key: SqliteVarint,
}

#[derive(Debug, PartialEq)]
pub struct IndexLeafCell<'a> {
    pub payload_size: SqliteVarint,
    pub payload: &'a [u8],
    pub overflow_page: Option<u32>,
}

#[derive(Debug, PartialEq)]
pub struct IndexInteriorCell<'a> {
    pub left_child_ptr: u32,
    pub payload_size: SqliteVarint,
    pub payload: &'a [u8],
    pub overflow_page: Option<u32>,
}

#[derive(Clone, Copy, Debug, PartialEq, TryFromPrimitive)]
#[repr(u8)]
pub enum BTreePageKind {
    IndexInterior = 2,
    TableInterior = 5,
    IndexLeaf = 10,
    TableLeaf = 13,
}

impl BTreePageKind {
    pub fn is_interior(self) -> bool {
        matches!(self, Self::TableInterior | Self::IndexInterior)
    }

    pub fn header_size(self) -> usize {
        if self.is_interior() {
            INTERIOR_BTREE_HEADER_SIZE
        } else {
            LEAF_BTREE_HEADER_SIZE
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FreelistPageKind {
    Trunk,
    Leaf,
}

#[derive(Debug)]
struct CellCursor<'a> {
    bytes: &'a [u8],
    base_offset: usize,
    offset: usize,
}

impl<'a> CellCursor<'a> {
    fn new(bytes: &'a [u8], base_offset: usize) -> Self {
        Self {
            bytes,
            base_offset,
            offset: 0,
        }
    }

    fn read_u32_be(&mut self) -> Result<u32> {
        if self.bytes.remaining() < U32_BYTE_LEN {
            bail!(SqliteParseError::CellPayloadOutOfBounds {
                offset: self.absolute_offset(),
            });
        }

        let value = self.bytes.get_u32();
        self.offset += U32_BYTE_LEN;
        Ok(value)
    }

    fn read_varint(&mut self) -> Result<SqliteVarint> {
        let varint = SqliteVarint::parse(&mut self.bytes)?;
        self.offset += varint.len();
        Ok(varint)
    }

    fn remaining_payload(&mut self, len: usize) -> Result<&'a [u8]> {
        let payload_end =
            self.offset
                .checked_add(len)
                .ok_or(SqliteParseError::CellPayloadOutOfBounds {
                    offset: self.absolute_offset(),
                })?;
        let payload = self
            .bytes
            .get(..len)
            .ok_or(SqliteParseError::CellPayloadOutOfBounds {
                offset: self.absolute_offset(),
            })?;
        self.bytes.advance(len);
        self.offset = payload_end;
        Ok(payload)
    }

    fn absolute_offset(&self) -> usize {
        self.base_offset + self.offset
    }
}
