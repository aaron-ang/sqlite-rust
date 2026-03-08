use anyhow::{Result, bail};
use num_enum::TryFromPrimitive;

use crate::error::SqliteParseError;
use crate::varint::SqliteVarint;

const LEAF_BTREE_HEADER_SIZE: usize = 8;
const INTERIOR_BTREE_HEADER_SIZE: usize = 12;
const RIGHT_MOST_POINTER_OFFSET: usize = 8;

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
    pub right_most_pointer: Option<u32>,
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

        let header = page
            .get(header_offset..header_offset + header_size)
            .ok_or(SqliteParseError::PageTooShort)?;

        let first_freeblock_offset = u16::from_be_bytes([header[1], header[2]]);
        let cell_count = u16::from_be_bytes([header[3], header[4]]);
        let cell_content_area_offset = u16::from_be_bytes([header[5], header[6]]);
        let fragmented_free_bytes = header[7];
        let right_most_pointer = kind.is_interior().then(|| {
            let bytes = header[RIGHT_MOST_POINTER_OFFSET..RIGHT_MOST_POINTER_OFFSET + 4]
                .try_into()
                .expect("Slice with length 4 required for right_most_pointer");
            u32::from_be_bytes(bytes)
        });

        let pointer_array_start = header_offset + header_size;
        let pointer_array_size = usize::from(cell_count) * 2;
        let pointer_array_end = pointer_array_start + pointer_array_size;
        let pointer_bytes = page
            .get(pointer_array_start..pointer_array_end)
            .ok_or(SqliteParseError::PageTooShort)?;

        let mut cell_pointers = Vec::with_capacity(usize::from(cell_count));
        for chunk in pointer_bytes.chunks_exact(2) {
            let cell_pointer = u16::from_be_bytes([chunk[0], chunk[1]]);
            let cell_offset = usize::from(cell_pointer);
            if cell_offset >= page.len() {
                bail!(SqliteParseError::CellPointerOutOfBounds(cell_offset));
            }
            cell_pointers.push(cell_pointer);
        }

        Ok(Self {
            kind,
            header_offset,
            first_freeblock_offset,
            cell_count,
            cell_content_area_offset,
            fragmented_free_bytes,
            right_most_pointer,
            cell_pointers,
        })
    }

    pub fn parse_page_one(page: &[u8]) -> Result<Self> {
        Self::parse(page, 100)
    }

    pub fn cells<'a>(&self, page: &'a [u8]) -> Result<Vec<BTreeCell<'a>>> {
        self.cell_pointers
            .iter()
            .map(|&cell_pointer| self.parse_cell(page, usize::from(cell_pointer)))
            .collect()
    }

    pub fn parse_cell<'a>(&self, page: &'a [u8], cell_offset: usize) -> Result<BTreeCell<'a>> {
        let cell = page
            .get(cell_offset..)
            .ok_or(SqliteParseError::CellPointerOutOfBounds(cell_offset))?;

        match self.kind {
            BTreePageKind::TableLeaf => {
                let payload_size = SqliteVarint::parse(cell)?;
                let rowid = SqliteVarint::parse(&cell[payload_size.len()..])?;
                let payload_offset = cell_offset + payload_size.len() + rowid.len();
                let payload_end = payload_offset
                    .checked_add(payload_size.value() as usize)
                    .ok_or(SqliteParseError::CellPayloadOutOfBounds {
                        offset: payload_offset,
                    })?;
                let payload = page.get(payload_offset..payload_end).ok_or(
                    SqliteParseError::CellPayloadOutOfBounds {
                        offset: payload_offset,
                    },
                )?;

                Ok(BTreeCell::TableLeaf(TableLeafCell {
                    payload_size,
                    rowid,
                    payload,
                    overflow_page: None,
                }))
            }
            BTreePageKind::TableInterior => {
                let left_child_pointer = parse_left_child_pointer(cell)?;
                let key = SqliteVarint::parse(&cell[4..])?;

                Ok(BTreeCell::TableInterior(TableInteriorCell {
                    left_child_pointer,
                    key,
                }))
            }
            BTreePageKind::IndexLeaf => {
                let payload_size = SqliteVarint::parse(cell)?;
                let payload_offset = cell_offset + payload_size.len();
                let payload_end = payload_offset
                    .checked_add(payload_size.value() as usize)
                    .ok_or(SqliteParseError::CellPayloadOutOfBounds {
                        offset: payload_offset,
                    })?;
                let payload = page.get(payload_offset..payload_end).ok_or(
                    SqliteParseError::CellPayloadOutOfBounds {
                        offset: payload_offset,
                    },
                )?;

                Ok(BTreeCell::IndexLeaf(IndexLeafCell {
                    payload_size,
                    payload,
                    overflow_page: None,
                }))
            }
            BTreePageKind::IndexInterior => {
                let left_child_pointer = parse_left_child_pointer(cell)?;
                let payload_size = SqliteVarint::parse(&cell[4..])?;
                let payload_offset = cell_offset + 4 + payload_size.len();
                let payload_end = payload_offset
                    .checked_add(payload_size.value() as usize)
                    .ok_or(SqliteParseError::CellPayloadOutOfBounds {
                        offset: payload_offset,
                    })?;
                let payload = page.get(payload_offset..payload_end).ok_or(
                    SqliteParseError::CellPayloadOutOfBounds {
                        offset: payload_offset,
                    },
                )?;

                Ok(BTreeCell::IndexInterior(IndexInteriorCell {
                    left_child_pointer,
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
    pub left_child_pointer: u32,
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
    pub left_child_pointer: u32,
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

fn parse_left_child_pointer(cell: &[u8]) -> Result<u32> {
    let child_pointer = cell
        .get(..4)
        .ok_or(SqliteParseError::CellPayloadOutOfBounds { offset: 0 })?;

    Ok(u32::from_be_bytes([
        child_pointer[0],
        child_pointer[1],
        child_pointer[2],
        child_pointer[3],
    ]))
}
