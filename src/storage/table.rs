use anyhow::{Result, bail};

use crate::db::SqliteDB;
use crate::error::SqliteParseError;
use super::page::{BTreeCell, BTreePageKind};
use super::record::Record;

pub struct TableScanner<'a> {
    db: &'a SqliteDB,
}

impl<'a> TableScanner<'a> {
    pub fn new(db: &'a SqliteDB) -> Self {
        Self { db }
    }

    pub fn count_cells(&self, table_name: &str, root_page: u32) -> Result<usize> {
        self.db.count_btree_leaf_cells(
            root_page,
            BTreePageKind::TableLeaf,
            BTreePageKind::TableInterior,
            |db, page, bytes| {
                let us = db.usable_page_size();
                let mut out = vec![];
                if let Some(r) = page.right_most_ptr {
                    out.push(r);
                }
                for cell in page.cells(bytes, us)?.into_iter().rev() {
                    let BTreeCell::TableInterior(c) = cell else {
                        unreachable!("table interior has table interior cells");
                    };
                    out.push(c.left_child_ptr);
                }
                Ok(out)
            },
            |pt| SqliteParseError::UnsupportedRootPageType {
                object_type: "table",
                object_name: table_name.to_owned(),
                page_type: pt,
            },
        )
    }

    pub fn visit_records<F>(&self, table_name: &str, root_page: u32, mut visitor: F) -> Result<()>
    where
        F: for<'record> FnMut(u64, Record<'record>) -> Result<()>,
    {
        let mut pages_to_visit = vec![root_page];
        let usable_page_size = self.db.usable_page_size();

        while let Some(page_number) = pages_to_visit.pop() {
            let (page_bytes, btree_page) = self.db.read_btree_page(page_number)?;

            match btree_page.kind {
                BTreePageKind::TableLeaf => {
                    for cell in btree_page.cells(&page_bytes, usable_page_size)? {
                        let BTreeCell::TableLeaf(cell) = cell else {
                            unreachable!("table leaf page should only contain table leaf cells");
                        };
                        let payload = self.db.read_full_payload(
                            cell.payload_size.value(),
                            cell.payload,
                            cell.overflow_page,
                        )?;
                        visitor(cell.rowid.value(), Record::parse(payload.as_ref())?)?;
                    }
                }
                BTreePageKind::TableInterior => {
                    if let Some(right_most_ptr) = btree_page.right_most_ptr {
                        pages_to_visit.push(right_most_ptr);
                    }

                    for cell in btree_page
                        .cells(&page_bytes, usable_page_size)?
                        .into_iter()
                        .rev()
                    {
                        let BTreeCell::TableInterior(cell) = cell else {
                            unreachable!(
                                "table interior page should only contain table interior cells"
                            );
                        };
                        pages_to_visit.push(cell.left_child_ptr);
                    }
                }
                _ => {
                    let page_type = page_bytes
                        .get(btree_page.header_offset)
                        .copied()
                        .unwrap_or_default();
                    bail!(SqliteParseError::UnsupportedRootPageType {
                        object_type: "table",
                        object_name: table_name.to_owned(),
                        page_type,
                    });
                }
            }
        }

        Ok(())
    }

    pub fn with_record_by_rowid<F>(
        &self,
        table_name: &str,
        root_page: u32,
        target_rowid: u64,
        mut visitor: F,
    ) -> Result<bool>
    where
        F: for<'record> FnMut(u64, Record<'record>) -> Result<()>,
    {
        let mut current_page = root_page;
        let usable_page_size = self.db.usable_page_size();

        loop {
            let (page_bytes, btree_page) = self.db.read_btree_page(current_page)?;

            match btree_page.kind {
                BTreePageKind::TableLeaf => {
                    for cell in btree_page.cells(&page_bytes, usable_page_size)? {
                        let BTreeCell::TableLeaf(cell) = cell else {
                            unreachable!("table leaf page should only contain table leaf cells");
                        };
                        if cell.rowid.value() == target_rowid {
                            let payload = self.db.read_full_payload(
                                cell.payload_size.value(),
                                cell.payload,
                                cell.overflow_page,
                            )?;
                            visitor(target_rowid, Record::parse(payload.as_ref())?)?;
                            return Ok(true);
                        }
                    }

                    return Ok(false);
                }
                BTreePageKind::TableInterior => {
                    let mut next_page = btree_page.right_most_ptr.ok_or(
                        SqliteParseError::UnsupportedRootPageType {
                            object_type: "table",
                            object_name: table_name.to_owned(),
                            page_type: page_bytes
                                .get(btree_page.header_offset)
                                .copied()
                                .unwrap_or_default(),
                        },
                    )?;

                    for cell in btree_page.cells(&page_bytes, usable_page_size)? {
                        let BTreeCell::TableInterior(cell) = cell else {
                            unreachable!(
                                "table interior page should only contain table interior cells"
                            );
                        };
                        if target_rowid <= cell.key.value() {
                            next_page = cell.left_child_ptr;
                            break;
                        }
                    }

                    current_page = next_page;
                }
                _ => {
                    let page_type = page_bytes
                        .get(btree_page.header_offset)
                        .copied()
                        .unwrap_or_default();
                    bail!(SqliteParseError::UnsupportedRootPageType {
                        object_type: "table",
                        object_name: table_name.to_owned(),
                        page_type,
                    });
                }
            }
        }
    }
}
