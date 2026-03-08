use anyhow::{Result, bail};

use crate::db::SqliteDB;
use crate::error::SqliteParseError;
use crate::page::{BTreeCell, BTreePageKind};
use crate::record::Record;

#[derive(Clone, Debug, PartialEq)]
pub struct TableRow {
    pub rowid: u64,
    pub payload: Vec<u8>,
}

pub struct TableScanner<'a> {
    db: &'a SqliteDB,
}

impl<'a> TableScanner<'a> {
    pub fn new(db: &'a SqliteDB) -> Self {
        Self { db }
    }

    pub fn visit_records<F>(&self, table_name: &str, root_page: u32, mut visitor: F) -> Result<()>
    where
        F: for<'record> FnMut(u64, Record<'record>) -> Result<()>,
    {
        let mut pages_to_visit = vec![root_page];

        while let Some(page_number) = pages_to_visit.pop() {
            let (page_bytes, btree_page) = self.db.read_btree_page(page_number)?;

            match btree_page.kind {
                BTreePageKind::TableLeaf => {
                    for cell in btree_page.cells(&page_bytes)? {
                        let BTreeCell::TableLeaf(cell) = cell else {
                            unreachable!("table leaf page should only contain table leaf cells");
                        };
                        visitor(cell.rowid.value(), Record::parse(cell.payload)?)?;
                    }
                }
                BTreePageKind::TableInterior => {
                    if let Some(right_most_ptr) = btree_page.right_most_ptr {
                        pages_to_visit.push(right_most_ptr);
                    }

                    for cell in btree_page.cells(&page_bytes)?.into_iter().rev() {
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

    pub fn find_record_by_rowid(
        &self,
        table_name: &str,
        root_page: u32,
        target_rowid: u64,
    ) -> Result<Option<TableRow>> {
        let mut current_page = root_page;

        loop {
            let (page_bytes, btree_page) = self.db.read_btree_page(current_page)?;

            match btree_page.kind {
                BTreePageKind::TableLeaf => {
                    for cell in btree_page.cells(&page_bytes)? {
                        let BTreeCell::TableLeaf(cell) = cell else {
                            unreachable!("table leaf page should only contain table leaf cells");
                        };
                        if cell.rowid.value() == target_rowid {
                            return Ok(Some(TableRow {
                                rowid: target_rowid,
                                payload: cell.payload.to_vec(),
                            }));
                        }
                    }

                    return Ok(None);
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

                    for cell in btree_page.cells(&page_bytes)? {
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
