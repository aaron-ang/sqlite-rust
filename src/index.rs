use anyhow::{Result, bail};

use crate::db::SqliteDB;
use crate::error::SqliteParseError;
use crate::page::{BTreeCell, BTreePageKind};
use crate::record::Record;

#[derive(Clone, Debug, PartialEq, PartialOrd)]
struct IndexKey {
    indexed_value: Option<String>,
    rowid: u64,
}

pub struct IndexScanner<'a> {
    db: &'a SqliteDB,
}

impl<'a> IndexScanner<'a> {
    pub fn new(db: &'a SqliteDB) -> Self {
        Self { db }
    }

    pub fn find_matching_rowids(
        &self,
        index_name: &str,
        root_page: u32,
        target: &str,
    ) -> Result<Vec<u64>> {
        let lower_bound = IndexKey {
            indexed_value: Some(target.to_owned()),
            rowid: 0,
        };
        let upper_bound = IndexKey {
            indexed_value: Some(target.to_owned()),
            rowid: u64::MAX,
        };
        let mut rowids = Vec::new();
        let mut pages_to_visit = vec![root_page];

        while let Some(page_number) = pages_to_visit.pop() {
            let (page_bytes, btree_page) = self.db.read_btree_page(page_number)?;

            match btree_page.kind {
                BTreePageKind::IndexLeaf => {
                    for cell in btree_page.cells(&page_bytes)? {
                        let BTreeCell::IndexLeaf(cell) = cell else {
                            unreachable!("index leaf page should only contain index leaf cells");
                        };
                        let index_key = parse_index_key(cell.payload, index_name)?;
                        if index_key.indexed_value.as_deref() == Some(target) {
                            rowids.push(index_key.rowid);
                        }
                    }
                }
                BTreePageKind::IndexInterior => {
                    let cells = btree_page.cells(&page_bytes)?;
                    let mut child_ranges = Vec::with_capacity(cells.len() + 1);
                    let mut lower_exclusive: Option<IndexKey> = None;

                    for cell in cells {
                        let BTreeCell::IndexInterior(cell) = cell else {
                            unreachable!(
                                "index interior page should only contain index interior cells"
                            );
                        };
                        let key = parse_index_key(cell.payload, index_name)?;
                        if key.indexed_value.as_deref() == Some(target) {
                            rowids.push(key.rowid);
                        }
                        child_ranges.push((
                            cell.left_child_ptr,
                            lower_exclusive.clone(),
                            Some(key.clone()),
                        ));
                        lower_exclusive = Some(key);
                    }

                    if let Some(right_most_ptr) = btree_page.right_most_ptr {
                        child_ranges.push((right_most_ptr, lower_exclusive, None));
                    }

                    for (child_page, lower, upper) in child_ranges.into_iter().rev() {
                        if range_overlaps(
                            lower.as_ref(),
                            upper.as_ref(),
                            &lower_bound,
                            &upper_bound,
                        ) {
                            pages_to_visit.push(child_page);
                        }
                    }
                }
                _ => {
                    let page_type = page_bytes
                        .get(btree_page.header_offset)
                        .copied()
                        .unwrap_or_default();
                    bail!(SqliteParseError::UnsupportedRootPageType {
                        object_type: "index",
                        object_name: index_name.to_owned(),
                        page_type,
                    });
                }
            }
        }

        Ok(rowids)
    }
}

fn parse_index_key(payload: &[u8], index_name: &str) -> Result<IndexKey> {
    let record = Record::parse(payload)?;
    let columns = record.columns();
    if columns.len() < 2 {
        bail!(SqliteParseError::MalformedIndexEntry {
            index_name: index_name.to_owned(),
        });
    }

    let indexed_value = columns[0].decode_nullable_text(format!("{index_name}.value"))?;
    let rowid = columns
        .last()
        .expect("index record must have at least 2 columns")
        .decode_optional_integer(format!("{index_name}.rowid"))?
        .ok_or_else(|| SqliteParseError::MalformedIndexEntry {
            index_name: index_name.to_owned(),
        })?;

    let rowid = u64::try_from(rowid).map_err(|_| SqliteParseError::MalformedIndexEntry {
        index_name: index_name.to_owned(),
    })?;

    Ok(IndexKey {
        indexed_value,
        rowid,
    })
}

fn range_overlaps(
    lower_exclusive: Option<&IndexKey>,
    upper_exclusive: Option<&IndexKey>,
    lower_bound: &IndexKey,
    upper_bound: &IndexKey,
) -> bool {
    let upper_overlaps = upper_exclusive.is_none_or(|upper| upper > lower_bound);
    let lower_overlaps = lower_exclusive.is_none_or(|lower| upper_bound > lower);
    upper_overlaps && lower_overlaps
}
