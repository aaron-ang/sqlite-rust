use std::cmp::Ordering;

use anyhow::{Result, bail};

use crate::db::SqliteDB;
use crate::error::SqliteParseError;
use crate::page::{BTreeCell, BTreePageKind};
use crate::record::Record;

#[derive(Clone, Copy, Debug)]
struct IndexKey<'a> {
    indexed_value: Option<&'a str>,
    rowid: u64,
}

impl<'a> IndexKey<'a> {
    fn cmp_index_value(&self, target: &str) -> Ordering {
        match self.indexed_value {
            Some(value) => value.cmp(target),
            None => Ordering::Less,
        }
    }

    fn cmp_lower_probe(&self, target: &str) -> Ordering {
        self.cmp_index_value(target)
            .then_with(|| self.rowid.cmp(&u64::MAX))
    }

    fn cmp_upper_probe(&self, target: &str) -> Ordering {
        self.cmp_index_value(target)
            .then_with(|| self.rowid.cmp(&0))
    }
}

#[derive(Debug)]
struct ChildRange<'a> {
    lower_exclusive: Option<IndexKey<'a>>,
    upper_exclusive: Option<IndexKey<'a>>,
}

impl<'a> ChildRange<'a> {
    fn between(lower_exclusive: Option<IndexKey<'a>>, upper_exclusive: IndexKey<'a>) -> Self {
        Self {
            lower_exclusive,
            upper_exclusive: Some(upper_exclusive),
        }
    }

    fn right_of(lower_exclusive: Option<IndexKey<'a>>) -> Self {
        Self {
            lower_exclusive,
            upper_exclusive: None,
        }
    }

    fn contains(&self, target: &str) -> bool {
        let upper_overlaps = self
            .upper_exclusive
            .is_none_or(|upper| upper.cmp_upper_probe(target) == Ordering::Greater);
        let lower_overlaps = self
            .lower_exclusive
            .is_none_or(|lower| lower.cmp_lower_probe(target) == Ordering::Less);
        upper_overlaps && lower_overlaps
    }
}

pub struct IndexScanner<'a> {
    db: &'a SqliteDB,
}

impl<'a> IndexScanner<'a> {
    pub fn new(db: &'a SqliteDB) -> Self {
        Self { db }
    }

    pub fn visit_matching_rowids<F>(
        &self,
        index_name: &str,
        root_page: u32,
        target: &str,
        mut visitor: F,
    ) -> Result<()>
    where
        F: FnMut(u64) -> Result<()>,
    {
        let mut pages_to_visit = vec![root_page];

        while let Some(page_number) = pages_to_visit.pop() {
            let (page_bytes, btree_page) = self.db.read_btree_page(page_number)?;

            match btree_page.kind {
                BTreePageKind::IndexLeaf => {
                    btree_page
                        .cells(&page_bytes)?
                        .into_iter()
                        .try_for_each(|cell| {
                            let BTreeCell::IndexLeaf(cell) = cell else {
                                unreachable!(
                                    "index leaf page should only contain index leaf cells"
                                );
                            };
                            let key = parse_index_key(cell.payload, index_name)?;
                            if key.indexed_value == Some(target) {
                                visitor(key.rowid)?;
                            }
                            Ok::<(), anyhow::Error>(())
                        })?;
                }
                BTreePageKind::IndexInterior => {
                    let mut lower_exclusive: Option<IndexKey> = None;

                    btree_page
                        .cells(&page_bytes)?
                        .into_iter()
                        .try_for_each(|cell| {
                            let BTreeCell::IndexInterior(cell) = cell else {
                                unreachable!(
                                    "index interior page should only contain index interior cells"
                                );
                            };
                            let key = parse_index_key(cell.payload, index_name)?;
                            if key.indexed_value == Some(target) {
                                visitor(key.rowid)?;
                            }
                            if ChildRange::between(lower_exclusive, key).contains(target) {
                                pages_to_visit.push(cell.left_child_ptr);
                            }
                            lower_exclusive = Some(key);
                            Ok::<(), anyhow::Error>(())
                        })?;

                    if let Some(right_most_ptr) = btree_page.right_most_ptr
                        && ChildRange::right_of(lower_exclusive).contains(target)
                    {
                        pages_to_visit.push(right_most_ptr);
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

        Ok(())
    }
}

fn parse_index_key<'a>(payload: &'a [u8], index_name: &str) -> Result<IndexKey<'a>> {
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

    Ok(IndexKey {
        indexed_value,
        rowid: u64::try_from(rowid).map_err(|_| SqliteParseError::MalformedIndexEntry {
            index_name: index_name.to_owned(),
        })?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn child_range_contains_target_inside_bounds() {
        let range = ChildRange::between(
            Some(IndexKey {
                indexed_value: Some("alpha"),
                rowid: 10,
            }),
            IndexKey {
                indexed_value: Some("gamma"),
                rowid: 5,
            },
        );

        assert!(range.contains("beta"));
    }

    #[test]
    fn child_range_includes_exact_lower_value_for_equal_probe() {
        let range = ChildRange::right_of(Some(IndexKey {
            indexed_value: Some("eritrea"),
            rowid: 17,
        }));

        assert!(range.contains("eritrea"));
    }

    #[test]
    fn child_range_includes_exact_upper_value_for_equal_probe() {
        let range = ChildRange::between(
            None,
            IndexKey {
                indexed_value: Some("eritrea"),
                rowid: 17,
            },
        );

        assert!(range.contains("eritrea"));
    }

    #[test]
    fn child_range_treats_none_bounds_as_unbounded() {
        let range = ChildRange::right_of(None);

        assert!(range.contains("eritrea"));
    }

    #[test]
    fn null_indexed_values_sort_before_text() {
        let key = IndexKey {
            indexed_value: None,
            rowid: 1,
        };

        assert_eq!(key.cmp_index_value("eritrea"), Ordering::Less);
    }

    #[test]
    fn eritrea_boundary_child_range_still_contains_target() {
        let range = ChildRange::between(
            Some(IndexKey {
                indexed_value: Some("egypt"),
                rowid: 99,
            }),
            IndexKey {
                indexed_value: Some("ethiopia"),
                rowid: 1,
            },
        );

        assert!(range.contains("eritrea"));
    }
}
