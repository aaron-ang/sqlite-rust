use std::borrow::Cow;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};

mod page_cache;
mod range_check;

use self::page_cache::PageCache;
use self::range_check::{record_satisfies_lower, record_satisfies_upper};
use crate::error::SqliteParseError;
use crate::query::{
    Conjunction, Disjunction, OrderByTerm, QueryValue, SortDirection, WhereOperator,
};
use crate::storage::{
    index::IndexScanner,
    page::{BTreeCell, BTreePage, BTreePageKind},
    record::{Record, RecordValue},
    schema::{SchemaTable, SchemaTableEntry},
    table::TableScanner,
};

const SQLITE_HEADER_LEN: usize = 100;
const SQLITE_MAGIC_HEADER: &[u8; 16] = b"SQLite format 3\0";
const SQLITE_MAX_PAGE_SIZE: u32 = 65_536;
const SQLITE_MAX_PAGE_SIZE_SENTINEL: u16 = 1;

#[derive(Clone, Copy, Debug)]
enum ResolvedColumn<'a> {
    RowIdAlias,
    RecordColumn { column_name: &'a str, index: usize },
}

#[derive(Clone, Copy, Debug)]
struct ResolvedPredicate<'a> {
    column: ResolvedColumn<'a>,
    op: WhereOperator,
    value: &'a QueryValue,
    second_value: Option<&'a QueryValue>,
}

#[derive(Clone, Copy, Debug)]
struct ResolvedOrderBy<'a> {
    column: ResolvedColumn<'a>,
    direction: SortDirection,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum SortValue {
    Null,
    Integer(i64),
    Text(String),
}

#[derive(Debug)]
struct MaterializedRow {
    rowid: u64,
    output: String,
    sort_keys: Vec<SortValue>,
}

#[derive(Clone, Copy, Debug)]
struct IndexMatch<'schema, 'q> {
    entry: &'schema SchemaTableEntry,
    prefix_len: usize,
    satisfies_order: bool,
    range: Option<IndexRange<'q>>,
}

#[derive(Clone, Copy, Debug)]
struct IndexBound<'q> {
    value: &'q QueryValue,
    inclusive: bool,
}

#[derive(Clone, Copy, Debug)]
struct IndexRange<'q> {
    lower: Option<IndexBound<'q>>,
    upper: Option<IndexBound<'q>>,
}

impl<'a> ResolvedColumn<'a> {
    fn resolve(entry: &SchemaTableEntry, column_name: &'a str) -> Result<Self> {
        let rowid_alias = entry.rowid_alias_column_name()?;
        if rowid_alias.is_some_and(|alias| alias.eq_ignore_ascii_case(column_name)) {
            return Ok(Self::RowIdAlias);
        }

        Ok(Self::RecordColumn {
            column_name,
            index: entry.column_index(column_name)?,
        })
    }

    fn decode_output(&self, table_name: &str, rowid: u64, record: &Record) -> Result<String> {
        match self {
            Self::RowIdAlias => Ok(rowid.to_string()),
            Self::RecordColumn { column_name, index } => {
                let column =
                    record
                        .column(*index)
                        .ok_or(SqliteParseError::RecordColumnOutOfBounds {
                            column_index: *index,
                        })?;
                column.decode_output_value(format!("{table_name}.{column_name}"))
            }
        }
    }

    fn decode_value<'record>(
        &self,
        table_name: &str,
        rowid: u64,
        record: &'record Record<'record>,
    ) -> Result<RecordValue<'record>> {
        match self {
            Self::RowIdAlias => Ok(RecordValue::Integer(
                i64::try_from(rowid)
                    .map_err(|_| SqliteParseError::InvalidRootPage(rowid as i64))?,
            )),
            Self::RecordColumn { column_name, index } => {
                let column =
                    record
                        .column(*index)
                        .ok_or(SqliteParseError::RecordColumnOutOfBounds {
                            column_index: *index,
                        })?;
                column.decode_value(format!("{table_name}.{column_name}"))
            }
        }
    }

    fn matches_query_value(
        &self,
        table_name: &str,
        rowid: u64,
        record: &Record,
        op: WhereOperator,
        expected: &QueryValue,
        second: Option<&QueryValue>,
    ) -> Result<bool> {
        let actual = self.decode_value(table_name, rowid, record)?;
        let (lower, upper) = SqliteDB::operator_bounds(op, expected, second);

        if let Some(bound) = lower {
            if !record_satisfies_lower(&actual, bound.value, bound.inclusive) {
                return Ok(false);
            }
        }
        if let Some(bound) = upper {
            if !record_satisfies_upper(&actual, bound.value, bound.inclusive) {
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn decode_sort_value(
        &self,
        table_name: &str,
        rowid: u64,
        record: &Record,
    ) -> Result<SortValue> {
        Ok(match self.decode_value(table_name, rowid, record)? {
            RecordValue::Null => SortValue::Null,
            RecordValue::Integer(value) => SortValue::Integer(value),
            RecordValue::Text(value) => SortValue::Text(value.to_owned()),
        })
    }
}

impl PartialOrd for SortValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortValue {
    fn cmp(&self, other: &Self) -> Ordering {
        use SortValue::{Integer, Null, Text};

        match (self, other) {
            (Null, Null) => Ordering::Equal,
            (Null, _) => Ordering::Less,
            (_, Null) => Ordering::Greater,
            (Integer(left), Integer(right)) => left.cmp(right),
            (Integer(_), Text(_)) => Ordering::Less,
            (Text(_), Integer(_)) => Ordering::Greater,
            (Text(left), Text(right)) => left.cmp(right),
        }
    }
}

#[derive(Debug)]
pub struct SqliteDB {
    path: PathBuf,
    header: DatabaseHeader,
    schema_table: SchemaTable,
    page_cache: RefCell<PageCache>,
}

impl SqliteDB {
    pub fn open(path: &Path) -> Result<Self> {
        let mut file =
            File::open(path).with_context(|| format!("failed to open {}", path.display()))?;

        let mut header_bytes = [0_u8; SQLITE_HEADER_LEN];
        file.read_exact(&mut header_bytes)
            .context("failed to read SQLite database header")?;

        let header = DatabaseHeader::parse(&header_bytes)?;
        let schema_table = load_schema_table(path, &header)?;
        let page_cache = PageCache::new(header.page_size, 1024);

        Ok(Self {
            path: path.to_path_buf(),
            header,
            schema_table,
            page_cache: RefCell::new(page_cache),
        })
    }

    pub fn db_info(&self) -> DbInfo {
        DbInfo {
            page_size: self.header.page_size,
            table_count: self.schema_table.table_count(),
        }
    }

    pub fn table_names(&self) -> Vec<&str> {
        self.schema_table.user_table_names()
    }

    pub fn read_page(&self, page_number: u32) -> Result<Vec<u8>> {
        Ok(self.read_page_cached(page_number)?.as_ref().to_vec())
    }

    pub fn count_rows(&self, table_name: &str) -> Result<usize> {
        let (_, rootpage) = self.resolve_table_root(table_name)?;
        let scanner = TableScanner::new(self);
        scanner.count_cells(table_name, rootpage)
    }

    pub fn select_rows(
        &self,
        table_name: &str,
        column_names: &[String],
        where_clause: Option<&Disjunction>,
        order_by: &[OrderByTerm],
    ) -> Result<Vec<String>> {
        let (entry, rootpage) = self.resolve_table_root(table_name)?;
        let resolved_columns = Self::resolve_columns(entry, column_names)?;
        let resolved_order_by = Self::resolve_order_by(entry, order_by)?;
        let mut seen_rowids = HashSet::new();
        let mut rows = Vec::new();
        let mut skip_sort = false;

        match where_clause {
            Some(disjunction) => {
                for conjunction in &disjunction.arms {
                    let resolved_predicates = Self::resolve_conjunction(entry, conjunction)?;
                    if let Some(index_match) =
                        self.choose_best_index(table_name, conjunction, &resolved_order_by)?
                    {
                        let prefix_values = Self::index_prefix_values(
                            index_match.entry,
                            &resolved_predicates,
                            index_match.prefix_len,
                        )?;
                        if Self::index_covers_requested_columns(
                            index_match.entry,
                            &resolved_columns,
                            &resolved_order_by,
                        )? {
                            self.select_rows_via_covering_index(
                                &resolved_columns,
                                &resolved_order_by,
                                &index_match,
                                &prefix_values,
                                &mut seen_rowids,
                                &mut rows,
                            )?;
                        } else {
                            self.select_rows_via_index_scan(
                                table_name,
                                rootpage,
                                &resolved_columns,
                                &resolved_order_by,
                                &resolved_predicates,
                                &index_match,
                                &mut seen_rowids,
                                &mut rows,
                            )?;
                        }
                        if disjunction.arms.len() == 1
                            && !resolved_order_by.is_empty()
                            && index_match.satisfies_order
                        {
                            skip_sort = true;
                        }
                    } else {
                        self.select_rows_via_table_scan(
                            table_name,
                            rootpage,
                            &resolved_columns,
                            &resolved_order_by,
                            &resolved_predicates,
                            &mut seen_rowids,
                            &mut rows,
                        )?;
                    }
                }
            }
            None => {
                self.select_rows_via_table_scan(
                    table_name,
                    rootpage,
                    &resolved_columns,
                    &resolved_order_by,
                    &[],
                    &mut seen_rowids,
                    &mut rows,
                )?;
            }
        }

        if !skip_sort {
            Self::sort_rows(&mut rows, &resolved_order_by);
        }

        Ok(rows.into_iter().map(|row| row.output).collect())
    }

    fn select_rows_via_table_scan<'a>(
        &self,
        table_name: &str,
        rootpage: u32,
        resolved_columns: &[ResolvedColumn<'a>],
        resolved_order_by: &[ResolvedOrderBy<'a>],
        predicates: &[ResolvedPredicate<'a>],
        seen_rowids: &mut HashSet<u64>,
        rows: &mut Vec<MaterializedRow>,
    ) -> Result<()> {
        let scanner = TableScanner::new(self);

        scanner.visit_records(table_name, rootpage, |rowid, record| {
            if !Self::matches_conjunction(table_name, rowid, &record, predicates)?
                || !seen_rowids.insert(rowid)
            {
                return Ok(());
            }

            rows.push(Self::materialize_row(
                table_name,
                rowid,
                &record,
                resolved_columns,
                resolved_order_by,
            )?);
            Ok(())
        })?;

        Ok(())
    }

    fn select_rows_via_index_scan<'a>(
        &self,
        table_name: &str,
        table_rootpage: u32,
        resolved_columns: &[ResolvedColumn<'a>],
        resolved_order_by: &[ResolvedOrderBy<'a>],
        predicates: &[ResolvedPredicate<'a>],
        index_match: &IndexMatch<'_, '_>,
        seen_rowids: &mut HashSet<u64>,
        rows: &mut Vec<MaterializedRow>,
    ) -> Result<()> {
        let index_rootpage =
            index_match
                .entry
                .rootpage
                .ok_or_else(|| SqliteParseError::MissingRootPage {
                    object_type: "index",
                    object_name: index_match.entry.name.clone(),
                })?;
        let index_scanner = IndexScanner::new(self);
        let table_scanner = TableScanner::new(self);
        let prefix_values =
            Self::index_prefix_values(index_match.entry, predicates, index_match.prefix_len)?;

        let mut handle_rowid = |rowid| {
            if seen_rowids.contains(&rowid) {
                return Ok(());
            }

            table_scanner.with_record_by_rowid(
                table_name,
                table_rootpage,
                rowid,
                |rowid, record| {
                    if !Self::matches_conjunction(table_name, rowid, &record, predicates)? {
                        return Ok(());
                    }
                    if !seen_rowids.insert(rowid) {
                        return Ok(());
                    }

                    rows.push(Self::materialize_row(
                        table_name,
                        rowid,
                        &record,
                        resolved_columns,
                        resolved_order_by,
                    )?);
                    Ok(())
                },
            )?;
            Ok(())
        };

        match (prefix_values.is_empty(), index_match.range) {
            (true, Some(range)) => index_scanner.visit_range_rowids(
                &index_match.entry.name,
                index_rootpage,
                range.lower.map(|b| (b.value, b.inclusive)),
                range.upper.map(|b| (b.value, b.inclusive)),
                &mut handle_rowid,
            )?,
            _ => index_scanner.visit_matching_rowids(
                &index_match.entry.name,
                index_rootpage,
                &prefix_values,
                &mut handle_rowid,
            )?,
        }

        Ok(())
    }

    fn index_covers_requested_columns(
        index_entry: &SchemaTableEntry,
        resolved_columns: &[ResolvedColumn],
        resolved_order_by: &[ResolvedOrderBy],
    ) -> Result<bool> {
        let Some(indexed_columns) = index_entry.indexed_column_names()? else {
            return Ok(false);
        };
        for col in resolved_columns {
            match col {
                ResolvedColumn::RowIdAlias => {}
                ResolvedColumn::RecordColumn { column_name, .. } => {
                    if !indexed_columns
                        .iter()
                        .any(|c| c.eq_ignore_ascii_case(column_name))
                    {
                        return Ok(false);
                    }
                }
            }
        }
        for order in resolved_order_by {
            match order.column {
                ResolvedColumn::RowIdAlias => {}
                ResolvedColumn::RecordColumn { column_name, .. } => {
                    if !indexed_columns
                        .iter()
                        .any(|c| c.eq_ignore_ascii_case(column_name))
                    {
                        return Ok(false);
                    }
                }
            }
        }
        Ok(true)
    }

    fn select_rows_via_covering_index<'a>(
        &self,
        resolved_columns: &[ResolvedColumn<'a>],
        resolved_order_by: &[ResolvedOrderBy<'a>],
        index_match: &IndexMatch<'_, '_>,
        prefix_values: &[QueryValue],
        seen_rowids: &mut HashSet<u64>,
        rows: &mut Vec<MaterializedRow>,
    ) -> Result<()> {
        let index_rootpage =
            index_match
                .entry
                .rootpage
                .ok_or_else(|| SqliteParseError::MissingRootPage {
                    object_type: "index",
                    object_name: index_match.entry.name.clone(),
                })?;
        let indexed_columns = index_match.entry.indexed_column_names()?.ok_or_else(|| {
            SqliteParseError::MalformedSchema {
                object_name: index_match.entry.name.clone(),
            }
        })?;

        let index_scanner = IndexScanner::new(self);
        let mut handle_entry = |key_values: &[RecordValue], rowid: u64| {
            if !seen_rowids.insert(rowid) {
                return Ok(());
            }
            let output = resolved_columns
                .iter()
                .map(|col| {
                    Self::covering_index_column_output(col, rowid, key_values, indexed_columns)
                })
                .collect::<Result<Vec<_>>>()?
                .join("|");
            let sort_keys = resolved_order_by
                .iter()
                .map(|order| {
                    Self::covering_index_sort_value(
                        &order.column,
                        rowid,
                        key_values,
                        indexed_columns,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            rows.push(MaterializedRow {
                rowid,
                output,
                sort_keys,
            });
            Ok(())
        };

        match (prefix_values.is_empty(), index_match.range) {
            (true, Some(range)) => index_scanner.visit_range_entries(
                &index_match.entry.name,
                index_rootpage,
                range.lower.map(|b| (b.value, b.inclusive)),
                range.upper.map(|b| (b.value, b.inclusive)),
                &mut handle_entry,
            )?,
            _ => index_scanner.visit_matching_entries(
                &index_match.entry.name,
                index_rootpage,
                prefix_values,
                &mut handle_entry,
            )?,
        }
        Ok(())
    }

    fn covering_index_column_output(
        column: &ResolvedColumn,
        rowid: u64,
        key_values: &[RecordValue],
        indexed_columns: &[String],
    ) -> Result<String> {
        match column {
            ResolvedColumn::RowIdAlias => Ok(rowid.to_string()),
            ResolvedColumn::RecordColumn { column_name, .. } => {
                let idx = indexed_columns
                    .iter()
                    .position(|c| c.eq_ignore_ascii_case(column_name))
                    .ok_or_else(|| SqliteParseError::ColumnNotFound {
                        table_name: String::new(),
                        column_name: column_name.to_string(),
                    })?;
                Ok(Self::record_value_to_output_string(
                    key_values.get(idx).unwrap_or(&RecordValue::Null),
                ))
            }
        }
    }

    fn covering_index_sort_value(
        column: &ResolvedColumn,
        rowid: u64,
        key_values: &[RecordValue],
        indexed_columns: &[String],
    ) -> Result<SortValue> {
        match column {
            ResolvedColumn::RowIdAlias => {
                Ok(SortValue::Integer(i64::try_from(rowid).map_err(|_| {
                    SqliteParseError::InvalidRootPage(rowid as i64)
                })?))
            }
            ResolvedColumn::RecordColumn { column_name, .. } => {
                let idx = indexed_columns
                    .iter()
                    .position(|c| c.eq_ignore_ascii_case(column_name))
                    .ok_or_else(|| SqliteParseError::ColumnNotFound {
                        table_name: String::new(),
                        column_name: column_name.to_string(),
                    })?;
                Ok(Self::record_value_to_sort_value(
                    key_values.get(idx).unwrap_or(&RecordValue::Null),
                ))
            }
        }
    }

    fn record_value_to_output_string(value: &RecordValue) -> String {
        match value {
            RecordValue::Null => String::new(),
            RecordValue::Integer(i) => i.to_string(),
            RecordValue::Text(s) => s.to_string(),
        }
    }

    fn record_value_to_sort_value(value: &RecordValue) -> SortValue {
        match value {
            RecordValue::Null => SortValue::Null,
            RecordValue::Integer(i) => SortValue::Integer(*i),
            RecordValue::Text(s) => SortValue::Text(s.to_string()),
        }
    }

    fn materialize_row(
        table_name: &str,
        rowid: u64,
        record: &Record,
        resolved_columns: &[ResolvedColumn],
        resolved_order_by: &[ResolvedOrderBy],
    ) -> Result<MaterializedRow> {
        let output = resolved_columns
            .iter()
            .map(|column| column.decode_output(table_name, rowid, record))
            .collect::<Result<Vec<_>>>()?
            .join("|");
        let sort_keys = resolved_order_by
            .iter()
            .map(|order_by| order_by.column.decode_sort_value(table_name, rowid, record))
            .collect::<Result<Vec<_>>>()?;

        Ok(MaterializedRow {
            rowid,
            output,
            sort_keys,
        })
    }

    fn resolve_columns<'a>(
        entry: &SchemaTableEntry,
        column_names: &'a [String],
    ) -> Result<Vec<ResolvedColumn<'a>>> {
        column_names
            .iter()
            .map(|column_name| ResolvedColumn::resolve(entry, column_name))
            .collect()
    }

    fn resolve_conjunction<'a>(
        entry: &SchemaTableEntry,
        conjunction: &'a Conjunction,
    ) -> Result<Vec<ResolvedPredicate<'a>>> {
        conjunction
            .terms
            .iter()
            .map(|term| {
                Ok(ResolvedPredicate {
                    column: ResolvedColumn::resolve(entry, &term.column_name)?,
                    op: term.op,
                    value: &term.value,
                    second_value: term.second_value.as_ref(),
                })
            })
            .collect()
    }

    fn resolve_order_by<'a>(
        entry: &SchemaTableEntry,
        order_by: &'a [OrderByTerm],
    ) -> Result<Vec<ResolvedOrderBy<'a>>> {
        order_by
            .iter()
            .map(|term| {
                Ok(ResolvedOrderBy {
                    column: ResolvedColumn::resolve(entry, &term.column_name)?,
                    direction: term.direction,
                })
            })
            .collect()
    }

    fn choose_best_index<'schema, 'q>(
        &'schema self,
        table_name: &str,
        conjunction: &'q Conjunction,
        order_by: &[ResolvedOrderBy],
    ) -> Result<Option<IndexMatch<'schema, 'q>>> {
        let mut best_match: Option<IndexMatch<'schema, 'q>> = None;

        for entry in self.schema_table.indexes_for_table(table_name) {
            let prefix_len = Self::index_prefix_len(entry, conjunction)?;
            let range = Self::index_range_on_first_column(entry, conjunction)?;
            if prefix_len == 0 && range.is_none() {
                continue;
            }

            let candidate = IndexMatch {
                entry,
                prefix_len,
                satisfies_order: Self::index_satisfies_order(entry, conjunction, order_by)?,
                range,
            };

            let replace = best_match.is_none_or(|current| {
                candidate.prefix_len > current.prefix_len
                    || (candidate.prefix_len == current.prefix_len
                        && candidate.satisfies_order
                        && !current.satisfies_order)
            });

            if replace {
                best_match = Some(candidate);
            }
        }

        Ok(best_match)
    }

    fn index_prefix_len(entry: &SchemaTableEntry, conjunction: &Conjunction) -> Result<usize> {
        let Some(indexed_columns) = entry.indexed_column_names()? else {
            return Ok(0);
        };

        Ok(indexed_columns
            .iter()
            .take_while(|indexed_column| {
                conjunction.terms.iter().any(|term| {
                    term.op == WhereOperator::Eq
                        && term.column_name.eq_ignore_ascii_case(indexed_column)
                })
            })
            .count())
    }

    fn operator_bounds<'q>(
        op: WhereOperator,
        value: &'q QueryValue,
        second: Option<&'q QueryValue>,
    ) -> (Option<IndexBound<'q>>, Option<IndexBound<'q>>) {
        match op {
            WhereOperator::Eq => (
                Some(IndexBound {
                    value,
                    inclusive: true,
                }),
                Some(IndexBound {
                    value,
                    inclusive: true,
                }),
            ),
            WhereOperator::Gt => (
                Some(IndexBound {
                    value,
                    inclusive: false,
                }),
                None,
            ),
            WhereOperator::Ge => (
                Some(IndexBound {
                    value,
                    inclusive: true,
                }),
                None,
            ),
            WhereOperator::Lt => (
                None,
                Some(IndexBound {
                    value,
                    inclusive: false,
                }),
            ),
            WhereOperator::Le => (
                None,
                Some(IndexBound {
                    value,
                    inclusive: true,
                }),
            ),
            WhereOperator::Between => match second {
                Some(high) => (
                    Some(IndexBound {
                        value,
                        inclusive: true,
                    }),
                    Some(IndexBound {
                        value: high,
                        inclusive: true,
                    }),
                ),
                None => (None, None),
            },
        }
    }

    fn index_range_on_first_column<'q>(
        entry: &SchemaTableEntry,
        conjunction: &'q Conjunction,
    ) -> Result<Option<IndexRange<'q>>> {
        let Some(indexed_columns) = entry.indexed_column_names()? else {
            return Ok(None);
        };
        let Some(first_indexed) = indexed_columns.first() else {
            return Ok(None);
        };

        let mut lower: Option<IndexBound<'q>> = None;
        let mut upper: Option<IndexBound<'q>> = None;

        for term in &conjunction.terms {
            if !term.column_name.eq_ignore_ascii_case(first_indexed) {
                continue;
            }
            let (term_lower, term_upper) =
                SqliteDB::operator_bounds(term.op, &term.value, term.second_value.as_ref());
            if let Some(bound) = term_lower {
                lower = Some(bound);
            }
            if let Some(bound) = term_upper {
                upper = Some(bound);
            }
        }

        if lower.is_none() && upper.is_none() {
            return Ok(None);
        }
        Ok(Some(IndexRange { lower, upper }))
    }

    fn index_satisfies_order(
        entry: &SchemaTableEntry,
        conjunction: &Conjunction,
        order_by: &[ResolvedOrderBy],
    ) -> Result<bool> {
        if order_by.is_empty() {
            return Ok(false);
        }

        let Some(indexed_columns) = entry.indexed_column_names()? else {
            return Ok(false);
        };
        let prefix_len = Self::index_prefix_len(entry, conjunction)?;
        let Some(first_direction) = order_by.first().map(|term| term.direction) else {
            return Ok(false);
        };

        if order_by
            .iter()
            .any(|term| term.direction != first_direction)
        {
            return Ok(false);
        }

        if prefix_len + order_by.len() > indexed_columns.len() {
            return Ok(false);
        }

        Ok(order_by
            .iter()
            .zip(indexed_columns.iter().skip(prefix_len))
            .all(|(order_term, indexed_column)| match order_term.column {
                ResolvedColumn::RowIdAlias => false,
                ResolvedColumn::RecordColumn { column_name, .. } => {
                    column_name.eq_ignore_ascii_case(indexed_column)
                }
            }))
    }

    fn index_prefix_values<'a>(
        entry: &SchemaTableEntry,
        predicates: &'a [ResolvedPredicate<'a>],
        prefix_len: usize,
    ) -> Result<Vec<QueryValue>> {
        let indexed_columns = entry
            .indexed_column_names()?
            .expect("index match should always have indexed columns");

        indexed_columns
            .iter()
            .take(prefix_len)
            .map(|indexed_column| {
                predicates
                    .iter()
                    .find(|predicate| match predicate.column {
                        ResolvedColumn::RowIdAlias => false,
                        ResolvedColumn::RecordColumn { column_name, .. } => {
                            column_name.eq_ignore_ascii_case(indexed_column)
                        }
                    })
                    .map(|predicate| predicate.value.clone())
                    .ok_or_else(|| SqliteParseError::MalformedIndexEntry {
                        index_name: entry.name.clone(),
                    })
                    .map_err(Into::into)
            })
            .collect()
    }

    fn matches_conjunction(
        table_name: &str,
        rowid: u64,
        record: &Record,
        predicates: &[ResolvedPredicate],
    ) -> Result<bool> {
        for predicate in predicates {
            if !predicate.column.matches_query_value(
                table_name,
                rowid,
                record,
                predicate.op,
                predicate.value,
                predicate.second_value,
            )? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn sort_rows(rows: &mut [MaterializedRow], order_by: &[ResolvedOrderBy]) {
        if order_by.is_empty() {
            return;
        }

        rows.sort_by(|left, right| {
            for (index, order_term) in order_by.iter().enumerate() {
                let mut comparison = left.sort_keys[index].cmp(&right.sort_keys[index]);
                if order_term.direction == SortDirection::Desc {
                    comparison = comparison.reverse();
                }
                if comparison != Ordering::Equal {
                    return comparison;
                }
            }

            left.rowid.cmp(&right.rowid)
        });
    }

    pub(crate) fn read_btree_page(&self, page_number: u32) -> Result<(Vec<u8>, BTreePage)> {
        let page = self.read_page_cached(page_number)?;
        let header_offset = if page_number == 1 {
            SQLITE_HEADER_LEN
        } else {
            0
        };
        let btree_page = BTreePage::parse(&page, header_offset)?;
        Ok((page.as_ref().to_vec(), btree_page))
    }

    pub(crate) fn count_btree_leaf_cells(
        &self,
        root_page: u32,
        leaf_kind: BTreePageKind,
        interior_kind: BTreePageKind,
        mut get_children: impl FnMut(&Self, &BTreePage, &[u8]) -> Result<Vec<u32>>,
        on_unsupported: impl Fn(u8) -> SqliteParseError,
    ) -> Result<usize> {
        let mut pages_to_visit = vec![root_page];
        let mut count = 0usize;
        while let Some(page_number) = pages_to_visit.pop() {
            let (page_bytes, btree_page) = self.read_btree_page(page_number)?;
            if btree_page.kind == leaf_kind {
                count += usize::from(btree_page.cell_count);
            } else if btree_page.kind == interior_kind {
                pages_to_visit.extend(get_children(self, &btree_page, &page_bytes)?);
            } else {
                let pt = page_bytes
                    .get(btree_page.header_offset)
                    .copied()
                    .unwrap_or(0);
                bail!(on_unsupported(pt));
            }
        }
        Ok(count)
    }

    pub(crate) fn usable_page_size(&self) -> usize {
        self.header.usable_page_size()
    }

    fn read_page_cached(&self, page_number: u32) -> Result<Arc<[u8]>> {
        let page_size = self.header.page_size;
        let path = self.path.clone();
        let mut cache = self.page_cache.borrow_mut();
        cache.get_or_load(page_number, |page_no| {
            read_page_from_path(&path, page_size, page_no)
        })
    }

    pub(crate) fn read_full_payload<'a>(
        &self,
        payload_size: u64,
        local_payload: &'a [u8],
        overflow_page: Option<u32>,
    ) -> Result<Cow<'a, [u8]>> {
        let payload_size = usize::try_from(payload_size)
            .map_err(|_| SqliteParseError::InvalidPayloadSize(payload_size))?;
        if overflow_page.is_none() || payload_size <= local_payload.len() {
            return Ok(Cow::Borrowed(local_payload));
        }

        let mut full_payload = Vec::with_capacity(payload_size);
        full_payload.extend_from_slice(local_payload);

        let mut next_page = overflow_page;
        let overflow_chunk_size = self.header.usable_page_size() - size_of::<u32>();

        while full_payload.len() < payload_size {
            let page_number = next_page.ok_or(SqliteParseError::TruncatedOverflowChain)?;
            let page = self.read_page_cached(page_number)?;

            let (next, _chunk) = page
                .split_first_chunk::<4>()
                .ok_or(SqliteParseError::TruncatedOverflowChain)?;
            next_page = match u32::from_be_bytes(*next) {
                0 => None,
                page_number => Some(page_number),
            };

            let remaining = payload_size - full_payload.len();
            let chunk_len = remaining.min(overflow_chunk_size);
            let chunk_end = size_of::<u32>() + chunk_len;
            full_payload.extend_from_slice(
                page.get(size_of::<u32>()..chunk_end)
                    .ok_or(SqliteParseError::TruncatedOverflowChain)?,
            );
        }

        Ok(Cow::Owned(full_payload))
    }

    fn resolve_table_root(&self, table_name: &str) -> Result<(&SchemaTableEntry, u32)> {
        let entry = self
            .schema_table
            .find_table(table_name)
            .ok_or_else(|| SqliteParseError::TableNotFound(table_name.to_owned()))?;
        let rootpage = entry
            .rootpage
            .ok_or_else(|| SqliteParseError::MissingRootPage {
                object_type: "table",
                object_name: table_name.to_owned(),
            })?;

        Ok((entry, rootpage))
    }
}

#[derive(Debug)]
pub struct DbInfo {
    pub page_size: u32,
    pub table_count: usize,
}

#[derive(Debug)]
struct DatabaseHeader {
    page_size: u32,
    reserved_bytes_per_page: u8,
}

impl DatabaseHeader {
    fn parse(bytes: &[u8; SQLITE_HEADER_LEN]) -> Result<Self> {
        if &bytes[..SQLITE_MAGIC_HEADER.len()] != SQLITE_MAGIC_HEADER {
            bail!(SqliteParseError::InvalidFileHeader);
        }

        let raw_page_size = u16::from_be_bytes([bytes[16], bytes[17]]);
        let page_size = match raw_page_size {
            SQLITE_MAX_PAGE_SIZE_SENTINEL => SQLITE_MAX_PAGE_SIZE,
            512..=32_768 if raw_page_size.is_power_of_two() => raw_page_size as u32,
            other => bail!(SqliteParseError::InvalidPageSize(other)),
        };
        let reserved_bytes_per_page = bytes[20];

        Ok(Self {
            page_size,
            reserved_bytes_per_page,
        })
    }

    fn usable_page_size(&self) -> usize {
        self.page_size as usize - usize::from(self.reserved_bytes_per_page)
    }
}

fn load_schema_table(path: &Path, header: &DatabaseHeader) -> Result<SchemaTable> {
    let mut pages_to_visit = vec![1_u32];
    let mut entries = Vec::new();

    while let Some(page_number) = pages_to_visit.pop() {
        let page_bytes = read_page_from_path(path, header.page_size, page_number)?;
        let header_offset = if page_number == 1 {
            SQLITE_HEADER_LEN
        } else {
            0
        };
        let btree_page = BTreePage::parse(&page_bytes, header_offset)?;

        match btree_page.kind {
            BTreePageKind::TableLeaf => {
                for cell in btree_page.cells(&page_bytes, header.usable_page_size())? {
                    let BTreeCell::TableLeaf(cell) = cell else {
                        unreachable!("schema leaf page should only contain table leaf cells");
                    };
                    let payload = read_full_payload_from_path(
                        path,
                        header,
                        cell.payload_size.value(),
                        cell.payload,
                        cell.overflow_page,
                    )?;
                    entries.push(SchemaTableEntry::parse_record_payload(payload.as_ref())?);
                }
            }
            BTreePageKind::TableInterior => {
                if let Some(right_most_ptr) = btree_page.right_most_ptr {
                    pages_to_visit.push(right_most_ptr);
                }

                for cell in btree_page
                    .cells(&page_bytes, header.usable_page_size())?
                    .into_iter()
                    .rev()
                {
                    let BTreeCell::TableInterior(cell) = cell else {
                        unreachable!(
                            "schema interior page should only contain table interior cells"
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
                    object_name: "sqlite_schema".to_owned(),
                    page_type,
                });
            }
        }
    }

    Ok(SchemaTable::from_entries(entries))
}

fn read_page_from_path(path: &Path, page_size: u32, page_number: u32) -> Result<Vec<u8>> {
    if page_number == 0 {
        bail!(SqliteParseError::InvalidPageNumber);
    }

    let mut file =
        File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let page_size = page_size as usize;
    let file_offset = u64::from(page_number - 1) * u64::from(page_size as u32);

    file.seek(SeekFrom::Start(file_offset))
        .with_context(|| format!("failed to seek to page {page_number}"))?;

    let mut page = vec![0_u8; page_size];
    file.read_exact(&mut page)
        .with_context(|| format!("failed to read page {page_number}"))?;

    Ok(page)
}

fn read_full_payload_from_path<'a>(
    path: &Path,
    header: &DatabaseHeader,
    payload_size: u64,
    local_payload: &'a [u8],
    overflow_page: Option<u32>,
) -> Result<Cow<'a, [u8]>> {
    let payload_size = usize::try_from(payload_size)
        .map_err(|_| SqliteParseError::InvalidPayloadSize(payload_size))?;
    if overflow_page.is_none() || payload_size <= local_payload.len() {
        return Ok(Cow::Borrowed(local_payload));
    }

    let mut full_payload = Vec::with_capacity(payload_size);
    full_payload.extend_from_slice(local_payload);

    let mut next_page = overflow_page;
    let overflow_chunk_size = header.usable_page_size() - size_of::<u32>();

    while full_payload.len() < payload_size {
        let page_number = next_page.ok_or(SqliteParseError::TruncatedOverflowChain)?;
        let page = read_page_from_path(path, header.page_size, page_number)?;

        let (next, _chunk) = page
            .split_first_chunk::<4>()
            .ok_or(SqliteParseError::TruncatedOverflowChain)?;
        next_page = match u32::from_be_bytes(*next) {
            0 => None,
            page_number => Some(page_number),
        };

        let remaining = payload_size - full_payload.len();
        let chunk_len = remaining.min(overflow_chunk_size);
        let chunk_end = size_of::<u32>() + chunk_len;
        full_payload.extend_from_slice(
            page.get(size_of::<u32>()..chunk_end)
                .ok_or(SqliteParseError::TruncatedOverflowChain)?,
        );
    }

    Ok(Cow::Owned(full_payload))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::WhereTerm;
    use std::fs;

    fn sample_db_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("sample.db")
    }

    fn superheroes_db_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("superheroes.db")
    }

    fn companies_db_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("companies.db")
    }

    fn temp_db_path(name: &str) -> PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("sqlite-rust-{name}-{unique}.db"))
    }

    fn build_header(page_size: u16) -> [u8; SQLITE_HEADER_LEN] {
        let mut header = [0_u8; SQLITE_HEADER_LEN];
        header[..SQLITE_MAGIC_HEADER.len()].copy_from_slice(SQLITE_MAGIC_HEADER);
        header[16..18].copy_from_slice(&page_size.to_be_bytes());
        header
    }

    fn encode_text_serial_type(len: usize) -> u8 {
        ((len * 2) + 13) as u8
    }

    fn build_schema_record(
        object_type: &str,
        name: &str,
        table_name: &str,
        rootpage: u8,
        sql: &str,
    ) -> Vec<u8> {
        let header_size = 6_u8;
        let mut payload = vec![
            header_size,
            encode_text_serial_type(object_type.len()),
            encode_text_serial_type(name.len()),
            encode_text_serial_type(table_name.len()),
            1,
            encode_text_serial_type(sql.len()),
        ];
        payload.extend_from_slice(object_type.as_bytes());
        payload.extend_from_slice(name.as_bytes());
        payload.extend_from_slice(table_name.as_bytes());
        payload.push(rootpage);
        payload.extend_from_slice(sql.as_bytes());
        payload
    }

    fn build_table_leaf_page(cell_rowid: u8, payload: &[u8]) -> Vec<u8> {
        let page_size = 512;
        let cell_len = 2 + payload.len();
        let cell_offset = page_size - cell_len;
        let mut page = vec![0_u8; page_size];
        page[0] = BTreePageKind::TableLeaf as u8;
        page[3..5].copy_from_slice(&1_u16.to_be_bytes());
        page[5..7].copy_from_slice(&(cell_offset as u16).to_be_bytes());
        page[8..10].copy_from_slice(&(cell_offset as u16).to_be_bytes());
        page[cell_offset] = payload.len() as u8;
        page[cell_offset + 1] = cell_rowid;
        page[cell_offset + 2..cell_offset + 2 + payload.len()].copy_from_slice(payload);
        page
    }

    fn build_schema_root_page(left_child_ptr: u32, right_most_ptr: u32) -> Vec<u8> {
        let page_size = 512;
        let cell_offset = page_size - 5;
        let mut page = vec![0_u8; page_size];
        let header = build_header(page_size as u16);
        page[..SQLITE_HEADER_LEN].copy_from_slice(&header);

        let header_offset = SQLITE_HEADER_LEN;
        page[header_offset] = BTreePageKind::TableInterior as u8;
        page[header_offset + 3..header_offset + 5].copy_from_slice(&1_u16.to_be_bytes());
        page[header_offset + 5..header_offset + 7]
            .copy_from_slice(&(cell_offset as u16).to_be_bytes());
        page[header_offset + 8..header_offset + 12].copy_from_slice(&right_most_ptr.to_be_bytes());
        page[header_offset + 12..header_offset + 14]
            .copy_from_slice(&(cell_offset as u16).to_be_bytes());

        page[cell_offset..cell_offset + 4].copy_from_slice(&left_child_ptr.to_be_bytes());
        page[cell_offset + 4] = 1;
        page
    }

    fn text_term(column_name: &str, value: &str) -> WhereTerm {
        WhereTerm {
            column_name: column_name.to_owned(),
            op: WhereOperator::Eq,
            value: QueryValue::Text(value.to_owned()),
            second_value: None,
        }
    }

    fn int_term(column_name: &str, value: i64) -> WhereTerm {
        WhereTerm {
            column_name: column_name.to_owned(),
            op: WhereOperator::Eq,
            value: QueryValue::Integer(value),
            second_value: None,
        }
    }

    fn disjunction(arms: Vec<Vec<WhereTerm>>) -> Disjunction {
        Disjunction {
            arms: arms
                .into_iter()
                .map(|terms| Conjunction { terms })
                .collect(),
        }
    }

    fn order_by(column_name: &str, direction: SortDirection) -> OrderByTerm {
        OrderByTerm {
            column_name: column_name.to_owned(),
            direction,
        }
    }

    #[test]
    fn parses_database_page_size() {
        let mut header = [0_u8; SQLITE_HEADER_LEN];
        header[..SQLITE_MAGIC_HEADER.len()].copy_from_slice(SQLITE_MAGIC_HEADER);
        header[16..18].copy_from_slice(&4096_u16.to_be_bytes());

        let parsed = DatabaseHeader::parse(&header).expect("header should parse");

        assert_eq!(parsed.page_size, 4096);
    }

    #[test]
    fn parses_special_case_page_size_65536() {
        let mut header = [0_u8; SQLITE_HEADER_LEN];
        header[..SQLITE_MAGIC_HEADER.len()].copy_from_slice(SQLITE_MAGIC_HEADER);
        header[16..18].copy_from_slice(&SQLITE_MAX_PAGE_SIZE_SENTINEL.to_be_bytes());

        let parsed = DatabaseHeader::parse(&header).expect("header should parse");

        assert_eq!(parsed.page_size, SQLITE_MAX_PAGE_SIZE);
    }

    #[test]
    fn parses_reserved_bytes_and_usable_page_size() {
        let mut header = build_header(4096);
        header[20] = 32;

        let parsed = DatabaseHeader::parse(&header).expect("header should parse");

        assert_eq!(parsed.page_size, 4096);
        assert_eq!(parsed.reserved_bytes_per_page, 32);
        assert_eq!(parsed.usable_page_size(), 4064);
    }

    #[test]
    fn parses_page_one_cell_count() {
        let mut page = vec![0_u8; SQLITE_HEADER_LEN + 8 + (3 * 2)];
        page[SQLITE_HEADER_LEN] = 13;
        page[SQLITE_HEADER_LEN + 3..SQLITE_HEADER_LEN + 5].copy_from_slice(&3_u16.to_be_bytes());

        let parsed = BTreePage::parse_page_one(&page).expect("page header should parse");

        assert_eq!(parsed.kind, BTreePageKind::TableLeaf);
        assert_eq!(parsed.cell_count, 3);
    }

    #[test]
    fn reconstructs_payload_from_overflow_pages() {
        let path = temp_db_path("overflow-payload");
        let header = DatabaseHeader {
            page_size: 512,
            reserved_bytes_per_page: 0,
        };
        let mut bytes = vec![0_u8; 1024];
        let local_payload = b"local";
        let overflow_bytes = b"-overflow";
        let overflow_page_offset = 512;
        bytes[overflow_page_offset..overflow_page_offset + 4].copy_from_slice(&0_u32.to_be_bytes());
        bytes[overflow_page_offset + 4..overflow_page_offset + 4 + overflow_bytes.len()]
            .copy_from_slice(overflow_bytes);
        fs::write(&path, bytes).expect("temp db should be writable");

        let payload = read_full_payload_from_path(
            &path,
            &header,
            (local_payload.len() + overflow_bytes.len()) as u64,
            local_payload,
            Some(2),
        )
        .expect("overflow payload should reconstruct");

        assert_eq!(payload.as_ref(), b"local-overflow");

        let _ = fs::remove_file(path);
    }

    #[test]
    fn parses_record_from_reconstructed_overflow_payload() {
        let path = temp_db_path("overflow-record");
        let header = DatabaseHeader {
            page_size: 512,
            reserved_bytes_per_page: 0,
        };
        let text = "x".repeat(500);
        let mut record_payload = vec![3, 0x87, 0x75];
        record_payload.extend_from_slice(text.as_bytes());

        let local_len = 39;
        let local_payload = &record_payload[..local_len];
        let overflow_payload = &record_payload[local_len..];
        let mut bytes = vec![0_u8; 1024];
        let overflow_page_offset = 512;
        bytes[overflow_page_offset..overflow_page_offset + 4].copy_from_slice(&0_u32.to_be_bytes());
        bytes[overflow_page_offset + 4..overflow_page_offset + 4 + overflow_payload.len()]
            .copy_from_slice(overflow_payload);
        fs::write(&path, bytes).expect("temp db should be writable");

        let payload = read_full_payload_from_path(
            &path,
            &header,
            record_payload.len() as u64,
            local_payload,
            Some(2),
        )
        .expect("overflow payload should reconstruct");
        let record = Record::parse(payload.as_ref()).expect("record should parse");

        assert_eq!(
            record
                .column(0)
                .expect("record should have first column")
                .decode_text("payload.text")
                .expect("column should decode"),
            text
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn loads_schema_table_from_multiple_pages() {
        let path = temp_db_path("schema-multipage");
        let apples = build_schema_record(
            "table",
            "apples",
            "apples",
            2,
            "CREATE TABLE apples (id integer)",
        );
        let oranges = build_schema_record(
            "table",
            "oranges",
            "oranges",
            3,
            "CREATE TABLE oranges (id integer)",
        );

        let mut bytes = build_schema_root_page(2, 3);
        bytes.extend_from_slice(&build_table_leaf_page(1, &apples));
        bytes.extend_from_slice(&build_table_leaf_page(2, &oranges));
        fs::write(&path, bytes).expect("temp db should be writable");

        let header = DatabaseHeader {
            page_size: 512,
            reserved_bytes_per_page: 0,
        };
        let schema = load_schema_table(&path, &header).expect("schema should load");

        assert_eq!(schema.table_count(), 2);
        assert!(schema.find_table("apples").is_some());
        assert!(schema.find_table("oranges").is_some());

        let _ = fs::remove_file(path);
    }

    #[test]
    fn reads_non_root_page() {
        let database = SqliteDB::open(&sample_db_path()).expect("sample db should open");

        let page = database.read_page(2).expect("page 2 should be readable");

        assert_eq!(page.len(), database.db_info().page_size as usize);
    }

    #[test]
    fn finds_apples_rootpage() {
        let database = SqliteDB::open(&sample_db_path()).expect("sample db should open");

        let apples = database
            .schema_table
            .find_table("apples")
            .expect("apples table should exist");

        assert_eq!(apples.rootpage, Some(2));
    }

    #[test]
    fn counts_rows_in_apples_table() {
        let database = SqliteDB::open(&sample_db_path()).expect("sample db should open");

        let row_count = database
            .count_rows("apples")
            .expect("apples row count should parse");

        assert_eq!(row_count, 4);
    }

    #[test]
    fn selects_name_values_from_apples_table() {
        let database = SqliteDB::open(&sample_db_path()).expect("sample db should open");

        let mut values = database
            .select_rows("apples", &["name".to_owned()], None, &[])
            .expect("apples names should parse");
        values.sort();

        assert_eq!(
            values,
            vec![
                "Fuji".to_owned(),
                "Golden Delicious".to_owned(),
                "Granny Smith".to_owned(),
                "Honeycrisp".to_owned(),
            ]
        );
    }

    #[test]
    fn missing_table_error_matches_sqlite_shape() {
        let database = SqliteDB::open(&sample_db_path()).expect("sample db should open");

        let error = database.count_rows("missing_table").unwrap_err();
        let error = error
            .downcast_ref::<SqliteParseError>()
            .expect("error should downcast to SqliteParseError");

        assert!(matches!(
            error,
            SqliteParseError::TableNotFound(table_name) if table_name == "missing_table"
        ));
    }

    #[test]
    fn missing_column_error_matches_sqlite_shape() {
        let database = SqliteDB::open(&sample_db_path()).expect("sample db should open");

        let error = database
            .select_rows("apples", &["missing_col".to_owned()], None, &[])
            .unwrap_err();
        let error = error
            .downcast_ref::<SqliteParseError>()
            .expect("error should downcast to SqliteParseError");

        assert!(matches!(
            error,
            SqliteParseError::ColumnNotFound {
                table_name,
                column_name,
            } if table_name == "apples" && column_name == "missing_col"
        ));
    }

    #[test]
    fn selects_multi_column_rows_from_apples_table() {
        let database = SqliteDB::open(&sample_db_path()).expect("sample db should open");

        let mut rows = database
            .select_rows(
                "apples",
                &["name".to_owned(), "color".to_owned()],
                None,
                &[],
            )
            .expect("apples rows should parse");
        rows.sort();

        assert_eq!(
            rows,
            vec![
                "Fuji|Red".to_owned(),
                "Golden Delicious|Yellow".to_owned(),
                "Granny Smith|Light Green".to_owned(),
                "Honeycrisp|Blush Red".to_owned(),
            ]
        );
    }

    #[test]
    fn preserves_projection_order_in_multi_column_rows() {
        let database = SqliteDB::open(&sample_db_path()).expect("sample db should open");

        let mut rows = database
            .select_rows(
                "apples",
                &["color".to_owned(), "name".to_owned()],
                None,
                &[],
            )
            .expect("apples rows should parse");
        rows.sort();

        assert_eq!(
            rows,
            vec![
                "Blush Red|Honeycrisp".to_owned(),
                "Light Green|Granny Smith".to_owned(),
                "Red|Fuji".to_owned(),
                "Yellow|Golden Delicious".to_owned(),
            ]
        );
    }

    #[test]
    fn missing_one_of_multiple_columns_returns_column_not_found() {
        let database = SqliteDB::open(&sample_db_path()).expect("sample db should open");

        let error = database
            .select_rows(
                "apples",
                &["name".to_owned(), "missing_col".to_owned()],
                None,
                &[],
            )
            .unwrap_err();
        let error = error
            .downcast_ref::<SqliteParseError>()
            .expect("error should downcast to SqliteParseError");

        assert!(matches!(
            error,
            SqliteParseError::ColumnNotFound {
                table_name,
                column_name,
            } if table_name == "apples" && column_name == "missing_col"
        ));
    }

    #[test]
    fn filters_rows_by_text_equality() {
        let database = SqliteDB::open(&sample_db_path()).expect("sample db should open");

        let rows = database
            .select_rows(
                "apples",
                &["name".to_owned(), "color".to_owned()],
                Some(&disjunction(vec![vec![text_term("color", "Yellow")]])),
                &[],
            )
            .expect("filtered rows should parse");

        assert_eq!(rows, vec!["Golden Delicious|Yellow".to_owned()]);
    }

    #[test]
    fn filters_on_non_projected_column() {
        let database = SqliteDB::open(&sample_db_path()).expect("sample db should open");

        let rows = database
            .select_rows(
                "apples",
                &["name".to_owned()],
                Some(&disjunction(vec![vec![text_term("color", "Yellow")]])),
                &[],
            )
            .expect("filtered rows should parse");

        assert_eq!(rows, vec!["Golden Delicious".to_owned()]);
    }

    #[test]
    fn missing_predicate_column_returns_column_not_found() {
        let database = SqliteDB::open(&sample_db_path()).expect("sample db should open");

        let error = database
            .select_rows(
                "apples",
                &["name".to_owned()],
                Some(&disjunction(vec![vec![text_term("missing_col", "Yellow")]])),
                &[],
            )
            .unwrap_err();
        let error = error
            .downcast_ref::<SqliteParseError>()
            .expect("error should downcast to SqliteParseError");

        assert!(matches!(
            error,
            SqliteParseError::ColumnNotFound {
                table_name,
                column_name,
            } if table_name == "apples" && column_name == "missing_col"
        ));
    }

    #[test]
    fn superheroes_root_page_is_table_interior() {
        let database = SqliteDB::open(&superheroes_db_path()).expect("superheroes db should open");

        let superheroes = database
            .schema_table
            .find_table("superheroes")
            .expect("superheroes table should exist");
        let rootpage = superheroes
            .rootpage
            .expect("superheroes should have root page");
        let (_, btree_page) = database
            .read_btree_page(rootpage)
            .expect("superheroes root page should be readable");

        assert_eq!(btree_page.kind, BTreePageKind::TableInterior);
    }

    #[test]
    fn counts_rows_in_superheroes_table() {
        let database = SqliteDB::open(&superheroes_db_path()).expect("superheroes db should open");

        let row_count = database
            .count_rows("superheroes")
            .expect("superheroes row count should parse");

        assert_eq!(row_count, 6_895);
    }

    #[test]
    fn selects_filtered_rows_from_multi_page_superheroes_table() {
        let database = SqliteDB::open(&superheroes_db_path()).expect("superheroes db should open");

        let mut rows = database
            .select_rows(
                "superheroes",
                &["id".to_owned(), "name".to_owned()],
                Some(&disjunction(vec![vec![text_term(
                    "eye_color",
                    "Pink Eyes",
                )]])),
                &[],
            )
            .expect("filtered superheroes rows should parse");
        rows.sort();

        assert_eq!(
            rows,
            vec![
                "1085|Felicity (New Earth)".to_owned(),
                "2729|Thrust (New Earth)".to_owned(),
                "297|Stealth (New Earth)".to_owned(),
                "3289|Angora Lapin (New Earth)".to_owned(),
                "3913|Matris Ater Clementia (New Earth)".to_owned(),
                "790|Tobias Whale (New Earth)".to_owned(),
            ]
        );
    }

    #[test]
    fn counts_rows_in_companies_table_via_index() {
        let database = SqliteDB::open(&companies_db_path()).expect("companies db should open");

        let row_count = database
            .count_rows("companies")
            .expect("companies row count should parse");

        assert_eq!(
            row_count, 55_991,
            "index-only COUNT(*) must match table row count"
        );
    }

    #[test]
    fn finds_companies_country_index() {
        let database = SqliteDB::open(&companies_db_path()).expect("companies db should open");

        let index = database
            .schema_table
            .find_index_for_column("companies", "country")
            .expect("country index should exist");

        assert_eq!(index.name, "idx_companies_country");
        assert_eq!(index.rootpage, Some(4));
    }

    #[test]
    fn companies_index_root_page_is_index_interior() {
        let database = SqliteDB::open(&companies_db_path()).expect("companies db should open");
        let index = database
            .schema_table
            .find_index_for_column("companies", "country")
            .expect("country index should exist");
        let rootpage = index.rootpage.expect("index should have root page");
        let (_, btree_page) = database
            .read_btree_page(rootpage)
            .expect("index root page should be readable");

        assert_eq!(btree_page.kind, BTreePageKind::IndexInterior);
    }

    #[test]
    fn finds_company_row_by_rowid() {
        let database = SqliteDB::open(&companies_db_path()).expect("companies db should open");
        let companies = database
            .schema_table
            .find_table("companies")
            .expect("companies table should exist");
        let rootpage = companies.rootpage.expect("companies should have root page");
        let scanner = TableScanner::new(&database);
        let mut company_name = None;

        let found = scanner
            .with_record_by_rowid("companies", rootpage, 121_311, |_, record| {
                company_name = Some(
                    record
                        .column(companies.column_index("name").unwrap())
                        .unwrap()
                        .decode_text("companies.name")
                        .unwrap()
                        .to_owned(),
                );
                Ok(())
            })
            .expect("row lookup should succeed");

        assert!(found);
        assert_eq!(company_name.as_deref(), Some("unilink s.c."));
    }

    #[test]
    fn selects_rows_from_companies_via_index_scan() {
        let database = SqliteDB::open(&companies_db_path()).expect("companies db should open");

        let mut rows = database
            .select_rows(
                "companies",
                &["id".to_owned(), "name".to_owned()],
                Some(&disjunction(vec![vec![text_term("country", "eritrea")]])),
                &[],
            )
            .expect("indexed companies rows should parse");
        rows.sort();

        assert_eq!(
            rows,
            vec![
                "121311|unilink s.c.".to_owned(),
                "2102438|orange asmara it solutions".to_owned(),
                "5729848|zara mining share company".to_owned(),
                "6634629|asmara rental".to_owned(),
            ]
        );
    }

    #[test]
    fn selects_country_via_covering_index() {
        let database = SqliteDB::open(&companies_db_path()).expect("companies db should open");

        let rows = database
            .select_rows(
                "companies",
                &["country".to_owned()],
                Some(&disjunction(vec![vec![text_term(
                    "country",
                    "dominican republic",
                )]])),
                &[],
            )
            .expect("covering index select should parse");

        assert!(
            !rows.is_empty(),
            "should return rows for dominican republic"
        );
        assert!(
            rows.iter().all(|r| r == "dominican republic"),
            "covering index must return only the indexed column value"
        );
    }

    #[test]
    fn filters_rows_by_integer_equality() {
        let database = SqliteDB::open(&sample_db_path()).expect("sample db should open");

        let rows = database
            .select_rows(
                "apples",
                &["name".to_owned(), "color".to_owned()],
                Some(&disjunction(vec![vec![int_term("id", 4)]])),
                &[],
            )
            .expect("integer-filtered rows should parse");

        assert_eq!(rows, vec!["Golden Delicious|Yellow".to_owned()]);
    }

    #[test]
    fn dedupes_rows_across_or_predicates() {
        let database = SqliteDB::open(&sample_db_path()).expect("sample db should open");

        let rows = database
            .select_rows(
                "apples",
                &["name".to_owned()],
                Some(&disjunction(vec![
                    vec![text_term("color", "Yellow")],
                    vec![int_term("id", 4)],
                ])),
                &[],
            )
            .expect("or-filtered rows should parse");

        assert_eq!(rows, vec!["Golden Delicious".to_owned()]);
    }

    #[test]
    fn orders_rows_by_projected_column() {
        let database = SqliteDB::open(&sample_db_path()).expect("sample db should open");

        let rows = database
            .select_rows(
                "apples",
                &["name".to_owned()],
                None,
                &[order_by("name", SortDirection::Asc)],
            )
            .expect("ordered rows should parse");

        assert_eq!(
            rows,
            vec![
                "Fuji".to_owned(),
                "Golden Delicious".to_owned(),
                "Granny Smith".to_owned(),
                "Honeycrisp".to_owned(),
            ]
        );
    }

    #[test]
    fn orders_rows_by_non_projected_column() {
        let database = SqliteDB::open(&sample_db_path()).expect("sample db should open");

        let rows = database
            .select_rows(
                "apples",
                &["name".to_owned()],
                None,
                &[order_by("color", SortDirection::Asc)],
            )
            .expect("ordered rows should parse");

        assert_eq!(
            rows,
            vec![
                "Honeycrisp".to_owned(),
                "Granny Smith".to_owned(),
                "Fuji".to_owned(),
                "Golden Delicious".to_owned(),
            ]
        );
    }

    #[test]
    fn orders_companies_by_indexed_country_without_final_sort() {
        let database = SqliteDB::open(&companies_db_path()).expect("companies db should open");

        let rows = database
            .select_rows(
                "companies",
                &["name".to_owned(), "country".to_owned()],
                Some(&disjunction(vec![vec![text_term(
                    "country",
                    "dominican republic",
                )]])),
                &[order_by("country", SortDirection::Asc)],
            )
            .expect("indexed and ordered rows should parse");

        assert!(
            !rows.is_empty(),
            "should return rows for dominican republic"
        );
        assert!(
            rows.iter().all(|row| row.ends_with("|dominican republic")),
            "all rows must be for dominican republic"
        );
    }

    #[test]
    fn selects_rows_with_between_predicate() {
        let database = SqliteDB::open(&sample_db_path()).expect("sample db should open");

        let between = WhereTerm {
            column_name: "id".to_owned(),
            op: WhereOperator::Between,
            value: QueryValue::Integer(2),
            second_value: Some(QueryValue::Integer(3)),
        };

        let rows = database
            .select_rows(
                "apples",
                &["id".to_owned(), "name".to_owned()],
                Some(&disjunction(vec![vec![between]])),
                &[order_by("id", SortDirection::Asc)],
            )
            .expect("between rows should parse");

        assert_eq!(rows, vec!["2|Fuji".to_owned(), "3|Honeycrisp".to_owned()]);
    }

    #[test]
    fn selects_rows_with_range_predicates_via_index() {
        let database = SqliteDB::open(&companies_db_path()).expect("companies db should open");

        let rows = database
            .select_rows(
                "companies",
                &["name".to_owned(), "country".to_owned()],
                Some(&disjunction(vec![vec![WhereTerm {
                    column_name: "country".to_owned(),
                    op: WhereOperator::Between,
                    value: QueryValue::Text("djibouti".to_owned()),
                    second_value: Some(QueryValue::Text("dominican republic".to_owned())),
                }]])),
                &[order_by("country", SortDirection::Asc)],
            )
            .expect("between on indexed column should parse");

        assert!(
            !rows.is_empty(),
            "should return rows for countries between djibouti and dominican republic"
        );

        let countries: Vec<&str> = rows
            .iter()
            .map(|row| row.rsplit('|').next().unwrap())
            .collect();

        assert!(
            countries
                .iter()
                .all(|c| *c >= "djibouti" && *c <= "dominican republic"),
            "all countries must be between djibouti and dominican republic (inclusive)"
        );
        assert!(
            countries.iter().any(|c| *c == "dominican republic"),
            "result set must include dominican republic to exercise upper-bound inclusivity"
        );
    }

    #[test]
    fn orders_filtered_or_results() {
        let database = SqliteDB::open(&sample_db_path()).expect("sample db should open");

        let rows = database
            .select_rows(
                "apples",
                &["id".to_owned(), "name".to_owned()],
                Some(&disjunction(vec![
                    vec![text_term("color", "Yellow")],
                    vec![text_term("color", "Red")],
                ])),
                &[order_by("id", SortDirection::Desc)],
            )
            .expect("ordered rows should parse");

        assert_eq!(
            rows,
            vec!["4|Golden Delicious".to_owned(), "2|Fuji".to_owned(),]
        );
    }
}
