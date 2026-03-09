use thiserror::Error;

#[derive(Debug, Error)]
pub enum SqliteParseError {
    #[error("invalid SQLite file header")]
    InvalidFileHeader,
    #[error("invalid SQLite page size: {0}")]
    InvalidPageSize(u16),
    #[error("page 1 is too short to contain a b-tree page header")]
    PageTooShort,
    #[error("unsupported sqlite_schema page type: 0x{0:02x}")]
    UnsupportedPageType(u8),
    #[error("invalid or unterminated SQLite varint")]
    InvalidVarint,
    #[error("invalid SQLite record header size: {0}")]
    InvalidRecordHeaderSize(u64),
    #[error("unsupported SQLite serial type: {0}")]
    UnsupportedSerialType(u64),
    #[error("expected {expected} serial type for {column}, found {serial_type}")]
    UnexpectedSerialType {
        column: String,
        expected: &'static str,
        serial_type: u64,
    },
    #[error("invalid UTF-8 in {column}")]
    InvalidUtf8 { column: String },
    #[error("invalid sqlite_schema.type value: {0}")]
    InvalidSchemaObjectType(String),
    #[error("invalid sqlite_schema.rootpage value: {0}")]
    InvalidRootPage(i64),
    #[error("in prepare, syntax error")]
    SqlSyntaxError,
    #[error("in prepare, near \"{0}\": syntax error")]
    SqlSyntaxErrorNear(String),
    #[error("in prepare, incomplete input")]
    SqlIncompleteInput,
    #[error("in prepare, unsupported SQL for this stage: {0}")]
    UnsupportedSql(String),
    #[error("unsupported shell command: {0}")]
    UnsupportedShellCommand(String),
    #[error("malformed database schema ({object_name})")]
    MalformedSchema { object_name: String },
    #[error("in prepare, no such table: {0}")]
    TableNotFound(String),
    #[error("in prepare, no such column: {column_name}")]
    ColumnNotFound {
        table_name: String,
        column_name: String,
    },
    #[error("{object_type} {object_name} does not have a root page")]
    MissingRootPage {
        object_type: &'static str,
        object_name: String,
    },
    #[error("record column index {column_index} is out of bounds")]
    RecordColumnOutOfBounds { column_index: usize },
    #[error("invalid cell payload size: {0}")]
    InvalidPayloadSize(u64),
    #[error("page number must be greater than zero")]
    InvalidPageNumber,
    #[error("{object_type} {object_name} root page has unsupported page type: 0x{page_type:02x}")]
    UnsupportedRootPageType {
        object_type: &'static str,
        object_name: String,
        page_type: u8,
    },
    #[error("in prepare, no usable index found for {table_name}.{column_name}")]
    NoUsableIndex {
        table_name: String,
        column_name: String,
    },
    #[error("malformed index entry in {index_name}")]
    MalformedIndexEntry { index_name: String },
    #[error("cell pointer {0} is out of bounds")]
    CellPointerOutOfBounds(usize),
    #[error("cell payload at offset {offset} exceeds page bounds")]
    CellPayloadOutOfBounds { offset: usize },
    #[error("overflow page chain ended before payload was fully read")]
    TruncatedOverflowChain,
}
