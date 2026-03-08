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
    #[error("expected text serial type for {column}, found {serial_type}")]
    UnexpectedTextSerialType { column: String, serial_type: u64 },
    #[error("expected integer serial type for {column}, found {serial_type}")]
    UnexpectedIntegerSerialType { column: String, serial_type: u64 },
    #[error("unsupported output serial type for {column}: {serial_type}")]
    UnsupportedOutputSerialType { column: String, serial_type: u64 },
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
    #[error("malformed database schema ({table_name})")]
    MissingCreateTableSql { table_name: String },
    #[error("malformed database schema ({table_name})")]
    UnsupportedCreateTableSql { table_name: String },
    #[error("in prepare, no such table: {0}")]
    TableNotFound(String),
    #[error("in prepare, no such column: {column_name}")]
    ColumnNotFound {
        table_name: String,
        column_name: String,
    },
    #[error("table {table_name} does not have a root page")]
    MissingTableRootPage { table_name: String },
    #[error("record column index {column_index} is out of bounds")]
    RecordColumnOutOfBounds { column_index: usize },
    #[error("page number must be greater than zero")]
    InvalidPageNumber,
    #[error("table {table_name} root page has unsupported page type: 0x{page_type:02x}")]
    UnsupportedTablePageType { table_name: String, page_type: u8 },
    #[error("cell pointer {0} is out of bounds")]
    CellPointerOutOfBounds(usize),
    #[error("cell payload at offset {offset} exceeds page bounds")]
    CellPayloadOutOfBounds { offset: usize },
}
