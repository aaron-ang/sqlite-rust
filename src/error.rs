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
    #[error("expected text serial type for sqlite_schema.{column}, found {serial_type}")]
    UnexpectedTextSerialType {
        column: &'static str,
        serial_type: u64,
    },
    #[error("expected integer serial type for sqlite_schema.{column}, found {serial_type}")]
    UnexpectedIntegerSerialType {
        column: &'static str,
        serial_type: u64,
    },
    #[error("invalid UTF-8 in sqlite_schema.{column}")]
    InvalidUtf8 { column: &'static str },
    #[error("invalid sqlite_schema.type value: {0}")]
    InvalidSchemaObjectType(String),
    #[error("invalid sqlite_schema.rootpage value: {0}")]
    InvalidRootPage(i64),
    #[error("table not found: {0}")]
    TableNotFound(String),
    #[error("table {table_name} does not have a root page")]
    MissingTableRootPage { table_name: String },
    #[error("page number must be greater than zero")]
    InvalidPageNumber,
    #[error("table {table_name} root page has unsupported page type: 0x{page_type:02x}")]
    UnsupportedTablePageType { table_name: String, page_type: u8 },
    #[error("cell pointer {0} is out of bounds")]
    CellPointerOutOfBounds(usize),
    #[error("cell payload at offset {offset} exceeds page bounds")]
    CellPayloadOutOfBounds { offset: usize },
}
