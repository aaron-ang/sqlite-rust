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
}
