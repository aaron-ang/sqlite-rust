use clap::{Parser, ValueEnum};
use std::path::PathBuf;

#[derive(Debug, Parser)]
pub struct Cli {
    pub database_path: PathBuf,
    #[arg(value_enum)]
    pub command: Command,
}

#[derive(Clone, Debug, ValueEnum)]
pub enum Command {
    #[value(name = ".dbinfo")]
    DbInfo,
    #[value(name = ".tables")]
    Tables,
}
