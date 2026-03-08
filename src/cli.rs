use std::path::PathBuf;
use std::str::FromStr;

use anyhow::Result;
use clap::Parser;
use strum::EnumString;

#[derive(Debug, Parser)]
pub struct Cli {
    pub database_path: PathBuf,
    pub input: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UserInput {
    DbInfo,
    Tables,
    CountRows { table_name: String },
}

#[derive(Clone, Debug, EnumString, PartialEq, Eq)]
enum DotCommand {
    #[strum(serialize = ".dbinfo")]
    DbInfo,
    #[strum(serialize = ".tables")]
    Tables,
}

impl Cli {
    pub fn user_input(&self) -> Result<UserInput> {
        UserInput::parse(&self.input)
    }
}

impl UserInput {
    pub fn parse(input: &str) -> Result<Self> {
        match DotCommand::from_str(input) {
            Ok(DotCommand::DbInfo) => Ok(Self::DbInfo),
            Ok(DotCommand::Tables) => Ok(Self::Tables),
            Err(_) => Self::parse_sql(input),
        }
    }

    fn parse_sql(input: &str) -> Result<Self> {
        let tokens: Vec<&str> = input.split_ascii_whitespace().collect();
        Ok(Self::CountRows {
            table_name: tokens[3].to_owned(),
        })
    }
}
