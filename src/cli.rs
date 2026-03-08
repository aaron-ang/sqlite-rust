use std::path::PathBuf;
use std::str::FromStr;

use anyhow::Result;
use clap::Parser;
use strum::EnumString;

use crate::query::SqlStatement;

#[derive(Debug, Parser)]
pub struct Cli {
    pub database_path: PathBuf,
    pub input: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum UserInput {
    Dot(DotCommand),
    Sql(SqlStatement),
}

#[derive(Clone, Debug, EnumString, PartialEq)]
pub enum DotCommand {
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
            Ok(dot_command) => Ok(Self::Dot(dot_command)),
            Err(_) => Ok(Self::Sql(SqlStatement::parse(input)?)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_dot_commands() {
        assert_eq!(
            UserInput::parse(".dbinfo").unwrap(),
            UserInput::Dot(DotCommand::DbInfo)
        );
        assert_eq!(
            UserInput::parse(".tables").unwrap(),
            UserInput::Dot(DotCommand::Tables)
        );
    }

    #[test]
    fn parses_count_query() {
        assert_eq!(
            UserInput::parse("SELECT COUNT(*) FROM apples").unwrap(),
            UserInput::Sql(SqlStatement::SelectCount {
                table_name: "apples".to_owned(),
            })
        );
    }

    #[test]
    fn parses_column_query() {
        assert_eq!(
            UserInput::parse("SELECT name FROM apples").unwrap(),
            UserInput::Sql(SqlStatement::SelectColumns {
                table_name: "apples".to_owned(),
                column_names: vec!["name".to_owned()],
                where_clause: None,
            })
        );
    }

    #[test]
    fn parses_multi_column_query() {
        assert_eq!(
            UserInput::parse("SELECT name, color FROM apples").unwrap(),
            UserInput::Sql(SqlStatement::SelectColumns {
                table_name: "apples".to_owned(),
                column_names: vec!["name".to_owned(), "color".to_owned()],
                where_clause: None,
            })
        );
    }

    #[test]
    fn parses_filtered_multi_column_query() {
        assert_eq!(
            UserInput::parse("SELECT name, color FROM apples WHERE color = 'Yellow'").unwrap(),
            UserInput::Sql(SqlStatement::SelectColumns {
                table_name: "apples".to_owned(),
                column_names: vec!["name".to_owned(), "color".to_owned()],
                where_clause: Some(crate::query::WhereClause::EqualsText {
                    column_name: "color".to_owned(),
                    value: "Yellow".to_owned(),
                }),
            })
        );
    }
}
