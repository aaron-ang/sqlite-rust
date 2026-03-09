use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{Result, bail};
use clap::Parser;
use strum::EnumString;

use crate::error::SqliteParseError;
use crate::query::SqlStatement;

#[derive(Debug, Parser)]
pub struct Cli {
    #[arg(long = "cmd")]
    pub cmds: Vec<String>,
    pub database_path: PathBuf,
    pub input: Option<String>,
}

#[derive(Debug, PartialEq)]
pub enum UserInput {
    Dot(DotCommand),
    Sql(Vec<SqlStatement>),
}

#[derive(Debug, EnumString, PartialEq)]
pub enum DotCommand {
    #[strum(serialize = ".dbinfo")]
    DbInfo,
    #[strum(serialize = ".tables")]
    Tables,
}

#[derive(Debug)]
pub enum ShellCommand {
    Timer(bool),
}

#[derive(Debug, Default, PartialEq)]
pub struct ShellConfig {
    pub timer_enabled: bool,
}

impl Cli {
    pub fn user_input(&self) -> Result<UserInput> {
        let input = self
            .input
            .as_deref()
            .expect("user_input requires positional input");

        UserInput::parse(input)
    }

    pub fn shell_config(&self) -> Result<ShellConfig> {
        let mut config = ShellConfig::default();
        for command in self.shell_commands()? {
            match command {
                ShellCommand::Timer(enabled) => config.timer_enabled = enabled,
            }
        }
        Ok(config)
    }

    fn shell_commands(&self) -> Result<Vec<ShellCommand>> {
        self.cmds
            .iter()
            .map(|cmd| ShellCommand::parse(cmd))
            .collect()
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

impl ShellCommand {
    fn parse(input: &str) -> Result<Self> {
        match input {
            ".timer on" => Ok(Self::Timer(true)),
            ".timer off" => Ok(Self::Timer(false)),
            _ => bail!(SqliteParseError::UnsupportedShellCommand(input.to_owned())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_without_cmds() {
        let cli = Cli::parse_from(["sqlite-rust", "sample.db", ".dbinfo"]);

        assert!(cli.cmds.is_empty());
        assert_eq!(cli.database_path, PathBuf::from("sample.db"));
        assert_eq!(cli.input.as_deref(), Some(".dbinfo"));
    }

    #[test]
    fn parses_without_positional_input() {
        let cli = Cli::parse_from(["sqlite-rust", "sample.db"]);

        assert!(cli.cmds.is_empty());
        assert_eq!(cli.database_path, PathBuf::from("sample.db"));
        assert_eq!(cli.input, None);
    }

    #[test]
    fn parses_timer_cmd() {
        let cli = Cli::parse_from(["sqlite-rust", "--cmd", ".timer on", "sample.db", ".dbinfo"]);

        assert_eq!(cli.cmds, vec![".timer on".to_owned()]);
        assert_eq!(
            cli.shell_config().unwrap(),
            ShellConfig {
                timer_enabled: true
            }
        );
    }

    #[test]
    fn last_timer_cmd_wins() {
        let cli = Cli::parse_from([
            "sqlite-rust",
            "--cmd",
            ".timer on",
            "--cmd",
            ".timer off",
            "sample.db",
            ".dbinfo",
        ]);

        assert_eq!(
            cli.shell_config().unwrap(),
            ShellConfig {
                timer_enabled: false
            }
        );
    }

    #[test]
    fn unsupported_cmd_returns_error() {
        let cli = Cli::parse_from(["sqlite-rust", "--cmd", ".mode json", "sample.db", ".dbinfo"]);

        let error = cli.shell_config().unwrap_err();
        let error = error
            .downcast_ref::<SqliteParseError>()
            .expect("error should downcast to SqliteParseError");

        assert!(matches!(
            error,
            SqliteParseError::UnsupportedShellCommand(command) if command == ".mode json"
        ));
    }

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
    fn parses_multiple_positional_sql_statements() {
        assert_eq!(
            UserInput::parse("SELECT COUNT(*) FROM apples; SELECT COUNT(*) FROM oranges;").unwrap(),
            UserInput::Sql(vec![
                SqlStatement::SelectCount {
                    table_name: "apples".to_owned()
                },
                SqlStatement::SelectCount {
                    table_name: "oranges".to_owned()
                }
            ])
        );
    }
}
