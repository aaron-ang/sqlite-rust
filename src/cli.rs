use std::path::PathBuf;

use anyhow::{Result, bail};
use clap::Parser;

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

#[derive(Debug, PartialEq)]
pub enum DotCommand {
    DbInfo,
    Tables,
    Open(PathBuf),
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
        for command in self.dot_commands()? {
            match command {
                DotCommand::Timer(enabled) => config.timer_enabled = enabled,
                _ => bail!(SqliteParseError::UnsupportedShellCommand(
                    command.to_string()
                )),
            }
        }
        Ok(config)
    }

    fn dot_commands(&self) -> Result<Vec<DotCommand>> {
        self.cmds.iter().map(|cmd| DotCommand::parse(cmd)).collect()
    }
}

impl UserInput {
    pub fn parse(input: &str) -> Result<Self> {
        if input.trim_start().starts_with('.') {
            Ok(Self::Dot(DotCommand::parse(input)?))
        } else {
            Ok(Self::Sql(SqlStatement::parse(input)?))
        }
    }
}

impl DotCommand {
    fn parse(input: &str) -> Result<Self> {
        match input {
            ".dbinfo" => Ok(Self::DbInfo),
            ".tables" => Ok(Self::Tables),
            ".timer on" => Ok(Self::Timer(true)),
            ".timer off" => Ok(Self::Timer(false)),
            _ => {
                if let Some(path) = input.strip_prefix(".open ") {
                    let path = path.trim();
                    if !path.is_empty() {
                        return Ok(Self::Open(PathBuf::from(path)));
                    }
                }
                bail!(SqliteParseError::UnsupportedShellCommand(input.to_owned()))
            }
        }
    }

    fn to_string(&self) -> String {
        match self {
            Self::DbInfo => ".dbinfo".to_owned(),
            Self::Tables => ".tables".to_owned(),
            Self::Open(path) => format!(".open {}", path.display()),
            Self::Timer(true) => ".timer on".to_owned(),
            Self::Timer(false) => ".timer off".to_owned(),
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
        assert_eq!(
            UserInput::parse(".open /tmp/sample.db").unwrap(),
            UserInput::Dot(DotCommand::Open(PathBuf::from("/tmp/sample.db")))
        );
        assert_eq!(
            UserInput::parse(".timer on").unwrap(),
            UserInput::Dot(DotCommand::Timer(true))
        );
        assert_eq!(
            UserInput::parse(".timer off").unwrap(),
            UserInput::Dot(DotCommand::Timer(false))
        );
    }

    #[test]
    fn rejects_open_without_path() {
        let error = UserInput::parse(".open ").unwrap_err();
        let error = error
            .downcast_ref::<SqliteParseError>()
            .expect("error should downcast to SqliteParseError");

        assert!(matches!(
            error,
            SqliteParseError::UnsupportedShellCommand(command) if command == ".open "
        ));
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

    #[test]
    fn non_timer_cmd_is_rejected_in_cmd_flag() {
        let cli = Cli::parse_from(["sqlite-rust", "--cmd", ".tables", "sample.db", ".dbinfo"]);

        let error = cli.shell_config().unwrap_err();
        let error = error
            .downcast_ref::<SqliteParseError>()
            .expect("error should downcast to SqliteParseError");

        assert!(matches!(
            error,
            SqliteParseError::UnsupportedShellCommand(command) if command == ".tables"
        ));
    }
}
