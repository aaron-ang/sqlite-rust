use anyhow::{Result, bail};
use std::ffi::OsString;
use std::io::{self, BufWriter, IsTerminal, Read, Write};
use std::path::Path;

use clap::Parser;
use sqlite_rust::{
    cli::{Cli, DotCommand, UserInput},
    db::SqliteDB,
    query::SqlStatement,
    timer::TimerState,
};

fn main() -> Result<()> {
    let args = normalize_sqlite_args(std::env::args_os());
    let cli = Cli::parse_from(args);
    let shell_config = cli.shell_config()?;
    let mut timer = TimerState::new(shell_config.timer_enabled);

    match cli.input.as_deref() {
        Some(_) => {
            let database = SqliteDB::open(&cli.database_path)?;
            execute_positional_input(&database, &cli, &mut timer)?;
        }
        None => execute_stdin_input(&cli.database_path, &mut timer)?,
    }

    Ok(())
}

fn execute_positional_input(database: &SqliteDB, cli: &Cli, timer: &mut TimerState) -> Result<()> {
    match cli.user_input()? {
        UserInput::Dot(DotCommand::Open(path)) => {
            SqliteDB::open(&path)?;
        }
        UserInput::Dot(dot_command) => execute_dot_command(database, dot_command, timer)?,
        UserInput::Sql(statements) => execute_sql_batch(database, &statements, timer)?,
    }
    Ok(())
}

fn execute_dot_command(
    database: &SqliteDB,
    dot_command: DotCommand,
    timer: &mut TimerState,
) -> Result<()> {
    match dot_command {
        DotCommand::DbInfo => timer.run(|| {
            let info = database.db_info();
            println!("database page size: {}", info.page_size);
            println!("number of tables: {}", info.table_count);
            Ok(())
        })?,
        DotCommand::Tables => timer.run(|| {
            println!("{}", database.table_names().join(" "));
            Ok(())
        })?,
        DotCommand::Open(_) => unreachable!("open commands are handled before execution"),
        DotCommand::Timer(enabled) => timer.set_enabled(enabled),
    }

    Ok(())
}

fn execute_stdin_input(db_path: &Path, timer: &mut TimerState) -> Result<()> {
    let mut stdin = io::stdin();
    if stdin.is_terminal() {
        bail!("no SQL input provided");
    }

    let mut sql = String::new();
    stdin.read_to_string(&mut sql)?;

    let mut database = SqliteDB::open(db_path)?;
    let mut sql_batch = String::new();

    for line in sql.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if trimmed.starts_with('.') {
            flush_sql_batch(&database, &mut sql_batch, timer)?;
            match UserInput::parse(trimmed)? {
                UserInput::Dot(DotCommand::Open(path)) => {
                    database = SqliteDB::open(&path)?;
                }
                UserInput::Dot(dot_command) => {
                    execute_dot_command(&database, dot_command, timer)?;
                }
                UserInput::Sql(_) => unreachable!("dot-prefixed input should not parse as SQL"),
            }
            continue;
        }

        sql_batch.push_str(line);
        sql_batch.push('\n');
    }

    flush_sql_batch(&database, &mut sql_batch, timer)
}

fn flush_sql_batch(database: &SqliteDB, sql_batch: &mut String, timer: &TimerState) -> Result<()> {
    if sql_batch.trim().is_empty() {
        sql_batch.clear();
        return Ok(());
    }

    let statements = SqlStatement::parse(sql_batch)?;
    execute_sql_batch(database, &statements, timer)?;
    sql_batch.clear();
    Ok(())
}

fn execute_sql_batch(
    database: &SqliteDB,
    statements: &[SqlStatement],
    timer: &TimerState,
) -> Result<()> {
    for statement in statements {
        timer.run(|| execute_sql_statement(database, statement))?;
    }
    Ok(())
}

fn execute_sql_statement(database: &SqliteDB, statement: &SqlStatement) -> Result<()> {
    let mut out = BufWriter::new(io::stdout().lock());
    database.execute(statement, &mut out)?;
    out.flush()?;

    Ok(())
}

fn normalize_sqlite_args<I, T>(args: I) -> Vec<OsString>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString>,
{
    args.into_iter()
        .map(Into::into)
        .map(|arg| {
            if arg == "-cmd" {
                OsString::from("--cmd")
            } else {
                arg
            }
        })
        .collect()
}
