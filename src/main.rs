use anyhow::{Result, bail};
use std::ffi::OsString;
use std::io::{self, BufWriter, IsTerminal, Read, Write};

use clap::Parser;
use sqlite_rust::{
    cli::{Cli, DotCommand, UserInput},
    db::SqliteDB,
    query::SqlStatement,
    timer::TimingSnapshot,
};

fn main() -> Result<()> {
    let args = normalize_sqlite_args(std::env::args_os());
    let cli = Cli::parse_from(args);
    let database = SqliteDB::open(&cli.database_path)?;

    let shell_config = cli.shell_config()?;

    match cli.input.as_deref() {
        Some(_) => execute_positional_input(&database, &cli, shell_config.timer_enabled)?,
        None => execute_stdin_input(&database, shell_config.timer_enabled)?,
    }

    Ok(())
}

fn execute_positional_input(database: &SqliteDB, cli: &Cli, timer_enabled: bool) -> Result<()> {
    match cli.user_input()? {
        UserInput::Dot(DotCommand::DbInfo) => {
            let timer = timer_enabled.then(TimingSnapshot::start).transpose()?;
            let info = database.db_info();
            println!("database page size: {}", info.page_size);
            println!("number of tables: {}", info.table_count);

            if let Some(timer) = timer {
                eprintln!("{}", timer.finish()?.format_sqlite());
            }
        }
        UserInput::Dot(DotCommand::Tables) => {
            let timer = timer_enabled.then(TimingSnapshot::start).transpose()?;
            println!("{}", database.table_names().join(" "));

            if let Some(timer) = timer {
                eprintln!("{}", timer.finish()?.format_sqlite());
            }
        }
        UserInput::Sql(statements) => execute_sql_batch(database, &statements, timer_enabled)?,
    }

    Ok(())
}

fn execute_stdin_input(database: &SqliteDB, timer_enabled: bool) -> Result<()> {
    let mut stdin = io::stdin();
    if stdin.is_terminal() {
        bail!("no SQL input provided");
    }

    let mut sql = String::new();
    stdin.read_to_string(&mut sql)?;

    execute_sql_batch(database, &SqlStatement::parse(&sql)?, timer_enabled)
}

fn execute_sql_batch(
    database: &SqliteDB,
    statements: &[SqlStatement],
    timer_enabled: bool,
) -> Result<()> {
    for statement in statements {
        let timer = timer_enabled.then(TimingSnapshot::start).transpose()?;
        execute_sql_statement(database, statement)?;
        if let Some(timer) = timer {
            eprintln!("{}", timer.finish()?.format_sqlite());
        }
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
