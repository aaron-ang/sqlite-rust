use anyhow::Result;
use std::ffi::OsString;

use clap::Parser;
use sqlite_rust::{
    cli::{Cli, DotCommand, UserInput},
    db::SqliteDB,
    query::SqlStatement,
    timer::TimingSnapshot,
};

fn main() -> Result<()> {
    let args = normalize_sqlite_args(std::env::args_os());
    let timer = TimingSnapshot::start()?;
    let cli = Cli::parse_from(args);

    let database = SqliteDB::open(&cli.database_path)?;

    match cli.user_input()? {
        UserInput::Dot(DotCommand::DbInfo) => {
            let info = database.db_info();
            println!("database page size: {}", info.page_size);
            println!("number of tables: {}", info.table_count);
        }
        UserInput::Dot(DotCommand::Tables) => {
            println!("{}", database.table_names().join(" "));
        }
        UserInput::Sql(SqlStatement::SelectCount { table_name }) => {
            println!("{}", database.count_rows(&table_name)?);
        }
        UserInput::Sql(SqlStatement::SelectColumns {
            table_name,
            column_names,
            where_clause,
            order_by,
        }) => {
            println!(
                "{}",
                database
                    .select_rows(&table_name, &column_names, where_clause.as_ref(), &order_by)?
                    .join("\n")
            );
        }
    }

    if cli.shell_config()?.timer_enabled {
        eprintln!("{}", timer.finish()?.format_sqlite());
    }

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
