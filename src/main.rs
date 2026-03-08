use anyhow::Result;
use clap::Parser;

use sqlite_rust::cli::{Cli, DotCommand, UserInput};
use sqlite_rust::query::SqlStatement;
use sqlite_rust::db::SqliteDB;

fn main() -> Result<()> {
    let cli = Cli::parse();
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
        }) => {
            println!(
                "{}",
                database
                    .select_rows(&table_name, &column_names)?
                    .join("\n")
            );
        }
    }

    Ok(())
}
