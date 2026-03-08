use anyhow::Result;
use clap::Parser;

use sqlite_rust::cli::{Cli, UserInput};
use sqlite_rust::db::SqliteDB;

fn main() -> Result<()> {
    let cli = Cli::parse();
    let database = SqliteDB::open(&cli.database_path)?;

    match cli.user_input()? {
        UserInput::DbInfo => {
            let info = database.db_info();
            println!("database page size: {}", info.page_size);
            println!("number of tables: {}", info.table_count);
        }
        UserInput::Tables => {
            println!("{}", database.table_names()?.join(" "));
        }
        UserInput::CountRows { table_name } => {
            println!("{}", database.count_rows(&table_name)?);
        }
    }

    Ok(())
}
