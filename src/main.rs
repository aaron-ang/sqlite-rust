use anyhow::Result;
use clap::Parser;

use sqlite_rust::cli::{Cli, Command};
use sqlite_rust::sqlite::SqliteDatabase;

fn main() -> Result<()> {
    let cli = Cli::parse();
    let database = SqliteDatabase::open(&cli.database_path)?;

    match cli.command {
        Command::DbInfo => {
            let info = database.db_info();
            println!("database page size: {}", info.page_size);
            println!("number of tables: {}", info.table_count);
        }
    }

    Ok(())
}
