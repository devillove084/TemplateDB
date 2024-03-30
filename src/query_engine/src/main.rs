use std::sync::Arc;

use anyhow::Result;
use query_engine::main_entry::{ClientContext, DatabaseInstance};
use query_engine::{cli, Database};

use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    // /// Optional name to operate on
    // db_path: Option<String>,
    /// Sets a custom config file
    #[arg(short, long, value_name = "DB_PATH")]
    db_path: Option<PathBuf>,
    // #[arg(short, long, value_name = "FILE")]
    // db_path: Option<PathBuf>,

    // /// Turn debugging information on
    // #[arg(short, long, action = clap::ArgAction::Count)]
    // debug: u8,

    // #[command(subcommand)]
    // command: Option<Commands>,
}

// #[derive(Subcommand)]
// enum Commands {
//     /// does testing things
//     Test {
//         /// lists test values
//         #[arg(short, long)]
//         list: bool,
//     },
// }

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let cli = Cli::parse();

    if let Some(db_path) = cli.db_path.as_deref() {
        let db = Database::new_on_templatedb(db_path);
        let dbv2 = Arc::new(DatabaseInstance::default());
        dbv2.initialize()?;
        let client_context = ClientContext::new(dbv2);
        cli::interactive(db, client_context).await?;
    }

    Ok(())
}

// fn create_csv_table(db: &Database, table_name: &str) -> Result<()> {
//     let table_name = table_name.to_string();
//     let filepath = format!("/home/luhuanbing/TemplateDB/tests/csv/{}.csv", table_name);
//     println!("file path is: {:?}", filepath);
//     db.create_csv_table(table_name, filepath)?;

//     Ok(())
// }

// fn create_template_db(db: &Database) -> Result<()> {
//     // db.create_mem_table()
//     todo!()
// }
