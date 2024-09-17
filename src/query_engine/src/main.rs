use std::sync::Arc;

use anyhow::Result;
use query_engine::main_entry::{ClientContext, DatabaseInstance};
use query_engine::{cli, Database};

use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(short, long, value_name = "DB_PATH")]
    db_path: Option<PathBuf>,
}

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
