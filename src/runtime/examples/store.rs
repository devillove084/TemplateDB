use std::path::PathBuf;

use clap::Parser;
use runtime::storage::{
    database::{dboption::DBOption, streamdb::StreamDB},
    server::StorageServer,
};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Parser, Debug, Default)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, default_value_t = String::from("0.0.0.0:21718"))]
    endpoint: String,

    #[clap(long)]
    db: PathBuf,
}

async fn bootstrap_service(endpoint: &str, db: StreamDB) -> Result<()> {
    let listener = TcpListener::bind(endpoint).await?;
    let store_server = StorageServer::new(db);
    Server::builder()
        .add_service(store_server.into_service())
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = Args::default();
    let mut opt = DBOption::default();
    opt.create_if_missing = true;
    args.endpoint = String::from("0.0.0.0:21718");
    args.db = PathBuf::from("current");
    let db = StreamDB::open(args.db, opt).await?;
    bootstrap_service(&args.endpoint, db).await?;
    println!("Bye");
    Ok(())
}
