// Copyright 2022 The template Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
    let start = Server::builder().add_service(store_server.into_service());
    start
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
    args.db = PathBuf::from("./db");
    let db = StreamDB::open(args.db, opt)?;
    bootstrap_service(&args.endpoint, db).await?;
    println!("Bye");
    Ok(())
}
