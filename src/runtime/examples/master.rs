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

use clap::Parser;
//use stream_engine_master::Server as MasterServer;
use runtime::stream::{
    master::server::Server as MasterServer,
    store::{db::stream_db::StreamDb, opt::DbOption, server::Server as StoreServer},
};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, default_value_t = String::from("0.0.0.0:21716"))]
    endpoint: String,

    #[clap(short, long, required = true)]
    stores: Vec<String>,
}

async fn bootstrap_service(endpoint: &str, replicas: &[String]) -> Result<()> {
    let master_server = MasterServer::new(replicas.to_owned());
    let listener = TcpListener::bind(endpoint).await?;
    tonic::transport::Server::builder()
        .add_service(master_server.into_service())
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await?;

    Ok(())
}

pub async fn build_store() -> Result<String> {
    let tmp = tempfile::tempdir()?;
    let db_opt = DbOption {
        create_if_missing: true,
        ..Default::default()
    };
    let db = StreamDb::open(tmp, db_opt)?;

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let local_addr = listener.local_addr()?;
    tokio::task::spawn(async move {
        let server = StoreServer::new(db);
        tonic::transport::Server::builder()
            .add_service(server.into_service())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    Ok(format!("http://{}", local_addr))
}

#[tokio::main]
async fn main() -> Result<()> {
    let addr = build_store().await?;
    bootstrap_service("0.0.0.0:21716", [addr].as_slice()).await?;

    println!("Bye");

    Ok(())
}
