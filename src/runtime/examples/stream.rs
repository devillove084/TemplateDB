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
//use tokio_stream::wrappers::TcpListenerStream;
use runtime::stream::server::HandleServer;
use tokio::net::TcpListener;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, default_value_t = String::from("0.0.0.0:21716"))]
    endpoint: String,

    #[clap(short, long)]
    stores: Vec<String>,
}

async fn bootstrap_service(endpoint: &str, replicas: &[String]) -> Result<()> {
    let server = HandleServer::new(replicas.to_owned());
    let _listener = TcpListener::bind(endpoint).await?;
    let _service = tonic::transport::Server::builder().add_service(server.into_service());
    //service.serve_with_incoming(TcpListenerStream::new(listener)).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = Args::parse();
    args.stores = vec!["".to_string()];
    bootstrap_service(&args.endpoint, &args.stores).await?;

    println!("Bye");

    Ok(())
}
