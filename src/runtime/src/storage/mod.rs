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

pub mod database;
pub mod fs;
pub mod log;
pub mod server;
pub mod util;

#[cfg(debug_assertions)]
pub use tests::build_store;

#[cfg(debug_assertions)]
mod tests {
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;

    use super::{
        database::{dboption::DBOption, streamdb::StreamDB},
        server::StorageServer,
    };
    use crate::stream::error::Result;

    pub async fn build_store() -> Result<String> {
        let tmp = tempfile::tempdir()?;
        let db_opt = DBOption {
            create_if_missing: true,
            ..Default::default()
        };
        let db = StreamDB::open(tmp, db_opt).await?;
        let listener = TcpListener::bind("127.0.0.1:9999").await?;
        let local_addr = listener.local_addr()?;
        tokio::task::spawn(async move {
            let server = StorageServer::new(db);
            tonic::transport::Server::builder()
                .add_service(server.into_service())
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        Ok(format!("http://{}", local_addr))
    }
}
