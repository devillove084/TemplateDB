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

use tonic::{Request, Response, Status};

use super::database::{reader::SegmentReader, streamdb::StreamDB};
use crate::{
    store_server::{Store, StoreServer},
    MutateRequest, MutateRequestUnion, MutateResponse, MutateResponseUnion, ReadRequest,
    SealRequest, SealResponse, TruncateRequest, TruncateResponse, WriteRequest, WriteResponse,
};

type Result<T> = std::result::Result<T, Status>;

#[derive(Clone)]
pub struct StorageServer {
    db: StreamDB,
}

impl StorageServer {
    pub fn new(db: StreamDB) -> Self {
        StorageServer { db }
    }

    pub fn into_service(self) -> StoreServer<StorageServer> {
        StoreServer::new(self)
    }

    async fn handle_mutate(&self, req: MutateRequest) -> Result<MutateResponse> {
        let mut resp = MutateResponse::default();
        if let Some(union_req) = req.request {
            resp.response = Some(
                self.handle_mutate_union(req.stream_id, req.writer_epoch, union_req)
                    .await?,
            );
        }
        Ok(resp)
    }

    async fn handle_mutate_union(
        &self,
        stream_id: u64,
        writer_epoch: u32,
        req: MutateRequestUnion,
    ) -> Result<MutateResponseUnion> {
        let req = req
            .request
            .ok_or_else(|| Status::invalid_argument("mutate request"))?;
        let res = match req {
            crate::mutate_request_union::Request::Write(req) => {
                crate::mutate_response_union::Response::Write(
                    self.handle_write(stream_id, writer_epoch, req).await?,
                )
            }
            crate::mutate_request_union::Request::Seal(req) => {
                crate::mutate_response_union::Response::Seal(
                    self.handle_seal(stream_id, writer_epoch, req).await?,
                )
            }
            crate::mutate_request_union::Request::Truncate(req) => {
                crate::mutate_response_union::Response::Truncate(
                    self.handle_truncate(stream_id, req).await?,
                )
            }
        };
        Ok(MutateResponseUnion {
            response: Some(res),
        })
    }

    async fn handle_write(
        &self,
        stream_id: u64,
        writer_epoch: u32,
        req: WriteRequest,
    ) -> Result<WriteResponse> {
        let (matched_index, acked_index) = self
            .db
            .write(
                stream_id,
                req.segment_epoch,
                writer_epoch,
                req.acked_seq.into(),
                req.first_index,
                req.entries.into_iter().map(Into::into).collect(),
            )
            .await?;
        Ok(WriteResponse {
            matched_index,
            acked_index,
        })
    }

    async fn handle_seal(
        &self,
        stream_id: u64,
        writer_epoch: u32,
        req: SealRequest,
    ) -> Result<SealResponse> {
        let acked_index = self
            .db
            .seal(stream_id, req.segment_epoch, writer_epoch)
            .await?;
        Ok(SealResponse { acked_index })
    }

    async fn handle_truncate(
        &self,
        stream_id: u64,
        req: TruncateRequest,
    ) -> Result<TruncateResponse> {
        self.db.truncate(stream_id, req.keep_seq.into()).await?;
        Ok(TruncateResponse {})
    }
}

#[async_trait::async_trait]
impl Store for StorageServer {
    type ReadStream = SegmentReader;

    async fn mutate(&self, input: Request<MutateRequest>) -> Result<Response<MutateResponse>> {
        Ok(Response::new(self.handle_mutate(input.into_inner()).await?))
    }

    async fn read(&self, input: Request<ReadRequest>) -> Result<Response<Self::ReadStream>> {
        let req = input.into_inner();
        let stream = self.db.read(
            req.stream_id,
            req.seg_epoch,
            req.start_index,
            req.limit as usize,
            req.require_acked,
        )?;
        Ok(Response::new(stream))
    }
}
