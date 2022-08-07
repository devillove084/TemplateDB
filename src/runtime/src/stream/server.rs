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
use tonic::{Status, Response, Request};

use crate::{node_server::NodeServer, TenantRequest, TenantResponse, StreamRequest, StreamResponse, SegmentRequest, SegmentResponse, HeartbeatRequest, HeartbeatResponse, TenantRequestUnion, TenantResponseUnion, ListTenantsRequest, ListTenantsResponse, CreateTenantRequest, CreateTenantResponse, DescribeTenantRequest, DescribeTenantResponse, StreamRequestUnion, StreamResponseUnion, ListStreamRequest, ListStreamResponse, CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest, DescribeStreamResponse, SegmentRequestUnion, SegmentResponseUnion, GetSegmentRequest, GetSegmentResponse, SealRequest, SealResponse};

use super::{node::{Node, Config}, error::Result, tenant::Tenant, streams::StreamInfo};

pub struct HandleServer {
    node: Node,
}

type TonicResult<T> = std::result::Result<T, Status>;

impl HandleServer {
    // TODO: Support address lookup
    pub fn new(stores: Vec<String>) -> Self {
        Self {
            node: Node::new(Config::default(), stores),
        }
    }

    pub fn into_service(self) -> NodeServer<Self> {
        NodeServer::new(self)
    }
}

#[tonic::async_trait]
impl crate::node_server::Node for HandleServer {
    async fn tenant(&self, req: Request<TenantRequest>) -> TonicResult<Response<TenantResponse>> {
        todo!()
    }

    async fn stream(&self, req: Request<StreamRequest>) -> TonicResult<Response<StreamResponse>> {
        todo!()
    }

    async fn segment(&self, req: Request<SegmentRequest>) -> TonicResult<Response<SegmentResponse>> {
        todo!()
    }

    async fn heartbeat(&self, req: Request<HeartbeatRequest>) -> TonicResult<Response<HeartbeatResponse>> {
        todo!()
    }
}

impl HandleServer {
    pub(crate) async fn handle_tenant(&self, req: TenantRequest) -> Result<TenantResponse> {
        todo!()
    }

    pub(crate) async fn handle_tenant_union(&self, req: TenantRequestUnion) -> Result<TenantResponseUnion> {
        todo!()
    }

    async fn handle_list_tenants(&self, _: ListTenantsRequest) -> Result<ListTenantsResponse> {
        todo!()
    }

    async fn handle_create_tenant(&self, req: CreateTenantRequest) -> Result<CreateTenantResponse> {
        todo!()
    }

    async fn handle_describe_tenant(&self, req: DescribeTenantRequest) -> Result<DescribeTenantResponse> {
        todo!()
    }

    async fn handle_stream(&self, req: StreamRequest) -> Result<StreamResponse> {
        todo!()
    }

    async fn handle_stream_union(&self, req: StreamRequestUnion) -> Result<StreamResponseUnion> {
        todo!()
    }

    async fn handle_list_streams(&self, tenant: Tenant, _: ListStreamRequest) -> Result<ListStreamResponse> {
        todo!()
    }

    async fn handle_create_stream(&self, tenant: Tenant, req: CreateStreamRequest) -> Result<CreateStreamResponse> {
        todo!()
    }

    async fn handle_describe_stream(&self, tenant: Tenant, req: DescribeStreamRequest) -> Result<DescribeStreamResponse> {
        todo!()
    }

    async fn handle_segment(&self, req: SegmentRequest) -> Result<SegmentResponse> {
        todo!()
    }

    async fn handle_segment_union(&self, stream: &StreamInfo, req: SegmentRequestUnion) -> Result<SegmentResponseUnion> {
        todo!()
    }

    async fn handle_get_segment(&self, stream: &StreamInfo, req: GetSegmentRequest) -> Result<GetSegmentResponse> {
        todo!()
    }

    async fn handle_seal_segment(&self, stream: &StreamInfo, req: SealRequest) -> Result<SealResponse> {
        todo!()
    }
}
