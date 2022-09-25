use futures::future::join_all;
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

use super::{
    error::{Error, Result},
    node::{Config, Node},
    streams::{ObserverMeta, StreamInfo},
    tenant::Tenant,
};
use crate::{
    node_server::NodeServer, segment_request_union, segment_response_union, stream_request_union,
    stream_response_union, tenant_request_union, tenant_response_union, CreateStreamRequest,
    CreateStreamResponse, CreateTenantRequest, CreateTenantResponse, DescribeStreamRequest,
    DescribeStreamResponse, DescribeTenantRequest, DescribeTenantResponse, GetSegmentRequest,
    GetSegmentResponse, HeartbeatRequest, HeartbeatResponse, ListStreamsRequest,
    ListStreamsResponse, ListTenantsRequest, ListTenantsResponse, SealSegmentRequest,
    SealSegmentResponse, SegmentRequest, SegmentRequestUnion, SegmentResponse,
    SegmentResponseUnion, StreamRequest, StreamRequestUnion, StreamResponse, StreamResponseUnion,
    TenantRequest, TenantRequestUnion, TenantResponse, TenantResponseUnion,
};

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
        let req = req.into_inner();
        let res = self.handle_tenant(req).await?;
        Ok(Response::new(res))
    }

    async fn stream(&self, req: Request<StreamRequest>) -> TonicResult<Response<StreamResponse>> {
        let req = req.into_inner();
        let res = self.handle_stream(req).await?;
        Ok(Response::new(res))
    }

    async fn segment(
        &self,
        req: Request<SegmentRequest>,
    ) -> TonicResult<Response<SegmentResponse>> {
        let req = req.into_inner();
        let res = self.handle_segment(req).await?;
        Ok(Response::new(res))
    }

    async fn heartbeat(
        &self,
        req: Request<HeartbeatRequest>,
    ) -> TonicResult<Response<HeartbeatResponse>> {
        let req = req.into_inner();
        let tenant = self.node.tenant(&req.tenant).await?;
        let stream_info = tenant.stream(req.stream_id).await?;

        let observer_meta = ObserverMeta {
            stream_name: stream_info.stream_name.clone(),
            observer_id: req.observer_id,
            state: req.observer_state.into(),
            epoch: req.writer_epoch,
            acked_seq: req.acked_seq.into(),
        };

        let commands = stream_info
            .heartbeat(
                &self.node.config,
                &self.node.stores,
                observer_meta,
                req.role.into(),
            )
            .await?;
        Ok(Response::new(HeartbeatResponse { commands }))
    }
}

impl HandleServer {
    pub(crate) async fn handle_tenant(&self, req: TenantRequest) -> Result<TenantResponse> {
        let mut res = TenantResponse::default();
        for req_union in req.requests {
            let ru = self.handle_tenant_union(req_union).await?;
            res.responses.push(ru);
        }
        Ok(res)
    }

    pub(crate) async fn handle_tenant_union(
        &self,
        req: TenantRequestUnion,
    ) -> Result<TenantResponseUnion> {
        type Request = tenant_request_union::Request;
        type Response = tenant_response_union::Response;

        let req = req
            .request
            .ok_or_else(|| Error::InvalidArgument("tenant request".into()))?;
        let res = match req {
            tenant_request_union::Request::ListTenants(req) => {
                let res = self.handle_list_tenants(req).await?;
                Response::ListTenants(res)
            }
            tenant_request_union::Request::CreateTenant(req) => {
                let res = self.handle_create_tenant(req).await?;
                Response::CreateTenant(res)
            }
            tenant_request_union::Request::UpdateTenant(_) => todo!(),
            tenant_request_union::Request::DeleteTenant(_) => todo!(),
            tenant_request_union::Request::DescribeTenant(req) => {
                let res = self.handle_describe_tenant(req).await?;
                Response::DescribeTenant(res)
            }
        };

        Ok(TenantResponseUnion {
            response: Some(res),
        })
    }

    async fn handle_list_tenants(&self, _: ListTenantsRequest) -> Result<ListTenantsResponse> {
        let tenants = self.node.tenants().await?;
        let descs = join_all(tenants.iter().map(Tenant::desc)).await;
        Ok(ListTenantsResponse { descs })
    }

    async fn handle_create_tenant(&self, req: CreateTenantRequest) -> Result<CreateTenantResponse> {
        let desc = req
            .desc
            .ok_or_else(|| Error::InvalidArgument("tenant request".into()))?;
        let new_desc = self.node.create_tenant(desc).await?;
        Ok(CreateTenantResponse {
            desc: Some(new_desc),
        })
    }

    async fn handle_describe_tenant(
        &self,
        req: DescribeTenantRequest,
    ) -> Result<DescribeTenantResponse> {
        let tenant = self.node.tenant(&req.name).await?;
        let desc = tenant.desc().await;
        Ok(DescribeTenantResponse { desc: Some(desc) })
    }

    async fn handle_stream(&self, req: StreamRequest) -> Result<StreamResponse> {
        let tenant = self.node.tenant(&req.tenant).await?;
        let mut resp = StreamResponse::default();
        for req_union in req.requests {
            let ru = self.handle_stream_union(tenant.clone(), req_union).await?;
            resp.responses.push(ru);
        }
        Ok(resp)
    }

    async fn handle_stream_union(
        &self,
        tenant: Tenant,
        req: StreamRequestUnion,
    ) -> Result<StreamResponseUnion> {
        type Request = stream_request_union::Request;
        type Response = stream_response_union::Response;

        let req = req
            .request
            .ok_or_else(|| Error::InvalidArgument("stream reqest".into()))?;
        let res = match req {
            stream_request_union::Request::ListStreams(req) => {
                let res = self.handle_list_streams(tenant, req).await?;
                Response::ListStreams(res)
            }
            stream_request_union::Request::CreateStream(req) => {
                let res = self.handle_create_stream(tenant, req).await?;
                Response::CreateStream(res)
            }
            stream_request_union::Request::UpdateStream(_) => todo!(),
            stream_request_union::Request::DeleteStream(_) => todo!(),
            stream_request_union::Request::DescribeStream(req) => {
                let res = self.handle_describe_stream(tenant, req).await?;
                Response::DescribeStream(res)
            }
        };

        Ok(StreamResponseUnion {
            response: Some(res),
        })
    }

    async fn handle_list_streams(
        &self,
        tenant: Tenant,
        _: ListStreamsRequest,
    ) -> Result<ListStreamsResponse> {
        let descs = tenant.stream_descs().await?;
        Ok(ListStreamsResponse { descs })
    }

    async fn handle_create_stream(
        &self,
        tenant: Tenant,
        req: CreateStreamRequest,
    ) -> Result<CreateStreamResponse> {
        let desc = req
            .desc
            .ok_or_else(|| Error::InvalidArgument("stream request".into()))?;
        let new_desc = tenant.create_stream(desc).await?;
        Ok(CreateStreamResponse {
            desc: Some(new_desc),
        })
    }

    async fn handle_describe_stream(
        &self,
        tenant: Tenant,
        req: DescribeStreamRequest,
    ) -> Result<DescribeStreamResponse> {
        let desc = tenant.stream_desc(&req.name).await?;
        Ok(DescribeStreamResponse { desc: Some(desc) })
    }

    async fn handle_segment(&self, req: SegmentRequest) -> Result<SegmentResponse> {
        let tenant = self.node.tenant(&req.tenant).await?;
        let stream = tenant.stream(req.stream_id).await?;
        let mut resp = SegmentResponse::default();
        for req_union in req.requests {
            let ru = self.handle_segment_union(&stream, req_union).await?;
            resp.responses.push(ru);
        }
        Ok(resp)
    }

    async fn handle_segment_union(
        &self,
        stream: &StreamInfo,
        req: SegmentRequestUnion,
    ) -> Result<SegmentResponseUnion> {
        type Request = segment_request_union::Request;
        type Response = segment_response_union::Response;

        let res = match req
            .request
            .ok_or_else(|| Error::InvalidArgument("segment request".into()))?
        {
            Request::GetSegment(req) => {
                Response::GetSegment(self.handle_get_segment(stream, req).await?)
            }
            Request::SealSegment(req) => {
                Response::SealSegment(self.handle_seal_segment(stream, req).await?)
            }
        };
        Ok(SegmentResponseUnion {
            response: Some(res),
        })
    }

    async fn handle_get_segment(
        &self,
        stream: &StreamInfo,
        req: GetSegmentRequest,
    ) -> Result<GetSegmentResponse> {
        let segment = stream.segment(req.segment_epoch).await;
        Ok(GetSegmentResponse { desc: segment })
    }

    async fn handle_seal_segment(
        &self,
        stream: &StreamInfo,
        req: SealSegmentRequest,
    ) -> Result<SealSegmentResponse> {
        stream.seal(req.segment_epoch).await?;
        Ok(SealSegmentResponse {})
    }
}
