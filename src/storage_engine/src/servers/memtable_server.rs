use actix::{Actor, AsyncContext, Context, Handler, Message, WrapFuture};
use tokio::runtime::Handle;
use tonic::{transport::Server, Status};

use crate::{
    memtable::value_format::ValueType,
    memtable_service::{
        memtable_service_server::MemtableServiceServer, ListKvRequest, ListKvResponse,
        UpdateKvRequest, UpdateKvResponse,
    },
    services::memtable_service::MemtableServiceHandler,
    util::comparator::Comparator,
};

pub struct MemTableServer<C: Comparator> {
    memtable_handler: MemtableServiceHandler<C>,
    // rpc_server: Option<tonic::transport::Server>,
    rpc_addr: String,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

impl<C: Comparator> Clone for MemTableServer<C> {
    fn clone(&self) -> Self {
        Self {
            memtable_handler: self.memtable_handler.clone(),
            rpc_addr: self.rpc_addr.clone(),
            shutdown: None,
        }
    }
}

impl<C: Comparator> MemTableServer<C> {
    pub fn new(memtable_handler: MemtableServiceHandler<C>, rpc_addr: String) -> Self {
        Self {
            memtable_handler,
            // rpc_server,
            rpc_addr,
            shutdown: None,
        }
    }
}

impl<C: Comparator + 'static> Actor for MemTableServer<C> {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let grpc_addr = self.rpc_addr.parse().unwrap();
        let grpc_service = MemtableServiceServer::new(self.memtable_handler.clone());
        let server = Server::builder().add_service(grpc_service).serve(grpc_addr);
        let mut server = Box::pin(server);
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.shutdown = Some(tx);

        ctx.spawn(
            async move {
                tokio::select! {
                    _ = server.as_mut() => {
                        if let Err(e) = server.await {
                            error!("tonic error on grpc in memtable actor: {:?}", e);
                        }
                    }
                    _ = rx => {}
                }
            }
            .into_actor(self),
        );
    }

    fn stopping(&mut self, _ctx: &mut Self::Context) -> actix::Running {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        actix::Running::Stop
    }
}

pub struct Insert {
    tenant: String,
    key: String,
    value: Option<String>,
    value_type: ValueType,
    seq: u64,
}

impl Into<UpdateKvRequest> for Insert {
    fn into(self) -> UpdateKvRequest {
        let tenant = self.tenant;
        let key = self.key;
        let value = self.value;
        let value_type = match self.value_type {
            ValueType::Deletion => 1,
            ValueType::Value => 0,
            ValueType::Unknown => 2,
        };
        let seq = self.seq;
        UpdateKvRequest {
            tenant,
            seq,
            value_type,
            key,
            value,
        }
    }
}

impl Message for Insert {
    type Result = Result<tonic::Response<UpdateKvResponse>, Status>;
}

pub struct Get {
    tenant: String,
    seq: u64,
    key: String,
}

impl Into<ListKvRequest> for Get {
    fn into(self) -> ListKvRequest {
        let tenant = self.tenant;
        let key = self.key;
        let seq = self.seq;
        ListKvRequest { tenant, seq, key }
    }
}

impl Message for Get {
    type Result = Result<tonic::Response<ListKvResponse>, Status>;
}

impl<C: Comparator + 'static> Handler<Insert> for MemTableServer<C> {
    type Result = Result<tonic::Response<UpdateKvResponse>, Status>;

    fn handle(&mut self, msg: Insert, _ctx: &mut Context<Self>) -> Self::Result {
        let result = Handle::current().block_on(async move {
            return self
                .memtable_handler
                .update_kv_handler(tonic::Request::new(msg.into()))
                .await;
        });
        return result;
    }
}

impl<C: Comparator + 'static> Handler<Get> for MemTableServer<C> {
    type Result = Result<tonic::Response<ListKvResponse>, Status>;

    fn handle(&mut self, msg: Get, _ctx: &mut Context<Self>) -> Self::Result {
        let result = Handle::current().block_on(async move {
            return self
                .memtable_handler
                .list_kv_handler(tonic::Request::new(msg.into()))
                .await;
        });
        return result;
    }
}
