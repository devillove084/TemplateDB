use tonic::Status;

use crate::{
    memtable::{
        key_format::{InternalKeyComparator, LookupKey},
        memtable::MemTable,
        value_format::ValueType,
    },
    memtable_service::{
        memtable_service_server::MemtableService, ListKvRequest, ListKvResponse, UpdateKvRequest,
        UpdateKvResponse,
    },
    util::comparator::Comparator,
};

#[derive(Clone)]
pub struct MemtableServiceHandler<C: Comparator> {
    memtable: MemTable<C>,
}

impl<C: Comparator> Unpin for MemtableServiceHandler<C> {}

impl<C: Comparator> MemtableServiceHandler<C> {
    pub fn new_with_memtable(memtable: MemTable<C>) -> Self {
        Self { memtable }
    }
}

impl<C> Default for MemtableServiceHandler<C>
where
    C: Comparator,
{
    fn default() -> Self {
        let comparator = C::default();
        let icmp = InternalKeyComparator::new(comparator);
        let memtable = MemTable::new(1 << 32, icmp);

        Self { memtable }
    }
}

impl<C: Comparator + 'static> MemtableServiceHandler<C> {
    pub async fn list_kv_handler(
        &self,
        req: tonic::Request<ListKvRequest>,
    ) -> Result<tonic::Response<ListKvResponse>, Status> {
        self.list_kv(req).await
    }

    pub async fn update_kv_handler(
        &self,
        req: tonic::Request<UpdateKvRequest>,
    ) -> Result<tonic::Response<UpdateKvResponse>, Status> {
        self.update_kv(req).await
    }
}

#[tonic::async_trait]
impl<C: Comparator + 'static> MemtableService for MemtableServiceHandler<C> {
    async fn list_kv(
        &self,
        req: tonic::Request<ListKvRequest>,
    ) -> Result<tonic::Response<ListKvResponse>, Status> {
        let req = req.get_ref();
        let tenant: String = req.tenant.clone();
        let seq = req.seq;
        let key: String = req.key.clone();
        info!("Now req is {:?} and {:?}", tenant, &key);
        let res = self
            .memtable
            .get(&LookupKey::new(key.as_bytes(), seq))
            .expect("memtable get failed");
        if res.is_ok() {
            let resp_value =
                String::from_utf8(res.unwrap()).expect("memtable get result to string failed");
            return Ok(tonic::Response::new(ListKvResponse { value: resp_value }));
        } else {
            error!("memtable get failed");
            return Err(Status::aborted("memtable process error"));
        }
    }

    async fn update_kv(
        &self,
        req: tonic::Request<UpdateKvRequest>,
    ) -> Result<tonic::Response<UpdateKvResponse>, Status> {
        let req = req.get_ref();
        let tenant: String = req.tenant.clone();
        let key: String = req.key.clone();
        let value: Option<String> = req.value.clone();
        let seq = req.seq;
        let r_type = req.value_type();

        info!(
            "Now write kv on tenant: {:?}, key and value is {:?}, {:?}, on seq is {:?}",
            tenant, key, value, seq
        );
        let value_type = match r_type {
            crate::memtable_service::ValueType::NormalValue => 0,
            crate::memtable_service::ValueType::Deletion => 1,
            crate::memtable_service::ValueType::Unknown => 2,
        };

        if value.is_some() {
            self.memtable.add(
                seq,
                ValueType::Value,
                key.as_bytes(),
                value.unwrap().as_bytes(),
            );
        }

        Ok(tonic::Response::new(UpdateKvResponse {
            tenant: tenant,
            ack: true,
            seq,
            value_type,
        }))
    }
}
