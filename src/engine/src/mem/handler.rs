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

use tonic::Status;

use super::MemTable;
use crate::{
    db::format::{InternalKeyComparator, LookupKey},
    memtable_service_server::MemtableService,
    Comparator, ListKvRequest, ListKvResponse, UpdateKvRequest, UpdateKvResponse, ValueType,
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
    pub async fn list_kv_handler(&self, req: tonic::Request<ListKvRequest>) -> Result<tonic::Response<ListKvResponse>, Status> {
        self.list_kv(req).await
    }

    pub async fn update_kv_handler(&self, req: tonic::Request<UpdateKvRequest>) -> Result<tonic::Response<UpdateKvResponse>, Status> {
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
            ValueType::NormalValue => 0,
            ValueType::Deletion => 1,
            ValueType::Unknown => 2,
        };

        if value.is_some() {
            self.memtable.add(
                seq,
                crate::db::format::ValueType::Value,
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
