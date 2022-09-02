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

use std::{collections::HashMap, io::ErrorKind, sync::Arc, task::Waker};

use futures::channel::oneshot;
use tokio::sync::Mutex;

use super::{tributary::PartialStream, txn::TxnContext};
use crate::{
    storage::log::manager::{LogEngine, LogFileManager},
    stream::error::IOKindResult,
};

pub struct PipelinedWriter {
    stream_id: u64,
    log: LogEngine,
    last_error_kind: Option<ErrorKind>,
    next_waiter_index: usize,
    waked_waiter_index: usize,
    txn_table: HashMap<usize, TxnContext>,
    waiter_table: HashMap<usize, Waker>,
    reading_waiters: Vec<Waker>,
}

impl PipelinedWriter {
    pub fn new(stream_id: u64, log_engine: LogEngine) -> Self {
        PipelinedWriter {
            stream_id,
            log: log_engine,
            last_error_kind: None,
            next_waiter_index: 1,
            waked_waiter_index: 0,
            txn_table: HashMap::new(),
            waiter_table: HashMap::new(),
            reading_waiters: Vec::new(),
        }
    }

    pub async fn submit<T: WriterOwner>(
        &mut self,
        owner: Arc<Mutex<T>>,
        result: IOKindResult<Option<TxnContext>>,
    ) -> WriterWaiter<T> {
        match result {
            Ok(txn) => self.submit_txn(owner, txn),
            Err(err) => self.submit_barrier(owner, err),
        }
    }

    pub fn submit_barrier<T: WriterOwner>(
        &mut self,
        owner: Arc<Mutex<T>>,
        err_kind: ErrorKind,
    ) -> WriterWaiter<T> {
        todo!()
    }

    pub fn submit_txn<T: WriterOwner>(
        &mut self,
        owner: Arc<Mutex<T>>,
        txn: Option<TxnContext>,
    ) -> WriterWaiter<T> {
        todo!()
    }
}

enum WaiterState {
    Writing(oneshot::Receiver<IOKindResult<u64>>),
    Received(Option<IOKindResult<u64>>),
}

pub trait WriterOwner {
    fn borrow_pipelined_writer_mut(
        &mut self,
    ) -> (&mut PartialStream<LogFileManager>, &mut PipelinedWriter);
}

pub struct WriterWaiter<T> {
    owner: Arc<Mutex<T>>,
    waiter_index: usize,
    state: WaiterState,
}
