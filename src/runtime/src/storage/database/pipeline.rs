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

use std::{
    collections::HashMap,
    io::ErrorKind,
    mem::take,
    pin::Pin,
    sync::Arc,
    task::{Poll, Waker},
};

use futures::{channel::oneshot, Future};
use parking_lot::Mutex;

use super::{tributary::PartialStream, txn::TxnContext};
use crate::{
    storage::{
        log::manager::{LogEngine, LogFileManager},
        util::convert_to_record,
    },
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

    pub fn submit<T: WriterOwner>(
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
        let waiter_index = self.next_waiter_index;
        self.next_waiter_index += 1;
        WriterWaiter::failed(owner, waiter_index, err_kind)
    }

    pub fn submit_txn<T: WriterOwner>(
        &mut self,
        owner: Arc<Mutex<T>>,
        txn: Option<TxnContext>,
    ) -> WriterWaiter<T> {
        let waiter_index = self.next_waiter_index;
        self.next_waiter_index += 1;
        if let Some(txn) = txn {
            let record = convert_to_record(self.stream_id, &txn);
            let receiver = self.log.add_record(record);
            self.txn_table.insert(waiter_index, txn);
            WriterWaiter::new(owner, waiter_index, receiver)
        } else {
            WriterWaiter::received(owner, waiter_index)
        }
    }

    pub fn register_reading_waiter(&mut self, waiter: Waker) {
        self.reading_waiters.push(waiter);
    }

    fn apply_txn(
        &mut self,
        stream: &mut PartialStream<LogFileManager>,
        waiter_index: usize,
        result: Option<IOKindResult<u64>>,
    ) -> IOKindResult<()> {
        debug_assert_eq!(self.waked_waiter_index + 1, waiter_index);
        let txn = self.txn_table.remove(&waiter_index);
        self.waked_waiter_index = waiter_index;

        match result {
            Some(Ok(log_number)) => {
                if let Some(txn) = txn {
                    // TODO: async it!
                    stream.commit(log_number, txn);
                    self.wake_reading_waiters();
                }
            }
            Some(Err(err)) => {
                self.last_error_kind = Some(err);
                if let Some(txn) = txn {
                    stream.rollback(txn);
                }
            }
            None => todo!(),
        }

        if let Some(a) = self.waiter_table.remove(&(waiter_index + 1)) {
            Waker::wake(a)
        }

        if let Some(err) = self.last_error_kind.take() {
            Err(err)
        } else {
            Ok(())
        }
    }

    fn wait_with_waker(&mut self, waiter_index: usize, waker: Waker) {
        self.waiter_table.insert(waiter_index, waker);
    }

    fn wake_reading_waiters(&mut self) {
        take(&mut self.reading_waiters)
            .into_iter()
            .for_each(Waker::wake);
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

impl<T: WriterOwner> WriterWaiter<T> {
    fn new(
        owner: Arc<Mutex<T>>,
        waiter_index: usize,
        receiver: oneshot::Receiver<IOKindResult<u64>>,
    ) -> Self {
        WriterWaiter {
            owner,
            waiter_index,
            state: WaiterState::Writing(receiver),
        }
    }

    fn received(owner: Arc<Mutex<T>>, waiter_index: usize) -> Self {
        WriterWaiter {
            owner,
            waiter_index,
            state: WaiterState::Received(None),
        }
    }

    fn failed(owner: Arc<Mutex<T>>, waiter_index: usize, err_kind: ErrorKind) -> Self {
        WriterWaiter {
            owner,
            waiter_index,
            state: WaiterState::Received(Some(Err(err_kind))),
        }
    }
}

impl<T: WriterOwner> Future for WriterWaiter<T> {
    type Output = IOKindResult<()>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                WaiterState::Writing(ref mut receiver) => {
                    this.state = match futures::ready!(Pin::new(receiver).poll(cx)) {
                        Ok(result) => WaiterState::Received(Some(result)),
                        Err(_) => panic!("a waiter is canceled"),
                    }
                }
                WaiterState::Received(result) => {
                    let mut owner = this.owner.lock();
                    let (stream, writer) = owner.borrow_pipelined_writer_mut();
                    if writer.waked_waiter_index + 1 == this.waiter_index {
                        return Poll::Ready(writer.apply_txn(
                            stream,
                            this.waiter_index,
                            result.take(),
                        ));
                    } else {
                        writer.wait_with_waker(this.waiter_index, cx.waker().clone());
                        return Poll::Pending;
                    }
                }
            }
        }
    }
}
