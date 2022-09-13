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

use std::{collections::HashSet, sync::Arc};

use futures::channel::oneshot;
use prost::Message;

use super::{logwriter::LogWriter, manager::LogFileManager};
use crate::{
    storage::database::dboption::DBOption,
    stream::{
        channel::Channel,
        error::{IOKindResult, IOResult, Result},
    },
    RecordGroup,
};

pub struct LogWorker {
    opt: Arc<DBOption>,
    channel: Channel,
    writer: LogWriter,
    log_file_mgr: LogFileManager,

    refer_streams: HashSet<u64>,
    grouped_requests: Vec<oneshot::Sender<IOKindResult<u64>>>,
}

impl LogWorker {
    pub fn new(
        channel: Channel,
        writer: Option<LogWriter>,
        log_file_mgr: LogFileManager,
    ) -> Result<Self> {
        let opt = log_file_mgr.option();
        let writer = match writer {
            Some(w) => w,
            None => {
                let (log_number, new_log_file) = log_file_mgr.allocate_file()?;
                LogWriter::new(new_log_file, log_number, 0, opt.log.log_file_size)?
            }
        };

        Ok(LogWorker {
            opt,
            channel,
            writer,
            log_file_mgr,
            refer_streams: HashSet::new(),
            grouped_requests: Vec::new(),
        })
    }

    pub async fn run(&mut self) {
        let mut shutdown = false;
        while !shutdown {
            let mut requests = self.channel.take().await;
            while !requests.is_empty() {
                let mut size = 0;
                let drained = requests.drain_filter(|req| {
                    size += req
                        .record
                        .as_ref()
                        .map(|r| r.encoded_len())
                        .unwrap_or_default();
                    size < 128 * 1024
                });
                let mut record_group = RecordGroup {
                    records: Vec::default(),
                };

                let mut refer_streams = HashSet::new();
                for req in drained {
                    if let Some(record) = req.record {
                        refer_streams.insert(record.stream_id);
                        record_group.records.push(record);
                        self.grouped_requests.push(req.sender);
                    } else {
                        shutdown = true;
                    }
                }

                if record_group.records.is_empty() {
                    continue;
                }

                if let Err(err) = self.submit_requests(record_group).await {
                    self.notify_grouped_requests(Err(err.kind()));
                    continue;
                }

                self.refer_streams.extend(refer_streams.into_iter());
                self.notify_grouped_requests(Ok(self.writer.log_number()));
            }
        }
    }

    async fn submit_requests(&mut self, request_group: RecordGroup) -> IOResult<()> {
        let content = request_group.encode_to_vec();
        if self.writer.avail_space() < content.len() {
            self.switch_writer().await?;
        }
        self.writer.add_record(&content)?;

        if self.opt.log.sync_data {
            self.writer.flush()?;
        }

        Ok(())
    }

    async fn switch_writer(&mut self) -> IOResult<()> {
        self.writer.fill_entire_avail_space()?;
        self.writer.flush()?;
        self.log_file_mgr.delegate(
            self.writer.log_number(),
            std::mem::take(&mut self.refer_streams),
        );

        let (log_number, new_log_file) = self.log_file_mgr.allocate_file()?;
        self.writer = LogWriter::new(new_log_file, log_number, 0, self.opt.log.log_file_size)?;
        Ok(())
    }

    fn notify_grouped_requests(&mut self, result: IOKindResult<u64>) {
        for sender in std::mem::take(&mut self.grouped_requests) {
            sender.send(result).unwrap_or_default();
        }
    }
}
