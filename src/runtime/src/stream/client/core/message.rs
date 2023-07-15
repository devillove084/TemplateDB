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

use std::{fmt::Display, ops::Range};

use derivative::Derivative;

use crate::stream::common::{Entry, Sequence};

/// Learn a stream base infomaton from other stream, the target is which
/// this stream learn from.
/// Segment epoch number is current segment in the
/// whole system life time.
/// Writer epoch number is which writer in which epoch,
/// make `happen-before` grantee in system.
/// Start index means the position where
/// we start copy.
/// ? Why we need to learn msg in the stream? This core inception
/// ? comes from consensus protocol.
#[derive(Debug, Clone)]
pub(crate) struct Learn {
    pub target: String,
    pub seg_epoch: u32,
    pub writer_epoch: u32,
    pub start_index: u32,
}

/// Mutate is same as Learn, start index is change to
/// mut kind value, which tag this message mutate epoch number.
#[derive(Debug, Clone)]
pub(crate) struct Mutate {
    pub target: String,
    pub seg_epoch: u32,
    pub writer_epoch: u32,
    pub kind: MutKind,
}

#[derive(Debug, Clone)]
pub(crate) enum MutKind {
    Write(Write),
    /// ? Why Seal do not have a embeded type?
    Seal,
}

/// The write request in the stream, carry with acked sequence,
/// range scope, all write bytes and all entries itself.
/// ? Why we need range, what range for, and how to use?
#[derive(Derivative, Clone)]
#[derivative(Debug)]
pub(crate) struct Write {
    pub acked_seq: Sequence,
    pub range: Range<u32>,
    pub bytes: usize,
    #[derivative(Debug = "ignore")]
    pub entries: Vec<Entry>,
}

/// Indicated the entries that we had been learned.
#[derive(Derivative, Clone)]
#[derivative(Debug)]
pub(crate) struct Learned {
    // The end is reached if entries is empty.
    pub entries: Vec<(u32, Entry)>,
}

/// Restored is used to notify the worker to send a message to the master to
/// seal the corresponding segment.
#[derive(Clone, Debug)]
pub(crate) struct Restored {
    pub segment_epoch: u32,
    pub writer_epoch: u32,
}

/// Detail in every stream client send to the server.
#[allow(dead_code)]
#[derive(Derivative, Clone)]
#[derivative(Debug)]
pub(crate) enum MsgDetail {
    Received {
        matched_index: u32,
        acked_index: u32,
    },
    Recovered,
    Rejected,
    Timeout {
        range: Option<Range<u32>>,
        bytes: usize,
    },
    Sealed {
        acked_index: u32,
    },
    Learned(Learned),
}

impl Display for MsgDetail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let desc = match self {
            MsgDetail::Received { .. } => "RECEIVED",
            MsgDetail::Recovered => "RECOVERED",
            MsgDetail::Rejected => "REJECTED",
            MsgDetail::Timeout { .. } => "TIMEOUT",
            MsgDetail::Sealed { .. } => "SEALED",
            MsgDetail::Learned(_) => "LEARNED",
        };
        write!(f, "{}", desc)
    }
}

/// An abstraction of data communication between `StreamStateMachine` and
/// journal servers.
#[derive(Debug, Clone)]
pub(crate) struct StreamLogMsg {
    pub target: String,
    pub segment_epoch: u32,
    pub writer_epoch: u32,
    pub detail: MsgDetail,
}

impl StreamLogMsg {
    /// Connect to distributed journal leaderless cluster timeout.
    pub fn con_cluster_timeout(segment_epoch: u32, writer_epoch: u32) -> Self {
        Self::store_timeout("<Command_leader>".into(), segment_epoch, writer_epoch)
    }

    /// Store stream infomation into journal cluster timeout, but it abstact a target in this.
    pub fn store_timeout(target: String, segment_epoch: u32, writer_epoch: u32) -> Self {
        Self::write_timeout(target, segment_epoch, writer_epoch, None, 0)
    }

    /// Generate a write timeout msg
    pub fn write_timeout(
        target: String,
        segment_epoch: u32,
        writer_epoch: u32,
        range: Option<Range<u32>>,
        bytes: usize,
    ) -> Self {
        StreamLogMsg {
            target,
            segment_epoch,
            writer_epoch,
            detail: MsgDetail::Timeout { range, bytes },
        }
    }

    /// Generate a seal msg, seal means stop there, so we only need acked index number.
    pub fn sealed(target: String, segment_epoch: u32, writer_epoch: u32, acked_index: u32) -> Self {
        StreamLogMsg {
            target,
            segment_epoch,
            writer_epoch,
            detail: MsgDetail::Sealed { acked_index },
        }
    }

    /// Generate a received msg, tell journal server we has been received until matched index and
    /// acked index.
    pub fn received(
        target: String,
        segment_epoch: u32,
        writer_epoch: u32,
        matched_index: u32,
        acked_index: u32,
    ) -> Self {
        StreamLogMsg {
            target,
            segment_epoch,
            writer_epoch,
            detail: MsgDetail::Received {
                matched_index,
                acked_index,
            },
        }
    }

    /// Tell journal server where has been covered.
    pub fn recovered(segment_epoch: u32, writer_epoch: u32) -> Self {
        StreamLogMsg {
            target: "<Command leader>".into(),
            segment_epoch,
            writer_epoch,
            detail: MsgDetail::Recovered,
        }
    }

    /// Tell journal server where has been learned.
    pub fn learned(
        target: String,
        segment_epoch: u32,
        writer_epoch: u32,
        learned: Learned,
    ) -> Self {
        StreamLogMsg {
            target,
            segment_epoch,
            writer_epoch,
            detail: MsgDetail::Learned(learned),
        }
    }
}
