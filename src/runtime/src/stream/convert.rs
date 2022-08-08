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

use futures::channel::oneshot;

use super::{error::Error, types::Sequence};
use crate::{ObserverState, Role};

impl From<i32> for Role {
    fn from(role: i32) -> Self {
        Role::from_i32(role).unwrap_or(Role::Follower)
    }
}

impl From<i32> for ObserverState {
    fn from(s: i32) -> Self {
        ObserverState::from_i32(s).unwrap_or(ObserverState::Following)
    }
}

impl From<ObserverState> for Role {
    fn from(s: ObserverState) -> Self {
        match s {
            ObserverState::Following => Role::Follower,
            ObserverState::Recovering | ObserverState::Leading => Role::Leader,
        }
    }
}

impl From<u64> for Sequence {
    fn from(v: u64) -> Self {
        Sequence {
            epoch: (v >> 32) as u32,
            index: (v as u32),
        }
    }
}

impl From<Sequence> for u64 {
    fn from(seq: Sequence) -> Self {
        (seq.epoch as u64) << 32 | (seq.index as u64)
    }
}

impl From<oneshot::Canceled> for Error {
    fn from(_: oneshot::Canceled) -> Self {
        Error::IO(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "task has been canceled",
        ))
    }
}

impl From<prost::DecodeError> for Error {
    fn from(err: prost::DecodeError) -> Self {
        Error::Corruption(err.to_string())
    }
}

impl From<tonic::Status> for Error {
    fn from(s: tonic::Status) -> Self {
        match s.code() {
            tonic::Code::NotFound => Error::NotFound(s.message().into()),
            tonic::Code::AlreadyExists => Error::AlreadyExists(s.message().into()),
            tonic::Code::InvalidArgument => Error::InvalidArgument(s.message().into()),
            tonic::Code::FailedPrecondition => Error::Staled(s.message().into()),
            tonic::Code::DataLoss => Error::Corruption(s.message().into()),
            _ => Error::Unknown(Box::new(s)),
        }
    }
}

impl From<tonic::transport::Error> for Error {
    fn from(e: tonic::transport::Error) -> Self {
        Error::Unknown(Box::new(e))
    }
}

impl From<Error> for tonic::Status {
    fn from(err: Error) -> Self {
        let (code, message) = match err {
            Error::NotFound(s) => (tonic::Code::NotFound, s),
            Error::AlreadyExists(s) => (tonic::Code::AlreadyExists, s),
            Error::InvalidArgument(s) => (tonic::Code::InvalidArgument, s),
            Error::InvalidResponse => (tonic::Code::InvalidArgument, "invalid response".into()),
            Error::IO(s) => (tonic::Code::Unknown, s.to_string()),
            Error::Unknown(s) => (tonic::Code::Unknown, s.to_string()),
            Error::Staled(s) => (tonic::Code::FailedPrecondition, s),
            Error::Corruption(s) => (tonic::Code::DataLoss, s),
            // TODO: build not command leader error
            Error::NotCommandLeader(_) => unreachable!(),
        };
        tonic::Status::new(code, message)
    }
}

impl From<std::io::ErrorKind> for Error {
    fn from(kind: std::io::ErrorKind) -> Self {
        if kind == std::io::ErrorKind::Other {
            Error::Staled("from std::io::ErrorKind::Others".to_string())
        } else {
            Error::IO(kind.into())
        }
    }
}
