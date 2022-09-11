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
#![allow(clippy::all)]
#![feature(hash_drain_filter)]
#![feature(btree_drain_filter)]
#![feature(async_closure)]
#![feature(pin_macro)]
#![feature(write_all_vectored)]
#![feature(drain_filter)]

pub mod accelerate;
pub mod allocator;
pub mod analysis;
pub mod consensus;
pub mod gc;
pub mod network;
pub mod storage;
pub mod stream;

#[macro_use]
extern crate derivative;

pub mod node {
    pub use v1::*;

    pub mod v1 {
        tonic::include_proto!("runtime.node.v1");
    }
}

pub use node::*;

pub mod store {
    pub use v1::*;

    pub mod meta {
        pub use v1::*;

        pub mod v1 {
            tonic::include_proto!("runtime.store.meta.v1");
        }
    }

    pub mod manifest {
        pub use v1::*;

        pub mod v1 {
            tonic::include_proto!("runtime.store.manifest.v1");
        }
    }

    pub mod v1 {
        tonic::include_proto!("runtime.store.v1");
    }
}

pub use store::{manifest, meta::v1::*, v1::*};
