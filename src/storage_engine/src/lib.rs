// Copyright 2019 Fullstop000 <fullstop1005@gmail.com>.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
#![deny(clippy::all)]
#![allow(clippy::field_reassign_with_default)]
#![allow(clippy::module_inception)]
#![allow(clippy::needless_range_loop)]
#![allow(clippy::question_mark)]
#![feature(async_closure)]
#![feature(allocator_api)]
#![allow(clippy::rc_buffer)]
#[macro_use]
extern crate log;
extern crate crc32fast;
extern crate crossbeam_channel;
extern crate crossbeam_utils;
extern crate slog;
extern crate slog_async;
extern crate slog_term;
#[macro_use]
extern crate num_derive;
extern crate bytes;
extern crate quick_error;
extern crate rand;
extern crate snap;
extern crate thiserror;

#[macro_use]
mod error;

pub mod cache;
pub mod compaction;
pub mod db_impl;
pub mod db_trait;
pub mod iterator;
mod logger;
pub mod manager;
pub mod memtable;
pub mod operator;
pub mod options;
pub mod servers;
pub mod services;
pub mod sstable;
pub mod storage;
pub mod util;
pub mod wal;

#[allow(clippy::all)]
pub mod memtable_service {
    tonic::include_proto!("memtable");
}

// // pub use batch::WriteBatch;
// pub use cache::Cache;
// // pub use compaction::ManualCompaction;
// // pub use db::{TemplateDB, DB};
// pub use error::{Error, Result};
// pub use cache::bloom_filter_cache::BloomFilter;
// pub use iterator::Iterator;
// pub use log::{LevelFilter, Log};
// pub use options::{CompressionType, Options, ReadOptions, WriteOptions};
// pub use sstable::block::Block;
// pub use storage::*;
// pub use util::{
//     comparator::{BytewiseComparator, Comparator},
//     varint::*,
// };
// pub use memtable_service::*;
