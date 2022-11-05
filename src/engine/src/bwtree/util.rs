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

use std::sync::atomic::AtomicU64;

/// An unsafe, litte-endian buffer reader
pub struct BufReader(*const u8);

macro_rules! get_int {
    ($name:ident, $t:ty) => {
        pub unsafe fn $name(&mut self) -> $t {
            let mut v: $t = 0;
            let ptr = &mut v as *mut $t as *mut u8;
            let len = std::mem::size_of::<$t>();
            self.0.copy_to_nonoverlapping(ptr, len);
            self.skip(len);
            <$t>::from_le(v)
        }
    };
}

impl BufReader {
    pub const fn new(ptr: *const u8) -> Self {
        Self(ptr)
    }

    pub unsafe fn skip(&mut self, n: usize) {
        self.0 = self.0.add(n);
    }

    get_int!(get_u8, u8);

    get_int!(get_u32, u32);

    get_int!(get_u64, u64);

    pub unsafe fn get_slice<'a>(&mut self, len: usize) -> &'a [u8] {
        let ptr = self.0;
        self.skip(len);
        std::slice::from_raw_parts(ptr, len)
    }

    pub unsafe fn get_length_prefixed_slice<'a>(&mut self) -> &'a [u8] {
        let len = self.get_u32();
        self.get_slice(len as usize)
    }
}

/// An unsafe, little-endian buffer writer.
pub struct BufWriter(*mut u8);

macro_rules! put_int {
    ($name:ident, $t:ty) => {
        pub unsafe fn $name(&mut self, v: $t) {
            let v = v.to_le();
            let ptr = &v as *const $t as *const u8;
            let len = std::mem::size_of::<$t>();
            self.0.copy_from_nonoverlapping(ptr, len);
            self.skip(len);
        }
    };
}

impl BufWriter {
    pub const fn new(ptr: *mut u8) -> Self {
        Self(ptr)
    }

    pub unsafe fn skip(&mut self, n: usize) {
        self.0 = self.0.add(n);
    }

    pub unsafe fn offset_from(&self, origin: *const u8) -> isize {
        self.0.offset_from(origin)
    }

    put_int!(put_u8, u8);

    put_int!(put_u32, u32);

    put_int!(put_u64, u64);

    pub unsafe fn put_slice(&mut self, slice: &[u8]) {
        self.0.copy_from_nonoverlapping(slice.as_ptr(), slice.len());
        self.skip(slice.len())
    }

    pub unsafe fn put_length_prefixed_slice(&mut self, slice: &[u8]) {
        assert!(slice.len() <= u32::MAX as usize);
        self.put_u32(slice.len() as u32);
        self.put_slice(slice);
    }

    pub const fn length_prefixed_slice_size(slice: &[u8]) -> usize {
        std::mem::size_of::<u32>() + slice.len()
    }
}

pub struct Counter(AtomicU64);

impl Default for Counter {
    fn default() -> Self {
        Self(AtomicU64::new(0))
    }
}

impl Counter {
    pub fn new(value: u64) -> Self {
        Self(AtomicU64::new(value))
    }

    pub fn inc(&self) -> u64 {
        self.add(1)
    }

    pub fn add(&self, value: u64) -> u64 {
        self.0
            .fetch_add(value, std::sync::atomic::Ordering::Relaxed)
    }

    pub fn get(&self) -> u64 {
        self.0.load(std::sync::atomic::Ordering::Relaxed)
    }
}

pub struct Sequence(AtomicU64);

impl Default for Sequence {
    fn default() -> Self {
        Self(AtomicU64::new(0))
    }
}

impl Sequence {
    pub fn new(id: u64) -> Self {
        return Sequence(AtomicU64::new(id));
    }

    pub fn inc(&self) -> u64 {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Options {
    cache_size: usize,
    data_node_size: usize,
    data_node_delta_length: usize,
    index_node_size: usize,
    index_node_delta_length: usize,
}

impl Default for Options {
    fn default() -> Self {
        return Self {
            cache_size: usize::MAX,
            data_node_size: 8 * 1024,
            data_node_delta_length: 8,
            index_node_size: 4 * 1024,
            index_node_delta_length: 4,
        };
    }
}
