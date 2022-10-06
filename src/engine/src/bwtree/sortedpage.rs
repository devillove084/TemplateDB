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

use super::{
    data::EncodeTo,
    iter::ForwardIter,
    page::{PageAlloc, PageBuilder, PageKind, PagePtr},
    util::BufWriter,
};

#[derive(Default)]
pub struct SortedPageBuilder {
    base: PageBuilder,
    offsets_size: usize,
    payload_size: usize,
}

impl SortedPageBuilder {
    pub fn new(kind: PageKind) -> Self {
        Self {
            base: PageBuilder::new(kind),
            ..Default::default()
        }
    }

    fn add<K: EncodeTo, V: EncodeTo>(&mut self, key: &K, value: &V) {
        self.offsets_size += std::mem::size_of::<u32>();
        self.payload_size += key.encode_size() + value.encode_size();
    }

    fn size(&self) -> usize {
        self.offsets_size + self.payload_size
    }

    pub fn build<A: PageAlloc>(self, alloc: &A) -> Result<PagePtr, A::Error> {
        let ptr = self.base.build(alloc, self.size());
        ptr.map(|p| unsafe {
            SortedPageBuf::new(p, self);
            p
        })
    }

    pub fn build_from_iter<
        A: PageAlloc,
        I: ForwardIter<Item = (K, V)>,
        K: EncodeTo,
        V: EncodeTo,
    >(
        mut self,
        alloc: &A,
        iter: &mut I,
    ) -> Result<PagePtr, A::Error> {
        iter.rewind();
        while let Some((k, v)) = iter.current() {
            self.add(k, v);
            iter.next();
        }

        let ptr = self.base.build(alloc, self.size());
        ptr.map(|p| unsafe {
            let mut buf = SortedPageBuf::new(p, self);
            iter.rewind();
            while let Some((k, v)) = iter.current() {
                buf.add(k, v);
                iter.next();
            }
            p
        })
    }
}

struct SortedPageBuf {
    offsets: *mut u32,
    payload: BufWriter,
    current: usize,
}

impl SortedPageBuf {
    unsafe fn new(mut base: PagePtr, builder: SortedPageBuilder) -> Self {
        let offsets = base.content_mut() as *mut u32;
        let mut payload = BufWriter::new(base.content_mut());
        payload.skip(builder.offsets_size);
        Self {
            offsets,
            payload,
            current: 0,
        }
    }

    unsafe fn add<K: EncodeTo, V: EncodeTo>(&mut self, key: &K, value: &V) {
        let offset = self.payload.offset_from(self.offsets as *mut u8) as u32;
        self.current += 1;
        key.encode_to(&mut self.payload);
        value.encode_to(&mut self.payload);
    }
}
