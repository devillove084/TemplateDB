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

use std::{marker::PhantomData, mem::size_of, ops::Deref};

use super::{
    data::{Compare, DecodeFrom, EncodeTo},
    iter::{ForwardIter, SeekableIter},
    page::{PageAlloc, PageBuilder, PageKind, PagePtr, PageRef},
    util::{BufReader, BufWriter},
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

    pub fn with_leaf(kind: PageKind, is_leaf: bool) -> Self {
        Self {
            base: PageBuilder::with_leaf(kind, is_leaf),
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
        self.offsets.add(self.current).write(offset.to_le());
        self.current += 1;
        key.encode_to(&mut self.payload);
        value.encode_to(&mut self.payload);
    }
}

pub struct SortedPageRef<'a, K, V> {
    base: PageRef<'a>,
    offset: &'a [u32],
    _mark: PhantomData<(K, V)>,
}

impl<'a, K: DecodeFrom + Ord, V: DecodeFrom> SortedPageRef<'a, K, V> {
    pub unsafe fn new(base: PageRef<'a>) -> Self {
        let offset_ptr = base.content() as *const u32;
        let offset_len = if base.content_size() == 0 {
            0
        } else {
            offset_ptr.read() as usize / size_of::<u32>()
        };
        let offsets = std::slice::from_raw_parts(offset_ptr, offset_len);
        Self {
            base,
            offset: offsets,
            _mark: PhantomData,
        }
    }

    pub fn base(&self) -> PageRef {
        self.base
    }

    pub fn item_len(&self) -> usize {
        self.offset.len()
    }

    pub fn content_at(&self, offset: u32) -> *const u8 {
        let offset = offset.to_le() as usize;
        unsafe { self.base.content().add(offset) }
    }

    pub fn get_item(&self, index: usize) -> Option<(K, V)> {
        if let Some(&offset) = self.offset.get(index) {
            let ptr = self.content_at(offset);
            let mut buf = BufReader::new(ptr);
            let key = unsafe { K::decode_from(&mut buf) };
            let value = unsafe { V::decode_from(&mut buf) };
            Some((key, value))
        } else {
            None
        }
    }

    pub fn rank_item<T: Compare<K> + ?Sized>(&self, target: &T) -> Result<usize, usize> {
        let mut left = 0;
        let mut right = self.item_len();
        while left < right {
            let mid = left + (right - left) / 2;
            let key = {
                let ptr = self.content_at(self.offset[mid]);
                let mut buf = BufReader::new(ptr);
                unsafe { K::decode_from(&mut buf) }
            };
            match target.compare(&key) {
                std::cmp::Ordering::Equal => return Ok(mid),
                std::cmp::Ordering::Greater => left = mid + 1,
                std::cmp::Ordering::Less => right = mid,
            }
        }
        Err(left)
    }

    pub fn seek_item<T: Compare<K> + ?Sized>(&self, target: &T) -> Option<(K, V)> {
        let index = match self.rank_item(target) {
            Ok(i) => i,
            Err(i) => i,
        };
        self.get_item(index)
    }
}

impl<'a, K, V> Clone for SortedPageRef<'a, K, V> {
    fn clone(&self) -> Self {
        Self {
            base: self.base,
            offset: self.offset,
            _mark: PhantomData,
        }
    }
}

impl<'a, K, V> Deref for SortedPageRef<'a, K, V> {
    type Target = PageRef<'a>;
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<'a, K, V> From<SortedPageRef<'a, K, V>> for PageRef<'a> {
    fn from(value: SortedPageRef<'a, K, V>) -> Self {
        value.base
    }
}

pub struct SortedPageIter<'a, K, V> {
    page: SortedPageRef<'a, K, V>,
    index: usize,
    current: Option<(K, V)>,
}

impl<'a, K: DecodeFrom + Ord, V: DecodeFrom> SortedPageIter<'a, K, V> {
    pub fn new(page: SortedPageRef<'a, K, V>) -> Self {
        let index = page.item_len();
        Self {
            page,
            index,
            current: None,
        }
    }
}

impl<'a, K: DecodeFrom + Ord, V: DecodeFrom> ForwardIter for SortedPageIter<'a, K, V> {
    type Item = (K, V);

    fn current(&self) -> Option<&Self::Item> {
        self.current.as_ref()
    }

    fn rewind(&mut self) {
        self.index = 0;
        self.current = self.page.get_item(0);
    }

    fn next(&mut self) {
        self.index += 1;
        self.current = self.page.get_item(self.index);
    }

    fn skip(&mut self, n: usize) {
        self.index = self.index.saturating_add(n).min(self.page.item_len());
        self.current = self.page.get_item(self.index);
    }

    fn skip_all(&mut self) {
        self.index = self.page.item_len();
        self.current = None
    }
}

impl<'a, K: DecodeFrom + Ord, V: DecodeFrom, T: Compare<K> + ?Sized> SeekableIter<T>
    for SortedPageIter<'a, K, V>
{
    fn seek(&mut self, target: &T) {
        let index = match self.page.rank_item(target) {
            Ok(i) => i,
            Err(i) => i,
        };
        self.current = self.page.get_item(index);
    }
}

impl<'a, K: DecodeFrom + Ord, V: DecodeFrom, T: Into<SortedPageRef<'a, K, V>>> From<T>
    for SortedPageIter<'a, K, V>
{
    fn from(value: T) -> Self {
        Self::new(value.into())
    }
}

#[cfg(test)]
mod tests {
    use std::alloc::System;

    use crate::bwtree::{
        iter::{ForwardIter, SliceIter},
        sortedpage::{SortedPageBuilder, SortedPageIter, SortedPageRef},
    };

    #[test]
    fn data_page() {
        let data = [(1, 0), (2, 0), (4, 0), (7, 0), (8, 0)];
        let mut iter = SliceIter::from(&data);
        let page = SortedPageBuilder::default()
            .build_from_iter(&System, &mut iter)
            .unwrap();

        let page = unsafe { SortedPageRef::new(page.into()) };
        assert_eq!(page.item_len(), data.len());
        assert_eq!(page.seek_item(&0), Some((1, 0)));
        assert_eq!(page.seek_item(&3), Some((4, 0)));
        assert_eq!(page.seek_item(&9), None);

        let mut iter = SortedPageIter::new(page);
        for _ in 0..2 {
            assert_eq!(iter.current(), None);
            iter.rewind();
            for item in data.iter() {
                assert_eq!(iter.current(), Some(item));
                iter.next();
            }
            assert_eq!(iter.current(), None);
        }
    }
}
