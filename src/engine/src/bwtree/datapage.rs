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

use std::ops::Deref;

use super::{
    data::{DataItem, Key, Value},
    iter::{BoundedIter, ForwardIter},
    page::{PageAlloc, PageKind, PagePtr, PageRef},
    sortedpage::{SortedPageBuilder, SortedPageIter, SortedPageRef},
};

pub struct DataPageBuilder(SortedPageBuilder);

pub type DataPageIter<'a> = SortedPageIter<'a, Key<'a>, Value<'a>>;

impl Default for DataPageBuilder {
    fn default() -> Self {
        Self(SortedPageBuilder::with_leaf(PageKind::Base, true))
    }
}

impl DataPageBuilder {
    /// Build a data page ptr from a specify allocator
    pub fn build<A: PageAlloc>(self, alloc: &A) -> Result<PagePtr, A::Error> {
        self.0.build(alloc)
    }

    /// Build a data page ptr from allocator and iterator
    pub fn build_from_iter<'a, A: PageAlloc, I: ForwardIter<Item = DataItem<'a>>>(
        self,
        alloc: &A,
        iter: &mut I,
    ) -> Result<PagePtr, A::Error> {
        self.0.build_from_iter(alloc, iter)
    }
}

pub struct DataPageRef<'a>(SortedPageRef<'a, Key<'a>, Value<'a>>);

impl<'a> DataPageRef<'a> {
    pub fn new(page: PageRef<'a>) -> Self {
        assert!(page.is_leaf());
        assert_eq!(page.kind(), PageKind::Base);
        unsafe { Self(SortedPageRef::new(page)) }
    }

    pub fn find(&self, target: Key<'_>) -> Option<DataItem<'a>> {
        if let Some((k, v)) = self.0.seek_item(&target) {
            if k.raw == target.raw {
                return Some((k, v));
            }
        }
        None
    }

    pub fn get_iter(&self) -> DataPageIter<'a> {
        DataPageIter::new(self.0.clone())
    }

    pub fn split(&self) -> Option<(&'a [u8], BoundedIter<DataPageIter<'a>>)> {
        if let Some((mut sep, _)) = self.0.get_item(self.0.item_len() / 2) {
            // Avoids splitting items of the same raw key.
            sep.lsn = u64::MAX;
            let index = match self.0.rank_item(&sep) {
                Ok(i) => i,
                Err(i) => i,
            };
            if index > 0 {
                let iter = BoundedIter::new(self.get_iter(), index);
                return Some((sep.raw, iter));
            }
        }
        None
    }
}

impl<'a> From<DataPageRef<'a>> for PageRef<'a> {
    fn from(value: DataPageRef<'a>) -> Self {
        value.0.into()
    }
}

impl<'a> From<PageRef<'a>> for DataPageRef<'a> {
    fn from(value: PageRef<'a>) -> Self {
        Self::new(value)
    }
}

impl<'a> From<DataPageRef<'a>> for SortedPageRef<'a, Key<'a>, Value<'a>> {
    fn from(value: DataPageRef<'a>) -> Self {
        value.0
    }
}

impl<'a> Deref for DataPageRef<'a> {
    type Target = SortedPageRef<'a, Key<'a>, Value<'a>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
