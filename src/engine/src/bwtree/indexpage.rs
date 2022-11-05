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
    data::{Index, IndexItem},
    iter::{BoundedIter, ForwardIter},
    page::{PageAlloc, PageKind, PagePtr, PageRef},
    sortedpage::{SortedPageBuilder, SortedPageIter, SortedPageRef},
};

pub struct IndexPageBuilder(SortedPageBuilder);

pub type IndexPageIter<'a> = SortedPageIter<'a, &'a [u8], Index>;

impl Default for IndexPageBuilder {
    fn default() -> Self {
        Self(SortedPageBuilder::with_leaf(PageKind::Base, false))
    }
}

impl IndexPageBuilder {
    pub fn build_from_iter<'a, A: PageAlloc, I: ForwardIter<Item = IndexItem<'a>>>(
        self,
        alloc: &A,
        iter: &mut I,
    ) -> Result<PagePtr, A::Error> {
        self.0.build_from_iter(alloc, iter)
    }
}

pub struct IndexPageRef<'a>(SortedPageRef<'a, &'a [u8], Index>);

impl<'a> IndexPageRef<'a> {
    pub fn new(base: PageRef<'a>) -> Self {
        assert!(!base.is_leaf());
        assert_eq!(base.kind(), PageKind::Base);
        unsafe { Self(SortedPageRef::new(base)) }
    }

    pub fn find(&self, target: &'a [u8]) -> (Option<IndexItem<'a>>, Option<IndexItem<'a>>) {
        match self.0.rank_item(target) {
            Ok(i) => (
                self.0.get_item(i),
                i.checked_add(1).and_then(|i| self.0.get_item(i)),
            ),
            Err(i) => (
                i.checked_sub(1).and_then(|i| self.0.get_item(i)),
                self.0.get_item(i),
            ),
        }
    }

    pub fn get_iter(&self) -> IndexPageIter<'a> {
        IndexPageIter::new(self.0.clone())
    }

    pub fn split(&self) -> Option<(&'a [u8], BoundedIter<IndexPageIter<'a>>)> {
        if let Some((sep, _)) = self.0.get_item(self.0.item_len() / 2) {
            let index = match self.0.rank_item(sep) {
                Ok(i) => i,
                Err(i) => i,
            };
            if index > 0 {
                let iter = BoundedIter::new(self.get_iter(), index);
                return Some((sep, iter));
            }
        }
        None
    }
}

// impl<'a> Deref for IndexPageRef<'a> {
//     type Target = SortedPageIter<'a, &'a [u8], Index>;

//     fn deref(&self) -> &Self::Target {
//         &self.0
//     }
// }

impl<'a> From<PageRef<'a>> for IndexPageRef<'a> {
    fn from(value: PageRef<'a>) -> Self {
        IndexPageRef::new(value)
    }
}

impl<'a> From<IndexPageRef<'a>> for PageRef<'a> {
    fn from(value: IndexPageRef<'a>) -> Self {
        value.0.into()
    }
}

impl<'a> From<IndexPageRef<'a>> for SortedPageRef<'a, &'a [u8], Index> {
    fn from(value: IndexPageRef<'a>) -> Self {
        value.0
    }
}
