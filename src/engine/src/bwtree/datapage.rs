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
    data::{DataItem, Index, Key, Value},
    indexpage::IndexPageRef,
    iter::{BoundedIter, ForwardIter, OptionIter},
    page::{PageAlloc, PageKind, PagePtr, PageRef},
    sortedpage::{SortedPageBuilder, SortedPageIter, SortedPageRef},
};

/// A builder to create data pages.
pub struct DataPageBuilder(SortedPageBuilder);

impl Default for DataPageBuilder {
    fn default() -> Self {
        Self(SortedPageBuilder::with_leaf(PageKind::Base, true))
    }
}

impl DataPageBuilder {
    /// Builds an empty data page.
    pub fn build<A>(self, alloc: &A) -> Result<PagePtr, A::Error>
    where
        A: PageAlloc,
    {
        self.0.build(alloc)
    }

    /// Builds a data page with items from the given iterator.
    pub fn build_from_iter<'a, A, I>(self, alloc: &A, iter: &mut I) -> Result<PagePtr, A::Error>
    where
        A: PageAlloc,
        I: ForwardIter<Item = DataItem<'a>>,
    {
        self.0.build_from_iter(alloc, iter)
    }
}

/// An immutable reference to a data page.
#[derive(Clone)]
pub struct DataPageRef<'a>(SortedPageRef<'a, Key<'a>, Value<'a>>);

impl<'a> DataPageRef<'a> {
    pub fn new(base: PageRef<'a>) -> Self {
        debug_assert_eq!(base.kind(), PageKind::Base);
        debug_assert!(base.is_leaf());
        Self(unsafe { SortedPageRef::new(base) })
    }

    /// Returns the item that matches `target`.
    pub fn find(&self, target: Key<'_>) -> Option<DataItem<'a>> {
        if let Some((k, v)) = self.0.seek_item(&target) {
            if k.raw == target.raw {
                return Some((k, v));
            }
        }
        None
    }

    /// Creates an iterator over items of this page.
    pub fn iter(&self) -> DataPageIter<'a> {
        DataPageIter::new(self.0.clone())
    }

    /// Returns a split key for split and an iterator over items at and after the split key.
    pub fn split(&self) -> Option<(&'a [u8], BoundedIter<DataPageIter<'a>>)> {
        if let Some((mut sep, _)) = self.0.get_item(self.0.item_len() / 2) {
            // Avoids splitting items of the same raw key.
            sep.lsn = u64::MAX;
            let index = match self.0.rank_item(&sep) {
                Ok(i) => i,
                Err(i) => i,
            };
            if index > 0 {
                let iter = BoundedIter::new(self.iter(), index);
                return Some((sep.raw, iter));
            }
        }
        None
    }
}

impl<'a> Deref for DataPageRef<'a> {
    type Target = SortedPageRef<'a, Key<'a>, Value<'a>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> From<PageRef<'a>> for DataPageRef<'a> {
    fn from(base: PageRef<'a>) -> Self {
        Self::new(base)
    }
}

impl<'a> From<DataPageRef<'a>> for PageRef<'a> {
    fn from(page: DataPageRef<'a>) -> Self {
        page.0.into()
    }
}

impl<'a> From<DataPageRef<'a>> for SortedPageRef<'a, Key<'a>, Value<'a>> {
    fn from(page: DataPageRef<'a>) -> Self {
        page.0
    }
}

pub type DataPageIter<'a> = SortedPageIter<'a, Key<'a>, Value<'a>>;

// A builder to create split pages.
///
/// Note: We use the sorted page layout here to make it possible to split a page into multiple ones
/// in the future.
pub struct SplitPageBuilder(SortedPageBuilder);

impl Default for SplitPageBuilder {
    fn default() -> Self {
        Self(SortedPageBuilder::new(PageKind::Split))
    }
}

impl SplitPageBuilder {
    pub fn build_with_index<A>(
        self,
        alloc: &A,
        start: &[u8],
        index: Index,
    ) -> Result<PagePtr, A::Error>
    where
        A: PageAlloc,
    {
        let mut iter = OptionIter::from((start, index));
        self.0.build_from_iter(alloc, &mut iter)
    }
}

/// An immutable reference to a split page.
#[derive(Clone)]
pub struct SplitPageRef<'a>(SortedPageRef<'a, &'a [u8], Index>);

impl<'a> SplitPageRef<'a> {
    pub fn new(base: PageRef<'a>) -> Self {
        debug_assert_eq!(base.kind(), PageKind::Split);
        Self(unsafe { SortedPageRef::new(base) })
    }

    pub fn split_index(&self) -> (&'a [u8], Index) {
        self.0.get_item(0).unwrap()
    }
}

impl<'a> Deref for SplitPageRef<'a> {
    type Target = PageRef<'a>;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl<'a> From<PageRef<'a>> for SplitPageRef<'a> {
    fn from(base: PageRef<'a>) -> Self {
        Self::new(base)
    }
}

impl<'a> From<SplitPageRef<'a>> for PageRef<'a> {
    fn from(page: SplitPageRef<'a>) -> Self {
        page.0.into()
    }
}

pub enum TypedPageRef<'a> {
    Data(DataPageRef<'a>),
    Index(IndexPageRef<'a>),
    Split(SplitPageRef<'a>),
}

impl<'a, T> From<T> for TypedPageRef<'a>
where
    T: Into<PageRef<'a>>,
{
    fn from(page: T) -> Self {
        let page = page.into();
        match page.kind() {
            PageKind::Base => {
                if page.is_leaf() {
                    Self::Data(page.into())
                } else {
                    Self::Index(page.into())
                }
            }
            PageKind::Split => Self::Split(page.into()),
        }
    }
}
