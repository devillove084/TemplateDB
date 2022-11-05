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

use super::{page::PageRef, pagestore::store::PageInfo};

pub struct Range<'a> {
    pub start: &'a [u8],
    pub end: Option<&'a [u8]>,
}

pub enum PageView<'a> {
    Mem(PageRef<'a>),
    Disk(PageInfo, u64),
}

impl PageView<'_> {
    pub fn ver(&self) -> u64 {
        match self {
            PageView::Mem(page) => page.ver(),
            PageView::Disk(info, _) => info.ver,
        }
    }

    pub fn len(&self) -> u8 {
        match self {
            PageView::Mem(page) => page.len(),
            PageView::Disk(info, _) => info.len,
        }
    }

    pub fn is_leaf(&self) -> bool {
        match self {
            PageView::Mem(page) => page.is_leaf(),
            PageView::Disk(info, _) => info.is_leaf,
        }
    }

    pub fn as_addr(&self) -> PageAddr {
        match *self {
            PageView::Mem(page) => PageAddr::Mem(page.into()),
            PageView::Disk(_, addr) => PageAddr::Disk(addr),
        }
    }
}

impl<'a, T: Into<PageRef<'a>>> From<T> for PageView<'a> {
    fn from(value: T) -> Self {
        Self::Mem(value.into())
    }
}

pub struct Node<'a> {
    pub id: u64,
    pub view: PageView<'a>,
    pub range: Range<'a>,
}

const MEM_DISK_MASK: u64 = 1 << 63;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum PageAddr {
    Mem(u64),
    Disk(u64),
}

impl From<u64> for PageAddr {
    fn from(value: u64) -> Self {
        if value & MEM_DISK_MASK == 0 {
            Self::Mem(value)
        } else {
            Self::Disk(value & MEM_DISK_MASK)
        }
    }
}

impl From<PageAddr> for u64 {
    fn from(value: PageAddr) -> Self {
        match value {
            PageAddr::Mem(addr) => addr,
            PageAddr::Disk(addr) => addr | MEM_DISK_MASK,
        }
    }
}
