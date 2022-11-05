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

use std::{
    ops::ControlFlow,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use crossbeam::epoch::Guard;

use super::{
    data::{Index, Key},
    datapage::DataPageBuilder,
    env::Env,
    error::{Error, Result},
    indexpage::IndexPageBuilder,
    iter::OptionIter,
    node::{Node, PageAddr, PageView},
    page::PageRef,
    pagecache::PageCache,
    pagestore::store::PageStore,
    pagetable::PageTable,
    stats::AtomicStats,
    util::Options,
};

struct MinLsn(AtomicU64);

impl MinLsn {
    fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    fn set(&self, lsn: u64) {
        let mut min = self.0.load(Ordering::Relaxed);
        while min < lsn {
            match self
                .0
                .compare_exchange(min, lsn, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(new) => min = new,
            }
        }
    }
}

pub struct Tree<E: Env> {
    opts: Options,
    table: PageTable,
    cache: PageCache,
    store: PageStore<E>,
    stats: AtomicStats,
    min_lsn: MinLsn,
}

impl<E: Env> Tree<E> {
    pub async fn new(env: E, root: PathBuf, opts: Options) -> Result<Self> {
        let table = PageTable::default();
        let cache = PageCache::default();
        let store = PageStore::open(env, root, opts).await?;
        let tree = Self {
            opts,
            table,
            cache,
            store,
            stats: AtomicStats::default(),
            min_lsn: MinLsn::new(),
        };
        tree.init()
    }

    pub fn init(self) -> Result<Self> {
        let root_id = self.table.alloc().unwrap();
        let leaf_id = self.table.alloc().unwrap();
        let leaf_page = DataPageBuilder::default().build(&self.cache)?;
        self.table.set(leaf_id, leaf_page.into());

        let mut root_iter = OptionIter::from(([].as_slice(), Index::new(leaf_id, 0)));
        let root_page = IndexPageBuilder::default().build_from_iter(&self.cache, &mut root_iter)?;
        self.table.set(root_id, root_page.into());
        Ok(self)
    }

    pub fn get<'a: 'g, 'g>(&'a self, key: Key<'_>, guard: &'g Guard) -> Result<Option<&'g [u8]>> {
        loop {
            match self.try_get(key, guard) {
                Ok(value) => {
                    self.stats.succeeded.num_gets.inc();
                    return Ok(value);
                }
                Err(Error::Again) => {
                    self.stats.conflicted.num_gets.inc();
                    continue;
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub fn page_add(&self, id: u64) -> PageAddr {
        self.table.get(id).into()
    }

    pub fn page_view<'a: 'g, 'g>(&'a self, addr: PageAddr, _: &'g Guard) -> Option<PageView<'g>> {
        match addr {
            PageAddr::Mem(addr) => {
                let page = unsafe { PageRef::new(addr as *mut u8) };
                page.map(PageView::Mem)
            }
            PageAddr::Disk(addr) => self
                .store
                .page_info(addr)
                .map(|info| PageView::Disk(into, addr)),
        }
    }

    pub fn try_get<'a: 'g, 'g>(
        &'a self,
        key: Key<'_>,
        guard: &'g Guard,
    ) -> Result<Option<&'g [u8]>> {
        // let (node, _) = self.find_leaf(key.raw, guard)?;
        // self.lookup_value(key, &node, guard)
        todo!()
    }

    pub fn find_leaf<'a: 'g, 'g>(
        &'a self,
        key: &'a [u8],
        guard: &'g Guard,
    ) -> Result<(Node<'g>, Option<Node<'g>>)> {
        let mut index = ROOT_INDEX;
        let mut range = Range::default();
        let mut parent = None;
        loop {
            let addr = self.page_add(index.id);
            let view = self.page_view(addr, view).expect("the node must be valid");
            let node = Node {
                id: index.id,
                view,
                range,
            };
        }
    }

    pub fn lookup_value<'a: 'g, 'g>(
        &'a self,
        key: Key<'_>,
        node: &Node<'g>,
        guard: &'g Guard,
    ) -> Result<Option<&'g [u8]>> {
        todo!()
    }
}
