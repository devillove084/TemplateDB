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

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Clone)]
pub struct PageCache {
    size: Arc<AtomicUsize>,
}

impl Default for PageCache {
    fn default() -> Self {
        Self {
            size: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl PageCache {
    pub fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }
}

#[cfg(not(miri))]
mod alloc {
    use std::{alloc::GlobalAlloc, sync::atomic::Ordering};

    use tikv_jemallocator::{usable_size, Jemalloc};

    use super::PageCache;
    use crate::mem::bwtree::{
        error::{Error, Result},
        page::{PageAlloc, PagePtr},
    };

    unsafe impl PageAlloc for PageCache {
        type Error = Error;

        fn alloc_page(&self, size: usize) -> Result<PagePtr> {
            unsafe {
                let ptr = Jemalloc.alloc(PagePtr::layout(size));
                let size = usable_size(ptr);
                self.size.fetch_add(size, Ordering::Relaxed);
                PagePtr::new(ptr).ok_or(Error::Alloc)
            }
        }

        unsafe fn dealloc_page(&self, page: PagePtr) {
            let ptr = page.as_raw();
            let size = usable_size(ptr);
            self.size.fetch_sub(size, Ordering::Relaxed);
            Jemalloc.dealloc(ptr, PagePtr::layout(size));
        }
    }
}

#[cfg(miri)]
mod alloc {
    use std::alloc::System;

    use super::*;

    unsafe impl PageAlloc for PageCache {
        type Error = Error;

        fn alloc_page(&self, size: usize) -> Result<PagePtr> {
            unsafe {
                let ptr = System.alloc(PagePtr::layout(size));
                self.size.fetch_add(size, Ordering::Relaxed);
                PagePtr::new(ptr).ok_or(Error::Alloc)
            }
        }

        unsafe fn dealloc_page(&self, page: PagePtr) {
            let size = page.size();
            self.size.fetch_sub(size, Ordering::Relaxed);
            System.dealloc(page.as_raw(), PagePtr::layout(size));
        }
    }
}
