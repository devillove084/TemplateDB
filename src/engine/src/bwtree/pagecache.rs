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

use std::sync::atomic::AtomicUsize;

pub struct PageCache(AtomicUsize);

impl Default for PageCache {
    fn default() -> Self {
        Self(AtomicUsize::new(0))
    }
}

impl PageCache {
    pub fn size(&self) -> usize {
        self.0.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[cfg(not(miri))]
mod alloc {
    use std::alloc::GlobalAlloc;

    use jemallocator::{usable_size, Jemalloc};

    use super::PageCache;
    use crate::bwtree::{
        error::Error,
        page::{PageAlloc, PagePtr},
    };

    unsafe impl PageAlloc for PageCache {
        type Error = Error;

        fn alloc_page(&self, size: usize) -> Result<PagePtr, Self::Error> {
            unsafe {
                let ptr = Jemalloc.alloc(PagePtr::layout(size));
                let size = usable_size(ptr);
                self.0.fetch_add(size, std::sync::atomic::Ordering::Relaxed);
                PagePtr::new(ptr).ok_or(Error::Alloc)
            }
        }

        unsafe fn dealloc_page(&self, page: PagePtr) {
            let ptr = page.as_raw();
            let size = usable_size(ptr);
            self.0.fetch_sub(size, std::sync::atomic::Ordering::SeqCst);
            Jemalloc.dealloc(ptr, PagePtr::layout(size));
        }
    }
}

#[cfg(miri)]
mod alloc {
    use std::{alloc::System, sync::atomic::Ordering};

    unsafe impl PageAlloc for PageCache {
        type Error = Error;

        fn alloc_page(&self, size: usize) -> Result<PagePtr, Self::Error> {
            unsafe {
                let ptr = System.alloc(PagePtr::layout(size));
                self.0.fetch_add(size, Ordering::Relaxed);
                PagePtr::new(ptr).ok_or(Error::Alloc)
            }
        }

        unsafe fn dealloc_page(&self, page: PagePtr) {
            let ptr = page.as_raw();
            self.0.fetch_sub(size, Ordering::Relaxed);
            System.dealloc(ptr, PagePtr::layout(size));
        }
    }
}
