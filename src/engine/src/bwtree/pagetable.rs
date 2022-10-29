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
    mem::MaybeUninit,
    sync::{
        atomic::{AtomicPtr, AtomicU64, Ordering},
        Arc,
    },
};

#[cfg(miri)]
const FANOUT: usize = 1 << 4;
#[cfg(not(miri))]
const FANOUT: usize = 1 << 16;

const L0_LEN: usize = FANOUT;
const L1_LEN: usize = FANOUT - 1;
const L2_LEN: usize = FANOUT - 1;

const L0_FANOUT: u64 = FANOUT as u64;
const L1_FANOUT: u64 = L0_FANOUT * FANOUT as u64;
const L2_FANOUT: u64 = L1_FANOUT * FANOUT as u64;

const MAX: u64 = L2_FANOUT - 1;
const MIN: u64 = 1;
const NAN: u64 = 0;

macro_rules! define_levels {
    ($level:ident, $child:ty, $fanout:expr) => {
        struct $level<const N: usize>([AtomicPtr<$child>; N]);

        impl<const N: usize> Default for $level<N> {
            fn default() -> Self {
                Self(unsafe { MaybeUninit::zeroed().assume_init() })
            }
        }

        impl<const N: usize> Drop for $level<N> {
            fn drop(&mut self) {
                for child in &self.0 {
                    let child_ptr = child.load(Ordering::Acquire);
                    if !child_ptr.is_null() {
                        unsafe { Box::from_raw(child_ptr) };
                    }
                }
            }
        }

        impl<const N: usize> $level<N> {
            fn index(&self, index: u64) -> &AtomicU64 {
                let i = index / $fanout;
                let j = index % $fanout;
                let p = self.0[i as usize].load(Ordering::Acquire);
                let child = unsafe {
                    p.as_ref()
                        .unwrap_or_else(|| self.install_or_acquire_child(i as usize))
                };
                &child.index(j)
            }

            fn install_or_acquire_child(&self, index: usize) -> &$child {
                let mut child = Box::into_raw(Box::default());
                if let Err(current) = self.0[index].compare_exchange(
                    std::ptr::null_mut(),
                    child,
                    Ordering::AcqRel,
                    Ordering::SeqCst,
                ) {
                    unsafe {
                        Box::from_raw(child);
                    }
                    child = current;
                }
                unsafe { &*child }
            }
        }
    };
}

struct L0<const N: usize>([AtomicU64; N]);

impl<const N: usize> Default for L0<N> {
    fn default() -> Self {
        Self(unsafe { MaybeUninit::zeroed().assume_init() })
    }
}

impl<const N: usize> L0<N> {
    fn index(&self, index: u64) -> &AtomicU64 {
        &self.0[index as usize]
    }
}

define_levels!(L1, L0<FANOUT>, L0_FANOUT);
define_levels!(L2, L1<FANOUT>, L1_FANOUT);

struct Inner {
    // Level 0: [0, L0_FANOUT)
    l0: Box<L0<L0_LEN>>,
    // Level 1: [L0_FANOUT, L1_FANOUT)
    l1: Box<L1<L1_LEN>>,
    // Level 2: [L1_FANOUT, L2_FANOUT)
    l2: Box<L2<L2_LEN>>,
    // The next id to allocate
    next: AtomicU64,
    // The head of free list
    free: AtomicU64,
}

impl Default for Inner {
    fn default() -> Self {
        return Self {
            l0: Box::default(),
            l1: Box::default(),
            l2: Box::default(),
            next: AtomicU64::new(MIN),
            free: AtomicU64::new(NAN),
        };
    }
}

impl Inner {
    fn index(&self, index: u64) -> &AtomicU64 {
        if index < L0_FANOUT {
            self.l0.index(index)
        } else if index < L1_FANOUT {
            self.l1.index(index - L0_FANOUT)
        } else if index < L2_FANOUT {
            self.l2.index(index - L1_FANOUT)
        } else {
            unreachable!()
        }
    }

    /**
     * @description:
     * @param {*} self
     * @return {*} Option<u64>
     */
    fn alloc(&self) -> Option<u64> {
        let mut id = self.free.load(Ordering::Acquire);
        while id != NAN {
            let next = self.index(id).load(Ordering::Acquire);
            match self
                .free
                .compare_exchange(id, next, Ordering::AcqRel, Ordering::SeqCst)
            {
                Ok(_) => break,
                Err(actual) => id = actual,
            }
        }
        if id == NAN {
            id = self.next.load(Ordering::Relaxed);
            if id < MAX {
                id = self.next.fetch_add(1, Ordering::Relaxed);
            }
        }
        if id < MAX {
            Some(id)
        } else {
            None
        }
    }

    /**
     * @description:
     * @param {*} self
     * @param {u64} id
     * @return {*}
     */
    fn dealloc(&self, id: u64) {
        let mut next = self.free.load(Ordering::Acquire);
        loop {
            self.index(id).store(next, Ordering::Release);
            match self
                .free
                .compare_exchange(next, id, Ordering::AcqRel, Ordering::SeqCst)
            {
                Ok(_) => break,
                Err(actual) => next = actual,
            }
        }
    }
}

pub struct PageTable {
    inner: Arc<Inner>,
}

impl PageTable {
    pub fn get(&self, id: u64) -> u64 {
        self.inner.index(id).load(Ordering::Acquire)
    }

    pub fn set(&self, id: u64, ptr: u64) {
        self.inner.index(id).store(ptr, Ordering::Release);
    }

    pub fn cas(&self, id: u64, old: u64, new: u64) -> Result<u64, u64> {
        self.inner
            .index(id)
            .compare_exchange(old, new, Ordering::AcqRel, Ordering::SeqCst)
    }

    pub fn alloc(&self) -> Option<u64> {
        self.inner.alloc()
    }

    pub fn dealloc(&self, id: u64) {
        self.inner.dealloc(id)
    }
}
