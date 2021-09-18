extern crate alloc;
#[cfg(not(feature = "std"))]
use alloc::boxed::Box;

use core::mem::ManuallyDrop;
use core::ptr::{self, NonNull};
use core::sync::atomic::Ordering::{Acquire, Relaxed, Release, SeqCst};

use crate::debra::common::epoch::Epoch;
use crate::debra::common::thread::{
    State::{Active, Inactive},
    ThreadState,
};
use crate::debra::Retired;

use crate::debra::config::{Config, CONFIG};
use crate::debra::global::{ABANDONED, EPOCH, THREADS};
use crate::debra::sealed::SealedList;

type BagPool = crate::debra::common::bag::BagPool<crate::debra::Debra>;
type EpochBagQueues = crate::debra::common::bag::EpochBagQueues<crate::debra::Debra>;
type ThreadStateIter = crate::debra::list::Iter<'static, ThreadState>;

#[derive(Debug)]
pub(super) struct LocalInner {
    advance_count: u32,

    bags: ManuallyDrop<EpochBagQueues>,

    bag_pool: BagPool,

    cached_local_epoch: Epoch,

    can_advance: bool,

    check_count: u32,

    config: Config,

    thread_iter: ThreadStateIter,
}

impl LocalInner {
    #[inline]
    pub fn new(global_epoch: Epoch) -> Self {
        Self {
            advance_count: 0,
            bags: ManuallyDrop::new(EpochBagQueues::new()),
            bag_pool: BagPool::new(),
            cached_local_epoch: global_epoch,
            can_advance: false,
            config: CONFIG.try_get().copied().unwrap_or_default(),
            check_count: 0,
            thread_iter: THREADS.iter(),
        }
    }

    #[inline]
    pub fn try_flush(&mut self, thread_state: &ThreadState) {
        let global_epoch = self.acquire_and_assess_global_epoch();

        if self.cached_local_epoch != global_epoch {
            thread_state.store(global_epoch, Inactive, Relaxed);
        }
    }

    #[inline]
    pub fn set_active(&mut self, thread_state: &ThreadState) {
        let global_epoch = self.acquire_and_assess_global_epoch();

        self.check_count += 1;
        if self.check_count == self.config.check_threshold() {
            self.check_count = 0;
            self.try_advance(thread_state, global_epoch);
        }

        thread_state.store(global_epoch, Active, SeqCst);
    }

    #[inline]
    pub fn set_inactive(&self, thread_state: &ThreadState) {
        thread_state.store(self.cached_local_epoch, Inactive, Release);
    }

    #[inline]
    pub fn retire_record(&mut self, record: Retired) {
        self.bags.retire_record(record, &mut self.bag_pool);
    }

    #[cold]
    pub unsafe fn retire_final_record(&mut self, record: Retired) {
        self.bags.retire_final_record(record);
    }

    #[inline]
    fn acquire_and_assess_global_epoch(&mut self) -> Epoch {
        let global_epoch = EPOCH.load(Acquire);

        if self.cached_local_epoch != global_epoch {
            unsafe { self.advance_local_epoch(global_epoch) };
        }

        global_epoch
    }

    #[cold]
    fn try_advance(&mut self, thread_state: &ThreadState, global_epoch: Epoch) {
        if let Ok(curr) = self.thread_iter.load_current_acquire() {
            let other = curr.unwrap_or_else(|| {
                self.can_advance = true;
                self.thread_iter = THREADS.iter();

                self.thread_iter
                    .load_head_acquire()
                    .unwrap_or_else(|| unreachable!())
            });

            if thread_state.is_same(other) || can_advance(global_epoch, other) {
                self.advance_count += 1;
                let _ = self.thread_iter.next();

                if self.can_advance && self.advance_count >= self.config.advance_threshold() {
                    EPOCH.compare_and_swap(global_epoch, global_epoch + 1, Release);
                }
            }
        }
    }

    #[cold]
    unsafe fn advance_local_epoch(&mut self, global_epoch: Epoch) {
        self.cached_local_epoch = global_epoch;
        self.can_advance = false;
        self.check_count = 0;
        self.advance_count = 0;
        self.thread_iter = THREADS.iter();

        self.rotate_and_reclaim();
    }

    #[inline]
    unsafe fn rotate_and_reclaim(&mut self) {
        self.bags.rotate_and_reclaim(&mut self.bag_pool);

        for sealed in ABANDONED.take_all() {
            if let Ok(age) = sealed.seal.relative_age(self.cached_local_epoch) {
                let retired = Retired::new_unchecked(NonNull::from(Box::leak(sealed)));
                self.bags
                    .retire_record_by_age(retired, age, &mut self.bag_pool);
            }
        }
    }
}

impl Drop for LocalInner {
    #[cold]
    fn drop(&mut self) {
        let bags = unsafe { ptr::read(&*self.bags) };
        if let Some(sealed) = SealedList::from_bags(bags, self.cached_local_epoch) {
            ABANDONED.push(sealed);
        }
    }
}

#[inline(always)]
fn can_advance(global_epoch: Epoch, other: &ThreadState) -> bool {
    let (epoch, state) = other.load(SeqCst);
    epoch == global_epoch || state == Inactive
}
