extern crate alloc;

#[cfg(not(feature = "std"))]
use alloc::boxed::Box;

use core::mem;

use crate::debra::reclaim::{Reclaim, Retired};
use arrayvec::ArrayVec;
use cfg_if::cfg_if;

use super::epoch::PossibleAge;

cfg_if! {
    if #[cfg(feature = "bag-size-1")] {
        const BAG_SIZE: usize = 1;
    } else if #[cfg(feature = "bag-size-2")] {
        const BAG_SIZE: usize = 2;
    } else if #[cfg(feature = "bag-size-4")] {
        const BAG_SIZE: usize = 4;
    } else if #[cfg(feature = "bag-size-8")] {
        const BAG_SIZE: usize = 8;
    } else if #[cfg(feature = "bag-size-16")] {
        const BAG_SIZE: usize = 16;
    } else if #[cfg(feature = "bag-size-32")] {
        const BAG_SIZE: usize = 32;
    } else if #[cfg(feature = "bag-size-64")] {
        const BAG_SIZE: usize = 64;
    } else if #[cfg(feature = "bag-size-128")] {
        const BAG_SIZE: usize = 128;
    } else if #[cfg(feature = "bag-size-256")] {
        const BAG_SIZE: usize = 256;
    } else if #[cfg(feature = "bag-size-512")] {
        const BAG_SIZE: usize = 512;
    } else {
        const BAG_SIZE: usize = 256;
    }
}

const BAG_POOL_SIZE: usize = 16;

#[derive(Debug)]
pub struct BagPool<R: Reclaim + 'static>(ArrayVec<Box<BagNode<R>>, BAG_POOL_SIZE>);

impl<R: Reclaim + 'static> Default for BagPool<R> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<R: Reclaim + 'static> BagPool<R> {
    #[inline]
    pub fn new() -> Self {
        Self(ArrayVec::default())
    }

    #[inline]
    pub fn with_bags() -> Self {
        Self((0..BAG_POOL_SIZE).map(|_| BagNode::boxed()).collect())
    }

    #[inline]
    fn allocate_bag(&mut self) -> Box<BagNode<R>> {
        self.0.pop().unwrap_or_else(BagNode::boxed)
    }

    #[inline]
    fn recycle_bag(&mut self, bag: Box<BagNode<R>>) {
        debug_assert!(bag.is_empty());
        if let Err(cap) = self.0.try_push(bag) {
            mem::drop(cap.element());
        }
    }
}

const BAG_QUEUE_COUNT: usize = 3;

#[derive(Debug)]
pub struct EpochBagQueues<R: Reclaim + 'static> {
    queues: [BagQueue<R>; BAG_QUEUE_COUNT],
    curr_idx: usize,
}

impl<R: Reclaim + 'static> Default for EpochBagQueues<R> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<R: Reclaim + 'static> EpochBagQueues<R> {
    #[inline]
    pub fn new() -> Self {
        Self {
            queues: [BagQueue::new(), BagQueue::new(), BagQueue::new()],
            curr_idx: 0,
        }
    }

    #[inline]
    pub fn into_sorted(self) -> [BagQueue<R>; BAG_QUEUE_COUNT] {
        let [a, b, c] = self.queues;
        match self.curr_idx {
            0 => [a, c, b],
            1 => [b, a, c],
            2 => [c, b, a],
            _ => unreachable!(),
        }
    }

    #[inline]
    pub fn retire_record(&mut self, record: Retired<R>, bag_pool: &mut BagPool<R>) {
        self.retire_record_by_age(record, PossibleAge::SameEpoch, bag_pool);
    }

    #[inline]
    pub fn retire_record_by_age(
        &mut self,
        record: Retired<R>,
        age: PossibleAge,
        bag_pool: &mut BagPool<R>,
    ) {
        let queue = match age {
            PossibleAge::SameEpoch => &mut self.queues[self.curr_idx],
            PossibleAge::OneEpoch => &mut self.queues[(self.curr_idx + 2) % BAG_QUEUE_COUNT],
            PossibleAge::TwoEpochs => &mut self.queues[(self.curr_idx + 1) % BAG_QUEUE_COUNT],
        };

        queue.retire_record(record, bag_pool);
    }

    #[inline]
    pub unsafe fn retire_final_record(&mut self, record: Retired<R>) {
        let curr = &mut self.queues[self.curr_idx];
        curr.head.retired_records.push_unchecked(record);
    }

    #[inline]
    pub unsafe fn rotate_and_reclaim(&mut self, bag_pool: &mut BagPool<R>) {
        self.curr_idx = (self.curr_idx + 1) % BAG_QUEUE_COUNT;
        self.queues[self.curr_idx].reclaim_full_bags(bag_pool);
    }
}

#[derive(Debug)]
pub struct BagQueue<R: Reclaim + 'static> {
    head: Box<BagNode<R>>,
}

impl<R: Reclaim + 'static> BagQueue<R> {
    #[inline]
    pub fn into_non_empty(self) -> Option<Box<BagNode<R>>> {
        if !self.is_empty() {
            Some(self.head)
        } else {
            None
        }
    }

    #[inline]
    fn new() -> Self {
        Self {
            head: BagNode::boxed(),
        }
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.head.is_empty()
    }

    #[inline]
    fn retire_record(&mut self, record: Retired<R>, bag_pool: &mut BagPool<R>) {
        unsafe { self.head.retired_records.push_unchecked(record) };
        if self.head.retired_records.is_full() {
            let mut old_head = bag_pool.allocate_bag();
            mem::swap(&mut self.head, &mut old_head);
            self.head.next = Some(old_head);
        }
    }

    #[inline]
    unsafe fn reclaim_full_bags(&mut self, bag_pool: &mut BagPool<R>) {
        let mut node = self.head.next.take();
        while let Some(mut bag) = node {
            bag.reclaim_all();
            node = bag.next.take();
            bag_pool.recycle_bag(bag);
        }
    }
}

#[derive(Debug)]
pub struct BagNode<R: Reclaim + 'static> {
    next: Option<Box<BagNode<R>>>,
    retired_records: ArrayVec<Retired<R>, BAG_SIZE>,
}

impl<R: Reclaim> BagNode<R> {
    #[inline]
    pub unsafe fn reclaim_all(&mut self) {
        self.reclaim_inner();

        let mut curr = self.next.take();
        while let Some(mut node) = curr {
            node.reclaim_inner();
            curr = node.next.take();
        }
    }

    #[inline]
    fn boxed() -> Box<Self> {
        Box::new(Self {
            next: None,
            retired_records: ArrayVec::default(),
        })
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.next.is_none() && self.retired_records.len() == 0
    }

    #[inline]
    unsafe fn reclaim_inner(&mut self) {
        for mut record in self.retired_records.drain(..) {
            record.reclaim();
        }
    }
}

impl<R: Reclaim + 'static> Drop for BagNode<R> {
    #[inline]
    fn drop(&mut self) {
        debug_assert!(
            self.is_empty(),
            "`BagNode`s must not be dropped unless empty (would leak memory)"
        );
    }
}
