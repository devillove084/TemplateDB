use std::cmp::Ordering;

use crate::{
    error::TemplateResult,
    iterator::{Direction, Iterator},
    util::comparator::Comparator,
};

pub struct KMergeIter<T: KMergeCore> {
    core: T,
    current: usize,
    direction: Direction,
}

impl<T: KMergeCore> KMergeIter<T> {
    pub fn new(core: T) -> Self {
        let current = core.iters_len();
        Self {
            core,
            current,
            direction: Direction::Forward,
        }
    }
}

/// An trait defines the operation in k merge sort
pub trait KMergeCore {
    type Cmp: Comparator;
    /// Returns current comparator
    fn cmp(&self) -> &Self::Cmp;

    /// The inner child iterators size
    fn iters_len(&self) -> usize;

    /// Updates the smallest if given `iter` has a smaller value and returns true.
    /// Otherwise returns false.
    fn smaller<'a>(&self, smallest: &mut Option<&'a [u8]>, iter: &'a dyn Iterator) -> bool {
        if iter.valid()
            && (smallest.is_none()
                || self.cmp().compare(iter.key(), smallest.as_ref().unwrap()) == Ordering::Less)
        {
            *smallest = Some(iter.key());
            true
        } else {
            false
        }
    }

    /// Updates the smallest if given `iter` has a smaller value and returns true.
    /// Otherwise returns false.
    fn larger<'a>(&self, largest: &mut Option<&'a [u8]>, iter: &'a dyn Iterator) -> bool {
        if iter.valid()
            && (largest.is_none()
                || self.cmp().compare(iter.key(), largest.as_ref().unwrap()) == Ordering::Greater)
        {
            *largest = Some(iter.key());
            true
        } else {
            false
        }
    }

    /// Find the iterator with the smallest 'key' and set it as current
    fn find_smallest(&mut self) -> usize;

    /// Find the iterator with the largest 'key' and set it as current
    fn find_largest(&mut self) -> usize;

    /// Returns an immutable borrow of ith child iterator
    fn get_child(&self, i: usize) -> &dyn Iterator;

    /// Returns a mutable borrow of ith child iterator
    fn get_child_mut(&mut self, i: usize) -> &mut dyn Iterator;

    /// Iterate each child iterator and call `f`
    fn for_each_child<F>(&mut self, f: F)
    where
        F: FnMut(&mut dyn Iterator);

    /// Iterate each child iterator except the ith iterator and call `f`
    fn for_not_ith<F>(&mut self, i: usize, f: F)
    where
        F: FnMut(&mut dyn Iterator, &Self::Cmp);

    /// Returns `Err` if inner children has errors.
    fn take_err(&mut self) -> TemplateResult<()>;
}

impl<T: KMergeCore> Iterator for KMergeIter<T> {
    fn valid(&self) -> bool {
        let i = self.current;
        if i < self.core.iters_len() {
            self.core.get_child(self.current).valid()
        } else {
            false
        }
    }

    fn seek_to_first(&mut self) {
        self.core.for_each_child(|i| i.seek_to_first());
        self.current = self.core.find_smallest();
        self.direction = Direction::Forward;
    }

    fn seek_to_last(&mut self) {
        self.core.for_each_child(|i| i.seek_to_last());
        self.current = self.core.find_largest();
        self.direction = Direction::Reverse;
    }

    fn seek(&mut self, target: &[u8]) {
        self.core.for_each_child(|i| i.seek(target));
        self.current = self.core.find_smallest();
        self.direction = Direction::Forward;
    }

    fn next(&mut self) {
        if self.direction != Direction::Forward {
            let key = self.key().to_vec();
            self.core.for_not_ith(self.current, |child, cmp| {
                child.seek(&key);
                if child.valid() && cmp.compare(&key, child.key()) == Ordering::Equal {
                    child.next();
                }
            });
            self.direction = Direction::Forward;
        }
        self.core.get_child_mut(self.current).next();
        self.current = self.core.find_smallest();
    }

    fn prev(&mut self) {
        if self.direction != Direction::Reverse {
            let key = self.key().to_vec();
            self.core.for_not_ith(self.current, |child, _| {
                child.seek(&key);
                if child.valid() {
                    child.prev();
                } else {
                    // Child has no key >= current key so point to the last
                    child.seek_to_last();
                }
            });
            self.direction = Direction::Reverse;
        }
        self.core.get_child_mut(self.current).prev();
        self.current = self.core.find_largest();
    }

    fn key(&self) -> &[u8] {
        self.core.get_child(self.current).key()
    }

    fn value(&self) -> &[u8] {
        self.core.get_child(self.current).value()
    }

    fn status(&mut self) -> TemplateResult<()> {
        self.core.take_err()
    }
}
