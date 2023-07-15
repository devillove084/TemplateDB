// Copyright 2019 Fullstop000 <fullstop1005@gmail.com>.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

use crate::util::comparator::Comparator;
use crate::{Error, Result};
use std::cmp::Ordering;

/// A common trait for iterating all the key/value entries.
///
/// An `Iterator` must be invalid once created
pub trait Iterator {
    /// An iterator is either positioned at a key/value pair, or
    /// not valid.  This method returns true iff the iterator is valid.
    fn valid(&self) -> bool;

    /// Position at the first key in the source.  The iterator is Valid()
    /// after this call iff the source is not empty.
    fn seek_to_first(&mut self);

    /// Position at the last key in the source.  The iterator is
    /// Valid() after this call iff the source is not empty.
    fn seek_to_last(&mut self);

    /// Position at the first key in the source that is at or past target.
    /// The iterator is valid after this call iff the source contains
    /// an entry that comes at or past target.
    fn seek(&mut self, target: &[u8]);

    /// Moves to the next entry in the source.  After this call, the iterator is
    /// valid iff the iterator was not positioned at the last entry in the source.
    /// REQUIRES: `valid()`
    fn next(&mut self);

    /// Moves to the previous entry in the source.  After this call, the iterator
    /// is valid iff the iterator was not positioned at the first entry in source.
    /// REQUIRES: `valid()`
    fn prev(&mut self);

    /// Return the key for the current entry.  The underlying storage for
    /// the returned slice is valid only until the next modification of
    /// the iterator.
    /// REQUIRES: `valid()`
    fn key(&self) -> &[u8];

    /// Return the value for the current entry.  The underlying storage for
    /// the returned slice is valid only until the next modification of
    /// the iterator.
    /// REQUIRES: `valid()`
    fn value(&self) -> &[u8];

    /// If an error has occurred, return it.  Else return an ok status.
    fn status(&mut self) -> Result<()>;
}

/// A concatenated iterator contains an original iterator `origin` and a `DerivedIterFactory`.
/// New derived iterator is generated by `factory(origin.value())`.
/// The origin Iterator should yield out the last key but not the first.
/// This is just like a bucket iterator with lazy generator.
pub struct ConcatenateIterator<I: Iterator, F: DerivedIterFactory> {
    origin: I,
    factory: F,
    derived: Option<F::Iter>,
    prev_derived_value: Vec<u8>,
    err: Option<Error>,
}

/// A factory that takes value from the origin and
pub trait DerivedIterFactory {
    type Iter: Iterator;

    /// Create a new `Iterator` based on value yield by original `Iterator`
    fn derive(&self, value: &[u8]) -> Result<Self::Iter>;
}

impl<I: Iterator, F: DerivedIterFactory> ConcatenateIterator<I, F> {
    pub fn new(origin: I, factory: F) -> Self {
        Self {
            origin,
            factory,
            derived: None,
            prev_derived_value: vec![],
            err: None,
        }
    }

    #[inline]
    fn maybe_save_err(old: &mut Option<Error>, new: Result<()>) {
        if old.is_none() {
            if let Err(e) = new {
                *old = Some(e);
            }
        }
    }

    // Create a derived iter from the current value of the origin iter.
    // Only works when current derived iter is `None` or the previous origin value has been changed.
    // Same as `InitDataBlock` in C++ implementation
    fn init_derived_iter(&mut self) {
        if !self.origin.valid() {
            self.derived = None
        } else {
            let v = self.origin.value();
            if self.derived.is_none()
                || v.cmp(self.prev_derived_value.as_slice()) != Ordering::Equal
            {
                match self.factory.derive(v) {
                    Ok(derived) => {
                        if derived.valid() {
                            self.prev_derived_value = v.to_vec();
                        }
                        self.set_derived(Some(derived))
                    }
                    Err(e) => {
                        Self::maybe_save_err(&mut self.err, Err(e));
                        self.set_derived(None);
                    }
                }
            }
        }
    }

    // Same as `SetDataIterator` in C++ implementation
    #[inline]
    fn set_derived(&mut self, iter: Option<F::Iter>) {
        if let Some(iter) = &mut self.derived {
            Self::maybe_save_err(&mut self.err, iter.status())
        }
        self.derived = iter
    }

    // Skip invalid results util finding a valid derived iter by `next()`
    // If found, set derived iter to the first
    fn skip_forward(&mut self) {
        while self.derived.is_none() || !self.derived.as_ref().unwrap().valid() {
            if !self.origin.valid() {
                self.set_derived(None);
                break;
            } else {
                self.origin.next();
                self.init_derived_iter();
                if let Some(i) = &mut self.derived {
                    // init to the first
                    i.seek_to_first();
                }
            }
        }
    }

    // Skip invalid results util finding a valid derived iter by `prev()`
    // If found, set derived iter to the last
    fn skip_backward(&mut self) {
        while self.derived.is_none() || !self.derived.as_ref().unwrap().valid() {
            if !self.origin.valid() {
                self.set_derived(None);
                break;
            } else {
                self.origin.prev();
                self.init_derived_iter();
                if let Some(i) = &mut self.derived {
                    // init to the last
                    i.seek_to_last();
                }
            }
        }
    }

    #[inline]
    fn valid_or_panic(&self) {
        assert!(
            self.valid(),
            "[concatenated iterator] invalid derived iterator"
        )
    }
}

impl<I: Iterator, F: DerivedIterFactory> Iterator for ConcatenateIterator<I, F> {
    fn valid(&self) -> bool {
        if let Some(e) = &self.err {
            error!("[concatenated iter] Error: {:?}", e);
            return false;
        }
        if let Some(di) = &self.derived {
            di.valid()
        } else {
            false
        }
    }

    fn seek_to_first(&mut self) {
        self.origin.seek_to_first();
        self.init_derived_iter();
        if let Some(di) = self.derived.as_mut() {
            di.seek_to_first();
        }
        // scan forward util finding the first valid entry
        self.skip_forward();
    }

    fn seek_to_last(&mut self) {
        self.origin.seek_to_last();
        self.init_derived_iter();
        if let Some(di) = self.derived.as_mut() {
            di.seek_to_last()
        }
        // scan backward util finding the first valid entry
        self.skip_backward();
    }

    fn seek(&mut self, target: &[u8]) {
        self.origin.seek(target);
        self.init_derived_iter();
        if let Some(di) = self.derived.as_mut() {
            di.seek(target)
        }
        self.skip_forward();
    }

    fn next(&mut self) {
        self.valid_or_panic();
        self.derived.as_mut().map_or((), |di| di.next());
        self.skip_forward();
    }

    fn prev(&mut self) {
        self.valid_or_panic();
        self.derived.as_mut().map_or((), |di| di.prev());
        self.skip_backward();
    }

    fn key(&self) -> &[u8] {
        self.valid_or_panic();
        self.derived.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.valid_or_panic();
        self.derived.as_ref().unwrap().value()
    }

    fn status(&mut self) -> Result<()> {
        self.origin.status()?;
        if let Some(di) = self.derived.as_mut() {
            di.status()?
        };
        if let Some(e) = self.err.take() {
            return Err(e);
        }
        Ok(())
    }
}

#[derive(Eq, PartialEq)]
enum IterDirection {
    Forward,
    Reverse,
}

pub struct KMergeIter<T: KMergeCore> {
    core: T,
    current: usize,
    direction: IterDirection,
}

impl<T: KMergeCore> KMergeIter<T> {
    pub fn new(core: T) -> Self {
        let current = core.iters_len();
        Self {
            core,
            current,
            direction: IterDirection::Forward,
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
    fn take_err(&mut self) -> Result<()>;
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
        self.direction = IterDirection::Forward;
    }

    fn seek_to_last(&mut self) {
        self.core.for_each_child(|i| i.seek_to_last());
        self.current = self.core.find_largest();
        self.direction = IterDirection::Reverse;
    }

    fn seek(&mut self, target: &[u8]) {
        self.core.for_each_child(|i| i.seek(target));
        self.current = self.core.find_smallest();
        self.direction = IterDirection::Forward;
    }

    fn next(&mut self) {
        if self.direction != IterDirection::Forward {
            let key = self.key().to_vec();
            self.core.for_not_ith(self.current, |child, cmp| {
                child.seek(&key);
                if child.valid() && cmp.compare(&key, child.key()) == Ordering::Equal {
                    child.next();
                }
            });
            self.direction = IterDirection::Forward;
        }
        self.core.get_child_mut(self.current).next();
        self.current = self.core.find_smallest();
    }

    fn prev(&mut self) {
        if self.direction != IterDirection::Reverse {
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
            self.direction = IterDirection::Reverse;
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

    fn status(&mut self) -> Result<()> {
        self.core.take_err()
    }
}

#[cfg(test)]
mod tests {
    use crate::iterator::*;
    use crate::rand::Rng;
    use crate::util::comparator::BytewiseComparator;
    use crate::Result;
    use std::cmp::Ordering;
    use std::str;

    /// An helper to merge several `I` in merge iterating style
    struct SimpleKMerger<I: Iterator, C: Comparator> {
        cmp: C,
        children: Vec<I>,
    }

    impl<I: Iterator, C: Comparator> KMergeCore for SimpleKMerger<I, C> {
        type Cmp = C;
        fn cmp(&self) -> &Self::Cmp {
            &self.cmp
        }

        fn iters_len(&self) -> usize {
            self.children.len()
        }

        fn find_smallest(&mut self) -> usize {
            let mut smallest: Option<&[u8]> = None;
            let mut index = self.iters_len();
            for (i, child) in self.children.iter().enumerate() {
                if self.smaller(&mut smallest, child) {
                    index = i
                }
            }
            index
        }

        fn find_largest(&mut self) -> usize {
            let mut largest: Option<&[u8]> = None;
            let mut index = self.iters_len();
            for (i, child) in self.children.iter().enumerate() {
                if self.larger(&mut largest, child) {
                    index = i
                }
            }
            index
        }

        fn get_child(&self, i: usize) -> &dyn Iterator {
            self.children.get(i).unwrap() as &dyn Iterator
        }

        fn get_child_mut(&mut self, i: usize) -> &mut dyn Iterator {
            self.children.get_mut(i).unwrap() as &mut dyn Iterator
        }

        fn for_each_child<F>(&mut self, mut f: F)
        where
            F: FnMut(&mut dyn Iterator),
        {
            self.children
                .iter_mut()
                .for_each(|i| f(i as &mut dyn Iterator));
        }

        fn for_not_ith<F>(&mut self, n: usize, mut f: F)
        where
            F: FnMut(&mut dyn Iterator, &Self::Cmp),
        {
            for (i, child) in self.children.iter_mut().enumerate() {
                if i != n {
                    f(child as &mut dyn Iterator, &self.cmp)
                }
            }
        }

        fn take_err(&mut self) -> Result<()> {
            for i in self.children.iter_mut() {
                let status = i.status();
                if status.is_err() {
                    return status;
                }
            }
            Ok(())
        }
    }
    // Divide given ordered `src` into `n` lists and then construct a `MergingIterator` with them
    fn new_test_merging_iter(
        mut src: Vec<String>,
        n: usize,
    ) -> KMergeIter<SimpleKMerger<TestSimpleArrayIter, BytewiseComparator>> {
        let mut children = vec![];
        for _ in 0..n {
            children.push(vec![]);
        }
        src.sort();
        let mut rnd = rand::thread_rng();
        // Separate value into all children randomly
        for v in src {
            let i = rnd.gen_range(0, n);
            let child = children.get_mut(i).unwrap();
            child.push(v);
        }
        let cmp = BytewiseComparator::default();
        let iters = children
            .drain(..)
            .map(|mut child| {
                child.sort();
                TestSimpleArrayIter::new(child)
            })
            .collect::<Vec<_>>();
        KMergeIter::new(SimpleKMerger {
            cmp,
            children: iters,
        })
    }

    struct SortedIterTestSuite<O: Iterator, S: Iterator> {
        origin: O, // A sorted array based iterator
        shadow: S, // The iterator to be tested
    }

    impl<O: Iterator, S: Iterator> SortedIterTestSuite<O, S> {
        fn new(origin: O, shadow: S) -> Self {
            Self { origin, shadow }
        }

        fn assert_valid(&self, expect: bool) {
            assert_eq!(self.origin.valid(), expect);
            assert_eq!(self.origin.valid(), self.shadow.valid());
        }

        fn assert_key_and_value(&self) {
            assert_eq!(self.origin.key(), self.shadow.key());
            assert_eq!(self.origin.value(), self.shadow.value());
        }

        fn assert_iter_forward(&mut self) {
            self.seek_to_first();
            while self.valid() {
                self.assert_key_and_value();
                self.next();
            }
            self.assert_valid(false);
        }

        fn assert_iter_backward(&mut self) {
            self.seek_to_last();
            while self.valid() {
                self.assert_key_and_value();
                self.prev();
            }
            self.assert_valid(false);
        }
    }

    impl<O: Iterator, S: Iterator> Iterator for SortedIterTestSuite<O, S> {
        fn valid(&self) -> bool {
            self.origin.valid() && self.shadow.valid()
        }

        fn seek_to_first(&mut self) {
            self.origin.seek_to_first();
            self.shadow.seek_to_first();
        }
        fn seek_to_last(&mut self) {
            self.origin.seek_to_last();
            self.shadow.seek_to_last();
        }

        fn seek(&mut self, target: &[u8]) {
            self.origin.seek(target);
            self.shadow.seek(target);
        }

        fn next(&mut self) {
            self.origin.next();
            self.shadow.next();
        }

        fn prev(&mut self) {
            self.origin.prev();
            self.shadow.prev();
        }

        fn key(&self) -> &[u8] {
            unimplemented!()
        }

        fn value(&self) -> &[u8] {
            unimplemented!()
        }

        fn status(&mut self) -> Result<()> {
            unimplemented!()
        }
    }

    #[derive(Debug)]
    struct TestSimpleArrayIter {
        inner: Vec<String>,
        current: usize,
    }

    impl TestSimpleArrayIter {
        fn new(mut inner: Vec<String>) -> Self {
            inner.sort();
            let current = inner.len();
            Self { inner, current }
        }

        fn valid_or_panic(&self) {
            if !self.valid() {
                panic!("Invalid iterator {:?}", &self)
            }
        }
    }

    impl Iterator for TestSimpleArrayIter {
        fn valid(&self) -> bool {
            self.current < self.inner.len() && self.inner.len() > 0
        }
        fn seek_to_first(&mut self) {
            self.current = 0;
        }
        fn seek_to_last(&mut self) {
            if self.inner.len() > 0 {
                self.current = self.inner.len() - 1
            }
        }

        fn seek(&mut self, target: &[u8]) {
            let mut current = self.inner.len() + 1;
            for (i, s) in self.inner.iter().enumerate() {
                match s.as_bytes().cmp(target) {
                    Ordering::Equal | Ordering::Greater => {
                        current = i;
                        break;
                    }
                    _ => continue,
                }
            }
            self.current = current;
        }

        fn next(&mut self) {
            self.valid_or_panic();
            self.current += 1;
        }

        fn prev(&mut self) {
            self.valid_or_panic();
            if self.current > 0 {
                self.current -= 1
            } else {
                // marked as invalid
                self.current = self.inner.len()
            }
        }

        fn key(&self) -> &[u8] {
            self.valid_or_panic();
            self.inner[self.current].as_bytes()
        }

        fn value(&self) -> &[u8] {
            self.key()
        }

        fn status(&mut self) -> Result<()> {
            Ok(())
        }
    }

    struct SimpleDeriveFactory {}
    impl SimpleDeriveFactory {
        fn new() -> Self {
            Self {}
        }
    }

    impl DerivedIterFactory for SimpleDeriveFactory {
        type Iter = TestSimpleArrayIter;
        fn derive(&self, value: &[u8]) -> Result<Self::Iter> {
            let c = str::from_utf8(value)
                .unwrap()
                .chars()
                .nth(0)
                .unwrap()
                .to_string();
            let inner = vec![c.clone(), c.as_str().repeat(2), c.as_str().repeat(3)];
            Ok(TestSimpleArrayIter::new(inner))
        }
    }

    #[test]
    fn test_concatenated_iterator() {
        // inner: [a, aa, aaa, b, bb, bbb, c, cc, ccc]
        let mut iter = ConcatenateIterator::new(
            TestSimpleArrayIter::new(vec!["aaa".to_owned(), "bbb".to_owned(), "ccc".to_owned()]),
            SimpleDeriveFactory::new(),
        );

        assert!(!iter.valid());
        iter.seek_to_first();
        assert_eq!(str::from_utf8(iter.key()).unwrap(), "a");
        assert_eq!(str::from_utf8(iter.value()).unwrap(), "a");

        iter.next();
        assert_eq!(str::from_utf8(iter.key()).unwrap(), "aa");

        iter.seek_to_last();
        assert_eq!(str::from_utf8(iter.key()).unwrap(), "ccc");

        iter.prev();
        assert_eq!(str::from_utf8(iter.key()).unwrap(), "cc");

        iter.seek_to_first();
        iter.seek("b".as_bytes());
        assert_eq!(str::from_utf8(iter.key()).unwrap(), "b");

        iter.seek("bb".as_bytes());
        assert_eq!(str::from_utf8(iter.key()).unwrap(), "bb");

        iter.seek("bbbb".as_bytes());
        assert_eq!(str::from_utf8(iter.key()).unwrap(), "c");
        // Test seeking out of range
        iter.seek("1".as_bytes());
        assert_eq!(str::from_utf8(iter.key()).unwrap(), "a");
        iter.seek("d".as_bytes());
        assert!(!iter.valid());
    }

    #[test]
    fn test_merging_iterator() {
        let mut input = vec![];
        for i in 1..100 {
            input.push(i.to_string());
        }
        input.sort();
        let tests = vec![1, 5, 10, 50];
        for t in tests {
            let merging_iter = new_test_merging_iter(input.clone(), t);
            let origin = TestSimpleArrayIter::new(input.clone());
            let mut suite = SortedIterTestSuite::new(origin, merging_iter);
            suite.assert_valid(false);
            suite.seek_to_first();
            suite.assert_key_and_value();
            suite.seek_to_last();
            suite.assert_key_and_value();
            suite.seek("3".as_bytes());
            suite.assert_key_and_value();
            suite.prev();
            suite.assert_key_and_value();
            suite.next();
            suite.assert_key_and_value();
            suite.seek("0".as_bytes());
            suite.assert_key_and_value();
            suite.seek("9999".as_bytes());
            suite.assert_valid(false);
            suite.assert_iter_forward();
            suite.assert_iter_backward();
        }
    }
}