use crate::error::TemplateResult;

pub mod concatenate_iter;
pub mod db_iter;
pub mod kmerge_iter;
pub mod memtable_iter;

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
    fn status(&mut self) -> TemplateResult<()>;
}

#[derive(Eq, PartialEq)]
enum Direction {
    // When moving forward, the internal iterator is positioned at
    // the exact entry that yields inner.key(), inner.value()
    Forward,
    // When moving backwards, the internal iterator is positioned
    // just before all entries whose user key == inner.key().
    Reverse,
}

#[cfg(test)]
mod tests {
    use std::{cmp::Ordering, str};

    use super::{
        concatenate_iter::DerivedIterFactory,
        kmerge_iter::{KMergeCore, KMergeIter},
        Iterator,
    };
    use crate::{
        error::TemplateResult,
        iterator::concatenate_iter::ConcatenateIterator,
        rand::Rng,
        util::comparator::{BytewiseComparator, Comparator},
    };

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

        fn take_err(&mut self) -> TemplateResult<()> {
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

        fn status(&mut self) -> TemplateResult<()> {
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

        fn status(&mut self) -> TemplateResult<()> {
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
        fn derive(&self, value: &[u8]) -> TemplateResult<Self::Iter> {
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
