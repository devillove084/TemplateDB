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

pub trait ForwardIter {
    type Item;

    /// Return the current item
    fn current(&self) -> Option<&Self::Item>;

    /// Rewind back to the first item
    fn rewind(&mut self);

    /// Advance to the next item
    fn next(&mut self);

    /// Skip the next `n` item
    fn skip(&mut self, mut n: usize) {
        while self.current().is_some() && n > 0 {
            self.next();
            n -= 1;
        }
    }

    /// Skip all items until the end
    fn skip_all(&mut self) {
        while self.current().is_some() {
            self.next();
        }
    }
}

impl<I: ForwardIter> ForwardIter for &mut I {
    type Item = I::Item;

    #[inline]
    fn current(&self) -> Option<&Self::Item> {
        (**self).current()
    }

    #[inline]
    fn rewind(&mut self) {
        (**self).rewind()
    }

    #[inline]
    fn next(&mut self) {
        (**self).next()
    }

    #[inline]
    fn skip(&mut self, mut n: usize) {
        (**self).skip(n)
    }

    #[inline]
    fn skip_all(&mut self) {
        (**self).skip_all()
    }
}

pub trait SeekableIter<T: ?Sized>: ForwardIter {
    fn seek(&mut self, target: &T);
}

impl<I: SeekableIter<T>, T: ?Sized> SeekableIter<T> for &mut I {
    fn seek(&mut self, target: &T) {
        (**self).seek(target)
    }
}

/// A wrapper that turns a slice into a `SeekableIter`
pub struct SliceIter<'a, I> {
    data: &'a [I],
    index: usize,
    current: Option<&'a I>,
}

impl<'a, I> SliceIter<'a, I> {
    pub fn new(data: &'a [I]) -> Self {
        SliceIter {
            data,
            index: data.len(),
            current: None,
        }
    }
}

impl<'a, I> ForwardIter for SliceIter<'a, I> {
    type Item = I;

    fn current(&self) -> Option<&Self::Item> {
        self.current
    }

    fn rewind(&mut self) {
        self.index = 0;
        self.current = self.data.get(0);
    }

    fn next(&mut self) {
        self.index += 1;
        self.current = self.data.get(self.index);
    }
}

impl<'a, I: Ord> SeekableIter<I> for SliceIter<'a, I> {
    fn seek(&mut self, target: &I) {
        self.index = match self.data.binary_search_by(|item| item.cmp(target)) {
            Ok(i) => i,
            Err(i) => i,
        };
        self.current = self.data.get(self.index)
    }
}

impl<'a, I> From<&'a [I]> for SliceIter<'a, I> {
    fn from(value: &'a [I]) -> Self {
        Self::new(value)
    }
}

impl<'a, I, const N: usize> From<&'a [I; N]> for SliceIter<'a, I> {
    fn from(value: &'a [I; N]) -> Self {
        Self::new(value.as_slice())
    }
}
