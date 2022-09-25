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
use std::{cmp::Ordering, collections::btree_map::Range, iter::Peekable};

use crate::stream::{Entry, Sequence};

pub struct MemTableIter<'a> {
    next_seq: Sequence,
    /// Iterators in reverse order.
    iters: Vec<Peekable<Range<'a, Sequence, Entry>>>,
}

impl<'a> MemTableIter<'a> {
    pub fn new(next_seq: Sequence, iters: Vec<Peekable<Range<'a, Sequence, Entry>>>) -> Self {
        MemTableIter { next_seq, iters }
    }
}

impl<'a> std::iter::Iterator for MemTableIter<'a> {
    type Item = (&'a Sequence, &'a Entry);

    fn next(&mut self) -> Option<Self::Item> {
        let mut cached: Option<(&'a Sequence, &'a Entry)> = None;

        'OUTER: for iter in self.iters.iter_mut().rev() {
            while let Some((seq, entry)) = iter.peek() {
                match (*seq).cmp(&self.next_seq) {
                    Ordering::Equal => {
                        cached = iter.next();
                        break 'OUTER;
                    }
                    Ordering::Less => {
                        iter.next();
                        continue;
                    }
                    Ordering::Greater => {
                        if !cached.as_ref().map(|(s, _)| *s <= *seq).unwrap_or_default() {
                            cached = Some((*seq, entry));
                        }
                        break;
                    }
                }
            }
        }

        if let Some((seq, _)) = &cached {
            self.next_seq = Sequence::new(seq.epoch, seq.index + 1);
        }
        cached
    }
}
