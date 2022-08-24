use std::{collections::btree_map::Range, iter::Peekable};

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
use crate::{stream::types::Sequence, Entry};

pub struct MemtableIter<'a> {
    next_seq: Sequence,
    /// iterators in reverse order
    iters: Vec<Peekable<Range<'a, Sequence, Entry>>>,
}

impl<'a> MemtableIter<'a> {
    pub fn new(next_seq: Sequence, iters: Vec<Peekable<Range<'a, Sequence, Entry>>>) -> Self {
        MemtableIter {
            next_seq,
            iters,
        }
    }
}

impl<'a> Iterator for MemtableIter<'a> {
    type Item = (&'a Sequence, &'a Entry);

    fn next(&mut self) -> Option<Self::Item> {
        let mut cached = None;
        'OUTER: for iter in self.iters.iter_mut().rev() {
            while let Some((seq, entry)) = iter.peek() {
                match (*seq).cmp(&self.next_seq) {
                    std::cmp::Ordering::Equal => {
                        cached = iter.next();
                        break 'OUTER;
                    }
                    std::cmp::Ordering::Less => {
                        iter.next();
                        continue;
                    }
                    std::cmp::Ordering::Greater => {
                        if !cached.as_ref().map(|(s, _)| *s <= *seq).unwrap_or_default() {
                            cached = Some((*seq, entry));
                        }
                        break;
                    }
                }
            }
        }
        cached
    }
}