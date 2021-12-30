mod serialization;

mod entry;
mod iter;
mod node;
mod sparse;
mod subtrie;
mod trie;
mod util;
mod my;
mod convec;

pub mod wrapper;

pub use entry::{Entry, OccupiedEntry, VacantEntry};
pub use iter::{IntoIter, Iter, IterMut};
pub use subtrie::SubTrie;
pub use trie::{Break, Trie};

extern crate test;

use test::Bencher;
use std::iter::repeat;

#[bench]
fn bench_std_vec_in_thread(b: &mut Bencher) {
    let vec = Vec::new();
    let chars: Vec<_> = repeat('a').take(1000000000).collect();
    for (i, c) in chars.iter().enumerate() {
        vec.push(*c);
    }
    b.iter(|| chars);
}

// #[bench]
// fn bench_my_vec_in_thread()