use std::{cell::RefCell, sync::Arc};

use spin::{mutex::Mutex, RwLock};

use crate::kv::KeyValue;

pub struct WormHoleMeta {
    pub key_len: usize,
    pub left_most: usize,
    pub bitmin: usize,
    pub bitmax: usize,
    pub right_most: usize,
    pub hash: u64,
    pub left_path: usize,
    pub bit_equal: bool,
}

pub type LeafLink = Option<Arc<RefCell<WormHoleLeaf>>>;
pub type AnchorLink = Option<Arc<RefCell<KeyValue>>>;

pub struct WormHoleLeaf {
    pub leaf_lock: RwLock<()>,
    pub sort_lock: Mutex<()>,
    pub version: u64,
    pub prev: LeafLink,
    pub next: LeafLink,
    pub anchor: AnchorLink,
    pub nr_sorted: u32,
    pub nr_keys: u32,
    pub reversed: [u64; 2],
    // sorted by hashes
    // sorted by keys
}

pub struct WormHoleMap {}

pub struct WormHole {
    pub hmap: WormHoleMap,
    pub leaf: WormHoleLeaf,
    pub slab: WormHoleLeaf,
    pub store: KeyValue,
    pub hmap_second: Vec<WormHoleMap>,
    pub metalock: RwLock<()>,
}


impl WormHoleMeta {
    
}