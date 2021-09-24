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

pub type LeafLink<K, V> = Option<Arc<RefCell<WormHoleLeaf<K, V>>>>;
pub type AnchorLink<K, V> = Option<Arc<RefCell<KeyValue<K, V>>>>;

pub struct WormHoleLeaf<K, V> {
    pub leaf_lock: RwLock<()>,
    pub sort_lock: Mutex<()>,
    pub version: u64,
    pub prev: LeafLink<K, V>,
    pub next: LeafLink<K, V>,
    pub anchor: AnchorLink<K, V>,
    pub nr_sorted: u32,
    pub nr_keys: u32,
    pub reversed: [u64; 2],
    // sorted by hashes
    // sorted by keys
}

pub struct WormHoleMap {}

pub struct WormHole<K, V> {
    pub hmap: WormHoleMap,
    pub leaf: WormHoleLeaf<K, V>,
    pub slab: WormHoleLeaf<K, V>,
    pub store: KeyValue<K, V>,
    pub hmap_second: Vec<WormHoleMap>,
    pub metalock: RwLock<()>,
}


impl WormHoleMeta {
    
}