use std::{alloc::{self, Allocator, Layout, LayoutError, dealloc}, mem, ptr::NonNull};
use std::alloc::alloc;
use std::ptr;

use bytes::Bytes;

use crate::{kv_crc32, kv_crc_extend};
use arrowkv::debra::Atomic;

pub trait SizeOpt{
    fn kvsize(&self) -> usize;
    fn key_size(&self) -> usize;
}

pub trait ConstructOpt {
    fn update_hash(&mut self);
    fn refill<K, V>(&mut self, key: Option<K>, value: Option<V>);
    fn convert<T>(&self, t: T) -> &[u8];
    // fn refill_value();
    // fn refill_str();
    // fn refill_str_pair();
    // fn refill_u64();
    // fn refill_hex32();
    // fn refill_hex64();
    // fn refill_hex64_klen();
    // fn refill_key();
    // fn refill_key_value();
    fn create<K, V>(&mut self, key: Option<K>, value: Option<V>);
    // fn create_str();
    // fn create_str_pair();
    // fn create_key();
    fn kv_null(&self);
}

pub trait DupicateOpt {
    fn dup(&self) -> Bytes;
    fn dup2<K, V>(&self, to: Bytes);
}

pub trait CompareOpt<T> {
    fn kv_match(&self, obj: &T);
    fn kv_match_full(&self, obj: &T);
    fn kv_match_hash(&self, obj: &T);
}


// pub struct KVMap {
//     pub kv: KeyValue,
// }

#[repr(C)]
#[derive(Clone)]
pub struct KeyValue<K, V> {
    pub key: K,
    pub value: V,
    pub hash: u64,
}


// impl<K, V> SizeOpt for KeyValue<K, V> {
//     fn kvsize(&self) -> usize {
//         return self.key.len() + self.value.len();
//     }

//     fn key_size(&self) -> usize {
//         return self.key.len();
//     }
// }

// impl<K, V> ConstructOpt for KeyValue<K, V> {
//     fn update_hash(&mut self) {
//         let lo: u32 = kv_crc32(&self.key, &self.value);
//         self.hash = kv_crc_extend(lo);
//     }

//     // fn refill(&mut self, key: Option<K>, value: Option<V>) where K: Sized, V: Sized{
//     //     if key.is_none() && value.is_none() {
//     //         return;
//     //     } else if !key.is_none() && value.is_none() {
//     //         // Just fill the value
//     //         self.value = Bytes::from(Box::new(self.convert(value.unwrap())).to_vec());
//     //         self.update_hash();
//     //     } else if key.is_none() && !value.is_none() {
//     //         // Just fill the key
//     //         self.key = Bytes::from(Box::new(self.convert(key.unwrap())).to_vec());
//     //         self.update_hash();
//     //     } else if !key.is_none() && !value.is_none() {
//     //         // fill key and value
//     //         self.key = Bytes::from(Box::new(self.convert(key.unwrap())).to_vec());
//     //         self.value = Bytes::from(Box::new(self.convert(value.unwrap())).to_vec());
//     //         self.update_hash();
//     //     }
//     // }

//     fn create(&mut self, key: Option<K>, value: Option<V>) {
//         self.refill(key, value);
//     }

//     fn kv_null(&self) -> () {
//         // XXX: ????
//     }

//     fn convert<T>(&self, t: T) -> &[u8] {
//         let slice = unsafe {
//             std::slice::from_raw_parts(&t as *const T as *const u8, std::mem::size_of_val(&t))
//         };
//         slice
//     }
    
// }


unsafe impl<K, V> Sync for KeyValue<K, V> {}
unsafe impl<K, V> Send for KeyValue<K, V> {}

impl<K, V> KeyValue<K, V> {
    pub unsafe fn get_layout(size: usize) -> Result<Layout, LayoutError> {
        let size_self = mem::size_of::<Self>();
        let align_self = mem::align_of::<Self>();
        let size_pointer = mem::size_of::<Atomic<Self>>();

        Layout::from_size_align(size_self + size_pointer * size, align_self)
    }
}

impl<K, V> Drop for KeyValue<K, V> {
    fn drop(&mut self) {
        todo!()
    }
}