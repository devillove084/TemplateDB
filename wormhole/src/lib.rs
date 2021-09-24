#![feature(allocator_api)]
#![feature(alloc_layout_extra)]
#![feature(ptr_as_uninit)]
#![feature(slice_ptr_len)]

use bytes::Bytes;

pub mod kv;
pub mod sst;
pub mod wh;
pub mod wormhole;

pub const WH_KPN: u32 = 128u32;

pub fn kv_crc32(key: &Bytes, value: &Bytes) -> u32{
    todo!()
}

pub fn kv_crc_extend(lo: u32) -> u64 {
    todo!()
}
