#![deny(clippy::pedantic)]

#![feature(async_closure)]
#![feature(btree_extract_if)]
#![feature(hash_extract_if)]
#![feature(extract_if)]
#![feature(write_all_vectored)]
#![feature(allocator_api)]
#![feature(vec_into_raw_parts)]
#![feature(type_alias_impl_trait)]
#![feature(associated_type_bounds)]
#![allow(incomplete_features)]

pub mod consensus;
pub mod network;
