#![deny(clippy::all)]
#![allow(clippy::enum_variant_names)]
#![allow(clippy::mutable_key_type)]
#![feature(async_closure)]
#![feature(btree_extract_if)]
#![feature(hash_extract_if)]
#![feature(extract_if)]
#![feature(write_all_vectored)]
#![feature(allocator_api)]
#![feature(vec_into_raw_parts)]
#![feature(type_alias_impl_trait)]
#![allow(incomplete_features)]

pub mod consensus;
pub mod network;
