#![deny(clippy::all)]
#![allow(clippy::diverging_sub_expression)]
#![allow(clippy::single_range_in_vec_init)]
#![allow(clippy::useless_vec)]
#![allow(clippy::module_inception)]
#![allow(clippy::only_used_in_recursion)]
#![feature(coroutines)]
#![feature(iterator_try_collect)]
#![feature(assert_matches)]
#![feature(error_generic_member_access)]

#[macro_use]
extern crate lazy_static;

pub mod binder;
pub mod catalog;
pub mod cli;
pub mod common;
pub mod db;
pub mod execution;
pub mod executor;
pub mod function;
pub mod main_entry;
pub mod optimizer;
pub mod parser;
pub mod planner;
pub mod planner_test;
pub mod storage;
pub mod types;
pub mod util;

pub use self::db::{Database, DatabaseError};
