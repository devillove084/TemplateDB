

#![warn(missing_docs)]
#![cfg_attr(not(any(test, feature = "std")), no_std)]
#![feature(result_copied)]
#![feature(associated_type_bounds)]

#[cfg(not(feature = "std"))]
extern crate alloc;


pub mod default;

pub mod abandoned;
pub mod config;
pub mod global;
pub mod guard;
pub mod list;
pub mod local;
pub mod sealed;
pub mod common;
pub mod reclaim;

use core::fmt;

pub use reclaim::typenum;

pub use crate::debra::config::{Config, ConfigBuilder, CONFIG};

#[cfg(not(feature = "std"))]
pub use crate::debra::local::Local;
#[cfg(feature = "std")]
use crate::local::Local;

use cfg_if::cfg_if;
use crate::debra::common::LocalAccess;
use reclaim::prelude::*;
use typenum::{Unsigned, U0};



pub type Atomic<T, N = U0> = reclaim::Atomic<T, Debra, N>;


pub type Owned<T, N = U0> = reclaim::Owned<T, Debra, N>;


pub type Shared<'g, T, N = U0> = reclaim::Shared<'g, T, Debra, N>;


pub type Unlinked<T, N = U0> = reclaim::Unlinked<T, Debra, N>;


pub type Unprotected<T, N = U0> = reclaim::Unprotected<T, Debra, N>;

cfg_if! {
    if #[cfg(feature = "std")] {
        
        
        pub type Guard = crate::guard::Guard<crate::default::DefaultAccess>;
    } else {
        
        
        pub type LocalGuard<'a> = crate::debra::guard::Guard<&'a Local>;
    }
}

type Retired = reclaim::Retired<Debra>;






#[derive(Copy, Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct Debra;



impl fmt::Display for Debra {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "distributed epoch based reclamation")
    }
}



unsafe impl Reclaim for Debra {
    type Local = Local;
    type RecordHeader = ();

    #[inline]
    unsafe fn retire_local<T: 'static, N: Unsigned>(local: &Self::Local, unlinked: Unlinked<T, N>) {
        Self::retire_local_unchecked(local, unlinked);
    }

    #[inline]
    unsafe fn retire_local_unchecked<T, N: Unsigned>(
        local: &Self::Local,
        unlinked: Unlinked<T, N>,
    ) {
        let unmarked = unlinked.into_marked_non_null().decompose_non_null();
        local.retire_record(Retired::new_unchecked(unmarked));
    }
}
