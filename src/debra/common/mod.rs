#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
extern crate alloc;
#[cfg(test)]
extern crate std;

pub use arrayvec;

use crate::debra::reclaim::{Reclaim, Retired};

pub mod bag;
pub mod epoch;
pub mod thread;

pub trait LocalAccess
where
    Self: Clone + Copy + Sized,
{
    type Reclaimer: Reclaim;

    fn is_active(self) -> bool;

    fn set_active(self);

    fn set_inactive(self);

    fn retire_record(self, record: Retired<Self::Reclaimer>);
}
