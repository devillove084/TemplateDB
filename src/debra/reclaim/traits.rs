

use core::sync::atomic::Ordering;

use typenum::Unsigned;

use crate::debra::reclaim::atomic::Atomic;
use crate::debra::reclaim::pointer::{Marked, MarkedPtr};
use crate::debra::reclaim::retired::Retired;
use crate::debra::reclaim::{NotEqualError, Shared};


pub unsafe trait Reclaim: Sized + 'static {
    
    type RecordHeader: Default + Sync + Sized;
}


pub unsafe trait Protect: Clone + Sized {
    
    type Reclaimer: Reclaim;

    
    
    fn release(&mut self);

    
    fn protect<T, N: Unsigned>(
        &mut self,
        src: &Atomic<T, Self::Reclaimer, N>,
        order: Ordering,
    ) -> Marked<Shared<T, Self::Reclaimer, N>>;

    
    fn protect_if_equal<T, N: Unsigned>(
        &mut self,
        src: &Atomic<T, Self::Reclaimer, N>,
        expected: MarkedPtr<T, N>,
        order: Ordering,
    ) -> Result<Marked<Shared<T, Self::Reclaimer, N>>, crate::debra::reclaim::NotEqualError>;
}


pub unsafe trait ProtectRegion: Protect {}


pub trait StoreRetired {
    type Reclaimer: Reclaim;

    
    unsafe fn retire(&self, record: Retired<Self::Reclaimer>);
}
