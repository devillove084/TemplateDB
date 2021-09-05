


use core::sync::atomic::Ordering;

use typenum::Unsigned;

use crate::debra::reclaim::pointer::{Marked, MarkedPointer, MarkedPtr};
use crate::debra::reclaim::{AcquireResult, GlobalReclaim, Protect, ProtectRegion, Reclaim};



pub type Atomic<T, N> = crate::debra::reclaim::Atomic<T, Leaking, N>;


pub type Shared<'g, T, N> = crate::debra::reclaim::Shared<'g, T, Leaking, N>;


pub type Owned<T, N> = crate::debra::reclaim::Owned<T, Leaking, N>;


pub type Unlinked<T, N> = crate::debra::reclaim::Unlinked<T, Leaking, N>;


pub type Unprotected<T, N> = crate::debra::reclaim::Unprotected<T, Leaking, N>;






#[derive(Debug, Default)]
pub struct Leaking;



impl Leaking {
    
    
    
    
    
    #[inline]
    pub fn leak<T, N: Unsigned>(unlinked: Unlinked<T, N>) {
        unsafe { Self::retire_unchecked(unlinked) };
    }
}






#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct Guard;



impl Guard {
    
    #[inline]
    pub fn new() -> Self {
        Self
    }
}





#[cfg(test)]
pub struct Header {
    pub checksum: usize,
}



#[cfg(test)]
impl Default for Header {
    #[inline]
    fn default() -> Self {
        Self { checksum: 0xDEAD_BEEF }
    }
}



unsafe impl GlobalReclaim for Leaking {
    type Guard = Guard;

    #[inline]
    fn try_reclaim() {}

    #[inline]
    unsafe fn retire<T: 'static, N: Unsigned>(_: Unlinked<T, N>) {}

    #[inline]
    unsafe fn retire_unchecked<T, N: Unsigned>(_: Unlinked<T, N>) {}
}



unsafe impl Reclaim for Leaking {
    type Local = ();

    #[cfg(test)]
    type RecordHeader = Header;
    #[cfg(not(test))]
    type RecordHeader = ();

    
    
    
    
    
    
    #[inline]
    unsafe fn retire_local<T: 'static, N: Unsigned>(_: &(), _: Unlinked<T, N>) {}

    
    
    
    
    
    
    #[inline]
    unsafe fn retire_local_unchecked<T, N: Unsigned>(_: &(), _: Unlinked<T, N>) {}
}



unsafe impl Protect for Guard {
    type Reclaimer = Leaking;

    
    #[inline]
    fn release(&mut self) {}

    
    #[inline]
    fn protect<T, N: Unsigned>(
        &mut self,
        atomic: &Atomic<T, N>,
        order: Ordering,
    ) -> Marked<Shared<T, N>> {
        unsafe { Marked::from_marked_ptr(atomic.load_raw(order)) }
    }

    
    #[inline]
    fn protect_if_equal<T, N: Unsigned>(
        &mut self,
        atomic: &Atomic<T, N>,
        expected: MarkedPtr<T, N>,
        order: Ordering,
    ) -> AcquireResult<T, Self::Reclaimer, N> {
        match atomic.load_raw(order) {
            raw if raw == expected => Ok(unsafe { Marked::from_marked_ptr(raw) }),
            _ => Err(crate::debra::reclaim::NotEqualError),
        }
    }
}



unsafe impl ProtectRegion for Guard {}
