use core::sync::atomic::Ordering;

use crate::debra::common::LocalAccess;
use crate::debra::reclaim::prelude::*;
use crate::debra::reclaim::{AcquireResult, MarkedPtr, NotEqualError};

use crate::debra::local::Local;
use crate::debra::typenum::Unsigned;
use crate::debra::{Atomic, Debra, Shared};

pub struct Guard<L: LocalAccess> {
    local_access: L,
}

impl<'a> Guard<&'a Local> {
    #[inline]
    pub fn new_with_local(local_access: &'a Local) -> Self {
        Self::with_local_access(local_access)
    }
}

impl<L: LocalAccess> Guard<L> {
    #[inline]
    pub fn with_local_access(local_access: L) -> Self {
        local_access.set_active();
        Self { local_access }
    }
}

impl<L: LocalAccess> Clone for Guard<L> {
    #[inline]
    fn clone(&self) -> Self {
        self.local_access.set_active();
        Self {
            local_access: self.local_access,
        }
    }
}

impl<L: LocalAccess + Default> Default for Guard<L> {
    #[inline]
    fn default() -> Self {
        Self::with_local_access(Default::default())
    }
}

impl<L: LocalAccess> Drop for Guard<L> {
    #[inline]
    fn drop(&mut self) {
        self.local_access.set_inactive();
    }
}

unsafe impl<L: LocalAccess<Reclaimer = Debra>> Protect for Guard<L> {
    type Reclaimer = Debra;

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
            ptr if ptr == expected => unsafe { Ok(Marked::from_marked_ptr(ptr)) },
            _ => Err(NotEqualError),
        }
    }
}

unsafe impl<L: LocalAccess<Reclaimer = Debra>> ProtectRegion for Guard<L> {}
