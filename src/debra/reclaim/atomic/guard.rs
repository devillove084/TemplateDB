use core::sync::atomic::Ordering;

use typenum::Unsigned;

use crate::debra::reclaim::atomic::Atomic;
use crate::debra::reclaim::internal::GuardRef;
use crate::debra::reclaim::pointer::{Marked, MarkedPointer, MarkedPtr};
use crate::debra::reclaim::{AcquireResult, Protect, ProtectRegion, Shared};

impl<'g, G> GuardRef<'g> for &'g mut G
where
    G: Protect,
{
    type Reclaimer = <G as Protect>::Reclaimer;

    #[inline]
    fn load_protected<T, N: Unsigned>(
        self,
        atomic: &Atomic<T, Self::Reclaimer, N>,
        order: Ordering,
    ) -> Marked<Shared<'g, T, Self::Reclaimer, N>> {
        self.protect(atomic, order)
    }

    #[inline]
    fn load_protected_if_equal<T, N: Unsigned>(
        self,
        atomic: &Atomic<T, Self::Reclaimer, N>,
        expected: MarkedPtr<T, N>,
        order: Ordering,
    ) -> AcquireResult<'g, T, Self::Reclaimer, N> {
        self.protect_if_equal(atomic, expected, order)
    }
}

impl<'g, G> GuardRef<'g> for &'g G
where
    G: ProtectRegion,
{
    type Reclaimer = <G as Protect>::Reclaimer;

    #[inline]
    fn load_protected<T, N: Unsigned>(
        self,
        atomic: &Atomic<T, Self::Reclaimer, N>,
        order: Ordering,
    ) -> Marked<Shared<'g, T, Self::Reclaimer, N>> {
        unsafe { Marked::from_marked_ptr(atomic.load_raw(order)) }
    }

    #[inline]
    fn load_protected_if_equal<T, N: Unsigned>(
        self,
        atomic: &Atomic<T, Self::Reclaimer, N>,
        expected: MarkedPtr<T, N>,
        order: Ordering,
    ) -> AcquireResult<'g, T, Self::Reclaimer, N> {
        match atomic.load_raw(order) {
            raw if raw == expected => Ok(unsafe { Marked::from_marked_ptr(raw) }),
            _ => Err(crate::debra::reclaim::NotEqualError),
        }
    }
}
