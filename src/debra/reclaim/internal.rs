use core::ptr::NonNull;
use core::sync::atomic::Ordering;

use typenum::Unsigned;

use crate::debra::reclaim::atomic::Atomic;
use crate::debra::reclaim::pointer::{Marked, MarkedPointer, MarkedPtr};
use crate::debra::reclaim::{AcquireResult, Reclaim, Shared};

pub trait GuardRef<'g> {
    type Reclaimer: Reclaim;

    fn load_protected<T, N: Unsigned>(
        self,
        atomic: &Atomic<T, Self::Reclaimer, N>,
        order: Ordering,
    ) -> Marked<Shared<'g, T, Self::Reclaimer, N>>;

    fn load_protected_if_equal<T, N: Unsigned>(
        self,
        atomic: &Atomic<T, Self::Reclaimer, N>,
        expected: MarkedPtr<T, N>,
        order: Ordering,
    ) -> AcquireResult<'g, T, Self::Reclaimer, N>;
}

pub trait Compare: MarkedPointer + Sized {
    type Reclaimer: Reclaim;
    type Unlinked: MarkedPointer<Item = Self::Item, MarkBits = Self::MarkBits>;
}

pub trait Store: MarkedPointer + Sized {
    type Reclaimer: Reclaim;
}

pub trait Internal {}

impl<'a, T> Internal for &'a T {}
impl<'a, T> Internal for &'a mut T {}
impl<T> Internal for NonNull<T> {}
