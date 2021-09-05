
use std::marker::PhantomData;
use super::common::LocalAccess;
use crate::debra::reclaim::{GlobalReclaim, MarkedPointer, Reclaim};
use super::guard::Guard;
use super::local::Local;
use super::typenum::Unsigned;
use super::{Debra, Retired, Unlinked};

thread_local!(static LOCAL: Local = Local::new());

impl Debra {
    #[inline]
    pub fn is_thread_active() -> bool {
        LOCAL.with(|local| local.is_active())
    }
}

unsafe impl GlobalReclaim for Debra {
    type Guard = Guard<DefaultAccess>;

    #[inline]
    unsafe fn retire<T: 'static, N: Unsigned>(unlinked: Unlinked<T, N>) {
        LOCAL.with(move |local| Self::retire_local(local, unlinked));
    }

    #[inline]
    unsafe fn retire_unchecked<T, N: Unsigned>(unlinked: Unlinked<T, N>) {
        LOCAL.with(move |local| Self::retire_local_unchecked(local, unlinked));
    }

    #[inline]
    fn try_reclaim() {
        LOCAL.with(|local| local.try_flush());
    }

    fn guard() -> Self::Guard {
        Self::Guard::default()
    }

    unsafe fn retire_raw<T, N: Unsigned>(ptr: super::reclaim::MarkedPtr<T, N>) {
        debug_assert!(!ptr.is_null());
        Self::retire_unchecked(crate::debra::reclaim::Unlinked::from_marked_ptr(ptr));
    }
}

impl Guard<DefaultAccess> {
    #[inline]
    pub fn new() -> Self {
        Self::with_local_access(DefaultAccess::default())
    }
}










#[derive(Copy, Clone, Debug, Default)]
pub struct DefaultAccess(PhantomData<*mut ()>);



impl LocalAccess for DefaultAccess {
    type Reclaimer = Debra;

    #[inline]
    fn is_active(self) -> bool {
        LOCAL.with(|local| local.is_active())
    }

    #[inline]
    fn set_active(self) {
        LOCAL.with(|local| local.set_active());
    }

    #[inline]
    fn set_inactive(self) {
        LOCAL.with(|local| local.set_inactive());
    }

    #[inline]
    fn retire_record(self, record: Retired) {
        LOCAL.with(move |local| local.retire_record(record));
    }
}
