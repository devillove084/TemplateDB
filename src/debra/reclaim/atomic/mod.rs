mod compare;
mod guard;
mod store;

use core::fmt;
use core::marker::PhantomData;
use core::sync::atomic::Ordering;

use typenum::Unsigned;

use crate::debra::reclaim::internal::{Compare, GuardRef, Internal, Store};
use crate::debra::reclaim::leak::Leaking;
use crate::debra::reclaim::pointer::{
    AtomicMarkedPtr, Marked, MarkedNonNull, MarkedPointer, MarkedPtr,
};
use crate::debra::reclaim::{
    AcquireResult, NotEqualError, Owned, Reclaim, Shared, Unlinked, Unprotected,
};

pub struct Atomic<T, R, N> {
    inner: AtomicMarkedPtr<T, N>,
    _marker: PhantomData<(T, R)>,
}

unsafe impl<T, R: Reclaim, N: Unsigned> Send for Atomic<T, R, N> where T: Send + Sync {}
unsafe impl<T, R: Reclaim, N: Unsigned> Sync for Atomic<T, R, N> where T: Send + Sync {}

impl<T, R, N> Atomic<T, R, N> {
    #[inline]
    pub const fn null() -> Self {
        Self {
            inner: AtomicMarkedPtr::null(),
            _marker: PhantomData,
        }
    }

    #[inline]
    pub const fn as_raw(&self) -> &AtomicMarkedPtr<T, N> {
        &self.inner
    }
}

impl<T, R: Reclaim, N: Unsigned> Atomic<T, R, N> {
    #[inline]
    pub fn new(val: T) -> Self {
        Self::from(Owned::from(val))
    }

    #[inline]
    pub unsafe fn from_raw(ptr: MarkedPtr<T, N>) -> Self {
        Self {
            inner: AtomicMarkedPtr::new(ptr),
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn load_raw(&self, order: Ordering) -> MarkedPtr<T, N> {
        self.inner.load(order)
    }

    #[inline]
    pub fn load_unprotected(&self, order: Ordering) -> Option<Unprotected<T, R, N>> {
        self.load_marked_unprotected(order).value()
    }

    #[inline]
    pub fn load_marked_unprotected(&self, order: Ordering) -> Marked<Unprotected<T, R, N>> {
        MarkedNonNull::new(self.inner.load(order)).map(|ptr| Unprotected {
            inner: ptr,
            _marker: PhantomData,
        })
    }

    #[inline]
    pub fn load<'g>(
        &self,
        order: Ordering,
        guard: impl GuardRef<'g, Reclaimer = R>,
    ) -> Option<Shared<'g, T, R, N>> {
        guard.load_protected(self, order).value()
    }

    #[inline]
    pub fn load_if_equal<'g>(
        &self,
        expected: MarkedPtr<T, N>,
        order: Ordering,
        guard: impl GuardRef<'g, Reclaimer = R>,
    ) -> Result<Option<Shared<'g, T, R, N>>, NotEqualError> {
        guard
            .load_protected_if_equal(self, expected, order)
            .map(Marked::value)
    }

    #[inline]
    pub fn load_marked<'g>(
        &self,
        order: Ordering,
        guard: impl GuardRef<'g, Reclaimer = R>,
    ) -> Marked<Shared<'g, T, R, N>> {
        guard.load_protected(self, order)
    }

    #[inline]
    pub fn load_marked_if_equal<'g>(
        &self,
        expected: MarkedPtr<T, N>,
        order: Ordering,
        guard: impl GuardRef<'g, Reclaimer = R>,
    ) -> AcquireResult<'g, T, R, N> {
        guard.load_protected_if_equal(self, expected, order)
    }

    #[inline]
    pub fn store(&self, ptr: impl Store<Item = T, MarkBits = N, Reclaimer = R>, order: Ordering) {
        self.inner.store(MarkedPointer::into_marked_ptr(ptr), order);
    }

    #[inline]
    pub fn swap(
        &self,
        ptr: impl Store<Item = T, Reclaimer = R, MarkBits = N>,
        order: Ordering,
    ) -> Option<Unlinked<T, R, N>> {
        let res = self.inner.swap(MarkedPointer::into_marked_ptr(ptr), order);

        unsafe { Option::from_marked_ptr(res) }
    }

    #[inline]
    pub fn compare_exchange<C, S>(
        &self,
        current: C,
        new: S,
        success: Ordering,
        failure: Ordering,
    ) -> Result<C::Unlinked, CompareExchangeFailure<T, R, S, N>>
    where
        C: Compare<Item = T, MarkBits = N, Reclaimer = R>,
        S: Store<Item = T, MarkBits = N, Reclaimer = R>,
    {
        let current = MarkedPointer::into_marked_ptr(current);
        let new = MarkedPointer::into_marked_ptr(new);

        self.inner
            .compare_exchange(current, new, success, failure)
            .map(|ptr| unsafe { C::Unlinked::from_marked_ptr(ptr) })
            .map_err(|ptr| CompareExchangeFailure {
                loaded: unsafe { Option::from_marked_ptr(ptr) },
                input: unsafe { S::from_marked_ptr(new) },
                _marker: PhantomData,
            })
    }

    #[inline]
    pub fn compare_exchange_weak<C, S>(
        &self,
        current: C,
        new: S,
        success: Ordering,
        failure: Ordering,
    ) -> Result<C::Unlinked, CompareExchangeFailure<T, R, S, N>>
    where
        C: Compare<Item = T, MarkBits = N, Reclaimer = R>,
        S: Store<Item = T, MarkBits = N, Reclaimer = R>,
    {
        let current = MarkedPointer::into_marked_ptr(current);
        let new = MarkedPointer::into_marked_ptr(new);

        self.inner
            .compare_exchange_weak(current, new, success, failure)
            .map(|ptr| unsafe { C::Unlinked::from_marked_ptr(ptr) })
            .map_err(|ptr| CompareExchangeFailure {
                loaded: unsafe { Option::from_marked_ptr(ptr) },
                input: unsafe { S::from_marked_ptr(new) },
                _marker: PhantomData,
            })
    }

    #[inline]
    pub fn take(&mut self) -> Option<Owned<T, R, N>> {
        MarkedNonNull::new(self.inner.swap(MarkedPtr::null(), Ordering::Relaxed))
            .map(|ptr| unsafe { Owned::from_marked_non_null(ptr) })
            .value()
    }
}

impl<T, N: Unsigned> Atomic<T, Leaking, N> {
    #[inline]
    pub fn load_shared(&self, order: Ordering) -> Option<Shared<T, Leaking, N>> {
        self.load_marked_shared(order).value()
    }

    #[inline]
    pub fn load_marked_shared(&self, order: Ordering) -> Marked<Shared<T, Leaking, N>> {
        MarkedNonNull::new(self.inner.load(order)).map(|ptr| Shared {
            inner: ptr,
            _marker: PhantomData,
        })
    }
}

impl<T, R: Reclaim, N: Unsigned> Default for Atomic<T, R, N> {
    #[inline]
    fn default() -> Self {
        Self::null()
    }
}

impl<T, R: Reclaim, N: Unsigned> From<T> for Atomic<T, R, N> {
    #[inline]
    fn from(val: T) -> Self {
        Self::new(val)
    }
}

impl<T, R: Reclaim, N: Unsigned> From<Owned<T, R, N>> for Atomic<T, R, N> {
    #[inline]
    fn from(owned: Owned<T, R, N>) -> Self {
        Self {
            inner: AtomicMarkedPtr::from(Owned::into_marked_ptr(owned)),
            _marker: PhantomData,
        }
    }
}

impl<T, R: Reclaim, N: Unsigned> fmt::Debug for Atomic<T, R, N> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let (ptr, tag) = self.inner.load(Ordering::SeqCst).decompose();
        f.debug_struct("Atomic")
            .field("ptr", &ptr)
            .field("tag", &tag)
            .finish()
    }
}

impl<T, R: Reclaim, N: Unsigned> fmt::Pointer for Atomic<T, R, N> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Pointer::fmt(&self.inner.load(Ordering::SeqCst), f)
    }
}

impl<T, R: Reclaim, N: Unsigned> Internal for Atomic<T, R, N> {}

#[derive(Debug)]
pub struct CompareExchangeFailure<T, R, S, N>
where
    R: Reclaim,
    S: Store<Item = T, MarkBits = N, Reclaimer = R>,
    N: Unsigned,
{
    pub loaded: Option<Unprotected<T, R, N>>,

    pub input: S,

    _marker: PhantomData<R>,
}
