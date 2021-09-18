use core::fmt;
use core::marker::PhantomData;
use core::sync::atomic::{AtomicUsize, Ordering};

use typenum::Unsigned;

use crate::debra::reclaim::pointer::{self, AtomicMarkedPtr, MarkedPtr};

unsafe impl<T, N> Send for AtomicMarkedPtr<T, N> {}
unsafe impl<T, N> Sync for AtomicMarkedPtr<T, N> {}

impl<T, N> AtomicMarkedPtr<T, N> {
    #[inline]
    pub const fn null() -> Self {
        Self {
            inner: AtomicUsize::new(0),
            _marker: PhantomData,
        }
    }
}

impl<T, N: Unsigned> AtomicMarkedPtr<T, N> {
    pub const MARK_BITS: usize = N::USIZE;

    pub const MARK_MASK: usize = pointer::mark_mask::<T>(Self::MARK_BITS);

    pub const POINTER_MASK: usize = !Self::MARK_MASK;

    #[inline]
    pub fn new(ptr: MarkedPtr<T, N>) -> Self {
        Self {
            inner: AtomicUsize::new(ptr.inner as usize),
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn into_inner(self) -> MarkedPtr<T, N> {
        MarkedPtr::from_usize(self.inner.into_inner())
    }

    #[inline]
    pub fn load(&self, order: Ordering) -> MarkedPtr<T, N> {
        MarkedPtr::from_usize(self.inner.load(order))
    }

    #[inline]
    pub fn store(&self, ptr: MarkedPtr<T, N>, order: Ordering) {
        self.inner.store(ptr.into_usize(), order);
    }

    #[inline]
    pub fn swap(&self, ptr: MarkedPtr<T, N>, order: Ordering) -> MarkedPtr<T, N> {
        MarkedPtr::from_usize(self.inner.swap(ptr.into_usize(), order))
    }

    #[inline]
    pub fn compare_and_swap(
        &self,
        current: MarkedPtr<T, N>,
        new: MarkedPtr<T, N>,
        order: Ordering,
    ) -> MarkedPtr<T, N> {
        MarkedPtr::from_usize(self.inner.compare_and_swap(
            current.into_usize(),
            new.into_usize(),
            order,
        ))
    }

    #[inline]
    pub fn compare_exchange(
        &self,
        current: MarkedPtr<T, N>,
        new: MarkedPtr<T, N>,
        success: Ordering,
        failure: Ordering,
    ) -> Result<MarkedPtr<T, N>, MarkedPtr<T, N>> {
        self.inner
            .compare_exchange(current.into_usize(), new.into_usize(), success, failure)
            .map(MarkedPtr::from_usize)
            .map_err(MarkedPtr::from_usize)
    }

    #[inline]
    pub fn compare_exchange_weak(
        &self,
        current: MarkedPtr<T, N>,
        new: MarkedPtr<T, N>,
        success: Ordering,
        failure: Ordering,
    ) -> Result<MarkedPtr<T, N>, MarkedPtr<T, N>> {
        self.inner
            .compare_exchange_weak(current.into_usize(), new.into_usize(), success, failure)
            .map(MarkedPtr::from_usize)
            .map_err(MarkedPtr::from_usize)
    }

    #[inline]
    pub fn fetch_and(&self, value: usize, order: Ordering) -> MarkedPtr<T, N> {
        MarkedPtr::from_usize(self.inner.fetch_and(value, order))
    }

    #[inline]
    pub fn fetch_nand(&self, value: usize, order: Ordering) -> MarkedPtr<T, N> {
        MarkedPtr::from_usize(self.inner.fetch_nand(value, order))
    }

    #[inline]
    pub fn fetch_or(&self, value: usize, order: Ordering) -> MarkedPtr<T, N> {
        MarkedPtr::from_usize(self.inner.fetch_or(value, order))
    }

    #[inline]
    pub fn fetch_xor(&self, value: usize, order: Ordering) -> MarkedPtr<T, N> {
        MarkedPtr::from_usize(self.inner.fetch_xor(value, order))
    }
}

impl<T, N: Unsigned> Default for AtomicMarkedPtr<T, N> {
    #[inline]
    fn default() -> Self {
        Self::null()
    }
}

impl<T, N: Unsigned> fmt::Debug for AtomicMarkedPtr<T, N> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let (ptr, tag) = self.load(Ordering::SeqCst).decompose();
        f.debug_struct("AtomicMarkedPtr")
            .field("ptr", &ptr)
            .field("tag", &tag)
            .finish()
    }
}

impl<T, N: Unsigned> fmt::Pointer for AtomicMarkedPtr<T, N> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Pointer::fmt(&self.load(Ordering::SeqCst).decompose_ptr(), f)
    }
}

impl<T, N: Unsigned> From<*const T> for AtomicMarkedPtr<T, N> {
    #[inline]
    fn from(ptr: *const T) -> Self {
        AtomicMarkedPtr::new(MarkedPtr::from(ptr))
    }
}

impl<T, N: Unsigned> From<*mut T> for AtomicMarkedPtr<T, N> {
    #[inline]
    fn from(ptr: *mut T) -> Self {
        AtomicMarkedPtr::new(MarkedPtr::from(ptr))
    }
}

impl<T, N: Unsigned> From<MarkedPtr<T, N>> for AtomicMarkedPtr<T, N> {
    #[inline]
    fn from(ptr: MarkedPtr<T, N>) -> Self {
        AtomicMarkedPtr::new(ptr)
    }
}
