use core::fmt;
use core::marker::PhantomData;
use core::ops::Deref;

use typenum::Unsigned;

use crate::debra::reclaim::internal::Internal;
use crate::debra::reclaim::pointer::{Marked, MarkedNonNull, MarkedNonNullable, MarkedPointer};
use crate::debra::reclaim::{GlobalReclaim, Reclaim, Unlinked, Unprotected};

impl<T, R: Reclaim, N: Unsigned> MarkedPointer for Unlinked<T, R, N> {
    impl_trait!(unlinked);
}

impl<T, R: Reclaim, N: Unsigned> Unlinked<T, R, N> {
    impl_inherent!(unlinked);

    #[inline]
    pub fn decompose_ref(unlinked: &Self) -> (&T, usize) {
        unsafe { unlinked.inner.decompose_ref() }
    }

    pub fn into_unprotected(shared: Self) -> Unprotected<T, R, N> {
        Unprotected {
            inner: shared.inner,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub unsafe fn cast<U>(unlinked: Self) -> Unlinked<U, R, N> {
        Unlinked {
            inner: unlinked.inner.cast(),
            _marker: PhantomData,
        }
    }

    #[inline]
    pub unsafe fn retire_local(self, local: &R::Local)
    where
        T: 'static,
    {
        R::retire_local(local, self)
    }

    #[inline]
    pub unsafe fn retire_local_unchecked(self, local: &R::Local) {
        R::retire_local_unchecked(local, self)
    }
}

impl<T, R: GlobalReclaim, N: Unsigned> Unlinked<T, R, N> {
    #[inline]
    pub unsafe fn retire(self)
    where
        T: 'static,
    {
        R::retire(self)
    }

    #[inline]
    pub unsafe fn retire_unchecked(self) {
        R::retire_unchecked(self)
    }
}

impl<T, R: Reclaim, N: Unsigned> AsRef<T> for Unlinked<T, R, N> {
    #[inline]
    fn as_ref(&self) -> &T {
        unsafe { self.inner.as_ref() }
    }
}

impl<T, R: Reclaim, N: Unsigned> Deref for Unlinked<T, R, N> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { self.inner.as_ref() }
    }
}

impl<T, R: Reclaim, N: Unsigned> fmt::Debug for Unlinked<T, R, N> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let (ptr, tag) = self.inner.decompose();
        f.debug_struct("Shared")
            .field("ptr", &ptr)
            .field("tag", &tag)
            .finish()
    }
}

impl<T, R: Reclaim, N: Unsigned> fmt::Pointer for Unlinked<T, R, N> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Pointer::fmt(&self.inner.decompose_ptr(), f)
    }
}

impl<T, R, N: Unsigned> MarkedNonNullable for Unlinked<T, R, N> {
    type Item = T;
    type MarkBits = N;

    #[inline]
    fn into_marked_non_null(self) -> MarkedNonNull<Self::Item, Self::MarkBits> {
        self.inner
    }
}

impl<T, R, N> Internal for Unlinked<T, R, N> {}
