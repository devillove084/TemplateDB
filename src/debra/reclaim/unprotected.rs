use core::fmt;
use core::marker::PhantomData;

use typenum::Unsigned;

use crate::debra::reclaim::internal::Internal;
use crate::debra::reclaim::pointer::{Marked, MarkedNonNull, MarkedNonNullable, MarkedPointer};
use crate::debra::reclaim::{Reclaim, Shared, Unprotected};

impl<T, R, N> Clone for Unprotected<T, R, N> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            inner: self.inner,
            _marker: PhantomData,
        }
    }
}

impl<T, R: Reclaim, N> Copy for Unprotected<T, R, N> {}

impl<T, R: Reclaim, N: Unsigned> MarkedPointer for Unprotected<T, R, N> {
    impl_trait!(unprotected);
}

impl<T, R: Reclaim, N: Unsigned> Unprotected<T, R, N> {
    impl_inherent!(unprotected);

    #[inline]
    pub unsafe fn deref_unprotected<'a>(self) -> &'a T {
        self.inner.as_ref_unbounded()
    }

    #[inline]
    pub unsafe fn decompose_ref_unprotected<'a>(self) -> (&'a T, usize) {
        self.inner.decompose_ref_unbounded()
    }

    #[inline]
    pub unsafe fn into_shared<'a>(unprotected: Self) -> Shared<'a, T, R, N> {
        Shared::from_marked_non_null(unprotected.inner)
    }

    #[inline]
    pub fn cast<U>(unprotected: Self) -> Unprotected<U, R, N> {
        Unprotected {
            inner: unprotected.inner.cast(),
            _marker: PhantomData,
        }
    }
}

impl<T, R: Reclaim, N: Unsigned> fmt::Debug for Unprotected<T, R, N> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let (ptr, tag) = self.inner.decompose();
        f.debug_struct("Shared")
            .field("ptr", &ptr)
            .field("tag", &tag)
            .finish()
    }
}

impl<T, R: Reclaim, N: Unsigned> fmt::Pointer for Unprotected<T, R, N> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Pointer::fmt(&self.inner.decompose_ptr(), f)
    }
}

impl<T, R, N: Unsigned> MarkedNonNullable for Unprotected<T, R, N> {
    type Item = T;
    type MarkBits = N;

    #[inline]
    fn into_marked_non_null(self) -> MarkedNonNull<Self::Item, Self::MarkBits> {
        self.inner
    }
}

impl<T, R, N> Internal for Unprotected<T, R, N> {}
