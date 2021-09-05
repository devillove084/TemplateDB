

use core::fmt;
use core::marker::PhantomData;
use core::ops::Deref;

use typenum::Unsigned;

use crate::debra::reclaim::internal::Internal;
use crate::debra::reclaim::pointer::{Marked, MarkedNonNull, MarkedNonNullable, MarkedPointer};
use crate::debra::reclaim::{Reclaim, Shared, Unprotected};



impl<'g, T, R, N> Clone for Shared<'g, T, N, R> {
    #[inline]
    fn clone(&self) -> Self {
        Self { inner: self.inner, _marker: PhantomData }
    }
}



impl<'g, T, R, N> Copy for Shared<'g, T, R, N> {}



impl<'g, T, R: Reclaim, N: Unsigned> MarkedPointer for Shared<'g, T, R, N> {
    impl_trait!(shared);
}



impl<'g, T, R: Reclaim, N: Unsigned> Shared<'g, T, R, N> {
    impl_inherent!(shared);

    
    
    #[inline]
    pub fn decompose_ref(shared: Self) -> (&'g T, usize) {
        let (ptr, tag) = shared.inner.decompose();
        unsafe { (&*ptr.as_ptr(), tag) }
    }

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    #[inline]
    pub fn into_ref(shared: Self) -> &'g T {
        unsafe { &*shared.inner.decompose_ptr() }
    }

    
    #[inline]
    pub fn into_unprotected(shared: Self) -> Unprotected<T, R, N> {
        Unprotected { inner: shared.inner, _marker: PhantomData }
    }

    
    
    
    
    
    
    
    
    
    #[inline]
    pub unsafe fn cast<'h, U>(shared: Self) -> Shared<'h, U, R, N> {
        Shared { inner: shared.inner.cast(), _marker: PhantomData }
    }
}



impl<'g, T, R: Reclaim, N: Unsigned> AsRef<T> for Shared<'g, T, R, N> {
    #[inline]
    fn as_ref(&self) -> &T {
        unsafe { self.inner.as_ref() }
    }
}



impl<'g, T, R: Reclaim, N: Unsigned> Deref for Shared<'g, T, R, N> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { self.inner.as_ref() }
    }
}



impl<'g, T, R: Reclaim, N: Unsigned> fmt::Debug for Shared<'g, T, R, N> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let (ptr, tag) = self.inner.decompose();
        f.debug_struct("Shared").field("ptr", &ptr).field("tag", &tag).finish()
    }
}



impl<'g, T, R: Reclaim, N: Unsigned> fmt::Pointer for Shared<'g, T, R, N> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Pointer::fmt(&self.inner.decompose_ptr(), f)
    }
}



impl<'g, T, R, N: Unsigned> MarkedNonNullable for Shared<'g, T, R, N> {
    type Item = T;
    type MarkBits = N;

    #[inline]
    fn into_marked_non_null(self) -> MarkedNonNull<Self::Item, Self::MarkBits> {
        self.inner
    }
}



impl<'g, T, R, N> Internal for Shared<'g, T, R, N> {}
