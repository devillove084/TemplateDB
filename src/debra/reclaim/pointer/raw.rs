use core::cmp::{self, PartialEq, PartialOrd};
use core::fmt;
use core::marker::PhantomData;
use core::ptr::{self, NonNull};

use typenum::{IsGreaterOrEqual, True, Unsigned};

use crate::debra::reclaim::pointer::{self, MarkedNonNull, MarkedPtr};

impl<T, N> Clone for MarkedPtr<T, N> {
    #[inline]
    fn clone(&self) -> Self {
        Self::new(self.inner)
    }
}

impl<T, N> Copy for MarkedPtr<T, N> {}

impl<T, N> MarkedPtr<T, N> {
    #[inline]
    pub const fn new(ptr: *mut T) -> Self {
        Self {
            inner: ptr,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub const fn null() -> Self {
        Self::new(ptr::null_mut())
    }

    #[inline]
    pub const fn cast<U>(self) -> MarkedPtr<U, N> {
        MarkedPtr::new(self.inner as *mut U)
    }

    #[inline]
    pub const fn from_usize(val: usize) -> Self {
        Self::new(val as *mut _)
    }
}

impl<T, N: Unsigned> MarkedPtr<T, N> {
    pub const MARK_BITS: usize = N::USIZE;

    pub const MARK_MASK: usize = pointer::mark_mask::<T>(Self::MARK_BITS);

    pub const POINTER_MASK: usize = !Self::MARK_MASK;

    #[inline]
    pub fn into_usize(self) -> usize {
        self.inner as usize
    }

    #[inline]
    pub fn into_ptr(self) -> *mut T {
        self.inner
    }

    #[inline]
    pub fn compose(ptr: *mut T, tag: usize) -> Self {
        debug_assert_eq!(
            0,
            ptr as usize & Self::MARK_MASK,
            "pointer must be properly aligned"
        );
        Self::new(pointer::compose::<_, N>(ptr, tag))
    }

    #[inline]
    pub fn convert<M: Unsigned>(other: MarkedPtr<T, M>) -> Self
    where
        N: IsGreaterOrEqual<M, Output = True>,
    {
        Self::new(other.inner)
    }

    #[inline]
    pub fn clear_tag(self) -> Self {
        Self::new(self.decompose_ptr())
    }

    #[inline]
    pub fn with_tag(self, tag: usize) -> Self {
        Self::compose(self.decompose_ptr(), tag)
    }

    #[inline]
    pub fn decompose(self) -> (*mut T, usize) {
        pointer::decompose(self.into_usize(), Self::MARK_BITS)
    }

    #[inline]
    pub fn decompose_ptr(self) -> *mut T {
        pointer::decompose_ptr(self.into_usize(), Self::MARK_BITS)
    }

    #[inline]
    pub fn decompose_tag(self) -> usize {
        pointer::decompose_tag::<T>(self.into_usize(), Self::MARK_BITS)
    }

    #[inline]
    pub unsafe fn decompose_ref<'a>(self) -> (Option<&'a T>, usize) {
        let (ptr, tag) = self.decompose();
        (ptr.as_ref(), tag)
    }

    #[inline]
    pub unsafe fn decompose_mut<'a>(self) -> (Option<&'a mut T>, usize) {
        let (ptr, tag) = self.decompose();
        (ptr.as_mut(), tag)
    }

    #[inline]
    pub unsafe fn as_ref<'a>(self) -> Option<&'a T> {
        self.decompose_ptr().as_ref()
    }

    #[inline]
    pub unsafe fn as_mut<'a>(self) -> Option<&'a mut T> {
        self.decompose_ptr().as_mut()
    }

    #[inline]
    pub fn is_null(self) -> bool {
        self.decompose_ptr().is_null()
    }
}

impl<T, N: Unsigned> Default for MarkedPtr<T, N> {
    #[inline]
    fn default() -> Self {
        Self::null()
    }
}

impl<T, N: Unsigned> fmt::Debug for MarkedPtr<T, N> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let (ptr, tag) = self.decompose();
        f.debug_struct("MarkedPtr")
            .field("ptr", &ptr)
            .field("tag", &tag)
            .finish()
    }
}

impl<T, N: Unsigned> fmt::Pointer for MarkedPtr<T, N> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Pointer::fmt(&self.decompose_ptr(), f)
    }
}

impl<T, N: Unsigned> From<*const T> for MarkedPtr<T, N> {
    #[inline]
    fn from(ptr: *const T) -> Self {
        Self::new(ptr as *mut _)
    }
}

impl<T, N: Unsigned> From<*mut T> for MarkedPtr<T, N> {
    fn from(ptr: *mut T) -> Self {
        Self::new(ptr)
    }
}

impl<'a, T, N: Unsigned> From<&'a T> for MarkedPtr<T, N> {
    #[inline]
    fn from(reference: &'a T) -> Self {
        Self::new(reference as *const _ as *mut _)
    }
}

impl<'a, T, N: Unsigned> From<&'a mut T> for MarkedPtr<T, N> {
    #[inline]
    fn from(reference: &'a mut T) -> Self {
        Self::new(reference)
    }
}

impl<T, N: Unsigned> From<NonNull<T>> for MarkedPtr<T, N> {
    #[inline]
    fn from(ptr: NonNull<T>) -> Self {
        Self::new(ptr.as_ptr())
    }
}

impl<T, N: Unsigned> From<(*mut T, usize)> for MarkedPtr<T, N> {
    #[inline]
    fn from(pair: (*mut T, usize)) -> Self {
        let (ptr, tag) = pair;
        Self::compose(ptr, tag)
    }
}

impl<T, N: Unsigned> From<(*const T, usize)> for MarkedPtr<T, N> {
    #[inline]
    fn from(pair: (*const T, usize)) -> Self {
        let (ptr, tag) = pair;
        Self::compose(ptr as *mut _, tag)
    }
}

impl<T, N> PartialEq for MarkedPtr<T, N> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl<T, N> PartialEq<MarkedNonNull<T, N>> for MarkedPtr<T, N> {
    #[inline]
    fn eq(&self, other: &MarkedNonNull<T, N>) -> bool {
        self.inner.eq(&other.inner.as_ptr())
    }
}

impl<T, N> PartialOrd for MarkedPtr<T, N> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        self.inner.partial_cmp(&other.inner)
    }
}

impl<T, N> PartialOrd<MarkedNonNull<T, N>> for MarkedPtr<T, N> {
    #[inline]
    fn partial_cmp(&self, other: &MarkedNonNull<T, N>) -> Option<cmp::Ordering> {
        self.inner.partial_cmp(&other.inner.as_ptr())
    }
}
