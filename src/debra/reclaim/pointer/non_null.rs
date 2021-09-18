use core::cmp;
use core::convert::TryFrom;
use core::fmt;
use core::marker::PhantomData;
use core::ptr::NonNull;

use typenum::{IsGreaterOrEqual, True, Unsigned};

use crate::debra::reclaim::internal::Internal;
use crate::debra::reclaim::pointer::{
    self, InvalidNullError,
    Marked::{self, Null, Value},
    MarkedNonNull, MarkedNonNullable, MarkedPtr,
};

impl<T, N> Clone for MarkedNonNull<T, N> {
    #[inline]
    fn clone(&self) -> Self {
        Self::from(self.inner)
    }
}

impl<T, N> Copy for MarkedNonNull<T, N> {}

impl<T, N> MarkedNonNull<T, N> {
    #[inline]
    pub const fn cast<U>(self) -> MarkedNonNull<U, N> {
        MarkedNonNull {
            inner: self.inner.cast(),
            _marker: PhantomData,
        }
    }

    #[inline]
    pub const fn dangling() -> Self {
        Self {
            inner: NonNull::dangling(),
            _marker: PhantomData,
        }
    }
}

impl<T, N: Unsigned> MarkedNonNull<T, N> {
    pub const MARK_BITS: usize = N::USIZE;

    pub const MARK_MASK: usize = pointer::mark_mask::<T>(Self::MARK_BITS);

    pub const POINTER_MASK: usize = !Self::MARK_MASK;

    #[inline]
    pub fn into_non_null(self) -> NonNull<T> {
        self.inner
    }

    #[inline]
    pub fn convert<M: Unsigned>(other: MarkedNonNull<T, M>) -> Self
    where
        N: IsGreaterOrEqual<M, Output = True>,
    {
        Self::from(other.inner)
    }

    #[inline]
    pub unsafe fn new_unchecked(ptr: MarkedPtr<T, N>) -> Self {
        Self::from(NonNull::new_unchecked(ptr.inner))
    }

    pub fn new(ptr: MarkedPtr<T, N>) -> Marked<Self> {
        match ptr.decompose() {
            (raw, _) if !raw.is_null() => unsafe { Value(Self::new_unchecked(ptr)) },
            (_, tag) => Null(tag),
        }
    }

    #[inline]
    pub fn clear_tag(self) -> Self {
        Self::from(self.decompose_non_null())
    }

    #[inline]
    pub fn with_tag(self, tag: usize) -> Self {
        Self::compose(self.decompose_non_null(), tag)
    }

    #[inline]
    pub fn into_marked_ptr(self) -> MarkedPtr<T, N> {
        MarkedPtr::new(self.inner.as_ptr())
    }

    #[inline]
    pub fn compose(ptr: NonNull<T>, tag: usize) -> Self {
        debug_assert_eq!(
            0,
            ptr.as_ptr() as usize & Self::MARK_MASK,
            "`ptr` is not well aligned"
        );
        unsafe {
            Self::from(NonNull::new_unchecked(pointer::compose::<_, N>(
                ptr.as_ptr(),
                tag,
            )))
        }
    }

    #[inline]
    pub fn decompose(self) -> (NonNull<T>, usize) {
        let (ptr, tag) = pointer::decompose(self.inner.as_ptr() as usize, Self::MARK_BITS);
        (unsafe { NonNull::new_unchecked(ptr) }, tag)
    }

    #[inline]
    pub fn decompose_ptr(self) -> *mut T {
        pointer::decompose_ptr(self.inner.as_ptr() as usize, Self::MARK_BITS)
    }

    #[inline]
    pub fn decompose_non_null(self) -> NonNull<T> {
        unsafe {
            NonNull::new_unchecked(pointer::decompose_ptr(
                self.inner.as_ptr() as usize,
                Self::MARK_BITS,
            ))
        }
    }

    #[inline]
    pub fn decompose_tag(self) -> usize {
        pointer::decompose_tag::<T>(self.inner.as_ptr() as usize, Self::MARK_BITS)
    }

    #[inline]
    pub unsafe fn decompose_ref(&self) -> (&T, usize) {
        let (ptr, tag) = self.decompose();
        (&*ptr.as_ptr(), tag)
    }

    #[inline]
    pub unsafe fn decompose_ref_unbounded<'a>(self) -> (&'a T, usize) {
        let (ptr, tag) = self.decompose();
        (&*ptr.as_ptr(), tag)
    }

    #[inline]
    pub unsafe fn decompose_mut(&mut self) -> (&mut T, usize) {
        let (ptr, tag) = self.decompose();
        (&mut *ptr.as_ptr(), tag)
    }

    #[inline]
    pub unsafe fn decompose_mut_unbounded<'a>(&mut self) -> (&'a mut T, usize) {
        let (ptr, tag) = self.decompose();
        (&mut *ptr.as_ptr(), tag)
    }

    #[inline]
    pub unsafe fn as_ref(&self) -> &T {
        &*self.decompose_non_null().as_ptr()
    }

    #[inline]
    pub unsafe fn as_ref_unbounded<'a>(self) -> &'a T {
        &*self.decompose_non_null().as_ptr()
    }

    #[inline]
    pub unsafe fn as_mut(&mut self) -> &mut T {
        &mut *self.decompose_non_null().as_ptr()
    }

    #[inline]
    pub unsafe fn as_mut_unbounded<'a>(self) -> &'a mut T {
        &mut *self.decompose_non_null().as_ptr()
    }
}

impl<T, N: Unsigned> fmt::Debug for MarkedNonNull<T, N> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let (ptr, tag) = self.decompose();
        f.debug_struct("MarkedNonNull")
            .field("ptr", &ptr)
            .field("tag", &tag)
            .finish()
    }
}

impl<T, N: Unsigned> fmt::Pointer for MarkedNonNull<T, N> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Pointer::fmt(&self.decompose_non_null(), f)
    }
}

impl<T, N> From<NonNull<T>> for MarkedNonNull<T, N> {
    #[inline]
    fn from(ptr: NonNull<T>) -> Self {
        Self {
            inner: ptr,
            _marker: PhantomData,
        }
    }
}

impl<'a, T, N: Unsigned> From<&'a T> for MarkedNonNull<T, N> {
    #[inline]
    fn from(reference: &'a T) -> Self {
        Self::from(NonNull::from(reference))
    }
}

impl<'a, T, N: Unsigned> From<&'a mut T> for MarkedNonNull<T, N> {
    #[inline]
    fn from(reference: &'a mut T) -> Self {
        Self::from(NonNull::from(reference))
    }
}

impl<T, N: Unsigned> TryFrom<MarkedPtr<T, N>> for MarkedNonNull<T, N> {
    type Error = InvalidNullError;

    #[inline]
    fn try_from(ptr: MarkedPtr<T, N>) -> Result<Self, Self::Error> {
        match ptr.decompose() {
            (raw, _) if raw.is_null() => Err(InvalidNullError),
            _ => unsafe { Ok(MarkedNonNull::new_unchecked(ptr)) },
        }
    }
}

impl<T, N> PartialEq for MarkedNonNull<T, N> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl<T, N> PartialEq<MarkedPtr<T, N>> for MarkedNonNull<T, N> {
    #[inline]
    fn eq(&self, other: &MarkedPtr<T, N>) -> bool {
        self.inner.as_ptr() == other.inner
    }
}

impl<T, N> PartialOrd for MarkedNonNull<T, N> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        self.inner.partial_cmp(&other.inner)
    }
}

impl<T, N> PartialOrd<MarkedPtr<T, N>> for MarkedNonNull<T, N> {
    #[inline]
    fn partial_cmp(&self, other: &MarkedPtr<T, N>) -> Option<cmp::Ordering> {
        self.inner.as_ptr().partial_cmp(&other.inner)
    }
}

impl<T, N> Eq for MarkedNonNull<T, N> {}

impl<T, N> Ord for MarkedNonNull<T, N> {
    #[inline]
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.inner.cmp(&other.inner)
    }
}

impl<T, N: Unsigned> MarkedNonNullable for MarkedNonNull<T, N> {
    type Item = T;
    type MarkBits = N;

    #[inline]
    fn into_marked_non_null(self) -> MarkedNonNull<Self::Item, Self::MarkBits> {
        self
    }
}

impl<T, N: Unsigned> Internal for MarkedNonNull<T, N> {}
