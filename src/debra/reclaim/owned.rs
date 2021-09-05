

use std::boxed::Box;

use core::borrow::{Borrow, BorrowMut};
use core::fmt;
use core::marker::PhantomData;
use core::mem;
use core::ops::{Deref, DerefMut};
use core::ptr::NonNull;

use typenum::Unsigned;

use crate::debra::reclaim::internal::Internal;
use crate::debra::reclaim::pointer::{Marked, MarkedNonNull, MarkedNonNullable, MarkedPointer};
use crate::debra::reclaim::{Owned, Reclaim, Record, Shared, Unprotected};



impl<T: Clone, R: Reclaim, N: Unsigned> Clone for Owned<T, R, N> {
    #[inline]
    fn clone(&self) -> Self {
        let (reference, tag) = unsafe { self.inner.decompose_ref() };
        Self::with_tag(reference.clone(), tag)
    }
}



unsafe impl<T, R: Reclaim, N: Unsigned> Send for Owned<T, R, N> where T: Send {}
unsafe impl<T, R: Reclaim, N: Unsigned> Sync for Owned<T, R, N> where T: Sync {}



impl<T, R: Reclaim, N: Unsigned> MarkedPointer for Owned<T, R, N> {
    impl_trait!(owned);
}



impl<T, R: Reclaim, N: Unsigned> Owned<T, R, N> {
    
    
    
    
    
    
    
    
    
    #[inline]
    pub fn new(owned: T) -> Self {
        Self { inner: MarkedNonNull::from(Self::alloc_record(owned)), _marker: PhantomData }
    }

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    #[inline]
    pub fn with_tag(owned: T, tag: usize) -> Self {
        Self { inner: MarkedNonNull::compose(Self::alloc_record(owned), tag), _marker: PhantomData }
    }

    
    
    
    
    #[inline]
    pub fn into_inner(self) -> T {
        unsafe {
            let ptr = self.inner.decompose_ptr();
            mem::forget(self);
            let boxed = Box::from_raw(Record::<_, R>::from_raw(ptr).as_ptr());
            (*boxed).elem
        }
    }

    impl_inherent!(owned);

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    #[inline]
    pub fn decompose_ref(owned: &Self) -> (&T, usize) {
        
        unsafe { owned.inner.decompose_ref() }
    }

    
    
    #[inline]
    pub fn decompose_mut(owned: &mut Self) -> (&mut T, usize) {
        
        unsafe { owned.inner.decompose_mut() }
    }

    
    
    
    
    
    #[inline]
    pub fn leak<'a>(owned: Self) -> (&'a mut T, usize)
    where
        T: 'a,
    {
        let (ptr, tag) = owned.inner.decompose();
        mem::forget(owned);
        unsafe { (&mut *ptr.as_ptr(), tag) }
    }

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    #[inline]
    pub fn leak_unprotected(owned: Self) -> Unprotected<T, R, N> {
        let inner = owned.inner;
        mem::forget(owned);
        Unprotected { inner, _marker: PhantomData }
    }

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    #[inline]
    pub unsafe fn leak_shared<'a>(owned: Self) -> Shared<'a, T, R, N> {
        let inner = owned.inner;
        mem::forget(owned);
        Shared { inner, _marker: PhantomData }
    }

    
    
    #[inline]
    fn alloc_record(owned: T) -> NonNull<T> {
        let record = Box::leak(Box::new(Record::<_, R>::new(owned)));
        NonNull::from(&record.elem)
    }
}



impl<T, R: Reclaim, N: Unsigned> AsRef<T> for Owned<T, R, N> {
    #[inline]
    fn as_ref(&self) -> &T {
        &**self
    }
}

impl<T, R: Reclaim, N: Unsigned> AsMut<T> for Owned<T, R, N> {
    #[inline]
    fn as_mut(&mut self) -> &mut T {
        &mut **self
    }
}



impl<T, R: Reclaim, N: Unsigned> Borrow<T> for Owned<T, R, N> {
    #[inline]
    fn borrow(&self) -> &T {
        &**self
    }
}

impl<T, R: Reclaim, N: Unsigned> BorrowMut<T> for Owned<T, R, N> {
    #[inline]
    fn borrow_mut(&mut self) -> &mut T {
        &mut **self
    }
}



impl<T: Default, R: Reclaim, N: Unsigned> Default for Owned<T, R, N> {
    #[inline]
    fn default() -> Self {
        Owned::new(T::default())
    }
}



impl<T, R: Reclaim, N: Unsigned> Deref for Owned<T, R, N> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { self.inner.as_ref() }
    }
}

impl<T, R: Reclaim, N: Unsigned> DerefMut for Owned<T, R, N> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.inner.as_mut() }
    }
}



impl<T, R: Reclaim, N: Unsigned> Drop for Owned<T, R, N> {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            let record = Record::<_, R>::from_raw(self.inner.decompose_ptr());
            mem::drop(Box::from_raw(record.as_ptr()));
        }
    }
}



impl<T, R: Reclaim, N: Unsigned> From<T> for Owned<T, R, N> {
    #[inline]
    fn from(owned: T) -> Self {
        Owned::new(owned)
    }
}



impl<T, R: Reclaim, N: Unsigned> fmt::Debug for Owned<T, R, N>
where
    T: fmt::Debug,
{
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let (reference, tag) = unsafe { self.inner.decompose_ref() };
        f.debug_struct("Owned").field("value", reference).field("tag", &tag).finish()
    }
}



impl<T, R: Reclaim, N: Unsigned> fmt::Pointer for Owned<T, R, N> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Pointer::fmt(&self.inner.decompose_ptr(), f)
    }
}



impl<T, R: Reclaim, N: Unsigned> MarkedNonNullable for Owned<T, R, N> {
    type Item = T;
    type MarkBits = N;

    #[inline]
    fn into_marked_non_null(self) -> MarkedNonNull<T, N> {
        let inner = self.inner;
        mem::forget(self);
        inner
    }
}



impl<T, R: Reclaim, N: Unsigned> Internal for Owned<T, R, N> {}