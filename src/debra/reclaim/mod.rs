















































































































































































#![cfg_attr(not(any(test, feature = "std")), no_std)]
#![warn(missing_docs)]

#[cfg(not(feature = "std"))]
extern crate alloc;

#[macro_use]
mod macros;

pub mod align;
pub mod leak;
pub mod prelude {
    
    

    pub use crate::debra::reclaim::pointer::{
        Marked::{self, Null, Value},
        MarkedNonNullable, MarkedPointer,
    };

    pub use crate::debra::reclaim::util::{UnwrapMutPtr, UnwrapPtr, UnwrapUnchecked};

    pub use crate::debra::reclaim::GlobalReclaim;
    pub use crate::debra::reclaim::Protect;
    pub use crate::debra::reclaim::ProtectRegion;
    pub use crate::debra::reclaim::Reclaim;
}

pub mod atomic;
pub mod internal;
pub mod owned;
pub mod pointer;
pub mod retired;
pub mod shared;
pub mod traits;
pub mod unlinked;
pub mod unprotected;
pub mod util;

#[cfg(feature = "std")]
use std::error::Error;

use core::fmt;
use core::marker::PhantomData;
use core::mem;
use core::ptr::NonNull;
use core::sync::atomic::Ordering;


pub use typenum;

use memoffset::offset_of;
use typenum::Unsigned;

pub use crate::debra::reclaim::atomic::{Atomic, CompareExchangeFailure};
pub use crate::debra::reclaim::pointer::{
    AtomicMarkedPtr, InvalidNullError, Marked, MarkedNonNull, MarkedNonNullable, MarkedPointer,
    MarkedPtr,
};
pub use crate::debra::reclaim::retired::Retired;
































pub unsafe trait GlobalReclaim
where
    Self: Reclaim,
{
    
    type Guard: Protect<Reclaimer = Self> + Default;

    
    
    
    
    
    
    fn guard() -> Self::Guard {
        Self::Guard::default()
    }

    
    
    
    
    
    
    
    
    
    
    fn try_reclaim();

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    unsafe fn retire<T: 'static, N: Unsigned>(unlinked: Unlinked<T, Self, N>);

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    unsafe fn retire_unchecked<T, N: Unsigned>(unlinked: Unlinked<T, Self, N>);

    
    
    
    
    
    
    
    
    
    
    unsafe fn retire_raw<T, N: Unsigned>(ptr: MarkedPtr<T, N>) {
        debug_assert!(!ptr.is_null());
        Self::retire_unchecked(Unlinked::from_marked_ptr(ptr));
    }
}




















pub unsafe trait Reclaim
where
    Self: Sized + 'static,
{
    
    type Local: Sized;

    
    
    
    type RecordHeader: Default + Sync + Sized;

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    unsafe fn retire_local<T: 'static, N: Unsigned>(
        local: &Self::Local,
        unlinked: Unlinked<T, Self, N>,
    );

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    unsafe fn retire_local_unchecked<T, N: Unsigned>(
        local: &Self::Local,
        unlinked: Unlinked<T, Self, N>,
    );

    
    
    
    
    
    
    
    
    
    
    
    
    unsafe fn retire_local_raw<T, N: Unsigned>(local: &Self::Local, ptr: MarkedPtr<T, N>) {
        debug_assert!(!ptr.is_null());
        Self::retire_local_unchecked(local, Unlinked::from_marked_ptr(ptr));
    }
}


























pub unsafe trait Protect
where
    Self: Clone + Sized,
{
    
    type Reclaimer: Reclaim;

    
    
    
    
    
    
    
    #[inline]
    fn try_fuse<T, N: Unsigned>(
        mut self,
        atomic: &Atomic<T, Self::Reclaimer, N>,
        order: Ordering,
    ) -> Result<Guarded<T, Self, N>, Self> {
        if let Marked::Value(shared) = self.protect(atomic, order) {
            let ptr = Shared::into_marked_non_null(shared);
            Ok(Guarded { guard: self, ptr })
        } else {
            Err(self)
        }
    }

    
    
    
    
    
    fn release(&mut self);

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    fn protect<T, N: Unsigned>(
        &mut self,
        atomic: &Atomic<T, Self::Reclaimer, N>,
        order: Ordering,
    ) -> Marked<Shared<T, Self::Reclaimer, N>>;

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    fn protect_if_equal<T, N: Unsigned>(
        &mut self,
        atomic: &Atomic<T, Self::Reclaimer, N>,
        expected: MarkedPtr<T, N>,
        order: Ordering,
    ) -> AcquireResult<T, Self::Reclaimer, N>;
}





























pub unsafe trait ProtectRegion
where
    Self: Protect,
{
}






pub type AcquireResult<'g, T, R, N> = Result<Marked<Shared<'g, T, R, N>>, NotEqualError>;







#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct NotEqualError;



impl fmt::Display for NotEqualError {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "acquired value does not match `expected`.")
    }
}



#[cfg(feature = "std")]
impl Error for NotEqualError {}












pub struct Record<T, R: Reclaim> {
    
    header: R::RecordHeader,
    
    elem: T,
}



impl<T, R: Reclaim> Record<T, R> {
    
    #[inline]
    pub fn new(elem: T) -> Self {
        Self { header: Default::default(), elem }
    }

    
    #[inline]
    pub fn with_header(elem: T, header: R::RecordHeader) -> Self {
        Self { header, elem }
    }

    
    #[inline]
    pub fn header(&self) -> &R::RecordHeader {
        &self.header
    }

    
    #[inline]
    pub fn elem(&self) -> &T {
        &self.elem
    }

    
    
    
    
    
    
    
    
    
    #[inline]
    pub unsafe fn from_raw_non_null(elem: NonNull<T>) -> NonNull<Self> {
        Self::from_raw(elem.as_ptr())
    }

    
    
    
    
    
    
    
    
    
    #[inline]
    pub unsafe fn from_raw(elem: *mut T) -> NonNull<Self> {
        let addr = (elem as usize) - Self::offset_elem();
        NonNull::new_unchecked(addr as *mut _)
    }

    
    
    
    
    
    
    
    
    
    #[inline]
    pub unsafe fn header_from_raw<'a>(elem: *mut T) -> &'a R::RecordHeader {
        let header = (elem as usize) - Self::offset_elem() + Self::offset_header();
        &*(header as *mut _)
    }

    
    
    
    
    
    
    
    
    
    #[inline]
    pub unsafe fn header_from_raw_non_null<'a>(elem: NonNull<T>) -> &'a R::RecordHeader {
        let header = (elem.as_ptr() as usize) - Self::offset_elem() + Self::offset_header();
        &*(header as *mut _)
    }

    
    
    #[inline]
    pub fn offset_header() -> usize {
        
        
        
        if mem::size_of::<R::RecordHeader>() == 0 {
            0
        } else {
            offset_of!(Self, header)
        }
    }

    
    
    #[inline]
    pub fn offset_elem() -> usize {
        
        
        
        if mem::size_of::<R::RecordHeader>() == 0 {
            0
        } else {
            offset_of!(Self, elem)
        }
    }
}






#[derive(Debug)]
pub struct Guarded<T, G, N: Unsigned> {
    guard: G,
    ptr: MarkedNonNull<T, N>,
}



impl<T, G: Protect, N: Unsigned> Guarded<T, G, N> {
    
    #[inline]
    pub fn shared(&self) -> Shared<T, G::Reclaimer, N> {
        Shared { inner: self.ptr, _marker: PhantomData }
    }

    
    
    
    
    #[inline]
    pub fn into_guard(self) -> G {
        let mut guard = self.guard;
        guard.release();
        guard
    }
}










#[derive(Eq, Ord, PartialEq, PartialOrd)]
pub struct Owned<T, R: Reclaim, N: Unsigned> {
    inner: MarkedNonNull<T, N>,
    _marker: PhantomData<(T, R)>,
}














pub struct Shared<'g, T, R, N> {
    inner: MarkedNonNull<T, N>,
    _marker: PhantomData<(&'g T, R)>,
}

















#[derive(Eq, Ord, PartialEq, PartialOrd)]
#[must_use = "unlinked values are meant to be retired, otherwise a memory leak is highly likely"]
pub struct Unlinked<T, R, N> {
    inner: MarkedNonNull<T, N>,
    _marker: PhantomData<(T, R)>,
}













#[derive(Eq, Ord, PartialEq, PartialOrd)]
pub struct Unprotected<T, R, N> {
    inner: MarkedNonNull<T, N>,
    _marker: PhantomData<R>,
}
