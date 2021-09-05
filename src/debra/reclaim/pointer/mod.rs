pub mod atomic;
pub mod marked;
pub mod non_null;
pub mod raw;

#[cfg(feature = "std")]
use std::error::Error;

use core::fmt;
use core::marker::PhantomData;
use core::mem;
use core::ptr::{self, NonNull};
use core::sync::atomic::AtomicUsize;

use typenum::Unsigned;

use crate::debra::reclaim::internal::Internal;

use self::Marked::{Null, Value};






pub trait MarkedPointer: Sized + Internal {
    
    type Pointer: MarkedNonNullable<Item = Self::Item, MarkBits = Self::MarkBits>;
    
    type Item: Sized;
    
    type MarkBits: Unsigned;

    
    
    
    
    
    
    
    
    
    
    fn as_marked_ptr(&self) -> MarkedPtr<Self::Item, Self::MarkBits>;

    
    
    
    
    
    
    
    
    
    
    fn into_marked_ptr(self) -> MarkedPtr<Self::Item, Self::MarkBits>;

    
    
    fn marked(_: Self, tag: usize) -> Marked<Self::Pointer>;

    
    fn unmarked(_: Self) -> Self;

    
    
    fn decompose(_: Self) -> (Self, usize);

    
    
    
    
    
    
    
    unsafe fn from_marked_ptr(marked: MarkedPtr<Self::Item, Self::MarkBits>) -> Self;

    
    
    
    
    
    
    unsafe fn from_marked_non_null(marked: MarkedNonNull<Self::Item, Self::MarkBits>) -> Self;
}













pub struct MarkedPtr<T, N> {
    inner: *mut T,
    _marker: PhantomData<N>,
}









pub struct MarkedNonNull<T, N> {
    inner: NonNull<T>,
    _marker: PhantomData<N>,
}














pub struct AtomicMarkedPtr<T, N> {
    inner: AtomicUsize,
    _marker: PhantomData<(*mut T, N)>,
}









#[derive(Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Marked<T: MarkedNonNullable> {
    
    Value(T),
    
    
    Null(usize),
}



impl<U, T, N: Unsigned> MarkedPointer for Option<U>
where
    U: MarkedPointer<Pointer = U, Item = T, MarkBits = N>
        + MarkedNonNullable<Item = T, MarkBits = N>,
{
    type Pointer = U;
    type Item = T;
    type MarkBits = N;

    #[inline]
    fn as_marked_ptr(&self) -> MarkedPtr<Self::Item, Self::MarkBits> {
        match self {
            Some(ptr) => Self::Pointer::as_marked_ptr(ptr),
            None => MarkedPtr::null(),
        }
    }

    #[inline]
    fn into_marked_ptr(self) -> MarkedPtr<Self::Item, Self::MarkBits> {
        match self {
            Some(ptr) => Self::Pointer::into_marked_ptr(ptr),
            None => MarkedPtr::null(),
        }
    }

    #[inline]
    fn marked(opt: Self, tag: usize) -> Marked<Self::Pointer> {
        match opt {
            Some(ptr) => Self::Pointer::marked(ptr, tag),
            None => Null(tag),
        }
    }

    #[inline]
    fn unmarked(opt: Self) -> Self {
        opt.map(Self::Pointer::unmarked)
    }

    #[inline]
    fn decompose(opt: Self) -> (Self, usize) {
        match opt {
            Some(ptr) => {
                let (ptr, tag) = Self::Pointer::decompose(ptr);
                (Some(ptr), tag)
            }
            None => (None, 0),
        }
    }

    #[inline]
    unsafe fn from_marked_ptr(marked: MarkedPtr<Self::Item, Self::MarkBits>) -> Self {
        if !marked.is_null() {
            Some(Self::Pointer::from_marked_non_null(MarkedNonNull::new_unchecked(marked)))
        } else {
            None
        }
    }

    #[inline]
    unsafe fn from_marked_non_null(marked: MarkedNonNull<Self::Item, Self::MarkBits>) -> Self {
        Some(Self::Pointer::from_marked_non_null(marked))
    }
}

impl<U, T, N: Unsigned> MarkedPointer for Marked<U>
where
    U: MarkedPointer<Pointer = U, Item = T, MarkBits = N>
        + MarkedNonNullable<Item = T, MarkBits = N>,
{
    type Pointer = U;
    type Item = T;
    type MarkBits = N;

    #[inline]
    fn as_marked_ptr(&self) -> MarkedPtr<Self::Item, Self::MarkBits> {
        match self {
            Value(ptr) => Self::Pointer::as_marked_ptr(ptr),
            Null(tag) => MarkedPtr::compose(ptr::null_mut(), *tag),
        }
    }

    #[inline]
    fn into_marked_ptr(self) -> MarkedPtr<Self::Item, Self::MarkBits> {
        match self {
            Value(ptr) => Self::Pointer::into_marked_ptr(ptr),
            Null(tag) => MarkedPtr::compose(ptr::null_mut(), tag),
        }
    }

    #[inline]
    fn marked(marked: Self, tag: usize) -> Marked<Self::Pointer> {
        match marked {
            Value(ptr) => Self::Pointer::marked(ptr, tag),
            Null(_) => Null(tag),
        }
    }

    #[inline]
    fn unmarked(marked: Self) -> Self {
        match marked {
            Value(ptr) => Value(Self::Pointer::unmarked(ptr)),
            Null(_) => Null(0),
        }
    }

    #[inline]
    fn decompose(marked: Self) -> (Self, usize) {
        match marked {
            Value(ptr) => {
                let (ptr, tag) = Self::Pointer::decompose(ptr);
                (Value(ptr), tag)
            }
            Null(tag) => (Null(0), tag),
        }
    }

    #[inline]
    unsafe fn from_marked_ptr(marked: MarkedPtr<Self::Item, Self::MarkBits>) -> Self {
        MarkedNonNull::new(marked).map(|ptr| Self::Pointer::from_marked_non_null(ptr))
    }

    #[inline]
    unsafe fn from_marked_non_null(marked: MarkedNonNull<Self::Item, Self::MarkBits>) -> Self {
        Value(Self::Pointer::from_marked_non_null(marked))
    }
}







#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct InvalidNullError;



impl fmt::Display for InvalidNullError {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "failed conversion of null pointer to non-nullable type")
    }
}



#[cfg(feature = "std")]
impl Error for InvalidNullError {}






pub trait MarkedNonNullable: Sized + Internal {
    
    type Item: Sized;
    
    type MarkBits: Unsigned;

    
    
    
    
    
    
    
    
    
    
    fn into_marked_non_null(self) -> MarkedNonNull<Self::Item, Self::MarkBits>;
}



impl<'a, T> MarkedNonNullable for &'a T {
    type Item = T;
    type MarkBits = typenum::U0;

    #[inline]
    fn into_marked_non_null(self) -> MarkedNonNull<Self::Item, Self::MarkBits> {
        MarkedNonNull::from(self)
    }
}

impl<'a, T> MarkedNonNullable for &'a mut T {
    type Item = T;
    type MarkBits = typenum::U0;

    #[inline]
    fn into_marked_non_null(self) -> MarkedNonNull<Self::Item, Self::MarkBits> {
        MarkedNonNull::from(self)
    }
}

impl<T> MarkedNonNullable for NonNull<T> {
    type Item = T;
    type MarkBits = typenum::U0;

    #[inline]
    fn into_marked_non_null(self) -> MarkedNonNull<Self::Item, Self::MarkBits> {
        MarkedNonNull::from(self)
    }
}



impl<U, T, N: Unsigned> Internal for Option<U> where
    U: MarkedPointer<Item = T, MarkBits = N> + MarkedNonNullable<Item = T, MarkBits = N>
{
}

impl<U, T, N: Unsigned> Internal for Marked<U> where
    U: MarkedPointer<Item = T, MarkBits = N> + MarkedNonNullable<Item = T, MarkBits = N>
{
}





#[inline]
const fn decompose<T>(marked: usize, mark_bits: usize) -> (*mut T, usize) {
    (decompose_ptr::<T>(marked, mark_bits), decompose_tag::<T>(marked, mark_bits))
}



#[inline]
const fn decompose_ptr<T>(marked: usize, mark_bits: usize) -> *mut T {
    (marked & !mark_mask::<T>(mark_bits)) as *mut _
}



#[inline]
const fn decompose_tag<T>(marked: usize, mark_bits: usize) -> usize {
    marked & mark_mask::<T>(mark_bits)
}



#[inline]
const fn lower_bits<T>() -> usize {
    mem::align_of::<T>().trailing_zeros() as usize
}



#[deny(const_err)]
#[inline]
const fn mark_mask<T>(mark_bits: usize) -> usize {
    let _assert_sufficient_alignment = lower_bits::<T>() - mark_bits;
    (1 << mark_bits) - 1
}




#[inline]
fn compose<T, N: Unsigned>(ptr: *mut T, tag: usize) -> *mut T {
    debug_assert_eq!(ptr as usize & mark_mask::<T>(N::USIZE), 0);
    ((ptr as usize) | (mark_mask::<T>(N::USIZE) & tag)) as *mut _
}