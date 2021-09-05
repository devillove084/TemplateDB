

use core::hint::unreachable_unchecked;
use core::ptr::{self, NonNull};

use crate::debra::reclaim::pointer::{
    Marked::{self, Null, Value},
    MarkedNonNullable,
};







pub trait UnwrapPtr {
    
    type Item: Sized;

    
    
    fn unwrap_ptr(self) -> *const Self::Item;
}



impl<'a, T> UnwrapPtr for Option<&'a T> {
    type Item = T;

    #[inline]
    fn unwrap_ptr(self) -> *const Self::Item {
        match self {
            Some(value) => value as *const _,
            None => ptr::null(),
        }
    }
}

impl<'a, T> UnwrapPtr for Option<&'a mut T> {
    type Item = T;

    #[inline]
    fn unwrap_ptr(self) -> *const Self::Item {
        match self {
            Some(value) => value as *mut _,
            None => ptr::null(),
        }
    }
}

impl<T> UnwrapPtr for Option<NonNull<T>> {
    type Item = T;

    #[inline]
    fn unwrap_ptr(self) -> *const Self::Item {
        match self {
            Some(value) => value.as_ptr() as *const _,
            None => ptr::null(),
        }
    }
}







pub trait UnwrapMutPtr: UnwrapPtr {
    
    
    fn unwrap_mut_ptr(self) -> *mut <Self as UnwrapPtr>::Item;
}



impl<'a, T> UnwrapMutPtr for Option<&'a mut T> {
    #[inline]
    fn unwrap_mut_ptr(self) -> *mut Self::Item {
        self.unwrap_ptr() as *mut _
    }
}

impl<T> UnwrapMutPtr for Option<NonNull<T>> {
    #[inline]
    fn unwrap_mut_ptr(self) -> *mut Self::Item {
        self.unwrap_ptr() as *mut _
    }
}






pub trait UnwrapUnchecked {
    
    type Item: Sized;

    
    
    
    
    
    
    
    
    
    
    
    
    unsafe fn unwrap_unchecked(self) -> Self::Item;
}



impl<T> UnwrapUnchecked for Option<T> {
    type Item = T;

    #[inline]
    unsafe fn unwrap_unchecked(self) -> Self::Item {
        debug_assert!(self.is_some(), "`unwrap_unchecked` called on a `None`");
        match self {
            Some(value) => value,
            None => unreachable_unchecked(),
        }
    }
}

impl<T, E> UnwrapUnchecked for Result<T, E> {
    type Item = T;

    #[inline]
    unsafe fn unwrap_unchecked(self) -> Self::Item {
        debug_assert!(self.is_ok(), "`unwrap_unchecked` called on an `Err`");
        match self {
            Ok(value) => value,
            Err(_) => unreachable_unchecked(),
        }
    }
}

impl<T: MarkedNonNullable> UnwrapUnchecked for Marked<T> {
    type Item = T;

    #[inline]
    unsafe fn unwrap_unchecked(self) -> Self::Item {
        debug_assert!(self.is_value(), "`unwrap_unchecked` called on a `Null`");
        match self {
            Value(value) => value,
            Null(_) => unreachable_unchecked(),
        }
    }
}
