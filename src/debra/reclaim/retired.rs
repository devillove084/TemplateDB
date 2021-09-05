use core::cmp;
use core::fmt;
use core::marker::PhantomData;
use core::mem;
use core::ptr::NonNull;


use std::boxed::Box;

use crate::debra::reclaim::{Reclaim, Record};






pub struct Retired<R>(NonNull<dyn Any + 'static>, PhantomData<R>);



impl<R: Reclaim + 'static> Retired<R> {
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    #[inline]
    pub unsafe fn new_unchecked<'a, T: 'a>(record: NonNull<T>) -> Self {
        let any: NonNull<dyn Any + 'a> = Record::<T, R>::from_raw_non_null(record);
        let any: NonNull<dyn Any + 'static> = mem::transmute(any);

        Self(any, PhantomData)
    }

    
    
    
    
    
    #[inline]
    pub fn as_ptr(&self) -> *const () {
        self.0.as_ptr() as *mut () as *const ()
    }

    
    
    #[inline]
    pub fn address(&self) -> usize {
        self.0.as_ptr() as *mut () as usize
    }

    
    
    
    
    
    
    #[inline]
    pub unsafe fn reclaim(&mut self) {
        mem::drop(Box::from_raw(self.0.as_ptr()));
    }
}



impl<R: Reclaim + 'static> PartialEq for Retired<R> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_ptr().eq(&other.as_ptr())
    }
}



impl<R: Reclaim + 'static> PartialOrd for Retired<R> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        self.as_ptr().partial_cmp(&other.as_ptr())
    }
}



impl<R: Reclaim + 'static> Ord for Retired<R> {
    #[inline]
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.as_ptr().cmp(&other.as_ptr())
    }
}



impl<R: Reclaim + 'static> Eq for Retired<R> {}



impl<R: Reclaim + 'static> fmt::Debug for Retired<R> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Retired").field("address", &self.as_ptr()).finish()
    }
}



impl<R: Reclaim + 'static> fmt::Display for Retired<R> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Pointer::fmt(&self.as_ptr(), f)
    }
}





trait Any {}
impl<T> Any for T {}
