use core::mem;

use crate::debra::reclaim::pointer::{
    Marked::{self, Null, Value},
    MarkedNonNullable,
};
use crate::debra::reclaim::MarkedPointer;



impl<T: MarkedNonNullable> Marked<T> {
    
    #[inline]
    pub fn is_value(&self) -> bool {
        match *self {
            Value(_) => true,
            _ => false,
        }
    }

    
    #[inline]
    pub fn is_null(&self) -> bool {
        match *self {
            Null(_) => true,
            _ => false,
        }
    }

    
    #[inline]
    pub fn as_ref(&self) -> Marked<&T> {
        match self {
            Value(value) => Value(value),
            Null(tag) => Null(*tag),
        }
    }

    
    #[inline]
    pub fn as_mut(&mut self) -> Marked<&mut T> {
        match self {
            Value(value) => Value(value),
            Null(tag) => Null(*tag),
        }
    }

    
    #[inline]
    pub fn unwrap_value(self) -> T {
        match self {
            Value(ptr) => ptr,
            _ => panic!("called `Marked::unwrap_value()` on a `Null` value"),
        }
    }

    
    #[inline]
    pub fn unwrap_null(self) -> usize {
        match self {
            Null(tag) => tag,
            _ => panic!("called `Marked::unwrap_tag()` on a `Value`"),
        }
    }

    
    #[inline]
    pub fn unwrap_value_or_else(self, func: impl (FnOnce(usize) -> T)) -> T {
        match self {
            Value(ptr) => ptr,
            Null(tag) => func(tag),
        }
    }

    
    
    #[inline]
    pub fn map<U: MarkedNonNullable>(self, func: impl (FnOnce(T) -> U)) -> Marked<U> {
        match self {
            Value(ptr) => Value(func(ptr)),
            Null(tag) => Null(tag),
        }
    }

    
    
    #[inline]
    pub fn map_or_else<U: MarkedNonNullable>(
        self,
        default: impl FnOnce(usize) -> U,
        func: impl FnOnce(T) -> U,
    ) -> U {
        match self {
            Value(ptr) => func(ptr),
            Null(tag) => default(tag),
        }
    }

    
    #[inline]
    pub fn value(self) -> Option<T> {
        match self {
            Value(ptr) => Some(ptr),
            _ => None,
        }
    }

    
    
    #[inline]
    pub fn take(&mut self) -> Self {
        mem::replace(self, Null(0))
    }

    
    
    #[inline]
    pub fn replace(&mut self, value: T) -> Self {
        mem::replace(self, Value(value))
    }
}

impl<T: MarkedNonNullable + MarkedPointer> Marked<T> {
    
    #[inline]
    pub fn decompose_tag(&self) -> usize {
        match self {
            Value(ptr) => ptr.as_marked_ptr().decompose_tag(),
            Null(tag) => *tag,
        }
    }
}



impl<T: MarkedNonNullable> Default for Marked<T> {
    #[inline]
    fn default() -> Self {
        Null(0)
    }
}



impl<T: MarkedNonNullable> From<Option<T>> for Marked<T> {
    #[inline]
    fn from(opt: Option<T>) -> Self {
        match opt {
            Some(ptr) => Value(ptr),
            None => Null(0),
        }
    }
}
