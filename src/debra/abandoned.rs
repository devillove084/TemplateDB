

extern crate alloc;


#[cfg(not(feature = "std"))]
use alloc::boxed::Box;

use core::ptr::{self, NonNull};
use core::sync::atomic::{
    AtomicPtr,
    Ordering::{Acquire, Relaxed, Release},
};

use super::sealed::{Sealed, SealedList};







#[derive(Debug)]
pub(crate) struct AbandonedQueue {
    head: AtomicPtr<Sealed>,
}



impl AbandonedQueue {
    
    #[inline]
    pub const fn new() -> Self {
        Self { head: AtomicPtr::new(ptr::null_mut()) }
    }

    
    #[inline]
    pub fn push(&self, sealed: SealedList) {
        let (head, mut tail) = sealed.into_inner();

        loop {
            let curr_head = self.head.load(Relaxed);
            unsafe { tail.as_mut().next = NonNull::new(curr_head) };

            
            if self.head.compare_exchange_weak(curr_head, head.as_ptr(), Release, Relaxed).is_ok() {
                return;
            }
        }
    }

    
    #[inline]
    pub fn take_all(&self) -> Iter {
        
        let head = self.head.swap(ptr::null_mut(), Acquire);
        Iter { curr: NonNull::new(head) }
    }
}







#[derive(Debug)]
pub(crate) struct Iter {
    curr: Option<NonNull<Sealed>>,
}



impl Iterator for Iter {
    type Item = Box<Sealed>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            self.curr.map(|ptr| {
                let curr = Box::from_raw(ptr.as_ptr());
                self.curr = curr.next;

                curr
            })
        }
    }
}
