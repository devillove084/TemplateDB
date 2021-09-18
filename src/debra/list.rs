extern crate alloc;

#[cfg(not(feature = "std"))]
use alloc::boxed::Box;

use core::marker::PhantomData;
use core::mem;
use core::ops::Deref;
use core::ptr::{self, NonNull};
use core::sync::atomic::Ordering::{Acquire, Relaxed, Release};

use crate::debra::reclaim::align::CacheAligned;
use crate::debra::reclaim::prelude::*;
use crate::debra::reclaim::{MarkedNonNull, MarkedPtr};
use typenum::U1;

type AtomicMarkedPtr<T> = crate::debra::reclaim::AtomicMarkedPtr<T, U1>;

const REMOVE_TAG: usize = 0b1;

#[derive(Debug)]
pub(crate) struct List<T> {
    head: AtomicMarkedPtr<Node<T>>,
}

impl<T> List<T> {
    pub const fn new() -> Self {
        Self {
            head: AtomicMarkedPtr::null(),
        }
    }

    #[inline]
    pub fn insert(&self, entry: T) -> ListEntry<T> {
        let entry = Box::leak(Box::new(Node::new(entry)));
        loop {
            let head = self.head.load(Acquire);
            entry.next().store(head, Relaxed);

            if self
                .head
                .compare_exchange_weak(head, MarkedPtr::new(entry), Release, Relaxed)
                .is_ok()
            {
                return ListEntry(NonNull::from(entry), PhantomData);
            }
        }
    }

    #[inline]
    pub fn remove(&self, entry: ListEntry<T>) -> NonNull<Node<T>> {
        let entry = entry.into_inner();
        loop {
            let pos = self
                .iter_inner(None)
                .find(|pos| pos.curr == entry)
                .expect("given `entry` does not exist in this set");

            let prev = unsafe { pos.prev.as_ref() };
            let curr = unsafe { pos.curr.as_ref() };
            let next = MarkedPtr::new(pos.next.unwrap_ptr());
            let next_marked = MarkedPtr::compose(pos.next.unwrap_ptr(), REMOVE_TAG);

            if curr
                .next
                .compare_exchange(next, next_marked, Acquire, Relaxed)
                .is_err()
            {
                continue;
            }

            if prev
                .compare_exchange(MarkedPtr::from(curr), next, Release, Relaxed)
                .is_err()
            {
                self.repeat_remove(entry);
            }

            return entry;
        }
    }

    #[inline]
    pub fn iter(&self) -> Iter<T> {
        Iter::new(self, &self.head)
    }

    #[inline]
    fn repeat_remove(&self, entry: NonNull<Node<T>>) {
        loop {
            let pos = self
                .iter_inner(Some(entry))
                .find(|pos| pos.curr == entry)
                .unwrap_or_else(|| unreachable!());

            let prev = unsafe { pos.prev.as_ref() };
            let curr = MarkedPtr::new(pos.curr.as_ptr());
            let next = MarkedPtr::new(pos.next.unwrap_ptr());

            if prev.compare_exchange(curr, next, Release, Relaxed).is_ok() {
                return;
            }
        }
    }

    #[inline]
    fn iter_inner(&self, ignore: Option<NonNull<Node<T>>>) -> IterInner<T> {
        IterInner {
            head: &self.head,
            prev: NonNull::from(&self.head),
            ignore,
        }
    }
}

impl<T> Drop for List<T> {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            let mut node = self.head.load(Relaxed).as_ref();
            while let Some(curr) = node {
                node = curr.next().load(Relaxed).as_ref();
                mem::drop(Box::from_raw(curr as *const _ as *mut Node<T>));
            }
        }
    }
}

#[derive(Debug)]
#[must_use]
pub(crate) struct ListEntry<'a, T>(NonNull<Node<T>>, PhantomData<&'a List<T>>);

impl<T> ListEntry<'_, T> {
    #[inline]
    fn into_inner(self) -> NonNull<Node<T>> {
        let inner = self.0;
        mem::forget(self);
        inner
    }
}

impl<T> Deref for ListEntry<'_, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        let node = unsafe { &*self.0.as_ptr() };
        &*node.elem
    }
}

impl<T> Drop for ListEntry<'_, T> {
    #[inline]
    fn drop(&mut self) {
        panic!("set entries must be used to remove their associated entry");
    }
}

#[derive(Debug, Default)]
pub(crate) struct Node<T> {
    elem: CacheAligned<T>,
    next: CacheAligned<AtomicMarkedPtr<Node<T>>>,
}

impl<T> Node<T> {
    #[inline]
    fn elem(&self) -> &T {
        &*self.elem
    }

    #[inline]
    fn next(&self) -> &AtomicMarkedPtr<Node<T>> {
        &*self.next
    }

    #[inline]
    fn new(elem: T) -> Self {
        Self {
            elem: CacheAligned(elem),
            next: CacheAligned(AtomicMarkedPtr::null()),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Iter<'a, T>(IterInner<'a, T>);

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|IterPos { curr, .. }| unsafe { &*curr.as_ptr() }.elem())
    }
}

impl<'a, T> Iter<'a, T> {
    #[inline]
    pub fn new(list: &'a List<T>, start: &AtomicMarkedPtr<Node<T>>) -> Self {
        Self(IterInner {
            head: &list.head,
            prev: NonNull::from(start),
            ignore: None,
        })
    }

    #[inline]
    pub fn load_current_acquire(&self) -> Result<Option<&'a T>, IterError> {
        let (curr, tag) = unsafe { self.0.prev.as_ref().load(Acquire).decompose_ref() };
        match tag {
            REMOVE_TAG => Err(IterError::Retry),
            _ => Ok(curr.map(|node| node.elem())),
        }
    }

    #[inline]
    pub fn load_head_acquire(&self) -> Option<&'a T> {
        unsafe { self.0.head.load(Acquire).as_ref().map(|node| node.elem()) }
    }
}

pub(crate) enum IterError {
    Retry,
}

#[derive(Debug)]
struct IterInner<'a, T> {
    head: &'a AtomicMarkedPtr<Node<T>>,
    prev: NonNull<AtomicMarkedPtr<Node<T>>>,
    ignore: Option<NonNull<Node<T>>>,
}

impl<T> Iterator for IterInner<'_, T> {
    type Item = IterPos<T>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        while let Value(curr) = unsafe { MarkedNonNull::new(self.prev.as_ref().load(Acquire)) } {
            let (curr, curr_tag) = unsafe { curr.decompose_ref_unbounded() };
            if curr_tag == REMOVE_TAG {
                self.restart();
                continue;
            }

            let curr_next = curr.next();

            let next = curr_next.load(Acquire);

            if unsafe { self.prev.as_ref().load(Relaxed) } != MarkedPtr::from(curr) {
                self.restart();
                continue;
            }

            let (next, next_tag) = next.decompose();
            if next_tag == REMOVE_TAG && !self.ignore_marked(curr) {
                continue;
            }

            let prev = self.prev;
            self.prev = NonNull::from(curr_next);
            return Some(IterPos {
                prev,
                curr: NonNull::from(curr),
                next: NonNull::new(next),
            });
        }

        None
    }
}

impl<T> IterInner<'_, T> {
    #[inline]
    fn restart(&mut self) {
        self.prev = NonNull::from(self.head);
    }

    #[inline]
    fn ignore_marked(&self, curr: *const Node<T>) -> bool {
        match self.ignore {
            Some(ignore) if ignore.as_ptr() as *const _ == curr => true,
            _ => false,
        }
    }
}

#[derive(Copy, Clone, Debug)]
struct IterPos<T> {
    prev: NonNull<AtomicMarkedPtr<Node<T>>>,
    curr: NonNull<Node<T>>,
    next: Option<NonNull<Node<T>>>,
}

trait UnwrapPtr {
    type Item;

    fn unwrap_ptr(self) -> *mut Self::Item;
}

impl<T> UnwrapPtr for Option<NonNull<T>> {
    type Item = T;

    #[inline]
    fn unwrap_ptr(self) -> *mut Self::Item {
        match self {
            Some(non_null) => non_null.as_ptr(),
            None => ptr::null_mut(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering::Relaxed;
    use std::thread::{self, ThreadId};

    use super::List;

    static LIST: List<ThreadId> = List::new();

    #[test]
    fn thread_ids() {
        for _ in 0..100_000 {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    thread::spawn(|| {
                        let token = LIST.insert(thread::current().id());
                        let _ = LIST.remove(token);
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }

            assert!(LIST.head.load(Relaxed).is_null());
        }
    }
}
