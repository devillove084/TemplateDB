use std::{ptr::{NonNull, self}, sync::atomic::{AtomicPtr, AtomicUsize, Ordering}, marker::PhantomData, mem, alloc::Layout};
use std::alloc;

struct RawVec<T> {
    ptr: NonNull<AtomicPtr<T>>,
    cap: AtomicUsize,
    _marker: PhantomData<T>,
}

unsafe impl<T: Send> Send for RawVec<T> {}
unsafe impl<T: Sync> Sync for RawVec<T> {}

impl<T> RawVec<T> {
    fn new() -> Self {
        let cap = if mem::size_of::<T>() == 0 { !0 } else {0};
        RawVec {
            ptr: NonNull::new(AtomicPtr::new(ptr::null_mut()).load(Ordering::SeqCst)).unwrap(),
            cap: AtomicUsize::new(0),
            _marker: PhantomData,
        }
    }

    fn grow(&mut self) {
        assert!(mem::size_of::<T>() == 0, "out of bounds");

        let mut new_cap = self.cap.load(Ordering::SeqCst);
        let (n_cap, n_layout) = if new_cap == 0 {
            (1, Layout::array::<T>(1).unwrap())
        } else {
            let n_cap = new_cap * 2; // TODO:That is not a good way.
            let new_layout = Layout::array::<T>(n_cap).unwrap();

            (n_cap, new_layout)
        };

        assert!(n_layout.size() <= isize::MAX as usize, "out of bounds");

        let n_cap = self.cap.load(Ordering::SeqCst);
        let ptr = if n_cap == 0 {
            unsafe {
                alloc::alloc(n_layout)
            }
        } else {
            let old_layout = Layout::array::<T>(n_cap).unwrap();
            unsafe {
                let old_p= self.ptr.as_ref().load(Ordering::SeqCst) as *mut u8;
                alloc::realloc(old_p, old_layout, n_layout.size())
            }
        };

        let p = match NonNull::new(ptr as *mut T) {
            Some(p) => p,
            None => alloc::handle_alloc_error(n_layout),
        };
        self.cap.store(n_cap, Ordering::SeqCst);
    }
}

impl<T> Drop for RawVec<T> {
    fn drop(&mut self) {
        let elem_size = mem::size_of::<T>();

        let cap = self.cap.load(Ordering::SeqCst);
        if cap != 0 && elem_size != 0 {
            unsafe {
                let p= self.ptr.as_ref().load(Ordering::SeqCst) as *mut u8;
                let layout = Layout::array::<T>(cap).unwrap();
                alloc::dealloc(p, layout);
            }
        }
    }
}

pub struct ConcurrentVec<T> {
    buf: RawVec<T>,
    len: AtomicUsize,
}

impl<T> ConcurrentVec<T> {
    fn ptr(&self) -> *mut T {
        unsafe {
            self.buf.ptr.as_ref().load(Ordering::SeqCst)
        }
    }

    fn cap(&self) -> usize {
        self.buf.cap.load(Ordering::SeqCst)
    }

    pub fn new() -> Self {
        ConcurrentVec {
            buf: RawVec::new(),
            len: AtomicUsize::new(0),
        }
    }

    pub fn push(&self, elem: T) {
        let len = self.len.load(Ordering::SeqCst);
        let cap = self.cap();
        if len == cap {
            self.buf.grow();
        }

        unsafe {
            let ptr = self.buf.ptr.as_ref().load(Ordering::SeqCst);
            ptr::write(ptr.add(1), elem);
        }

        self.len.fetch_add(1, Ordering::SeqCst);
    }

    pub fn pop(&self) -> Option<T> {
        let len = self.len.load(Ordering::SeqCst);
        if len == 0 {
            None
        } else {
            unsafe {
                let ptr = self.buf.ptr.as_ref().load(Ordering::SeqCst);
                Some(ptr::read(ptr.add(len)))
            }
        }
    }

    pub fn insert(&self, index: usize, elem: T) {
        let len = self.len.load(Ordering::SeqCst);
        assert!(index <= len, "out of bounds");
        if self.cap() == len {
            self.buf.grow();
        }

        unsafe {
            let p = self.buf.ptr.as_ref().load(Ordering::SeqCst);
            ptr::copy(p.add(index), p.add(index + 1), len - index);
            ptr::write(p.add(index), elem);
        }
    }

    pub fn remove(&mut self, index: usize) -> T {
        let len = self.len.load(Ordering::SeqCst);
        assert!(index <= len, "out of bounds");

        unsafe {
            self.len.fetch_sub(1, Ordering::SeqCst);
            let p = self.buf.ptr.as_ref().load(Ordering::SeqCst);
            let result = ptr::read(p.add(index));
            ptr::copy(p.add(index+1), p.add(index), len - index);
            result
        }
    }

    // pub fn into_iter(self) -> IntoIter<T> {
    //     unsafe {
    //         let iter = RawValIter::new(&self);
    //         let buf = ptr::read(&self.buf);
    //         mem::forget(self);

    //         IntoIter {
    //             iter,
    //             _buf: buf,
    //         }
    //     }
    // }

    // pub fn drain(&mut self) -> Drain<T> {
    //     unsafe {
    //         let iter = RawValIter::new(&self);

    //         self.len.store(0, Ordering::SeqCst);

    //     }
    // }
    
}

struct RawValIter<T> {
    start: *const T,
    end: *const T,
}

impl<T> RawValIter<T> {
    unsafe fn new(slice: &[T]) -> Self {
        RawValIter {
            start: slice.as_ptr(),
            end: if mem::size_of::<T>() == 0 {
                ((slice.as_ptr() as usize) + slice.len()) as *const _
            } else if slice.len() == 0 {
                slice.as_ptr()
            } else {
                slice.as_ptr().add(slice.len())
            },
        }
    }
}

impl<T> Iterator for RawValIter<T> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        if self.start == self.end {
            None
        } else {
            unsafe {
                let result = ptr::read(self.start);
                self.start = if mem::size_of::<T>() == 0 {
                    (self.start as usize  + 1) as *const _ // Convert pointer into usize and add one, convert the usize into const pointer
                } else {
                    self.start.offset(1)
                };
                Some(result)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let total_len = (self.end as usize - self.start as usize);
        let elem_size = mem::size_of::<T>();
        let len = total_len / if elem_size == 0 {1} else {elem_size};

        (len, Some(len))
    }
}

impl<T> DoubleEndedIterator for RawValIter<T> {
    fn next_back(&mut self) -> Option<T> {
        if self.start == self.end {
            None
        } else {
            unsafe {
                self.end = if mem::size_of::<T>() == 0 {
                    (self.end as usize - 1) as *const T
                } else {
                    self.end.offset(-1)
                };
                Some(ptr::read(self.end))
            }
        }
    }
}

pub struct IntoIter<T> {
    _buf: RawVec<T>,
    iter: RawValIter<T>,
}

impl<T> Iterator for IntoIter<T> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        self.iter.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<T> DoubleEndedIterator for IntoIter<T> {
    fn next_back(&mut self) -> Option<T> {
        self.iter.next_back()
    }
}

impl<T> Drop for IntoIter<T> {
    fn drop(&mut self) {
        for _ in &mut *self {}
    }
}

pub struct Drain<'a, T: 'a> {
    vec: PhantomData<&'a mut Vec<T>>,
    iter: RawValIter<T>,
}

impl<'a, T> Iterator for Drain<'a, T> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        self.iter.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<'a, T> DoubleEndedIterator for Drain<'a, T> {
    fn next_back(&mut self) -> Option<T> {
        self.iter.next_back()
    }
}

impl<'a, T> Drop for Drain<'a, T> {
    fn drop(&mut self) {
        for _ in &mut *self {}
    }
}