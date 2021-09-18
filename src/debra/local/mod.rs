mod inner;

use core::cell::{Cell, UnsafeCell};
use core::mem::ManuallyDrop;
use core::ptr;
use core::sync::atomic::Ordering;

use super::common::thread::ThreadState;
use super::common::LocalAccess;

use super::global::{EPOCH, THREADS};
use super::{Debra, Retired};

use self::inner::LocalInner;

type ThreadEntry = crate::debra::list::ListEntry<'static, ThreadState>;

#[derive(Debug)]
pub struct Local {
    state: ManuallyDrop<ThreadEntry>,
    guard_count: Cell<usize>,
    inner: UnsafeCell<LocalInner>,
}

impl Local {
    pub fn new() -> Self {
        let global_epoch = EPOCH.load(Ordering::SeqCst);
        let thread_epoch = ThreadState::new(global_epoch);
        let state = THREADS.insert(thread_epoch);

        Self {
            state: ManuallyDrop::new(state),
            guard_count: Cell::default(),
            inner: UnsafeCell::new(LocalInner::new(global_epoch)),
        }
    }

    #[inline]
    pub fn try_flush(&self) {
        unsafe { &mut *self.inner.get() }.try_flush(&**self.state);
    }
}

impl<'a> LocalAccess for &'a Local {
    type Reclaimer = Debra;

    #[inline]
    fn is_active(self) -> bool {
        self.guard_count.get() > 0
    }

    #[inline]
    fn set_active(self) {
        let count = self.guard_count.get();

        self.guard_count.set(count + 1);

        if count == 0 {
            let inner = unsafe { &mut *self.inner.get() };
            inner.set_active(&**self.state);
        }
    }

    #[inline]
    fn set_inactive(self) {
        let count = self.guard_count.get();
        self.guard_count.set(count - 1);
        if count == 1 {
            let inner = unsafe { &*self.inner.get() };
            inner.set_inactive(&**self.state);
        } else if count == 0 {
            panic!("guard count overflow");
        }
    }

    #[inline]
    fn retire_record(self, record: Retired) {
        let inner = unsafe { &mut *self.inner.get() };
        inner.retire_record(record);
    }
}

impl Default for Local {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Local {
    #[inline]
    fn drop(&mut self) {
        let state = unsafe { ptr::read(&*self.state) };
        let entry = THREADS.remove(state);

        unsafe {
            let retired = Retired::new_unchecked(entry);
            let inner = &mut *self.inner.get();
            inner.retire_final_record(retired);
        }
    }
}
