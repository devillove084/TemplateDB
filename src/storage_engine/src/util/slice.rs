use std::{
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    ops::Index,
    ptr, slice,
};

use crate::util::hash::hash;

#[derive(Clone, Eq)]
pub struct Slice {
    data: *const u8,
    size: usize,
}

impl Slice {
    pub fn new(data: *const u8, size: usize) -> Self {
        Self { data, size }
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        if !self.data.is_null() {
            unsafe { slice::from_raw_parts(self.data, self.size) }
        } else {
            panic!("try to convert a empty(invalid) Slice as a &[u8] ")
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_null() || self.size == 0
    }

    #[inline]
    pub fn compare(&self, other: &Slice) -> Ordering {
        self.as_slice().cmp(other.as_slice())
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        if self.is_empty() {
            ""
        } else {
            unsafe { ::std::str::from_utf8_unchecked(self.as_slice()) }
        }
    }
}

impl Default for Slice {
    fn default() -> Self {
        Self::new(ptr::null(), 0)
    }
}

impl fmt::Debug for Slice {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl PartialEq for Slice {
    fn eq(&self, other: &Slice) -> bool {
        self.compare(other) == Ordering::Equal
    }
}

impl Index<usize> for Slice {
    type Output = u8;

    /// Return the ith byte in the referenced data
    fn index(&self, index: usize) -> &u8 {
        assert!(
            index < self.size,
            "[slice] out of range. Slice size is [{}] but try to get [{}]",
            self.size,
            index
        );
        unsafe { &*self.data.add(index) }
    }
}

impl Hash for Slice {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let hash = hash(self.as_slice(), 0xbc9f_1d34);
        state.write_u32(hash);
        let _ = state.finish();
    }
}

impl AsRef<[u8]> for Slice {
    fn as_ref(&self) -> &[u8] {
        if !self.data.is_null() {
            unsafe { slice::from_raw_parts(self.data, self.size) }
        } else {
            panic!("try to convert a empty(invalid) Slice as a &[u8] ")
        }
    }
}

impl<'a> From<&'a [u8]> for Slice {
    #[inline]
    fn from(v: &'a [u8]) -> Self {
        Slice::new(v.as_ptr(), v.len())
    }
}

impl<'a> From<&'a Vec<u8>> for Slice {
    #[inline]
    fn from(v: &'a Vec<u8>) -> Self {
        Slice::new(v.as_ptr(), v.len())
    }
}

impl<'a> From<&'a str> for Slice {
    #[inline]
    fn from(s: &'a str) -> Self {
        Slice::new(s.as_ptr(), s.len())
    }
}
