// Copyright 2022 The template Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    marker::PhantomData,
    ops::Deref,
    ptr::NonNull,
};

pub const ALIGNMENT: usize = 8;

// Page header: ver (6B) | len (1B) | tags (1B) | next (8B) | content_size (4B) |
const PAGE_HEADER_SIZE: usize = 20;
const PAGR_VERSION_MAX: u64 = (1 << 48) - 1;
const PAGE_VERSION_SIZE: usize = 6;

#[derive(Clone, Copy)]
pub struct PagePtr(NonNull<u8>);

impl PagePtr {
    /// Return a `Layout` for a page with the given size
    pub const unsafe fn layout(size: usize) -> Layout {
        Layout::from_size_align_unchecked(size, ALIGNMENT)
    }

    /// Create a new `PagePtr` is ptr is non-null
    ///
    /// # Safety
    ///
    /// This function is unsafe because it does not check if `ptr` points to a valid page
    pub unsafe fn new(ptr: *mut u8) -> Option<Self> {
        NonNull::new(ptr).map(Self)
    }

    /// Return the raw pointer to this page.
    pub const fn as_raw(self) -> *mut u8 {
        self.0.as_ptr()
    }

    unsafe fn ver_ptr(self) -> *mut u8 {
        self.as_raw()
    }

    unsafe fn len_ptr(self) -> *mut u8 {
        self.as_raw().add(PAGE_VERSION_SIZE)
    }

    unsafe fn tags_ptr(self) -> *mut u8 {
        self.as_raw().add(PAGE_VERSION_SIZE + 1)
    }

    unsafe fn next_ptr(self) -> *mut u64 {
        (self.as_raw() as *mut u64).add(1)
    }

    unsafe fn content_size_ptr(self) -> *mut u32 {
        (self.as_raw() as *mut u32).add(4)
    }

    pub fn ver(&self) -> u64 {
        unsafe {
            let ptr = self.ver_ptr() as *mut u64;
            let ver = u64::from_le(ptr.read());
            ver & PAGR_VERSION_MAX
        }
    }

    pub fn set_ver(&mut self, ver: u64) {
        assert!(ver <= PAGR_VERSION_MAX);
        unsafe {
            let ver = ver.to_le();
            let ver_ptr = &ver as *const u64 as *const u8;
            ver_ptr.copy_to_nonoverlapping(self.ver_ptr(), PAGE_VERSION_SIZE);
        }
    }

    pub fn len(&self) -> u8 {
        unsafe { self.len_ptr().read() }
    }

    pub fn set_len(&mut self, len: u8) {
        unsafe { self.len_ptr().write(len) }
    }

    pub fn next(&self) -> u64 {
        let next = unsafe { self.next_ptr().read() };
        u64::from_le(next)
    }

    pub fn set_next(&mut self, next: u64) {
        unsafe {
            self.next_ptr().write(next);
        }
    }

    pub fn tags(&self) -> PageTags {
        unsafe { self.tags_ptr().read().into() }
    }

    pub fn set_tags(&mut self, tags: PageTags) {
        unsafe { self.tags_ptr().write(tags.into()) }
    }

    /// Return the PageKind of this page
    pub fn kind(&self) -> PageKind {
        self.tags().kind()
    }

    pub fn set_kind(&mut self, kind: PageKind) {
        self.set_tags(self.tags().with_kind(kind))
    }

    /// Return true if this page is a leaf page
    pub fn is_leaf(&self) -> bool {
        self.tags().is_leaf()
    }

    pub fn set_leaf(&mut self, is_leaf: bool) {
        self.set_tags(self.tags().with_leaf(is_leaf))
    }

    /// Set the header of this page to default
    pub fn set_default(&mut self) {
        unsafe {
            self.as_raw().write_bytes(0, PAGE_HEADER_SIZE);
        }
    }

    pub fn size(&self) -> usize {
        PAGE_HEADER_SIZE + self.content_size() as usize
    }

    pub fn content_size(&self) -> usize {
        let size = unsafe { self.content_size_ptr().read() };
        u32::from_le(size) as usize
    }

    fn set_content_size(&mut self, size: usize) {
        assert!(size <= u32::MAX as usize);
        unsafe {
            self.content_size_ptr().write((size as u32).to_le());
        }
    }

    pub fn content(&self) -> *const u8 {
        unsafe { self.as_raw().add(PAGE_HEADER_SIZE) }
    }

    pub fn content_mut(&mut self) -> *mut u8 {
        unsafe { self.as_raw().add(PAGE_HEADER_SIZE) }
    }
}

impl From<PagePtr> for u64 {
    fn from(value: PagePtr) -> Self {
        value.as_raw() as u64
    }
}

impl std::fmt::Debug for PagePtr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Page")
            .field("ver", &self.ver())
            .field("len", &self.len())
            .field("next", &self.next())
            .field("kind", &self.kind())
            .field("is_leaf", &self.is_leaf())
            .field("content_size", &self.content_size())
            .finish()
    }
}

/// An immutable reference to a page
#[derive(Clone, Copy)]
pub struct PageRef<'a> {
    ptr: PagePtr,
    _p: PhantomData<&'a ()>,
}

impl PageRef<'_> {
    pub unsafe fn new(ptr: *mut u8) -> Option<Self> {
        PagePtr::new(ptr).map(Self::from)
    }
}

impl Deref for PageRef<'_> {
    type Target = PagePtr;

    fn deref(&self) -> &Self::Target {
        &self.ptr
    }
}

impl From<PagePtr> for PageRef<'_> {
    fn from(value: PagePtr) -> Self {
        Self {
            ptr: value,
            _p: PhantomData,
        }
    }
}

impl From<PageRef<'_>> for u64 {
    fn from(value: PageRef<'_>) -> Self {
        value.ptr.into()
    }
}

impl std::fmt::Debug for PageRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.ptr.fmt(f)
    }
}

bitflags::bitflags! {
    #[derive(Default)]
    pub struct PageTags: u8 {
        const LEAF = 0b1000_0000;
        const KIND = 0b0111_1111;
    }
}

impl PageTags {
    fn kind(self) -> PageKind {
        PageKind::new((self & Self::KIND).bits())
    }

    fn with_kind(self, kind: PageKind) -> Self {
        self | Self::from(kind as u8)
    }

    const fn is_leaf(self) -> bool {
        self.contains(Self::LEAF)
    }

    fn with_leaf(self, is_leaf: bool) -> Self {
        if is_leaf {
            self | Self::LEAF
        } else {
            self | !Self::LEAF
        }
    }
}

impl From<u8> for PageTags {
    fn from(value: u8) -> Self {
        unsafe { Self::from_bits_unchecked(value) }
    }
}

impl From<PageTags> for u8 {
    fn from(value: PageTags) -> Self {
        value.bits()
    }
}

impl From<PageKind> for PageTags {
    fn from(value: PageKind) -> Self {
        Self::from(value as u8)
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageKind {
    /// Pages with base entries
    Base = 0,
    /// Pages with split infomation
    Split = 2,
}

impl PageKind {
    const fn new(kind: u8) -> Self {
        match kind {
            0 => Self::Base,
            2 => Self::Split,
            _ => panic!("invalid page kind"),
        }
    }
}

impl From<PageKind> for u8 {
    fn from(value: PageKind) -> Self {
        value as u8
    }
}

/// An interface to allocate and deallocate pages
///
/// # Safety
///
/// Similar to the `std::alloc::Allocator` trait
pub unsafe trait PageAlloc {
    type Error;

    fn alloc_page(&self, size: usize) -> Result<PagePtr, Self::Error>;

    unsafe fn dealloc_page(&self, page: PagePtr);
}

unsafe impl PageAlloc for System {
    type Error = ();

    fn alloc_page(&self, size: usize) -> Result<PagePtr, Self::Error> {
        unsafe {
            let ptr = self.alloc(PagePtr::layout(size));
            PagePtr::new(ptr).ok_or(())
        }
    }

    unsafe fn dealloc_page(&self, page: PagePtr) {
        let ptr = page.as_raw();
        self.dealloc(ptr, PagePtr::layout(page.size()));
    }
}

#[derive(Default)]
pub struct PageBuilder {
    tags: PageTags,
}

impl PageBuilder {
    pub fn new(kind: PageKind) -> Self {
        Self { tags: kind.into() }
    }

    pub fn with_leaf(kind: PageKind, is_leaf: bool) -> Self {
        Self {
            tags: PageTags::from(kind).with_leaf(is_leaf),
        }
    }

    pub fn build<A: PageAlloc>(&self, alloc: &A, content_size: usize) -> Result<PagePtr, A::Error> {
        let ptr = alloc.alloc_page(PAGE_HEADER_SIZE + content_size);
        ptr.map(|mut p| {
            p.set_default();
            p.set_tags(self.tags);
            p.set_content_size(content_size);
            p
        })
    }
}

#[cfg(test)]
pub mod tests {
    use std::alloc::System;

    use super::{PageAlloc, PAGE_HEADER_SIZE};
    use crate::bwtree::page::PageKind;

    #[test]
    fn base_page() {
        let mut ptr = System.alloc_page(PAGE_HEADER_SIZE).unwrap();
        ptr.set_default();
        assert_eq!(ptr.ver(), 0);
        ptr.set_ver(1);
        assert_eq!(ptr.ver(), 1);

        assert_eq!(ptr.len(), 0);
        ptr.set_len(2);
        assert_eq!(ptr.len(), 2);

        assert_eq!(ptr.next(), 0);
        ptr.set_next(3);
        assert_eq!(ptr.next(), 3);

        assert_eq!(ptr.kind(), PageKind::Base);
        ptr.set_kind(PageKind::Split);
        assert_eq!(ptr.kind(), PageKind::Split);

        assert!(!ptr.is_leaf());
        ptr.set_leaf(true);
        assert!(ptr.is_leaf());

        assert_eq!(ptr.content_size(), 0);
        ptr.set_content_size(4);
        assert_eq!(ptr.content_size(), 4);
    }
}
