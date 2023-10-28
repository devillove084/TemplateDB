use std::alloc::Allocator;

use actix::{Actor, Context};

#[derive(Default)]
pub struct AllocatorActor {}

impl Actor for AllocatorActor {
    type Context = Context<AllocatorActor>;
}

#[allow(unused_variables)]
unsafe impl Allocator for AllocatorActor {
    fn allocate(
        &self,
        layout: std::alloc::Layout,
    ) -> Result<std::ptr::NonNull<[u8]>, std::alloc::AllocError> {
        todo!()
    }

    unsafe fn deallocate(&self, ptr: std::ptr::NonNull<u8>, layout: std::alloc::Layout) {
        todo!()
    }
}
