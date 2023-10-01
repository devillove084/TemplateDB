use std::{alloc::Allocator, path::Path};

use super::error::OperatorOnResult;

pub trait GetSource {
    async fn read_from_file(&self, path: Path) -> OperatorOnResult<()>;

    async fn read_from_buffer<A: Allocator>(&self, buffer: Buffer<A>) -> OperatorOnResult<()>;
}

#[derive(Default)]
pub struct Buffer<A: Allocator> {
    allocator: A,
    size: usize,
    elem_count: usize,
    start: usize,
}

// impl<A: Allocator> Buffer<A> {
//     pub fn new() -> Self {
        
//     }
// }

pub struct SinkSource<S: GetSource> {
    source: S,
}

pub struct SinkOperator<S: GetSource, A: Allocator> {
    sink_source: SinkSource<S>,
    buffer: Buffer<A>,
}
