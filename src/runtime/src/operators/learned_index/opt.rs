use std::ops::Range;

use super::error::OperatorOnResult;

// pub struct EntryOperator {
//     entry: Box<dyn OpeartionOn>,
// }

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Operation {
    Read,
    Write,
    Delete,
    Update,
    Split,
    Combine,
    Concurrency,
}

pub trait PointerOpeartion<T> {
    async fn load(&self, address: usize) -> OperatorOnResult<T>;

    async fn store(&self, address: usize, v: T) -> OperatorOnResult<()>;

    async fn move_to_range(&self, range: Range<usize>) -> OperatorOnResult<()>;
}

pub trait ContinousOperation<T> {
    async fn load_current(&self, index: usize) -> OperatorOnResult<T>;

    async fn load_next(&self) -> OperatorOnResult<T>;

    async fn load_range(&self, range: Range<usize>) -> OperatorOnResult<Vec<T>>;

    async fn store(&self, address: usize, v: T) -> OperatorOnResult<()>;

    async fn store_in_range(&self, address: Range<usize>, to: Range<usize>)
        -> OperatorOnResult<()>;
}

pub trait AdvanceOperation<T> {
    // 1->N
    async fn split(&self) -> OperatorOnResult<()>;

    // N->M
    async fn combine(&self) -> OperatorOnResult<()>;
}
