use super::id::Dot;

// Compact representation of which `Dot`s have been committed and executed.
#[allow(dead_code)]
pub(crate) type CommittedAndExecuted = (u64, Vec<Dot>);
