#[derive(Debug)]
pub enum OperatorError {}

pub type OperatorOnResult<T> = std::result::Result<T, OperatorError>;
