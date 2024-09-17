#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ValueType {
    /// A value indicates that the key is deleted
    Deletion = 0,
    /// A normal value
    Value = 1,

    /// Unknown type
    Unknown,
}

pub const VALUE_TYPE_FOR_SEEK: ValueType = ValueType::Value;

impl From<u64> for ValueType {
    fn from(v: u64) -> Self {
        match v {
            1 => ValueType::Value,
            0 => ValueType::Deletion,
            _ => ValueType::Unknown,
        }
    }
}
