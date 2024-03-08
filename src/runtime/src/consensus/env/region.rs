use std::{collections::HashSet, fmt::Debug};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Region {
    id: u64,
    name: String,
    set: HashSet<u64>,
}

impl Debug for Region {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the region is {}", self.id)?;
        write!(f, "name is {}", self.name)?;
        write!(f, "which has range is {:?}", self.set)
    }
}
