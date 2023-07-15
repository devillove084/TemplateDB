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

#[derive(Clone, Copy, Debug, Default, Hash, PartialOrd, PartialEq, Eq, Ord)]
pub(crate) struct ContainerID {
    id: u32,
}

#[allow(dead_code)]
impl ContainerID {
    pub const fn new(id: u32) -> Self {
        Self { id }
    }

    pub const fn placeholder() -> Self {
        Self { id: u32::MAX - 1 }
    }

    pub fn get_container_id(&self) -> u32 {
        self.id
    }
}

impl From<u32> for ContainerID {
    fn from(value: u32) -> Self {
        Self::new(value)
    }
}

impl From<&u32> for ContainerID {
    fn from(value: &u32) -> Self {
        Self::new(*value)
    }
}

impl From<ContainerID> for u32 {
    fn from(value: ContainerID) -> Self {
        value.id
    }
}
