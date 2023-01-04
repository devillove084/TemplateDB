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
    fmt::{Debug, Display, Formatter},
    hash::Hash,
};

/// Essential trait bound for node-id, except serde.
#[doc(hidden)]
pub trait NodeIDEssential:
    Sized
    + Send
    + Sync
    + Eq
    + PartialEq
    + Ord
    + PartialOrd
    + Debug
    + Display
    + Hash
    + Copy
    + Clone
    + Default
    + 'static
{
}

impl<T> NodeIDEssential for T where
    T: Sized
        + Send
        + Sync
        + Eq
        + PartialEq
        + Ord
        + PartialOrd
        + Debug
        + Display
        + Hash
        + Copy
        + Clone
        + Default
        + 'static
{
}

/// A  node's ID.
///
/// A `NodeId` uniquely identifies a node in the Raft cluster.
#[cfg(feature = "serde")]
pub trait NodeID: NodeIDEssential + serde::Serialize + for<'a> serde::Deserialize<'a> {}

#[cfg(feature = "serde")]
impl<T> NodeID for T where T: NodeIDEssential + serde::Serialize + for<'a> serde::Deserialize<'a> {}

#[cfg(not(feature = "serde"))]
pub trait NodeID: NodeIDEssential {}

#[cfg(not(feature = "serde"))]
impl<T> NodeID for T where T: NodeIDEssential {}

/// Essential trait bound for application level node-data, except serde.
pub trait NodeEssential:
    Sized + Send + Sync + Eq + PartialEq + Debug + Clone + Default + 'static
{
}
impl<T> NodeEssential for T where
    T: Sized + Send + Sync + Eq + PartialEq + Debug + Clone + Default + 'static
{
}

/// A Raft `Node`, this trait holds all relevant node information.
///
/// For the most generic case `BasicNode` provides an example implementation including the node's
/// network address, but the used `Node` implementation can be customized to include additional
/// information.
#[cfg(feature = "serde")]
pub trait Node: NodeEssential + serde::Serialize + for<'a> serde::Deserialize<'a> {}

#[cfg(feature = "serde")]
impl<T> Node for T where T: NodeEssential + serde::Serialize + for<'a> serde::Deserialize<'a> {}

#[cfg(not(feature = "serde"))]
pub trait Node: NodeEssential {}

#[cfg(not(feature = "serde"))]
impl<T> Node for T where T: NodeEssential {}

/// Minimal node information.
///
/// The most common usage is to store the connecting address of a node.
/// So that an application does not need an additional store to support its RaftNetwork
/// implementation.
///
/// An application is also free not to use this storage and implements its own node-id to address
/// mapping.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct BasicNode {
    pub addr: String,
}

impl BasicNode {
    pub fn new(addr: impl ToString) -> Self {
        Self {
            addr: addr.to_string(),
        }
    }
}

impl Display for BasicNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.addr)
    }
}
