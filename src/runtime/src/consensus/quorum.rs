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

use std::sync::Arc;

/// A set of quorums is a collection of quorum.
///
/// A quorum is a collection of nodes that a read or write operation in distributed system has to
/// contact to. See: http://web.mit.edu/6.033/2005/wwwdocs/quorum_note.html
pub(crate) trait QuorumSet<ID: 'static> {
    type Iter: Iterator<Item = ID>;

    /// Check if a series of ID constitute a quorum that is defined by this quorum set.
    fn is_quorum<'a, I: Iterator<Item = &'a ID> + Clone>(&self, ids: I) -> bool;

    /// Returns all ids in this QuorumSet
    fn ids(&self) -> Self::Iter;
}

impl<ID: 'static, T: QuorumSet<ID>> QuorumSet<ID> for Arc<T> {
    type Iter = T::Iter;

    fn is_quorum<'a, I: Iterator<Item = &'a ID> + Clone>(&self, ids: I) -> bool {
        self.as_ref().is_quorum(ids)
    }

    fn ids(&self) -> Self::Iter {
        self.as_ref().ids()
    }
}
