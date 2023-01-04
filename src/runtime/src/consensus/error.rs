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

use std::{collections::BTreeSet, error::Error};

use anyerror::AnyError;

use super::node::{Node, NodeID};
//use crate::storage::error::StorageError;

/// Fatal is unrecoverable and shuts down consensus protocol at once.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Fatal {
    // #[error(transparent)]
    // StorageError(#[from] StorageError),
    #[error("panicked")]
    Panicked,

    #[error("raft stopped")]
    Stopped,
}

/// Extract Fatal from a Result.
///
/// Fatal will shutdown the raft and needs to be dealt separately,
/// such as StorageError.
pub trait ExtractFatal<NID>
where
    Self: Sized,
    NID: NodeID,
{
    fn extract_fatal(self) -> Result<Self, Fatal>;
}

impl<NID, T, E> ExtractFatal<NID> for Result<T, E>
where
    NID: NodeID,
    E: TryInto<Fatal> + Clone,
{
    fn extract_fatal(self) -> Result<Self, Fatal> {
        if let Err(e) = &self {
            let fatal = e.clone().try_into();
            if let Ok(f) = fatal {
                return Err(f);
            }
        }
        Ok(self)
    }
}

/// An error related to a is_leader request.
#[derive(Debug, Clone, thiserror::Error)]
pub enum CheckIsLeaderError<NID, N>
where
    NID: NodeID,
    N: Node,
{
    #[error(transparent)]
    ForwardToLeader(#[from] ForwardToLeader<NID, N>),

    #[error(transparent)]
    QuorumNotEnough(#[from] QuorumNotEnough<NID>),

    #[error(transparent)]
    Fatal(#[from] Fatal),
}

/// An error related to a client write request.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum ClientWriteError<NID, N>
where
    NID: NodeID,
    N: Node,
{
    #[error(transparent)]
    ForwardToLeader(#[from] ForwardToLeader<NID, N>),

    /// When writing a change-membership entry.
    // #[error(transparent)]
    // ChangeMembershipError(#[from] ChangeMembershipError<NID>),

    #[error(transparent)]
    Fatal(#[from] Fatal),
}

/// The set of errors which may take place when requesting to propose a config change.
// #[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
// pub enum ChangeMembershipError<NID: NodeID> {
//     #[error(transparent)]
//     InProgress(#[from] InProgress<NID>),

//     #[error(transparent)]
//     EmptyMembership(#[from] EmptyMembership),

//     #[error(transparent)]
//     LearnerNotFound(#[from] LearnerNotFound<NID>),

//     #[error(transparent)]
//     LearnerIsLagging(#[from] LearnerIsLagging<NID>),
// }

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AddLearnerError<NID, N>
where
    NID: NodeID,
    N: Node,
{
    #[error(transparent)]
    ForwardToLeader(#[from] ForwardToLeader<NID, N>),

    // TODO: do we really need this error? An app may check an target node if it wants to.
    #[error(transparent)]
    NetworkError(#[from] NetworkError),

    #[error(transparent)]
    Fatal(#[from] Fatal),
}

impl<NID, N> TryFrom<AddLearnerError<NID, N>> for ForwardToLeader<NID, N>
where
    NID: NodeID,
    N: Node,
{
    type Error = AddLearnerError<NID, N>;

    fn try_from(value: AddLearnerError<NID, N>) -> Result<Self, Self::Error> {
        if let AddLearnerError::ForwardToLeader(e) = value {
            return Ok(e);
        }
        Err(value)
    }
}

/// The set of errors which may take place when initializing a pristine Raft node.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InitializeError {
    #[error(transparent)]
    Fatal(#[from] Fatal),
    // #[error(transparent)]
    // NotAllowed(#[from] NotAllowed<NID>),

    // #[error(transparent)]
    // NotInMembers(#[from] NotInMembers<NID, N>),
    #[error(transparent)]
    NotAMembershipEntry(#[from] NotAMembershipEntry),
}

// impl<NID> From<StorageError<NID>> for AppendEntriesError<NID>
// where
//     NID: NodeID,
// {
//     fn from(s: StorageError<NID>) -> Self {
//         let f: Fatal<NID> = s.into();
//         f.into()
//     }
// }
// impl<NID> From<StorageError<NID>> for VoteError<NID>
// where
//     NID: NodeID,
// {
//     fn from(s: StorageError<NID>) -> Self {
//         let f: Fatal<NID> = s.into();
//         f.into()
//     }
// }
// impl<NID> From<StorageError<NID>> for InstallSnapshotError<NID>
// where
//     NID: NodeID,
// {
//     fn from(s: StorageError<NID>) -> Self {
//         let f: Fatal<NID> = s.into();
//         f.into()
//     }
// }
// impl<NID, N> From<StorageError> for CheckIsLeaderError<NID, N>
// where
//     NID: NodeID,
//     N: Node,
// {
//     fn from(s: StorageError) -> Self {
//         let f: Fatal = s.into();
//         f.into()
//     }
// }
// impl<NID, N> From<StorageError<NID>> for InitializeError<NID, N>
// where
//     NID: NodeID,
//     N: Node,
// {
//     fn from(s: StorageError<NID>) -> Self {
//         let f: Fatal<NID> = s.into();
//         f.into()
//     }
// }
// impl<NID, N> From<StorageError> for AddLearnerError<NID, N>
// where
//     NID: NodeID,
//     N: Node,
// {
//     fn from(s: StorageError) -> Self {
//         let f: Fatal = s.into();
//         f.into()
//     }
// }

#[derive(Debug, thiserror::Error)]
pub(crate) enum ConsensusError<NID: NodeID> {
    #[error(transparent)]
    ProposeError(#[from] ProposeError<NID>),

    #[error(transparent)]
    ProposeReplyError(#[from] ProposeReplyError<NID>),

    #[error(transparent)]
    PreAcceptError(#[from] PreAcceptError<NID>),

    #[error(transparent)]
    PreAcceptreplyError(#[from] PreAcceptReplyError<NID>),

    #[error(transparent)]
    AcceptError(#[from] AcceptError<NID>),

    #[error(transparent)]
    AcceptReplyError(#[from] AcceptReplyError<NID>),

    #[error(transparent)]
    CommitError(#[from] CommitError<NID>),

    #[error(transparent)]
    CommitReplyError(#[from] CommitReplyError<NID>),

    #[error(transparent)]
    ApplyError(#[from] ApplyError<NID>),

    #[error(transparent)]
    ApplyReplyError(#[from] ApplyReplyError<NID>),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("apply reply error error on {node_id}: {source}")]
pub(crate) struct ApplyReplyError<NID: NodeID> {
    node_id: NID,
    source: AnyError,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("apply error on {node_id}: {source}")]
pub(crate) struct ApplyError<NID: NodeID> {
    node_id: NID,
    source: AnyError,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("commit reply error on {node_id}: {source}")]
pub(crate) struct CommitReplyError<NID: NodeID> {
    node_id: NID,
    source: AnyError,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("commit error on {node_id}: {source}")]
pub(crate) struct CommitError<NID: NodeID> {
    node_id: NID,
    source: AnyError,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("accept reply error on {node_id}: {source}")]
pub(crate) struct AcceptReplyError<NID: NodeID> {
    node_id: NID,
    source: AnyError,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("accept error on {node_id}: {source}")]
pub(crate) struct AcceptError<NID: NodeID> {
    node_id: NID,
    source: AnyError,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("preacceptreply error on {node_id}: {source}")]
pub(crate) struct PreAcceptReplyError<NID: NodeID> {
    node_id: NID,
    source: AnyError,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("preaccept error on {node_id}: {source}")]
pub(crate) struct PreAcceptError<NID: NodeID> {
    node_id: NID,
    source: AnyError,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("propose reply error on {node_id}: {source}")]
pub(crate) struct ProposeReplyError<NID: NodeID> {
    node_id: NID,
    source: AnyError,
}

// impl<NID: NodeID> Display for ConsensusError<NID> {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         todo!()
//     }
// }

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("propose error on {node_id}: {source}")]
pub(crate) struct ProposeError<NID: NodeID> {
    node_id: NID,
    source: AnyError,
}

// /// Error variants related to the Replication.
// #[derive(Debug, thiserror::Error)]
// #[allow(clippy::large_enum_variant)]
// pub(crate) enum ReplicationError {
//     // #[error(transparent)]
//     // HigherVote(#[from] HigherVote<NID>),
//     // #[error("Replication is closed")]
//     // Closed,

//     // #[error(transparent)]
//     // LackEntry(#[from] LackEntry<NID>),
//     #[error(transparent)]
//     CommittedAdvanceTooMany(#[from] CommittedAdvanceTooMany),

//     #[error(transparent)]
//     StorageError(#[from] StorageError),

//     // #[error(transparent)]
//     // NodeNotFound(#[from] NodeNotFound<NID>),

//     // #[error(transparent)]
//     // Timeout(#[from] Timeout<NID>),
//     #[error(transparent)]
//     Network(#[from] NetworkError),
//     // #[error(transparent)]
//     // RemoteError(#[from] RemoteError<NID, N, AppendEntriesError<NID>>),
// }

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RPCError<NID: NodeID, N: Node, T: Error> {
    #[error(transparent)]
    NodeNotFound(#[from] NodeNotFound<NID>),

    // #[error(transparent)]
    // Timeout(#[from] Timeout<NID>),
    #[error(transparent)]
    Network(#[from] NetworkError),

    #[error(transparent)]
    RemoteError(#[from] RemoteError<NID, N, T>),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("error occur on remote peer {target}: {source}")]
pub struct RemoteError<NID: NodeID, N: Node, T: std::error::Error> {
    // #[serde(bound = "")]
    #[cfg_attr(feature = "serde", serde(bound = ""))]
    pub target: NID,
    #[cfg_attr(feature = "serde", serde(bound = ""))]
    pub target_node: Option<N>,
    pub source: T,
}

impl<NID: NodeID, N: Node, T: std::error::Error> RemoteError<NID, N, T> {
    pub fn new(target: NID, e: T) -> Self {
        Self {
            target,
            target_node: None,
            source: e,
        }
    }
    pub fn new_with_node(target: NID, node: N, e: T) -> Self {
        Self {
            target,
            target_node: Some(node),
            source: e,
        }
    }
}

// #[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
// #[cfg_attr(
//     feature = "serde",
//     derive(serde::Deserialize, serde::Serialize),
//     serde(bound = "")
// )]
// #[error("seen a higher vote: {higher} GT mine: {mine}")]
// pub struct HigherVote<NID: NodeID> {
//     pub higher: Vote<NID>,
//     pub mine: Vote<NID>,
// }

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "leader committed index {committed_index} advances target log index {target_index} too many"
)]
pub struct CommittedAdvanceTooMany {
    pub committed_index: u64,
    pub target_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("NetworkError: {source}")]
pub struct NetworkError {
    #[from]
    source: AnyError,
}

impl NetworkError {
    pub fn new<E: Error + 'static>(e: &E) -> Self {
        Self {
            source: AnyError::new(e),
        }
    }
}

// #[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
// #[error("timeout after {timeout:?} when {action} {id}->{target}")]
// pub struct Timeout<NID: NodeID> {
//     pub action: RPCTypes,
//     pub id: NID,
//     pub target: NID,
//     pub timeout: Duration,
// }

// #[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
// #[cfg_attr(
//     feature = "serde",
//     derive(serde::Deserialize, serde::Serialize),
//     serde(bound = "")
// )]
// #[error("store has no log at: {index:?}, last purged: {last_purged_log_id:?}")]
// pub struct LackEntry<NID: NodeID> {
//     pub index: Option<u64>,
//     pub last_purged_log_id: Option<LogId<NID>>,
// }

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("has to forward request to: {leader_id:?}, {leader_node:?}")]
pub struct ForwardToLeader<NID, N>
where
    NID: NodeID,
    N: Node,
{
    pub leader_id: Option<NID>,
    pub leader_node: Option<N>,
}

// #[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
// #[cfg_attr(
//     feature = "serde",
//     derive(serde::Deserialize, serde::Serialize),
//     serde(bound = "")
// )]
// #[error("snapshot segment id mismatch, expect: {expect}, got: {got}")]
// pub struct SnapshotMismatch {
//     pub expect: SnapshotSegmentId,
//     pub got: SnapshotSegmentId,
// }

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("not enough for a quorum, cluster: {cluster}, got: {got:?}")]
pub struct QuorumNotEnough<NID: NodeID> {
    pub cluster: String,
    pub got: BTreeSet<NID>,
}

// #[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
// #[error("the cluster is already undergoing a configuration change at log {membership_log_id:?},
// committed log id: {committed:?}")] pub struct InProgress<NID: NodeID> {
//     pub committed: Option<LogId<NID>>,
//     pub membership_log_id: Option<LogId<NID>>,
// }

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("Learner {node_id} not found: add it as learner before adding it as a voter")]
pub struct CommandLearnerNotFound<NID: NodeID> {
    pub node_id: NID,
}

// #[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
// #[error("replication to learner {node_id} is lagging {distance}, matched: {matched:?}, can not
// add as member")] pub struct LearnerIsLagging<NID: NodeID> {
//     pub node_id: NID,
//     pub matched: Option<LogId<NID>>,
//     pub distance: u64,
// }

// #[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
// #[cfg_attr(
//     feature = "serde",
//     derive(serde::Deserialize, serde::Serialize),
//     serde(bound = "")
// )]
// #[error("not allowed to initialize due to current raft state: last_log_id: {last_log_id:?} vote:
// {vote}")] pub struct NotAllowed<NID: NodeID> {
//     pub last_log_id: Option<LogId<NID>>,
//     pub vote: Vote<NID>,
// }

// #[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
// #[cfg_attr(
//     feature = "serde",
//     derive(serde::Deserialize, serde::Serialize),
//     serde(bound = "")
// )]
// #[error("node {node_id} has to be a member. membership:{membership:?}")]
// pub struct NotInMembers<NID, N>
// where
//     NID: NodeID,
//     N: Node,
// {
//     pub node_id: NID,
//     pub membership: Membership<NID, N>,
// }

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("initializing log entry has to be a membership config entry")]
pub struct NotAMembershipEntry {}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("new membership can not be empty")]
pub struct EmptyMembership {}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("node not found: {node_id}, source: {source}")]
pub struct NodeNotFound<NID: NodeID> {
    pub node_id: NID,
    pub source: AnyError,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[error("infallible")]
pub enum Infallible {}

// #[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
// #[cfg_attr(
//     feature = "serde",
//     derive(serde::Deserialize, serde::Serialize),
//     serde(bound = "")
// )]
// pub(crate) enum RejectVoteRequest<NID: NodeID> {
//     #[error("reject vote request by a greater vote: {0}")]
//     ByVote(Vote<NID>),

//     #[error("reject vote request by a greater last-log-id: {0:?}")]
//     ByLastLogId(Option<LogId<NID>>),
// }

// impl<NID: NodeID> From<RejectVoteRequest<NID>> for AppendEntriesResponse<NID> {
//     fn from(r: RejectVoteRequest<NID>) -> Self {
//         match r {
//             RejectVoteRequest::ByVote(v) => AppendEntriesResponse::HigherVote(v),
//             RejectVoteRequest::ByLastLogId(_) => {
//                 unreachable!("the leader should always has a greater last log id")
//             }
//         }
//     }
// }
