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

use serde::{Deserialize, Serialize};

use super::types::{Ballot, Command, CommandLeaderID, InstanceID, LocalInstanceID, Seq};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PreAccept<C>
where
    C: Command,
{
    pub(crate) command_leader_id: CommandLeaderID,
    pub(crate) instance_id: InstanceID,
    pub(crate) seq: Seq,
    pub(crate) ballot: Ballot,
    pub(crate) cmds: Vec<C>,
    pub(crate) deps: Vec<Option<LocalInstanceID>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PreAcceptReply {
    pub(crate) instance_id: InstanceID,
    pub(crate) seq: Seq,
    pub(crate) ballot: Ballot,
    pub(crate) ok: bool,
    pub(crate) deps: Vec<Option<LocalInstanceID>>,
    pub(crate) committed_deps: Vec<Option<LocalInstanceID>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PreAcceptOk {
    pub(crate) instance_id: InstanceID,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Accept {
    pub(crate) leader_id: CommandLeaderID,
    pub(crate) instance_id: InstanceID,
    pub(crate) ballot: Ballot,
    pub(crate) seq: Seq,
    pub(crate) cmd_cnt: usize,
    pub(crate) deps: Vec<Option<LocalInstanceID>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct AcceptReply {
    pub(crate) instance_id: InstanceID,
    pub(crate) ok: bool,
    pub(crate) ballot: Ballot,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Commit<C>
where
    C: Command,
{
    pub(crate) command_leader_id: CommandLeaderID,
    pub(crate) instance_id: InstanceID,
    pub(crate) seq: Seq,
    pub(crate) cmds: Vec<C>,
    pub(crate) deps: Vec<Option<LocalInstanceID>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct CommitShort {
    leader_id: CommandLeaderID,
    instance_id: InstanceID,
    seq: Seq,
    cmd_cnt: usize,
    deps: Vec<InstanceID>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Propose<C>
where
    C: Command + Serialize,
{
    pub(crate) cmds: Vec<C>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum Message<C>
where
    C: Command + Serialize,
{
    PreAccept(PreAccept<C>),
    PreAcceptReply(PreAcceptReply),
    PreAcceptOk(PreAcceptOk),
    Accept(Accept),
    AcceptReply(AcceptReply),
    Commit(Commit<C>),
    CommitShort(CommitShort),
    Propose(Propose<C>),
}
