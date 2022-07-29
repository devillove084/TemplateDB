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
use std::{hash::Hash, sync::Arc};

use async_trait::async_trait;
use pro_macro::FromInner;
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, RwLock, RwLockMappedWriteGuard, RwLockReadGuard, RwLockWriteGuard};

use super::{config::Configure, error::ExecuteError, util::instance_exist};

#[async_trait]
pub trait CommandExecutor<C: Command> {
    async fn execute(&self, cmd: &C) -> Result<(), ExecuteError>;
}

#[async_trait]
pub trait Command: Sized {
    type K: Eq + Hash + Send + Sync + Clone + 'static;

    fn key(&self) -> &Self::K;

    async fn execute<E: CommandExecutor<Self> + Send + Sync>(
        &self,
        e: &E,
    ) -> Result<(), ExecuteError> {
        <E as CommandExecutor<Self>>::execute(e, self).await
    }
}

#[derive(
    Default, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, FromInner,
)]
pub(crate) struct ReplicaID(usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, FromInner)]
pub(crate) struct LocalInstanceID(usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, FromInner)]
pub(crate) struct Seq(usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
pub(crate) struct Ballot {
    epoch: usize,
    base: usize,
    replica: ReplicaID,
}

impl Ballot {
    pub(crate) fn new(replica: ReplicaID, conf: &Configure) -> Self {
        Ballot {
            epoch: conf.epoch,
            base: 0,
            replica,
        }
    }

    pub(crate) fn is_init(&self) -> bool {
        self.base == 0
    }

    // pub(crate) fn new_with_epoch(replica: ReplicaID, epoch: usize) -> Ballot {
    //     Ballot {
    //         replica,
    //         epoch,
    //         base: 0,
    //     }
    // }
}

#[derive(Debug, FromInner, Serialize, Deserialize)]
pub(crate) struct CommandLeaderID(usize);

#[derive(Debug, FromInner, Serialize, Deserialize)]
pub(crate) struct AcceptorID(usize);

impl From<ReplicaID> for CommandLeaderID {
    fn from(r: ReplicaID) -> Self {
        Self(*r)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct InstanceID {
    pub(crate) replica: ReplicaID,
    pub(crate) local: LocalInstanceID,
}

impl InstanceID {
    pub(crate) fn new(replica: ReplicaID, local: LocalInstanceID) -> Self {
        Self { replica, local }
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Debug)]
pub(crate) enum InstanceStatus {
    PreAccepted,
    PreAcceptedEq,
    Accepted,
    Committed,
    Executed,
}

#[derive(Debug, Clone, Default)]
pub struct LeaderBook {
    pub(crate) accept_ok: usize,
    pub(crate) preaccept_ok: usize,
    pub(crate) nack: usize,
    pub(crate) max_ballot: Ballot,
    pub(crate) all_equal: bool,
}

impl LeaderBook {
    pub(crate) fn new(replica: ReplicaID, conf: &Configure) -> Self {
        LeaderBook {
            accept_ok: 0,
            preaccept_ok: 0,
            nack: 0,
            max_ballot: Ballot::new(replica, conf),
            all_equal: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Instance<C: Command> {
    pub(crate) id: InstanceID,
    pub(crate) seq: Seq,
    pub(crate) ballot: Ballot,
    pub(crate) cmds: Vec<C>,
    pub(crate) deps: Vec<Option<LocalInstanceID>>,
    pub(crate) status: InstanceStatus,
    pub(crate) lb: LeaderBook,
}

impl<C: Command> Instance<C> {
    pub(crate) fn local_id(&self) -> LocalInstanceID {
        self.id.local
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SharedInstanceInner<C: Command + Clone> {
    instance: Option<Instance<C>>,
    notify: Option<Vec<Arc<Notify>>>,
}

#[derive(Debug, Clone)]
pub(crate) struct SharedInstance<C: Command + Clone> {
    inner: Arc<RwLock<SharedInstanceInner<C>>>,
}

impl<C: Command + Clone> PartialEq for SharedInstance<C> {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl<C: Command + Clone> Eq for SharedInstance<C> {}

impl<C: Command + Clone> Hash for SharedInstance<C> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.inner).hash(state);
    }
}

impl<C: Command + Clone> SharedInstance<C> {
    pub(crate) fn none() -> Self {
        Self::new(None, None)
    }

    pub(crate) fn new(instance: Option<Instance<C>>, notify: Option<Vec<Arc<Notify>>>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(SharedInstanceInner { instance, notify })),
        }
    }

    pub(crate) async fn match_status(&self, status: &[InstanceStatus]) -> bool {
        let d_ins_read = self.get_instance_read().await;
        if d_ins_read.is_none() {
            return false;
        }
        let d_ins_read_inner = d_ins_read.as_ref().unwrap();
        status.contains(&d_ins_read_inner.status)
    }

    pub(crate) async fn get_instance_read(&self) -> RwLockReadGuard<Option<Instance<C>>> {
        RwLockReadGuard::map(self.inner.read().await, |i| &i.instance)
    }

    pub(crate) async fn get_instance_write(&self) -> RwLockMappedWriteGuard<Option<Instance<C>>> {
        RwLockWriteGuard::map(self.inner.write().await, |i| &mut i.instance)
    }

    pub(crate) async fn get_raw_read(
        option_instance: RwLockReadGuard<'_, Option<Instance<C>>>,
    ) -> RwLockReadGuard<'_, Instance<C>> {
        RwLockReadGuard::<Option<Instance<C>>>::map(option_instance, |f| f.as_ref().unwrap())
    }

    pub(crate) async fn get_notify_read(&self) -> RwLockReadGuard<Option<Vec<Arc<Notify>>>> {
        RwLockReadGuard::map(self.inner.read().await, |i| &i.notify)
    }

    pub(crate) async fn clear_notify(&self) {
        let mut inner = self.inner.write().await;
        inner.notify = None;
    }

    pub(crate) async fn add_notify(&self, notify: Arc<Notify>) {
        let mut inner = self.inner.write().await;
        if inner.notify.is_none() {
            inner.notify = Some(vec![notify]);
        } else if let Some(v) = inner.notify.as_mut() {
            v.push(notify);
        }
    }

    pub(crate) async fn notify_commit(&self) {
        let notify_vec = self.get_notify_read().await;
        if let Some(vec) = notify_vec.as_ref() {
            vec.iter().for_each(|notify| notify.notify_one());
        }

        drop(notify_vec);
        self.clear_notify().await;
    }
}

#[async_trait]
pub(crate) trait InstanceSpace<C: Command + Clone + Send + Sync + 'static> {
    /// Construct a instance space
    fn new(peer_cnt: usize) -> Self;

    /// Get the instance, if the it returns a notify it means the instance
    /// is not ready, and the notify is stored in the notify vec. The returned
    /// notify always freshes new, so we do not reuse it to avoid racing.
    async fn get_instance_or_notify(
        &self,
        replica: &ReplicaID,
        instance_id: &LocalInstanceID,
    ) -> (Option<SharedInstance<C>>, Option<Arc<Notify>>);

    /// Get instance if it has created, or None will be returned.
    /// TODO: unify this function and above one!
    async fn get_instance(
        &self,
        replica: &ReplicaID,
        instance_id: &LocalInstanceID,
    ) -> Option<SharedInstance<C>>;

    /// Insert instance into instance space
    async fn insert_instance(
        &self,
        replica: &ReplicaID,
        instance_id: &LocalInstanceID,
        instance: SharedInstance<C>,
    );
}

// TODO: Maybe hashmap or others are more fit in this case.
// (replica_id, instance_id) pair to instance mapping, and
// a big rwlock it not efficient here.
pub struct VecInstanceSpace<C: Command + Clone + Send + 'static> {
    inner: RwLock<Vec<Vec<SharedInstance<C>>>>,
}

#[async_trait]
impl<C> InstanceSpace<C> for VecInstanceSpace<C>
where
    C: Command + Clone + Send + Sync + 'static,
{
    fn new(peer_cnt: usize) -> Self {
        let mut peer_vec = Vec::with_capacity(peer_cnt);
        (0..peer_cnt).for_each(|_| {
            peer_vec.push(vec![]);
        });

        Self {
            inner: RwLock::new(peer_vec),
        }
    }

    /// Get the instance, if the it returns a notify it means the instance
    /// is not ready, and the notify is stored in the notify vec. The returned
    /// notify always freshes new, so we do not reuse it to avoid racing.
    async fn get_instance_or_notify(
        &self,
        replica: &ReplicaID,
        instance_id: &LocalInstanceID,
    ) -> (Option<SharedInstance<C>>, Option<Arc<Notify>>) {
        // TODO: Lock is huge!!!
        let mut space = self.inner.write().await;
        // ** means get the usize(index).
        let instance_id = **instance_id;
        let replica_id = **replica;

        let space_len = space[replica_id].len();
        if instance_id >= space_len {
            if Self::need_notify(&None).await {
                space[replica_id]
                    .extend((space_len..(instance_id + 1)).map(|_| SharedInstance::none()));
                let notify = Arc::new(Notify::new());
                space[replica_id][instance_id]
                    .add_notify(notify.clone())
                    .await;
                (None, Some(notify))
            } else {
                (None, None)
            }
        } else {
            // We have check the bound in the if branch
            let instance = space[replica_id][instance_id].clone();
            if Self::need_notify(&Some(instance.clone())).await {
                let notify = Arc::new(Notify::new());
                instance.add_notify(notify.clone()).await;
                (Some(instance), Some(notify))
            } else {
                (Some(instance), None)
            }
        }
    }

    async fn get_instance(
        &self,
        replica: &ReplicaID,
        instance_id: &LocalInstanceID,
    ) -> Option<SharedInstance<C>> {
        let space = self.inner.read().await;
        let instance_id = **instance_id;
        let replica_id = **replica;

        if instance_id >= space[replica_id].len() {
            None
        } else {
            Some(space[replica_id][instance_id].clone())
        }
    }

    async fn insert_instance(
        &self,
        replica: &ReplicaID,
        instance_id: &LocalInstanceID,
        instance: SharedInstance<C>,
    ) {
        let mut space = self.inner.write().await;
        let instance_id = **instance_id;
        let replica_id = **replica;
        let space_len = space[replica_id].len();
        match instance_id.partial_cmp(&space_len) {
            Some(std::cmp::Ordering::Greater) => {
                space[replica_id]
                    .extend((space_len..(instance_id + 1)).map(|_| SharedInstance::none()));
            }
            Some(std::cmp::Ordering::Less) => {
                space[replica_id][instance_id] = instance;
            }
            Some(std::cmp::Ordering::Equal) => {
                space[replica_id].push(instance);
            }
            None => {}
        }
    }
}

impl<C: Command + Clone + Send + Sync + 'static> VecInstanceSpace<C> {
    async fn need_notify(instance: &Option<SharedInstance<C>>) -> bool {
        if !instance_exist(instance).await {
            true
        } else {
            let d_ins = instance.as_ref().unwrap();
            let d_ins_read = d_ins.get_instance_read().await;
            let d_ins_read_inner = d_ins_read.as_ref().unwrap();

            !matches! {
                d_ins_read_inner.status,
                InstanceStatus::Committed | InstanceStatus::Executed
            }
        }
    }
}
