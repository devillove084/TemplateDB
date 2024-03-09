use std::{collections::HashMap, fmt::Debug, iter, marker::PhantomData, sync::Arc};

use itertools::Itertools;
use tokio::sync::mpsc;

use super::{
    execute::Executor,
    types::{
        Command, CommandExecutor, Instance, InstanceID, InstanceSpace, LocalInstanceID, ReplicaID,
        Seq, SharedInstance,
    },
};

type Conflict<C> = Vec<HashMap<<C as Command>::K, SharedInstance<C>>>;
pub(crate) struct Replica<C, E, S>
where
    C: Command + Clone + Send + Sync + 'static,
    E: CommandExecutor<C>,
    S: InstanceSpace<C>,
{
    pub(crate) id: ReplicaID,
    pub(crate) peer_cnt: usize,
    pub(crate) instance_space: Arc<S>,
    pub(crate) cur_max_instances: Vec<LocalInstanceID>,
    pub(crate) commited_upto: Vec<Option<LocalInstanceID>>,
    pub(crate) conflicts: Conflict<C>,
    pub(crate) max_seq: Seq,
    pub(crate) exec_send: mpsc::Sender<SharedInstance<C>>,
    _phantom: PhantomData<E>,
}

impl<C, E, S> Replica<C, E, S>
where
    C: Command + Debug + Clone + Send + Sync + 'static,
    E: CommandExecutor<C> + Debug + Clone + Sync + Send + 'static,
    S: InstanceSpace<C> + Send + Sync + 'static,
{
    pub(crate) fn new(id: usize, peer_cnt: usize, cmd_exe: E) -> Self {
        let instance_space = Arc::new(S::new(peer_cnt));
        let mut cur_max_instances = Vec::with_capacity(peer_cnt);
        let mut commited_upto = Vec::with_capacity(peer_cnt);
        let mut conflicts = Vec::with_capacity(peer_cnt);

        for _ in 0..peer_cnt {
            cur_max_instances.push(0.into());
            commited_upto.push(None);
            conflicts.push(HashMap::new());
        }

        let (sender, receiver) = mpsc::channel(10);
        let space_clone = instance_space.clone();
        tokio::spawn(async move {
            let executor = Executor::new(space_clone, cmd_exe);
            executor.execute(receiver).await;
        });

        Self {
            id: id.into(),
            peer_cnt,
            instance_space,
            cur_max_instances,
            commited_upto,
            conflicts,
            max_seq: 0.into(),
            exec_send: sender,
            _phantom: PhantomData,
        }
    }

    pub(crate) fn cur_instance(&self, r: &ReplicaID) -> LocalInstanceID {
        self.cur_max_instances[**r]
    }

    pub(crate) fn set_cur_instance(&mut self, instance: &InstanceID) {
        self.cur_max_instances[*instance.replica] = instance.local;
    }

    pub(crate) fn local_cur_instance(&self) -> &LocalInstanceID {
        self.cur_max_instances.get(*self.id).unwrap()
    }

    pub(crate) fn inc_local_cur_instance(&mut self) -> &LocalInstanceID {
        let mut instance_id = self.cur_max_instances.get_mut(*self.id).unwrap();
        instance_id += 1;
        instance_id
    }

    pub(crate) fn merge_seq_deps(
        &self,
        instance: &mut Instance<C>,
        new_seq: &Seq,
        new_deps: &[Option<LocalInstanceID>],
    ) -> bool {
        let mut equal = true;
        if &instance.seq != new_seq {
            equal = false;
            if new_seq > &instance.seq {
                instance.seq = *new_seq;
            }
        }

        instance
            .deps
            .iter_mut()
            .zip(new_deps.iter())
            .for_each(|(o, n)| {
                if o != n {
                    equal = false;
                    if o.is_none() || (n.is_none() && o.as_ref().unwrap() < n.as_ref().unwrap()) {
                        *o = *n;
                    }
                }
            });
        equal
    }

    pub(crate) async fn get_seq_deps(&self, cmds: &[C]) -> (Seq, Vec<Option<LocalInstanceID>>) {
        let mut new_seq = 0.into();
        let mut deps: Vec<Option<LocalInstanceID>> =
            iter::repeat_with(|| None).take(self.peer_cnt).collect();
        for (r_id, command) in (0..self.peer_cnt).cartesian_product(cmds.iter()) {
            if r_id != *self.id {
                if let Some(instance) = self.conflicts[r_id].get(command.key()) {
                    let conflict_instance = instance.get_instance_read().await;
                    if conflict_instance.is_none() {
                        continue;
                    }
                    let conflict_instance = SharedInstance::get_raw_read(conflict_instance);
                    // update deps
                    deps[r_id] = match deps[r_id] {
                        Some(dep_instance_id) => {
                            if conflict_instance.local_id() > dep_instance_id {
                                Some(conflict_instance.local_id())
                            } else {
                                Some(dep_instance_id)
                            }
                        }
                        None => Some(conflict_instance.local_id()),
                    };
                    let s = &conflict_instance.seq;
                    if s >= &new_seq {
                        new_seq = (**s + 1).into();
                    }
                }
            }
        }
        (new_seq, deps)
    }

    // TODO: merge with get_seq_deps
    pub(crate) async fn update_seq_deps(
        &self,
        mut seq: Seq,
        mut deps: Vec<Option<LocalInstanceID>>,
        cmds: &[C],
    ) -> (Seq, Vec<Option<LocalInstanceID>>, bool) {
        let mut changed = false;
        for (r_id, command) in (0..self.peer_cnt).cartesian_product(cmds.iter()) {
            if r_id != *self.id {
                if let Some(instance) = self.conflicts[r_id].get(command.key()) {
                    let conflict_instance = instance.get_instance_read().await;
                    if conflict_instance.is_none() {
                        continue;
                    }
                    let conflict_instance = SharedInstance::get_raw_read(conflict_instance);
                    if deps[r_id].is_some() && deps[r_id].unwrap() < conflict_instance.local_id() {
                        changed = true;

                        // update deps
                        deps[r_id] = match deps[r_id] {
                            Some(dep_instance_id) => {
                                if conflict_instance.local_id() > dep_instance_id {
                                    Some(conflict_instance.local_id())
                                } else {
                                    Some(dep_instance_id)
                                }
                            }
                            None => Some(conflict_instance.local_id()),
                        };

                        // update seq
                        let conflict_seq = &conflict_instance.seq;
                        if conflict_seq >= &seq {
                            seq = (**conflict_seq + 1).into();
                        }
                    }
                }
            }
        }
        (seq, deps, changed)
    }

    pub(crate) async fn update_conflicts(
        &mut self,
        replica: &ReplicaID,
        cmds: &[C],
        new_inst: SharedInstance<C>,
    ) {
        for c in cmds {
            let new_inst = match self.conflicts[**replica].get(c.key()) {
                None => Some(new_inst.clone()),
                Some(ins) => {
                    let ins = ins.get_instance_read().await;
                    if ins.is_some()
                        && SharedInstance::get_raw_read(ins).local_id()
                            >= SharedInstance::get_raw_read(new_inst.get_instance_read().await)
                                .local_id()
                    {
                        None
                    } else {
                        Some(new_inst.clone())
                    }
                }
            };

            if let Some(ninst) = new_inst {
                self.conflicts[*self.id].insert(c.key().clone(), ninst);
            }
        }
    }
}
