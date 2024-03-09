use std::{fmt::Debug, sync::Arc};

use futures::{stream, StreamExt};
use log::trace;
use serde::{de::DeserializeOwned, Serialize};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Mutex, RwLock},
    task::JoinHandle,
};

use super::{
    config::Configure,
    error::RpcError,
    message::{Message, Propose},
    replica::Replica,
    types::{
        Ballot, Command, CommandExecutor, CommandLeaderID, InstanceSpace, LeaderBook, ReplicaID,
        VecInstanceSpace,
    },
    util::{send_message_arc, send_message_arc2},
};
use crate::consensus::epaxos::{
    message::{Accept, AcceptReply, Commit, PreAccept, PreAcceptOk, PreAcceptReply},
    types::{Instance, InstanceID, InstanceStatus, SharedInstance},
    util::{instance_exist, recv_message},
};

pub(crate) struct RpcServer<C, E, S>
where
    C: Command + Clone + Send + Sync + 'static,
    E: CommandExecutor<C> + Clone,
    S: InstanceSpace<C>,
{
    server: Arc<InnerServer<C, E, S>>,
    listener: TcpListener,
}

impl<C, E, S> RpcServer<C, E, S>
where
    C: Command + Debug + Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
    E: CommandExecutor<C> + Debug + Clone + Send + Sync + 'static,
    S: InstanceSpace<C> + Send + Sync + 'static,
{
    pub(crate) async fn new(conf: &Configure, server: Arc<InnerServer<C, E, S>>) -> Self {
        let listener = TcpListener::bind(conf.peer.get(conf.index).unwrap())
            .await
            .map_err(|e| panic!("bind server address error, {e}"))
            .unwrap();
        Self { server, listener }
    }

    pub(crate) async fn serve(&self) -> Result<(), RpcError> {
        loop {
            let (mut stream, _) = self.listener.accept().await?;
            let server = self.server.clone();
            tokio::spawn(async move {
                trace!("Got a connection");
                loop {
                    let message = recv_message(&mut stream).await;
                    server.handle_message(message).await;
                }
            });
        }
    }
}

pub(crate) struct InnerServer<C, E, S>
where
    C: Command + Clone + Send + Sync + 'static,
    E: CommandExecutor<C> + Clone,
    S: InstanceSpace<C>,
{
    conf: Configure,
    conns: RwLock<Vec<Option<Arc<Mutex<TcpStream>>>>>,
    replica: Arc<Mutex<Replica<C, E, S>>>,
}

impl<C, E, S> InnerServer<C, E, S>
where
    C: Command + Clone + Send + Sync + Serialize + 'static + Debug,
    E: CommandExecutor<C> + Clone + Send + Sync + 'static + Debug,
    S: InstanceSpace<C> + Send + Sync + 'static,
{
    pub(crate) async fn new(conf: Configure, cmd_exe: E) -> Self {
        let peer_cnt = conf.peer_cnt;
        let id = conf.index;
        Self {
            conf,
            conns: RwLock::new(vec![]),
            replica: Arc::new(Mutex::new(Replica::new(id, peer_cnt, cmd_exe))),
        }
    }

    pub(crate) fn conf(&self) -> &Configure {
        &self.conf
    }

    pub(crate) async fn new_leaderbook(&self) -> LeaderBook {
        LeaderBook::new(self.replica.lock().await.id, &self.conf)
    }

    pub(crate) async fn new_ballot(&self) -> Ballot {
        Ballot::new(self.replica.lock().await.id, &self.conf)
    }

    pub(crate) async fn handle_message(&self, message: Message<C>)
    where
        C: Command + Serialize,
    {
        match message {
            Message::PreAccept(preaccept) => self.handle_preaccept(preaccept).await,
            Message::PreAcceptReply(preacceptreply) => {
                self.handle_preaccept_reply(preacceptreply).await;
            }
            Message::PreAcceptOk(preacceptok) => self.handle_preaccept_ok(preacceptok).await,
            Message::Accept(accept) => self.handle_accept(accept).await,
            Message::AcceptReply(accept_reply) => self.handle_accept_reply(accept_reply).await,
            Message::Commit(commit) => self.handle_commit(commit).await,
            Message::CommitShort(_) => todo!(),
            Message::Propose(propose) => self.handle_propose(propose).await,
        }
    }

    async fn broadcast_message(&self, replica: ReplicaID, message: Message<C>) {
        let mut conn = self.conns.read().await;
        if conn.is_empty() {
            drop(conn);
            self.init_connections(&replica).await;
            conn = self.conns.read().await;
        }

        let cnt = self.conf.peer.len();
        let message = Arc::new(message);
        let tasks: Vec<JoinHandle<()>> = conn
            .iter()
            .filter(|c| c.is_some())
            .map(|c| {
                let c = c.as_ref().unwrap().clone();
                let message = message.clone();
                tokio::spawn(async move {
                    send_message_arc2(&c, &message).await;
                })
            })
            .collect();

        let stream_of_futures = stream::iter(tasks);
        let mut buffered = stream_of_futures.buffer_unordered(self.conf.peer.len());

        for _ in 0..cnt {
            buffered.next().await;
        }
    }

    async fn reply(
        &self,
        replica: ReplicaID,
        command_leader: &CommandLeaderID,
        message: Message<C>,
    ) {
        let mut all_connect = self.conns.read().await;
        if all_connect.is_empty() {
            drop(all_connect);
            self.init_connections(&replica).await;
            all_connect = self.conns.read().await;
        }

        assert!(
            **command_leader < all_connect.len(),
            "all connection number is {}, but the leader id is {}",
            all_connect.len(),
            **command_leader
        );

        let conn = all_connect.get(**command_leader).unwrap();
        assert!(!conn.is_none(), "should not reply to self");

        send_message_arc(conn.as_ref().unwrap(), &message).await;
    }

    async fn init_connections(&self, current_replica: &ReplicaID) {
        let mut conn_write = self.conns.write().await;
        if conn_write.is_empty() {
            for (id, p) in self.conf.peer.iter().enumerate() {
                let stream = TcpStream::connect(p)
                    .await
                    .map_err(|e| panic!("connect to {p} failed, {e}"))
                    .unwrap();
                conn_write.push(if id == **current_replica {
                    None
                } else {
                    Some(Arc::new(Mutex::new(stream)))
                });
            }
        }
    }

    async fn handle_propose(&self, propose: Propose<C>) {
        trace!("handle propose");
        let mut replica = self.replica.lock().await;
        let instance_id = *replica.local_cur_instance();
        replica.inc_local_cur_instance();
        let (seq, deps) = replica.get_seq_deps(&propose.cmds).await;

        let new_instance = SharedInstance::new(
            Some(Instance {
                id: InstanceID {
                    local: instance_id,
                    replica: replica.id,
                },
                seq,
                ballot: self.new_ballot().await,
                cmds: propose.cmds,
                deps,
                status: InstanceStatus::PreAccepted,
                lb: self.new_leaderbook().await,
            }),
            None,
        );

        let new_instance_read = new_instance.get_instance_read().await;
        let new_instance_read_inner = new_instance_read.as_ref().unwrap();
        let replica_id = replica.id;
        replica
            .update_conflicts(
                &replica_id,
                &new_instance_read_inner.cmds,
                new_instance.clone(),
            )
            .await;

        let replica_id = replica.id; // rebind
        replica
            .instance_space
            .insert_instance(&replica_id, &instance_id, new_instance.clone())
            .await;

        if seq > replica.max_seq {
            replica.max_seq = (*seq + 1).into();
        }

        // TODO: Flush the content to disk
        self.broadcast_message(
            replica.id,
            Message::PreAccept(PreAccept {
                command_leader_id: replica.id.into(),
                instance_id: InstanceID {
                    replica: replica.id,
                    local: instance_id,
                },
                seq,
                ballot: self.new_ballot().await,
                cmds: new_instance_read_inner.cmds.clone(),
                deps: new_instance_read_inner.deps.clone(),
            }),
        )
        .await;
    }

    async fn handle_preaccept(&self, preaccept: PreAccept<C>) {
        trace!("handle preaccept {:?}", preaccept);
        let mut replica = self.replica.lock().await;
        let instance = replica
            .instance_space
            .get_instance(&preaccept.instance_id.replica, &preaccept.instance_id.local)
            .await;

        if instance_exist(&instance).await {
            // TODO: abstract to a macro
            let instance = instance.unwrap();
            let instance_read = instance.get_instance_read().await;
            let instance_read_inner = instance_read.as_ref().unwrap();

            // We have got accept or commit before, do not reply
            if matches!(
                instance_read_inner.status,
                InstanceStatus::Committed | InstanceStatus::Accepted
            ) {
                // Later message may not contain commands, we should fill it here
                if instance_read_inner.cmds.is_empty() {
                    drop(instance_read);
                    // TODO: abstract to a macro
                    let mut instance_write = instance.get_instance_write().await;
                    let instance_write_inner = instance_write.as_mut().unwrap();
                    instance_write_inner.cmds = preaccept.cmds;
                }
                return;
            }

            // smaller ballot number
            if preaccept.ballot < instance_read_inner.ballot {
                self.reply(
                    replica.id,
                    &preaccept.command_leader_id,
                    Message::<C>::PreAcceptReply(PreAcceptReply {
                        instance_id: preaccept.instance_id,
                        seq: instance_read_inner.seq,
                        ballot: instance_read_inner.ballot,
                        ok: false,
                        deps: instance_read_inner.deps.clone(),
                        committed_deps: replica.commited_upto.clone(),
                    }),
                )
                .await;
                return;
            }
        }

        if preaccept.instance_id.local > replica.cur_instance(&preaccept.instance_id.replica) {
            replica.set_cur_instance(&preaccept.instance_id);
        }

        // TODO: We have better not copy dep vec.
        let (seq, deps, changed) = replica
            .update_seq_deps(preaccept.seq, preaccept.deps.clone(), &preaccept.cmds)
            .await;

        let status = if changed {
            InstanceStatus::PreAccepted
        } else {
            InstanceStatus::PreAcceptedEq
        };

        let uncommited_deps = replica
            .commited_upto
            .iter()
            .enumerate()
            .map(|cu| {
                if let Some(cu_id) = cu.1 {
                    // 1 -> localinstance id
                    if let Some(d) = deps[cu.0] {
                        if cu_id < &d {
                            return true;
                        }
                    }
                }
                false
            })
            .filter(|a| *a)
            .count()
            > 0;

        let new_instance = SharedInstance::new(
            Some(Instance {
                id: preaccept.instance_id,
                seq,
                ballot: preaccept.ballot,
                // TODO: cmds and deps should not copy
                cmds: preaccept.cmds.clone(),
                deps: deps.clone(),
                status,
                lb: self.new_leaderbook().await,
            }),
            None,
        );

        replica
            .instance_space
            .insert_instance(
                &preaccept.instance_id.replica,
                &preaccept.instance_id.local,
                new_instance.clone(),
            )
            .await;
        replica
            .update_conflicts(
                &preaccept.instance_id.replica,
                &preaccept.cmds,
                new_instance,
            )
            .await;

        // TODO: sync to disk

        // Send reply
        if changed
            || uncommited_deps
            || *preaccept.instance_id.replica != *preaccept.command_leader_id
            || !preaccept.ballot.is_init()
        {
            self.reply(
                replica.id,
                &preaccept.command_leader_id,
                Message::<C>::PreAcceptReply(PreAcceptReply {
                    instance_id: preaccept.instance_id,
                    seq,
                    ballot: preaccept.ballot,
                    ok: true,
                    deps,
                    // TODO: should not copy
                    committed_deps: replica.commited_upto.clone(),
                }),
            )
            .await;
        } else {
            trace!("reply preaccept ok");
            self.reply(
                replica.id,
                &preaccept.command_leader_id,
                Message::<C>::PreAcceptOk(PreAcceptOk {
                    instance_id: preaccept.instance_id,
                }),
            )
            .await;
        }
    }

    async fn handle_preaccept_reply(&self, preacceptreply: PreAcceptReply) {
        trace!("handle preaccept reply");
        let replica = self.replica.lock().await;

        let instance = replica
            .instance_space
            .get_instance(
                &preacceptreply.instance_id.replica,
                &preacceptreply.instance_id.local,
            )
            .await;

        // TODO: Error process
        assert!(
            (instance_exist(&instance).await),
            "this instance should already in the space"
        );

        // we have checked the existence
        let orig = instance.unwrap();
        let mut instance_w = orig.get_instance_write().await;
        let instance_w_inner = instance_w.as_mut().unwrap();

        if !matches!(instance_w_inner.status, InstanceStatus::PreAccepted) {
            // we have translated to the later states
            return;
        }

        if instance_w_inner.ballot != preacceptreply.ballot {
            // other advanced (large ballot) command leader is handling
            return;
        }

        if !preacceptreply.ok {
            instance_w_inner.lb.nack += 1;
            if preacceptreply.ballot > instance_w_inner.lb.max_ballot {
                instance_w_inner.lb.max_ballot = preacceptreply.ballot;
            }
            return;
        }
        instance_w_inner.lb.preaccept_ok += 1;

        let equal =
            replica.merge_seq_deps(instance_w_inner, &preacceptreply.seq, &preacceptreply.deps);
        if instance_w_inner.lb.preaccept_ok > 1 {
            instance_w_inner.lb.all_equal = instance_w_inner.lb.all_equal && equal;
        }

        if instance_w_inner.lb.preaccept_ok >= replica.peer_cnt / 2
            && instance_w_inner.lb.all_equal
            && instance_w_inner.ballot.is_init()
        {
            instance_w_inner.status = InstanceStatus::Committed;
            // TODO: sync to disk
            self.broadcast_message(
                replica.id,
                Message::<C>::Commit(Commit {
                    command_leader_id: replica.id.into(),
                    instance_id: preacceptreply.instance_id,
                    seq: instance_w_inner.seq,
                    cmds: instance_w_inner.cmds.clone(),
                    deps: instance_w_inner.deps.clone(),
                }),
            )
            .await;
            drop(instance_w);
            let _ = replica.exec_send.send(orig.clone()).await;
            orig.notify_commit().await;
        } else if instance_w_inner.lb.preaccept_ok >= replica.peer_cnt / 2 {
            instance_w_inner.status = InstanceStatus::Accepted;
            self.broadcast_message(
                replica.id,
                Message::<C>::Accept(Accept {
                    leader_id: replica.id.into(),
                    instance_id: preacceptreply.instance_id,
                    ballot: instance_w_inner.ballot,
                    seq: instance_w_inner.seq,
                    cmd_cnt: instance_w_inner.cmds.len(),
                    deps: instance_w_inner.deps.clone(),
                }),
            )
            .await;
        }
    }

    async fn handle_preaccept_ok(&self, preaccept_ok: PreAcceptOk) {
        trace!("handle preaccpet ok");
        let replica = self.replica.lock().await;

        let instance = replica
            .instance_space
            .get_instance(
                &preaccept_ok.instance_id.replica,
                &preaccept_ok.instance_id.local,
            )
            .await;

        assert!(
            (instance_exist(&instance).await),
            "This instance should already in the space"
        );

        let instance = instance.unwrap();
        let mut instance_write = instance.get_instance_write().await;
        let instance_write_inner = instance_write.as_mut().unwrap();

        if !matches!(instance_write_inner.status, InstanceStatus::PreAccepted) {
            // We have translated to the later states
            return;
        }

        if !instance_write_inner.ballot.is_init() {
            // only the first leader can send ok
            return;
        }

        instance_write_inner.lb.preaccept_ok += 1;

        // TODO: remove duplicate code
        if instance_write_inner.lb.preaccept_ok >= replica.peer_cnt / 2
            && instance_write_inner.lb.all_equal
            && instance_write_inner.ballot.is_init()
        {
            instance_write_inner.status = InstanceStatus::Committed;
            // TODO: sync to disk
            self.broadcast_message(
                replica.id,
                Message::<C>::Commit(Commit {
                    command_leader_id: replica.id.into(),
                    instance_id: preaccept_ok.instance_id,
                    seq: instance_write_inner.seq,
                    cmds: instance_write_inner.cmds.clone(),
                    deps: instance_write_inner.deps.clone(),
                }),
            )
            .await;
            drop(instance_write);
            let _ = replica.exec_send.send(instance.clone()).await;
            instance.notify_commit().await;
        } else if instance_write_inner.lb.preaccept_ok >= replica.peer_cnt / 2 {
            instance_write_inner.status = InstanceStatus::Accepted;
            self.broadcast_message(
                replica.id,
                Message::<C>::Accept(Accept {
                    leader_id: replica.id.into(),
                    instance_id: preaccept_ok.instance_id,
                    ballot: instance_write_inner.ballot,
                    seq: instance_write_inner.seq,
                    cmd_cnt: instance_write_inner.cmds.len(),
                    deps: instance_write_inner.deps.clone(),
                }),
            )
            .await;
        }
    }

    async fn handle_accept(&self, accept: Accept) {
        trace!("handle accept");
        let mut replica = self.replica.lock().await;

        if accept.instance_id.local >= replica.cur_instance(&accept.instance_id.replica) {
            replica.set_cur_instance(&InstanceID::new(
                accept.instance_id.replica,
                (*accept.instance_id.local + 1).into(),
            ));
        }

        let instance = replica
            .instance_space
            .get_instance(&accept.instance_id.replica, &accept.instance_id.local)
            .await;

        let exist = instance_exist(&instance).await;
        if exist {
            let instance = instance.unwrap();
            let instance_read = instance.get_instance_read().await;
            let instance_read_inner = instance_read.as_ref().unwrap();
            if matches!(
                instance_read_inner.status,
                InstanceStatus::Committed | InstanceStatus::Executed
            ) {
                // We' ve tanslated to the later states
                return;
            }

            let instance_ballot = instance_read_inner.ballot;
            if accept.ballot < instance_ballot {
                self.reply(
                    replica.id,
                    &accept.leader_id,
                    Message::<C>::AcceptReply(AcceptReply {
                        instance_id: accept.instance_id,
                        ok: false,
                        ballot: instance_ballot,
                    }),
                )
                .await;

                return;
            }

            drop(instance_read);

            let mut instance_write = instance.get_instance_write().await;
            let instance_write_inner = instance_write.as_mut().unwrap();

            instance_write_inner.status = InstanceStatus::Accepted;
            instance_write_inner.seq = accept.seq;
            instance_write_inner.deps = accept.deps;
        } else {
            // FIXME: Message reordering?
            let new_instance = SharedInstance::new(
                Some(Instance {
                    id: accept.instance_id,
                    seq: accept.seq,
                    ballot: accept.ballot,
                    cmds: vec![],
                    deps: accept.deps,
                    status: InstanceStatus::Accepted,
                    lb: self.new_leaderbook().await,
                }),
                None,
            );
            replica
                .instance_space
                .insert_instance(
                    &accept.instance_id.replica,
                    &accept.instance_id.local,
                    new_instance,
                )
                .await;
        }

        // TODO: sync to disk

        self.reply(
            replica.id,
            &accept.leader_id,
            Message::<C>::AcceptReply(AcceptReply {
                instance_id: accept.instance_id,
                ok: true,
                ballot: accept.ballot,
            }),
        )
        .await;
    }

    async fn handle_accept_reply(&self, accept_reply: AcceptReply) {
        trace!("handle accept reply");

        let replica = self.replica.lock().await;

        let instance = replica
            .instance_space
            .get_instance(
                &accept_reply.instance_id.replica,
                &accept_reply.instance_id.local,
            )
            .await;

        // TODO: Error processing
        assert!(
            (instance_exist(&instance).await),
            "The instance {:?} should exist",
            accept_reply.instance_id
        );

        let instance = instance.unwrap();
        let mut instance_write = instance.get_instance_write().await;
        let instance_write_inner = instance_write.as_mut().unwrap();

        if !accept_reply.ok {
            instance_write_inner.lb.nack += 1;
            if accept_reply.ballot > instance_write_inner.lb.max_ballot {
                instance_write_inner.lb.max_ballot = accept_reply.ballot;
            }
            return;
        }

        instance_write_inner.lb.accept_ok += 1;

        if instance_write_inner.lb.accept_ok >= replica.peer_cnt / 2 {
            instance_write_inner.status = InstanceStatus::Committed;
            // TODO: sync to disk

            self.broadcast_message(
                replica.id,
                Message::<C>::Commit(Commit {
                    command_leader_id: replica.id.into(),
                    instance_id: accept_reply.instance_id,
                    seq: instance_write_inner.seq,
                    cmds: instance_write_inner.cmds.clone(),
                    deps: instance_write_inner.deps.clone(),
                }),
            )
            .await;

            drop(instance_write);
            // TODO: sync to disk
            let _ = replica.exec_send.send(instance.clone()).await;
            instance.notify_commit().await;
        }
    }

    async fn handle_commit(&self, commit: Commit<C>) {
        trace!("handle commit");

        let mut replica = self.replica.lock().await;

        if commit.instance_id.local >= replica.cur_instance(&commit.instance_id.replica) {
            replica.set_cur_instance(&InstanceID {
                replica: commit.instance_id.replica,
                local: (*commit.instance_id.local + 1).into(),
            });
        }

        let instance = replica
            .instance_space
            .get_instance(&commit.instance_id.replica, &commit.instance_id.local)
            .await;
        let exist = instance_exist(&instance).await;

        let instance = if exist {
            let instance = instance.unwrap();
            let mut instance_write = instance.get_instance_write().await;
            let instance_write_inner = instance_write.as_mut().unwrap();
            instance_write_inner.seq = commit.seq;
            instance_write_inner.deps = commit.deps;
            instance_write_inner.status = InstanceStatus::Committed;
            drop(instance_write);
            instance
        } else {
            let new_instance = SharedInstance::new(
                Some(Instance {
                    id: commit.instance_id,
                    seq: commit.seq,
                    ballot: self.new_ballot().await,
                    cmds: commit.cmds.clone(),
                    deps: commit.deps,
                    status: InstanceStatus::Committed,
                    lb: self.new_leaderbook().await,
                }),
                None,
            );
            replica
                .update_conflicts(
                    &commit.instance_id.replica,
                    &commit.cmds,
                    new_instance.clone(),
                )
                .await;
            new_instance
        };

        // TODO: sync to disk
        // TODO: handle errors

        let _ = replica.exec_send.send(instance.clone()).await;
        instance.notify_commit().await;
    }
}

pub(crate) struct Server<C, E, S>
where
    C: Command + Clone + Send + Sync + 'static,
    E: CommandExecutor<C> + Clone,
    S: InstanceSpace<C>,
{
    rpc_server: RpcServer<C, E, S>,
}

impl<C, E, S> Server<C, E, S>
where
    C: Command + Debug + Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
    E: CommandExecutor<C> + Debug + Clone + Send + Sync + 'static,
    S: InstanceSpace<C> + Send + Sync + 'static,
{
    pub async fn new(conf: Configure, cmd_exe: E) -> Self {
        let inner = Arc::new(InnerServer::new(conf, cmd_exe).await);
        let rpc_server = RpcServer::new(inner.conf(), inner.clone()).await;
        Self { rpc_server }
    }

    pub async fn run(&self) {
        let _ = self.rpc_server.serve().await;
    }
}

pub struct DefaultServer<C, E>
where
    C: Command + Clone + Send + Sync + 'static,
    E: CommandExecutor<C> + Clone,
{
    inner: Server<C, E, VecInstanceSpace<C>>,
}

impl<C, E> DefaultServer<C, E>
where
    C: Command + Debug + Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
    E: CommandExecutor<C> + Debug + Clone + Send + Sync + 'static,
{
    pub async fn new(conf: Configure, cmd_exe: E) -> Self {
        Self {
            inner: Server::new(conf, cmd_exe).await,
        }
    }

    pub async fn run(&self) {
        self.inner.run().await;
    }
}
