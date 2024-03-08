use std::collections::HashSet;

use super::id::ProcessId;
use crate::consensus::abstruct::ConsensusProtocol;

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Action<P: ConsensusProtocol + ?Sized> {
    ToSend {
        target: HashSet<ProcessId>,
        msg: <P as ConsensusProtocol>::Message,
    },
    ToForward {
        msg: <P as ConsensusProtocol>::Message,
    },
}
