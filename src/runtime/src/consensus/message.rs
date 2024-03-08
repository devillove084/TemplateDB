use std::fmt::Debug;

use serde::{de::DeserializeOwned, Serialize};

#[allow(dead_code)]
#[async_trait::async_trait]
pub(crate) trait MessageIndexTrait {
    /// This trait is used to decide to which worker some messages should be
    /// forwarded to, ensuring that messages with the same index are forwarded
    /// to the same process. If `None` is returned, then the message is sent to
    /// all workers. In particular, if the protocol is not parallel, the
    /// message is sent to the single protocol worker.
    ///
    /// There only 2 types of indexes are supported:
    /// - Some((reserved, index)): `index` will be used to compute working index making sure that
    ///   index is higher than `reserved`
    /// - None: no indexing; message will be sent to all workers
    async fn index(&self) -> Option<(usize, usize)>;
}

pub(crate) trait MessageTrait:
    Debug + Clone + PartialEq + Eq + Serialize + DeserializeOwned + Send + Sync + MessageIndexTrait
{
}

impl<T> MessageTrait for T where
    T: Debug
        + Clone
        + PartialEq
        + Eq
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + MessageIndexTrait
{
}

#[allow(dead_code)]
pub(crate) trait PeriodicTrait:
    Debug + Clone + Send + Sync + MessageIndexTrait + Eq
{
}

impl<T> PeriodicTrait for T where T: Debug + Clone + Send + Sync + MessageIndexTrait + Eq {}
