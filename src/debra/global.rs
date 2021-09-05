

use crate::debra::common::epoch::AtomicEpoch;
use crate::debra::common::thread::ThreadState;

use super::abandoned::AbandonedQueue;
use super::list::List;





pub(crate) static ABANDONED: AbandonedQueue = AbandonedQueue::new();
pub(crate) static EPOCH: AtomicEpoch = AtomicEpoch::new();
pub(crate) static THREADS: List<ThreadState> = List::new();
