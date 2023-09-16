use super::{entry::EntryOperator, sink::SinkOperator, state::OperatorState};

pub struct ReadOperator {
    read_from: EntryOperator,
    sink_to: SinkOperator,
    state: OperatorState,
}
