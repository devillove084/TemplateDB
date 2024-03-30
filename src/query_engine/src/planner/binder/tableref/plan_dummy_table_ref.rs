use super::BoundDummyTableRef;
use crate::planner::{BindError, Binder, LogicalDummyScan, LogicalOperator};

impl Binder {
    pub fn create_plan_for_dummy_table_ref(
        &mut self,
        bound_ref: BoundDummyTableRef,
    ) -> Result<LogicalOperator, BindError> {
        Ok(LogicalOperator::LogicalDummyScan(LogicalDummyScan::new(
            bound_ref.bind_index,
        )))
    }
}
