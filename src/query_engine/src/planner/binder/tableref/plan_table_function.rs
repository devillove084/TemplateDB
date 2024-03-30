use super::BoundTableFunction;
use crate::planner::{BindError, Binder, LogicalOperator};

impl Binder {
    pub fn create_plan_for_table_function(
        &mut self,
        bound_ref: BoundTableFunction,
    ) -> Result<LogicalOperator, BindError> {
        Ok(bound_ref.get)
    }
}
