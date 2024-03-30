use derive_new::new;

use super::LogicalOperatorBase;
use crate::planner::BoundExpression;

#[derive(new, Debug, Clone)]
pub struct LogicalLimit {
    pub(crate) base: LogicalOperatorBase,
    pub(crate) limit_value: u64,
    pub(crate) offsert_value: u64,
    pub(crate) limit: Option<BoundExpression>,
    pub(crate) offset: Option<BoundExpression>,
}
