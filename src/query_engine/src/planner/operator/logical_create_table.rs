use derive_new::new;

use super::LogicalOperatorBase;
use crate::planner::BoundCreateTableInfo;

#[derive(new, Debug, Clone)]
pub struct LogicalCreateTable {
    #[new(default)]
    pub(crate) base: LogicalOperatorBase,
    pub(crate) info: BoundCreateTableInfo,
}
