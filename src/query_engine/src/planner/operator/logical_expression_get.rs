use derive_new::new;

use super::LogicalOperatorBase;
use crate::planner::BoundExpression;
use crate::types::LogicalType;

/// LogicalExpressionGet represents a scan operation over a set of to-be-executed expressions
#[derive(new, Debug, Clone)]
pub struct LogicalExpressionGet {
    pub(crate) base: LogicalOperatorBase,
    pub(crate) table_idx: usize,
    /// The types of the expressions
    pub(crate) expr_types: Vec<LogicalType>,
    /// The set of expressions
    pub(crate) expressions: Vec<Vec<BoundExpression>>,
}
