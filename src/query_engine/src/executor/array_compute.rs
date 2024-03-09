use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array};
use arrow::compute::kernels::numeric::{add, div, mul, sub};
use arrow::compute::{and_kleene, or_kleene};
use arrow::datatypes::DataType;
use arrow_ord::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use sqlparser::ast::BinaryOperator;

use super::ExecutorError;

/// Copied from datafusion binary.rs
macro_rules! compute_op {
    // invoke binary operator
    ($LEFT:expr, $RIGHT:expr, $OP:ident, $DT:ident) => {{
        let ll = $LEFT
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        let rr = $RIGHT
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        Ok(Arc::new($OP(&ll, &rr)?))
    }};
    // invoke unary operator
    ($OPERAND:expr, $OP:ident, $DT:ident) => {{
        let operand = $OPERAND
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        Ok(Arc::new($OP(&operand)?))
    }};
}

macro_rules! arithmetic_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
        match $LEFT.data_type() {
            DataType::Int32 => compute_op!($LEFT, $RIGHT, $OP, Int32Array),
            DataType::Int64 => compute_op!($LEFT, $RIGHT, $OP, Int64Array),
            DataType::Float64 => compute_op!($LEFT, $RIGHT, $OP, Float64Array),
            _ => todo!("unsupported data type"),
        }
    }};
}

macro_rules! boolean_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
        if *$LEFT.data_type() != DataType::Boolean || *$RIGHT.data_type() != DataType::Boolean {
            return Err(ExecutorError::InternalError(format!(
                "Cannot evaluate binary expression with types {:?} and {:?}, only Boolean supported",
                $LEFT.data_type(),
                $RIGHT.data_type()
            )));
        }

        let ll = $LEFT
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean_op failed to downcast array");
        let rr = $RIGHT
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean_op failed to downcast array");
        Ok(Arc::new($OP(&ll, &rr)?))
    }};
}

pub fn binary_op(
    left: &ArrayRef,
    right: &ArrayRef,
    op: &BinaryOperator,
) -> Result<ArrayRef, ExecutorError> {
    match op {
        BinaryOperator::Plus => arithmetic_op!(left, right, add),
        BinaryOperator::Minus => arithmetic_op!(left, right, sub),
        BinaryOperator::Multiply => arithmetic_op!(left, right, mul),
        BinaryOperator::Divide => arithmetic_op!(left, right, div),
        BinaryOperator::Gt => Ok(Arc::new(gt(left, right)?)),
        BinaryOperator::Lt => Ok(Arc::new(lt(left, right)?)),
        BinaryOperator::GtEq => Ok(Arc::new(gt_eq(left, right)?)),
        BinaryOperator::LtEq => Ok(Arc::new(lt_eq(left, right)?)),
        BinaryOperator::Eq => Ok(Arc::new(eq(left, right)?)),
        BinaryOperator::NotEq => Ok(Arc::new(neq(left, right)?)),
        BinaryOperator::And => boolean_op!(left, right, and_kleene),
        BinaryOperator::Or => boolean_op!(left, right, or_kleene),
        _ => todo!(),
    }
}
