use std::sync::Arc;

use super::{ComparisonFunc, ComparisonFunction};
use crate::function::FunctionError;
use crate::types::LogicalType;
use arrow::array::ArrayRef;
use arrow_ord::cmp::*;
use sqlparser::ast::BinaryOperator;

pub struct DefaultComparisonFunctions;

impl DefaultComparisonFunctions {
    fn default_gt_function(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef, FunctionError> {
        Ok(Arc::new(gt(left, right)?))
    }

    fn default_gt_eq_function(
        left: &ArrayRef,
        right: &ArrayRef,
    ) -> Result<ArrayRef, FunctionError> {
        Ok(Arc::new(gt_eq(left, right)?))
    }

    fn default_lt_function(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef, FunctionError> {
        Ok(Arc::new(lt(left, right)?))
    }

    fn default_lt_eq_function(
        left: &ArrayRef,
        right: &ArrayRef,
    ) -> Result<ArrayRef, FunctionError> {
        Ok(Arc::new(lt_eq(left, right)?))
    }

    fn default_eq_function(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef, FunctionError> {
        Ok(Arc::new(eq(left, right)?))
    }

    fn default_neq_function(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef, FunctionError> {
        Ok(Arc::new(neq(left, right)?))
    }

    fn get_comparison_function_internal(
        op: &BinaryOperator,
    ) -> Result<(&str, ComparisonFunc), FunctionError> {
        Ok(match op {
            BinaryOperator::Eq => ("eq", Self::default_eq_function),
            BinaryOperator::NotEq => ("neq", Self::default_neq_function),
            BinaryOperator::Lt => ("lt", Self::default_lt_function),
            BinaryOperator::LtEq => ("lt_eq", Self::default_lt_eq_function),
            BinaryOperator::Gt => ("gt", Self::default_gt_function),
            BinaryOperator::GtEq => ("gt_eq", Self::default_gt_eq_function),
            _ => {
                return Err(FunctionError::ComparisonError(format!(
                    "Unsupported comparison operator {:?}",
                    op
                )))
            }
        })
    }

    pub fn get_comparison_function(
        op: &BinaryOperator,
        comparison_type: &LogicalType,
    ) -> Result<ComparisonFunction, FunctionError> {
        if comparison_type == &LogicalType::Invalid {
            return Err(FunctionError::ComparisonError(
                "Invalid comparison type".to_string(),
            ));
        }
        let (name, func) = Self::get_comparison_function_internal(op)?;
        Ok(ComparisonFunction::new(
            name.to_string(),
            func,
            comparison_type.clone(),
        ))
    }
}
