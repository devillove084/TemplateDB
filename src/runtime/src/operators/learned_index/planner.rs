use super::error::OperatorOnResult;

pub trait Planner {
    async fn plan(&self) -> OperatorOnResult<Plan>;
}

pub struct Plan {}
