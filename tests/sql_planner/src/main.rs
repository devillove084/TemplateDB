use std::path::Path;

use anyhow::Result;
use sql_planner::DatabaseWrapper;
use sqlplannertest::planner_test_apply;

#[tokio::main]
async fn main() -> Result<()> {
    planner_test_apply(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("planner"),
        || async { Ok(DatabaseWrapper::new("tests/csv/**/*.csv")) },
    )
    .await?;
    Ok(())
}
