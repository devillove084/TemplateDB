use std::path::Path;

use anyhow::Result;
use sql_planner::DatabaseWrapper;
use sqlplannertest::planner_test_runner;

fn main() -> Result<()> {
    planner_test_runner(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("planner"),
        || async { Ok(DatabaseWrapper::new("../csv/**/*.csv")) },
    )?;
    Ok(())
}
