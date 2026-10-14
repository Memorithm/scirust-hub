use std::fs;
use std::path::{Path, PathBuf};

use hub_executor::gvisor::{GvisorDockerExecutor, GvisorExecutionRequestV1};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() == 3 && (args[0] == "--inspect" || args[0] == "--reconcile") {
        let executor = GvisorDockerExecutor::new(PathBuf::from(&args[2]));
        let record = if args[0] == "--inspect" {
            executor.inspect_attempt(&args[1])
        } else {
            executor.reconcile_attempt(&args[1])
        }.map_err(|e| format!("gVisor controller metadata refused: {e}"))?;
        println!("{}", serde_json::to_string(&record)?);
        return Ok(());
    }
    if !(args.len() == 2 || args.len() == 4) {
        return Err(
            "usage: scirust-hub-gvisor REQUEST.json WORKSPACE_ROOT [QUALIFICATION.json RECEIPT.json]".into(),
        );
    }
    let request: GvisorExecutionRequestV1 =
        serde_json::from_slice(&fs::read(Path::new(&args[0]))?)?;
    let workspace_root = PathBuf::from(&args[1]);
    let executor = if args.len() == 4 {
        GvisorDockerExecutor::from_qualification_artifacts(
            workspace_root,
            Path::new(&args[2]),
            Path::new(&args[3]),
        )
        .map_err(|error| format!("gVisor qualification rejected: {error}"))?
    } else {
        GvisorDockerExecutor::new(workspace_root)
    };
    let receipt = executor
        .execute(&request)
        .map_err(|error| format!("gVisor execution failed: {error}"))?;
    println!("{}", serde_json::to_string(&receipt)?);
    if receipt.exit_code == Some(0) && !receipt.timed_out {
        Ok(())
    } else {
        Err("gVisor task did not complete successfully".into())
    }
}
