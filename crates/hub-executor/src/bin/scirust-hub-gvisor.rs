use std::fs;
use std::path::Path;

use hub_executor::gvisor::{GvisorDockerExecutor, GvisorExecutionRequestV1};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let request_path = std::env::args()
        .nth(1)
        .ok_or("usage: scirust-hub-gvisor REQUEST.json")?;
    let request: GvisorExecutionRequestV1 =
        serde_json::from_slice(&fs::read(Path::new(&request_path))?)?;
    let receipt = GvisorDockerExecutor::new()
        .execute(&request)
        .map_err(|error| format!("gVisor execution failed: {error}"))?;
    println!("{}", serde_json::to_string(&receipt)?);
    if receipt.exit_code == Some(0) && !receipt.timed_out {
        Ok(())
    } else {
        Err("gVisor task did not complete successfully".into())
    }
}
