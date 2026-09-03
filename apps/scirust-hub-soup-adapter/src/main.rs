use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};

use clap::{Parser, Subcommand};

const SOUP_EXIT_SHIP: i32 = 0;
const SOUP_EXIT_RUNTIME_ERROR: i32 = 1;
const SOUP_EXIT_DONT_SHIP: i32 = 2;
const SOUP_EXIT_USAGE_ERROR: i32 = 3;

#[derive(Debug, Parser)]
#[command(
    name = "scirust-hub-soup-adapter",
    about = "Versioned process adapter between SciRust Hub and SOUP"
)]
struct Cli {
    /// SOUP executable to invoke. Hub manifests normally rely on `soup` in PATH.
    #[arg(long, default_value = "soup")]
    soup_bin: String,

    #[command(subcommand)]
    command: AdapterCommand,
}

#[derive(Debug, Subcommand)]
enum AdapterCommand {
    /// Replay pre-computed SOUP evidence and emit a SHIP / DON'T-SHIP verdict.
    ///
    /// SOUP deliberately uses exit code 2 for a valid DON'T-SHIP decision. The
    /// adapter maps both semantic verdict exits (0 and 2) to adapter success so
    /// Hub ingests the verdict artifact. Runtime and usage failures remain
    /// process failures.
    ShipOffline {
        #[arg(long)]
        evidence: PathBuf,
        #[arg(long)]
        verdict: PathBuf,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("scirust-hub-soup-adapter: {message}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<(), String> {
    match cli.command {
        AdapterCommand::ShipOffline { evidence, verdict } => {
            ensure_regular_input(&evidence, "evidence")?;
            ensure_output_parent(&verdict)?;

            let status = Command::new(&cli.soup_bin)
                .arg("ship")
                .arg("--evidence")
                .arg(&evidence)
                .arg("--output")
                .arg(&verdict)
                .stdin(Stdio::null())
                .status()
                .map_err(|error| format!("failed to start {:?}: {error}", cli.soup_bin))?;

            classify_ship_exit(status.code(), verdict.is_file())
        }
    }
}

fn ensure_regular_input(path: &Path, label: &str) -> Result<(), String> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| format!("{label} {:?} is not readable: {error}", path))?;
    if metadata.file_type().is_symlink() {
        return Err(format!("{label} {:?} must not be a symbolic link", path));
    }
    if !metadata.is_file() {
        return Err(format!("{label} {:?} must be a regular file", path));
    }
    Ok(())
}

fn ensure_output_parent(path: &Path) -> Result<(), String> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    if parent.as_os_str().is_empty() {
        return Ok(());
    }
    std::fs::create_dir_all(parent)
        .map_err(|error| format!("cannot create verdict parent {:?}: {error}", parent))
}

fn classify_ship_exit(code: Option<i32>, verdict_exists: bool) -> Result<(), String> {
    match code {
        Some(SOUP_EXIT_SHIP | SOUP_EXIT_DONT_SHIP) if verdict_exists => Ok(()),
        Some(SOUP_EXIT_SHIP | SOUP_EXIT_DONT_SHIP) => Err(
            "SOUP returned a semantic verdict exit but produced no verdict artifact".to_owned(),
        ),
        Some(SOUP_EXIT_RUNTIME_ERROR) => Err("SOUP reported a runtime error (exit 1)".to_owned()),
        Some(SOUP_EXIT_USAGE_ERROR) => Err("SOUP rejected the request (exit 3)".to_owned()),
        Some(other) => Err(format!("SOUP exited with unsupported status {other}")),
        None => Err("SOUP terminated without a process exit code".to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ship_and_dont_ship_are_both_successful_hub_outcomes_when_evidence_exists() {
        assert!(classify_ship_exit(Some(SOUP_EXIT_SHIP), true).is_ok());
        assert!(classify_ship_exit(Some(SOUP_EXIT_DONT_SHIP), true).is_ok());
    }

    #[test]
    fn semantic_exit_without_verdict_artifact_fails_closed() {
        assert!(classify_ship_exit(Some(SOUP_EXIT_SHIP), false).is_err());
        assert!(classify_ship_exit(Some(SOUP_EXIT_DONT_SHIP), false).is_err());
    }

    #[test]
    fn soup_runtime_usage_and_unknown_exits_remain_failures() {
        for code in [SOUP_EXIT_RUNTIME_ERROR, SOUP_EXIT_USAGE_ERROR, 7] {
            assert!(classify_ship_exit(Some(code), true).is_err());
        }
        assert!(classify_ship_exit(None, true).is_err());
    }

    #[test]
    fn shipped_component_manifest_is_valid_hub_v1() {
        let raw = include_str!("../../../examples/soup-ship-component.json");
        let manifest: hub_core::component::ComponentManifest =
            serde_json::from_str(raw).expect("SOUP component manifest must deserialize");
        manifest
            .validate()
            .expect("SOUP component manifest must satisfy Hub validation");
    }
}
