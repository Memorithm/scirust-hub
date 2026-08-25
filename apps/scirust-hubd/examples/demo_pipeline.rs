//! Walking-skeleton demo, runnable without any daemon:
//!
//! ```text
//! cargo run -p scirust-hubd --example demo_pipeline
//! ```
//!
//! It wires the real in-process stack (in-memory registries + file-backed
//! blob store + process executor) and runs a two-stage deterministic
//! pipeline:
//!
//! ```text
//! input artifact ──▶ stage 1: demo.upper (tr via argv) ──▶ artifact
//!                ──▶ stage 2: demo.cat  (cat)           ──▶ artifact
//! ```
//!
//! The point is the architecture, not the science: every stage is a
//! registered component discovered by capability, executed under caps, with
//! full provenance printed at the end.

use std::collections::BTreeMap;
use std::sync::Arc;

use hub_core::capability::{Capability, CapabilityName, Port};
use hub_core::clock::SystemClock;
use hub_core::component::{ComponentKind, ComponentName, ExecutionBinding, ProcessBinding};
use hub_core::limits::Limits;
use hub_core::memory::{
    FileSystemArtifactStore, InMemoryArtifactMeta, InMemoryComponents, InMemoryRuns,
};
use hub_core::orchestrator::Orchestrator;
use hub_core::run::{InputBinding, RunSpec};
use hub_core::{ComponentId, ComponentManifest, Version};
use hub_executor::ProcessExecutor;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::temp_dir().join(format!("hub-demo-{}", std::process::id()));
    let orchestrator = Orchestrator::new(
        Arc::new(SystemClock),
        Arc::new(InMemoryComponents::default()),
        Arc::new(InMemoryRuns::default()),
        Arc::new(InMemoryArtifactMeta::default()),
        Arc::new(hub_core::InMemoryWorkflows::default()),
        FileSystemArtifactStore::open(data_dir.join("blobs"))?,
        Arc::new(ProcessExecutor::new()),
        Limits::default(),
        data_dir.join("workdirs"),
    );

    // Stage components: fixed argv bindings, no shell anywhere.
    let emit = manifest(
        "demo-emit",
        "demo.emit",
        Vec::new(),
        ProcessBinding {
            program: resolve("echo"),
            // Parameters arrive as one literal argv entry via substitution;
            // no shell interpolation ever happens.
            args: vec!["{params}".into()],
            working_dir: None,
            outputs: Vec::new(),
        },
    );
    let upper = upper_component();
    orchestrator.register_component(emit.clone())?;
    orchestrator.register_component(upper.clone())?;

    let found = orchestrator.discover_by_capability(&CapabilityName::parse("demo.upper")?)?;
    println!("[discover] {} component(s) offer demo.upper", found.len());

    // Stage 1: produce a constant output artifact.
    let spec1 = RunSpec {
        component: emit.id,
        capability: CapabilityName::parse("demo.emit")?,
        parameters: BTreeMap::from([("text".into(), serde_json::json!("scirust-hub"))]),
        inputs: vec![],
        timeout_ms: 10_000,
    };
    let submitted1 = orchestrator.submit_run(spec1)?;
    let done1 = orchestrator.execute_run(submitted1.id)?;
    report("stage 1", &done1)?;
    let seed = done1
        .outcome
        .as_ref()
        .and_then(|o| o.outputs.first())
        .ok_or("stage 1 produced no output artifact")?
        .artifact;

    // Stage 2: consume stage 1's artifact by id.
    let spec2 = RunSpec {
        component: upper.id,
        capability: CapabilityName::parse("demo.upper")?,
        parameters: BTreeMap::new(),
        inputs: vec![InputBinding {
            name: "source".into(),
            artifact: seed,
        }],
        timeout_ms: 10_000,
    };
    let submitted2 = orchestrator.submit_run(spec2)?;
    let done2 = orchestrator.execute_run(submitted2.id)?;

    report("stage 2", &done2)?;
    if let Some(outcome) = &done2.outcome {
        for out in &outcome.outputs {
            let (_meta, bytes) = orchestrator.artifact_bytes(&out.artifact)?;
            println!(
                "[final] pipeline output: {}",
                String::from_utf8_lossy(&bytes).trim_end()
            );
        }
    }

    // Provenance is complete enough to re-verify both stages offline.
    for record in orchestrator.runs() {
        println!(
            "[provenance] {} {} params={} backend={}",
            record.id,
            record.state,
            hex_digest(&record.spec.params_digest()?),
            record
                .outcome
                .as_ref()
                .map(|o| o.executor_backend.as_str())
                .unwrap_or("?"),
        );
    }

    let _ = std::fs::remove_dir_all(data_dir);
    Ok(())
}

fn manifest(
    name: &str,
    capability: &str,
    inputs: Vec<Port>,
    binding: ProcessBinding,
) -> ComponentManifest {
    ComponentManifest::new_v1(
        ComponentId::generate(),
        ComponentName::parse(name).expect("valid name"),
        Version::parse("0.1.0").expect("valid version"),
        ComponentKind::parse(ComponentKind::TOOL).expect("valid kind"),
        vec![Capability {
            name: CapabilityName::parse(capability).expect("valid capability"),
            contract_version: Version::parse("1.0.0").expect("valid contract version"),
            inputs,
            outputs: vec![Port {
                name: "stdout".into(),
                description: String::new(),
            }],
            properties: BTreeMap::from([("deterministic".into(), "true".into())]),
        }],
        Some(ExecutionBinding::Process(binding)),
        None,
        BTreeMap::new(),
    )
    .expect("valid manifest")
}

fn upper_component() -> ComponentManifest {
    manifest(
        "demo-upper",
        "demo.upper",
        vec![Port {
            name: "source".into(),
            description: String::new(),
        }],
        ProcessBinding {
            program: resolve("sed"),
            // Uppercase every line of the materialized input file; the
            // placeholder becomes one literal argv entry (file operand).
            args: vec![r"s/.*/\U&/".into(), "{input:source}".into()],
            working_dir: None,
            outputs: Vec::new(),
        },
    )
}

fn report(stage: &str, record: &hub_core::RunRecord) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "[{stage}] run {} -> {:?} ({} ms)",
        record.id,
        record.state,
        record.outcome.as_ref().map(|o| o.duration_ms).unwrap_or(0),
    );
    Ok(())
}

fn hex_digest(digest: &hub_core::ContentDigest) -> String {
    digest.to_hex()
}

fn resolve(tool: &str) -> String {
    let path = std::env::var("PATH").unwrap_or_else(|_| "/usr/bin:/bin".into());
    for dir in std::env::split_paths(&path) {
        let candidate = dir.join(tool);
        if candidate.is_file() {
            return candidate.display().to_string();
        }
    }
    panic!("demo requires `{tool}` on PATH");
}
