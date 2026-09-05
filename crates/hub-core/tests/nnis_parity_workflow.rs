use std::path::PathBuf;
use std::sync::Arc;

use hub_core::{
    ComponentManifest, ExecutionOutcome, ExecutionRequest, Executor, ExecutorFailure,
    FileSystemArtifactStore, InMemoryArtifactMeta, InMemoryComponents, InMemoryRuns,
    InMemoryWorkflows, InputSource, Limits, Orchestrator, WorkflowSpec, WorkflowState,
};

const VALIDATOR_MANIFEST: &str =
    include_str!("../../../examples/nnis-parity-validation-component.json");
const VERIFY_MANIFEST: &str = include_str!("../../../examples/nnis-parity-verify-component.json");
const WORKFLOW_EXAMPLE: &str = include_str!("../../../examples/nnis-parity-verify-workflow.json");

const PARITY_BYTES: &[u8] = br#"{"kind":"nnis-nnml1-reference-parity-record-v1"}"#;
const VALIDATION_BYTES: &[u8] = br#"{"status":"validated","source":"test"}"#;
const DOSSIER_BYTES: &[u8] = b"sealed-dossier-fixture";

struct WiringExecutor;

impl WiringExecutor {
    fn arg_path(request: &ExecutionRequest, flag: &str) -> Result<PathBuf, ExecutorFailure> {
        let Some(index) = request.args.iter().position(|arg| arg == flag) else {
            return Err(ExecutorFailure::Backend {
                reason: format!("missing argument {flag}"),
            });
        };
        request
            .args
            .get(index + 1)
            .map(PathBuf::from)
            .ok_or_else(|| ExecutorFailure::Backend {
                reason: format!("missing value after {flag}"),
            })
    }

    fn require_bytes(
        request: &ExecutionRequest,
        flag: &str,
        expected: &[u8],
    ) -> Result<(), ExecutorFailure> {
        let path = Self::arg_path(request, flag)?;
        let actual = std::fs::read(&path).map_err(|error| ExecutorFailure::Backend {
            reason: format!("cannot read {} at {}: {error}", flag, path.display()),
        })?;
        if actual != expected {
            return Err(ExecutorFailure::Backend {
                reason: format!("unexpected bytes materialized for {flag}"),
            });
        }
        Ok(())
    }

    fn write_output(
        request: &ExecutionRequest,
        flag: &str,
        bytes: &[u8],
    ) -> Result<(), ExecutorFailure> {
        let path = Self::arg_path(request, flag)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| ExecutorFailure::Backend {
                reason: format!("cannot create {}: {error}", parent.display()),
            })?;
        }
        std::fs::write(&path, bytes).map_err(|error| ExecutorFailure::Backend {
            reason: format!("cannot write {}: {error}", path.display()),
        })
    }
}

impl Executor for WiringExecutor {
    fn backend_id(&self) -> &str {
        "nnis-workflow-wiring-test"
    }

    fn execute(
        &self,
        request: &ExecutionRequest,
        cancel: &hub_core::CancelToken,
    ) -> Result<ExecutionOutcome, ExecutorFailure> {
        if cancel.is_cancelled() {
            return Ok(ExecutionOutcome {
                exit_code: None,
                signal: None,
                timed_out: false,
                cancelled: true,
                start_error: None,
                duration_ms: 0,
                stdout: Vec::new(),
                stdout_truncated: false,
                stderr: Vec::new(),
                stderr_truncated: false,
            });
        }

        match request.program.as_str() {
            "/usr/bin/python3" => {
                Self::require_bytes(request, "--evidence", PARITY_BYTES)?;
                Self::write_output(request, "--result", VALIDATION_BYTES)?;
            }
            "/opt/scirust-hub/libexec/scirust-verify-nnis-parity" => {
                Self::require_bytes(request, "--parity-evidence", PARITY_BYTES)?;
                Self::require_bytes(request, "--validation", VALIDATION_BYTES)?;
                Self::write_output(request, "--output", DOSSIER_BYTES)?;
            }
            other => {
                return Err(ExecutorFailure::Backend {
                    reason: format!("unexpected program {other}"),
                });
            }
        }

        Ok(ExecutionOutcome {
            exit_code: Some(0),
            signal: None,
            timed_out: false,
            cancelled: false,
            start_error: None,
            duration_ms: 1,
            stdout: Vec::new(),
            stdout_truncated: false,
            stderr: Vec::new(),
            stderr_truncated: false,
        })
    }
}

#[test]
fn nnis_workflow_passes_declared_validation_artifact_to_verify_unchanged() {
    let validator: ComponentManifest =
        serde_json::from_str(VALIDATOR_MANIFEST).expect("parse validator manifest");
    let verifier: ComponentManifest =
        serde_json::from_str(VERIFY_MANIFEST).expect("parse Verify manifest");
    validator.validate().expect("validate producer manifest");
    verifier.validate().expect("validate Verify manifest");

    let envelope: serde_json::Value =
        serde_json::from_str(WORKFLOW_EXAMPLE).expect("parse workflow example envelope");
    assert_eq!(envelope["schema_version"], 1);
    let mut workflow: WorkflowSpec = serde_json::from_value(envelope["workflow"].clone())
        .expect("parse workflow specification");
    workflow.validate().expect("validate workflow structure");

    let verify_step = workflow
        .steps
        .iter()
        .find(|step| step.key == "verify")
        .expect("verify step");
    assert!(matches!(
        verify_step.inputs.get("validation"),
        Some(InputSource::FromStep { key, output })
            if key == "validate" && output == "file:validation"
    ));

    let root = std::env::temp_dir().join(format!(
        "hub-nnis-parity-workflow-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&root).expect("create temp root");
    let orch = Orchestrator::new(
        Arc::new(hub_core::ManualClock::starting_at(10_000)),
        Arc::new(InMemoryComponents::default()),
        Arc::new(InMemoryRuns::default()),
        Arc::new(InMemoryArtifactMeta::default()),
        Arc::new(InMemoryWorkflows::default()),
        FileSystemArtifactStore::open(root.join("blobs")).expect("open blob store"),
        Arc::new(WiringExecutor),
        Limits::default(),
        root.join("workdirs"),
    );
    orch.register_component(validator).expect("register validator");
    orch.register_component(verifier).expect("register verifier");

    let parity = orch
        .ingest_artifact(
            "nnml1-parity-evidence.json".into(),
            "application/vnd.nnis.nnml1.parity-evidence.v1+json".into(),
            PARITY_BYTES,
        )
        .expect("ingest parity evidence");

    for step in &mut workflow.steps {
        for source in step.inputs.values_mut() {
            if let InputSource::Artifact { artifact } = source {
                *artifact = parity.id;
            }
        }
    }

    let submitted = orch.submit_workflow(workflow).expect("submit workflow");
    let finished = orch
        .execute_workflow(submitted.id)
        .expect("execute workflow");
    assert_eq!(finished.state, WorkflowState::Succeeded);

    let validation_step = finished
        .steps
        .iter()
        .find(|step| step.key == "validate")
        .expect("validation step result");
    let validation_run = orch.run(&validation_step.run).expect("validation run");
    let validation_outcome = validation_run.outcome.expect("validation outcome");
    let validation_ref = validation_outcome
        .outputs
        .iter()
        .find(|output| output.name == "file:validation")
        .expect("declared validation artifact");
    let validation_artifact = validation_ref.artifact;
    let (_, validation_bytes) = orch
        .artifact_bytes(&validation_artifact)
        .expect("read validation artifact");
    assert_eq!(validation_bytes, VALIDATION_BYTES);

    let verify_step = finished
        .steps
        .iter()
        .find(|step| step.key == "verify")
        .expect("Verify step result");
    let verify_run = orch.run(&verify_step.run).expect("Verify run");
    let verify_outcome = verify_run.outcome.expect("Verify outcome");
    assert!(verify_outcome
        .inputs
        .iter()
        .any(|input| input.name == "validation" && input.artifact == validation_artifact));
    assert!(verify_outcome
        .inputs
        .iter()
        .any(|input| input.name == "parity_evidence" && input.artifact == parity.id));

    let dossier_ref = verify_outcome
        .outputs
        .iter()
        .find(|output| output.name == "file:dossier")
        .expect("declared dossier artifact");
    let (_, dossier_bytes) = orch
        .artifact_bytes(&dossier_ref.artifact)
        .expect("read dossier artifact");
    assert_eq!(dossier_bytes, DOSSIER_BYTES);

    let _ = std::fs::remove_dir_all(root);
}
