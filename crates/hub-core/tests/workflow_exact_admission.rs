use std::collections::BTreeMap;
use std::sync::Arc;

use hub_core::store::ComponentRepository as _;
use hub_core::{
    Capability, CapabilityName, ComponentAdmissionPin, ComponentId, ComponentKind,
    ComponentManifest, ComponentName, ExecutionBinding, ExecutionOutcome, ExecutionRequest,
    Executor, ExecutorFailure, FileSystemArtifactStore, InMemoryArtifactMeta, InMemoryComponents,
    InMemoryRuns, InMemoryWorkflows, Limits, Orchestrator, ProcessBinding, Step, Version,
    WorkflowAdmissionPins, WorkflowSpec, WORKFLOW_ADMISSION_SCHEMA_VERSION,
    WORKFLOW_SCHEMA_VERSION,
};

#[derive(Default)]
struct SuccessExecutor;

impl Executor for SuccessExecutor {
    fn backend_id(&self) -> &str {
        "success"
    }

    fn execute(
        &self,
        _request: &ExecutionRequest,
        _cancel: &hub_core::CancelToken,
    ) -> Result<ExecutionOutcome, ExecutorFailure> {
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

struct Fixture {
    orch: Orchestrator,
    components: Arc<InMemoryComponents>,
    root: std::path::PathBuf,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn fixture() -> Fixture {
    let root = std::env::temp_dir().join(format!(
        "hub-workflow-exact-admission-{}",
        uuid::Uuid::new_v4()
    ));
    let components = Arc::new(InMemoryComponents::default());
    let orch = Orchestrator::new(
        Arc::new(hub_core::ManualClock::starting_at(1_000)),
        components.clone(),
        Arc::new(InMemoryRuns::default()),
        Arc::new(InMemoryArtifactMeta::default()),
        Arc::new(InMemoryWorkflows::default()),
        FileSystemArtifactStore::open(root.join("blobs")).expect("blobs"),
        Arc::new(SuccessExecutor),
        Limits::default(),
        root.join("runs"),
    );
    Fixture {
        orch,
        components,
        root,
    }
}

fn manifest(id: ComponentId, version: &str, contract: &str, program: &str) -> ComponentManifest {
    ComponentManifest::new_v1(
        id,
        ComponentName::parse("workflow-exact-component").expect("name"),
        Version::parse(version).expect("version"),
        ComponentKind::parse(ComponentKind::TOOL).expect("kind"),
        vec![Capability {
            name: CapabilityName::parse("test.run").expect("capability"),
            contract_version: Version::parse(contract).expect("contract"),
            inputs: Vec::new(),
            outputs: Vec::new(),
            properties: BTreeMap::new(),
        }],
        Some(ExecutionBinding::Process(ProcessBinding {
            program: program.into(),
            args: Vec::new(),
            working_dir: None,
            outputs: Vec::new(),
        })),
        None,
        BTreeMap::new(),
    )
    .expect("manifest")
}

fn workflow(id: ComponentId) -> WorkflowSpec {
    WorkflowSpec {
        schema_version: WORKFLOW_SCHEMA_VERSION,
        name: "exact-admission".into(),
        max_concurrency: 1,
        steps: vec![Step {
            key: "one".into(),
            component: id,
            capability: CapabilityName::parse("test.run").expect("capability"),
            parameters: BTreeMap::new(),
            inputs: BTreeMap::new(),
            timeout_ms: 5_000,
            after: Vec::new(),
            retry: None,
        }],
    }
}

fn pins(manifest: &ComponentManifest) -> WorkflowAdmissionPins {
    WorkflowAdmissionPins {
        schema_version: WORKFLOW_ADMISSION_SCHEMA_VERSION,
        steps: BTreeMap::from([(
            "one".into(),
            ComponentAdmissionPin {
                component_version: manifest.version.clone(),
                manifest_digest: manifest.content_digest().expect("digest"),
                capability_contract_version: manifest
                    .capability(&CapabilityName::parse("test.run").expect("capability"))
                    .expect("declared")
                    .contract_version
                    .clone(),
            },
        )]),
    }
}

#[test]
fn pinned_workflow_executes_recorded_version_after_registry_evolution() {
    let f = fixture();
    let id = ComponentId::generate();
    let v1 = manifest(id, "1.0.0", "1.0.0", "v1");
    f.components.put(&v1).expect("v1");
    let admitted = f
        .orch
        .submit_workflow_pinned(workflow(id), pins(&v1))
        .expect("admit");
    assert!(admitted.admission.is_some());

    f.components
        .put(&manifest(id, "2.0.0", "2.0.0", "v2"))
        .expect("v2");
    let finished = f.orch.execute_workflow(admitted.id).expect("execute");
    assert_eq!(finished.state, hub_core::WorkflowState::Succeeded);
    let run = f.orch.run(&finished.steps[0].run).expect("run");
    assert_eq!(run.component_version.as_str(), "1.0.0");
    assert_eq!(run.contract_version.as_str(), "1.0.0");
}

#[test]
fn pinned_workflow_rejects_wrong_digest_contract_and_incomplete_coverage() {
    let f = fixture();
    let id = ComponentId::generate();
    let v1 = manifest(id, "1.0.0", "1.0.0", "v1");
    f.components.put(&v1).expect("v1");

    let mut wrong_digest = pins(&v1);
    wrong_digest
        .steps
        .get_mut("one")
        .expect("pin")
        .manifest_digest = hub_core::ContentDigest::from_bytes([0x5A; 32]);
    assert!(f
        .orch
        .submit_workflow_pinned(workflow(id), wrong_digest)
        .is_err());

    let mut wrong_contract = pins(&v1);
    wrong_contract
        .steps
        .get_mut("one")
        .expect("pin")
        .capability_contract_version = Version::parse("9.9.9").expect("version");
    assert!(f
        .orch
        .submit_workflow_pinned(workflow(id), wrong_contract)
        .is_err());

    let incomplete = WorkflowAdmissionPins {
        schema_version: WORKFLOW_ADMISSION_SCHEMA_VERSION,
        steps: BTreeMap::new(),
    };
    assert!(f
        .orch
        .submit_workflow_pinned(workflow(id), incomplete)
        .is_err());
}

#[test]
fn legacy_unpinned_workflow_preserves_latest_at_attempt_semantics() {
    let f = fixture();
    let id = ComponentId::generate();
    f.components
        .put(&manifest(id, "1.0.0", "1.0.0", "v1"))
        .expect("v1");
    let admitted = f.orch.submit_workflow(workflow(id)).expect("legacy admit");
    assert!(admitted.admission.is_none());
    f.components
        .put(&manifest(id, "2.0.0", "2.0.0", "v2"))
        .expect("v2");
    let finished = f
        .orch
        .execute_workflow(admitted.id)
        .expect("legacy execute");
    let run = f.orch.run(&finished.steps[0].run).expect("run");
    assert_eq!(run.component_version.as_str(), "2.0.0");
    assert_eq!(run.contract_version.as_str(), "2.0.0");
}
