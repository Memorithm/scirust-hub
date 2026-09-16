use std::collections::BTreeMap;
use std::sync::Arc;

use hub_core::store::ComponentRepository as _;
use hub_core::{
    Capability, CapabilityName, ComponentAdmissionPin, ComponentId, ComponentKind,
    ComponentManifest, ComponentName, ExecutionBinding, ExecutionOutcome, ExecutionRequest,
    Executor, ExecutorFailure, FileSystemArtifactStore, InMemoryArtifactMeta, InMemoryComponents,
    InMemoryRuns, InMemoryWorkflows, Limits, Orchestrator, ProcessBinding, RunSpec, Version,
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
    let root =
        std::env::temp_dir().join(format!("hub-exact-run-admission-{}", uuid::Uuid::new_v4()));
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
        ComponentName::parse("exact-admission-component").expect("name"),
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

fn spec(component: ComponentId) -> RunSpec {
    RunSpec {
        component,
        capability: CapabilityName::parse("test.run").expect("capability"),
        parameters: BTreeMap::new(),
        inputs: Vec::new(),
        timeout_ms: 5_000,
    }
}

#[test]
fn queued_run_executes_exact_admitted_manifest_after_newer_registration() {
    let f = fixture();
    let id = ComponentId::generate();
    let v1 = manifest(id, "1.0.0", "1.0.0", "v1");
    let v1_digest = v1.content_digest().expect("digest");
    f.components.put(&v1).expect("register v1");

    let queued = f.orch.submit_run(spec(id)).expect("submit");
    assert_eq!(queued.component_version.as_str(), "1.0.0");
    assert_eq!(queued.component_manifest_digest, Some(v1_digest));

    f.components
        .put(&manifest(id, "2.0.0", "2.0.0", "v2"))
        .expect("register v2");

    let finished = f.orch.execute_run(queued.id).expect("execute pinned v1");
    assert_eq!(finished.component_version.as_str(), "1.0.0");
    assert_eq!(finished.component_manifest_digest, Some(v1_digest));
    assert!(finished.state.is_terminal());

    let reproduced = f
        .orch
        .reproduce_run(finished.id)
        .expect("reproduce exact v1");
    assert_eq!(reproduced.component_version.as_str(), "1.0.0");
    assert_eq!(reproduced.component_manifest_digest, Some(v1_digest));
}

#[test]
fn explicit_pin_rejects_wrong_digest_and_contract_and_ignores_latest() {
    let f = fixture();
    let id = ComponentId::generate();
    let v1 = manifest(id, "1.0.0", "1.0.0", "v1");
    let v1_digest = v1.content_digest().expect("digest");
    f.components.put(&v1).expect("register v1");
    f.components
        .put(&manifest(id, "2.0.0", "2.0.0", "v2"))
        .expect("register v2");

    let pinned = f
        .orch
        .submit_run_pinned(
            spec(id),
            ComponentAdmissionPin {
                component_version: Version::parse("1.0.0").expect("version"),
                manifest_digest: v1_digest,
                capability_contract_version: Version::parse("1.0.0").expect("contract"),
            },
        )
        .expect("exact v1 admission");
    assert_eq!(pinned.component_version.as_str(), "1.0.0");

    let wrong_digest = hub_core::ContentDigest::from_bytes([0xA5; 32]);
    let digest_error = f.orch.submit_run_pinned(
        spec(id),
        ComponentAdmissionPin {
            component_version: Version::parse("1.0.0").expect("version"),
            manifest_digest: wrong_digest,
            capability_contract_version: Version::parse("1.0.0").expect("contract"),
        },
    );
    assert!(matches!(
        digest_error,
        Err(hub_core::CoreError::Validation(_))
    ));

    let contract_error = f.orch.submit_run_pinned(
        spec(id),
        ComponentAdmissionPin {
            component_version: Version::parse("1.0.0").expect("version"),
            manifest_digest: v1_digest,
            capability_contract_version: Version::parse("9.9.9").expect("contract"),
        },
    );
    assert!(matches!(
        contract_error,
        Err(hub_core::CoreError::Validation(_))
    ));
}
