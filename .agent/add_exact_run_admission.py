from pathlib import Path


def replace_once(path: str, old: str, new: str, label: str) -> None:
    p = Path(path)
    s = p.read_text()
    count = s.count(old)
    if count != 1:
        raise SystemExit(f"{path}: {label}: expected one match, found {count}")
    p.write_text(s.replace(old, new, 1))

# Repository exact-version lookup.
replace_once(
    "crates/hub-core/src/store.rs",
    "use crate::id::{ArtifactId, ComponentId, RunId};\n",
    "use crate::id::{ArtifactId, ComponentId, RunId};\nuse crate::version::Version;\n",
    "Version import",
)
replace_once(
    "crates/hub-core/src/store.rs",
    '''    fn latest(\n        &self,\n        id: &ComponentId,\n    ) -> Result<Option<crate::component::ComponentManifest>, CoreError>;\n\n    /// All manifests, deterministically ordered by `(id, version)`.\n''',
    '''    fn latest(\n        &self,\n        id: &ComponentId,\n    ) -> Result<Option<crate::component::ComponentManifest>, CoreError>;\n\n    /// Exact registered manifest for `(id, version)`, if present.\n    ///\n    /// # Errors\n    /// Backend failures only.\n    fn get(\n        &self,\n        id: &ComponentId,\n        version: &Version,\n    ) -> Result<Option<crate::component::ComponentManifest>, CoreError>;\n\n    /// All manifests, deterministically ordered by `(id, version)`.\n''',
    "exact get trait",
)
replace_once(
    "crates/hub-core/src/memory.rs",
    '''    fn list(&self) -> Result<Vec<ComponentManifest>, CoreError> {\n''',
    '''    fn get(\n        &self,\n        id: &ComponentId,\n        version: &Version,\n    ) -> Result<Option<ComponentManifest>, CoreError> {\n        let inner = self.0.lock().map_err(poison)?;\n        Ok(inner.manifests.get(&(*id, version.clone())).cloned())\n    }\n\n    fn list(&self) -> Result<Vec<ComponentManifest>, CoreError> {\n''',
    "in-memory exact get",
)
replace_once(
    "crates/hub-store-sqlite/src/lib.rs",
    '''    fn list(&self) -> Result<Vec<ComponentManifest>, CoreError> {\n''',
    '''    fn get(\n        &self,\n        id: &hub_core::ComponentId,\n        version: &hub_core::Version,\n    ) -> Result<Option<ComponentManifest>, CoreError> {\n        let conn = self.lock()?;\n        let json: Option<String> = conn\n            .query_row(\n                "SELECT manifest_json FROM components WHERE id = ?1 AND version = ?2",\n                rusqlite::params![id.to_string(), version.as_str()],\n                |row| row.get(0),\n            )\n            .optional()\n            .map_err(storage("loading exact component"))?;\n        json.map(|j| {\n            serde_json::from_str(&j).map_err(|e| {\n                CoreError::Storage(format!("stored manifest failed to deserialize: {e}"))\n            })\n        })\n        .transpose()\n    }\n\n    fn list(&self) -> Result<Vec<ComponentManifest>, CoreError> {\n''',
    "sqlite exact get",
)

# Public admission pin + durable manifest digest on run records.
replace_once(
    "crates/hub-core/src/run.rs",
    '''/// Complete provenance-bearing record of one run.\n#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]\npub struct RunRecord {\n''',
    '''/// Exact registry identity required when admitting a run.\n///\n/// Component id and capability name remain in [`RunSpec`]; this pin freezes\n/// the independently evolving versioned registry fields and the canonical\n/// Hub manifest digest that gives those fields their executable meaning.\n#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]\npub struct ComponentAdmissionPin {\n    pub component_version: Version,\n    pub manifest_digest: ContentDigest,\n    pub capability_contract_version: Version,\n}\n\n/// Complete provenance-bearing record of one run.\n#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]\npub struct RunRecord {\n''',
    "admission pin type",
)
replace_once(
    "crates/hub-core/src/run.rs",
    '''    pub component_name: String,\n    pub component_version: Version,\n    pub contract_version: Version,\n''',
    '''    pub component_name: String,\n    pub component_version: Version,\n    /// Canonical domain-separated digest of the exact registered manifest.\n    /// Legacy records deserialize with `None`; every newly submitted run is\n    /// populated by the orchestrator before it is persisted.\n    #[serde(default, skip_serializing_if = "Option::is_none")]\n    pub component_manifest_digest: Option<ContentDigest>,\n    pub contract_version: Version,\n''',
    "run record digest",
)
replace_once(
    "crates/hub-core/src/run.rs",
    '''            component_name,\n            component_version,\n            contract_version,\n''',
    '''            component_name,\n            component_version,\n            component_manifest_digest: None,\n            contract_version,\n''',
    "constructor digest default",
)
replace_once(
    "crates/hub-core/src/lib.rs",
    '''pub use run::{\n    InputBinding, InputProvenance, OutputRef, RunOutcome, RunRecord, RunSpec, RunState, Transition,\n};\n''',
    '''pub use run::{\n    ComponentAdmissionPin, InputBinding, InputProvenance, OutputRef, RunOutcome, RunRecord, RunSpec,\n    RunState, Transition,\n};\n''',
    "pin re-export",
)

# Orchestrator exact admission, execution, and reproduction.
replace_once(
    "crates/hub-core/src/orchestrator.rs",
    '''use crate::run::{OutputRef, RunOutcome, RunRecord, RunSpec, RunState};\n''',
    '''use crate::run::{\n    ComponentAdmissionPin, OutputRef, RunOutcome, RunRecord, RunSpec, RunState,\n};\n''',
    "orchestrator pin import",
)
replace_once(
    "crates/hub-core/src/orchestrator.rs",
    '''    pub fn submit_run(&self, spec: RunSpec) -> Result<RunRecord, CoreError> {\n        self.submit_run_internal(spec, None, None)\n    }\n\n    fn submit_run_internal(\n        &self,\n        spec: RunSpec,\n        reproduced_from: Option<RunId>,\n        required_component_version: Option<crate::Version>,\n    ) -> Result<RunRecord, CoreError> {\n        spec.validate(&self.limits)?;\n        let manifest = self\n            .components\n            .latest(&spec.component)?\n            .ok_or(CoreError::ComponentNotFound(spec.component))?;\n        if let Some(required_version) =\n            required_component_version.filter(|version| manifest.version != *version)\n        {\n            return Err(CoreError::Validation(format!(\n                "component {} evolved to {} since the original run (recorded {}); reproduction requires the same version",\n                manifest.id, manifest.version, required_version\n            )));\n        }\n        let capability: Capability = manifest\n''',
    '''    pub fn submit_run(&self, spec: RunSpec) -> Result<RunRecord, CoreError> {\n        self.submit_run_internal(spec, None, None)\n    }\n\n    /// Admits one run against an exact immutable registry identity.\n    ///\n    /// This is the run-level primitive used by versioned workflow admission:\n    /// the component version, canonical manifest digest and capability contract\n    /// version must all match the same registered manifest before a run record\n    /// is queued. Registration remains immutable under `(id, version)`.\n    ///\n    /// # Errors\n    /// The ordinary [`Self::submit_run`] errors plus [`CoreError::Validation`]\n    /// when any pin disagrees with the registered manifest.\n    pub fn submit_run_pinned(\n        &self,\n        spec: RunSpec,\n        pin: ComponentAdmissionPin,\n    ) -> Result<RunRecord, CoreError> {\n        self.submit_run_internal(spec, None, Some(pin))\n    }\n\n    fn submit_run_internal(\n        &self,\n        spec: RunSpec,\n        reproduced_from: Option<RunId>,\n        admission_pin: Option<ComponentAdmissionPin>,\n    ) -> Result<RunRecord, CoreError> {\n        spec.validate(&self.limits)?;\n        let manifest = if let Some(pin) = &admission_pin {\n            self.components\n                .get(&spec.component, &pin.component_version)?\n                .ok_or(CoreError::ComponentNotFound(spec.component))?\n        } else {\n            self.components\n                .latest(&spec.component)?\n                .ok_or(CoreError::ComponentNotFound(spec.component))?\n        };\n        let manifest_digest = manifest.content_digest()?;\n        if let Some(pin) = &admission_pin {\n            if manifest_digest != pin.manifest_digest {\n                return Err(CoreError::Validation(format!(\n                    "component {} version {} manifest digest {} does not match required {}",\n                    manifest.id, manifest.version, manifest_digest, pin.manifest_digest\n                )));\n            }\n        }\n        let capability: Capability = manifest\n''',
    "submit exact pin",
)
replace_once(
    "crates/hub-core/src/orchestrator.rs",
    '''            .clone();\n\n        if spec.capability.as_str() == crate::scicapsule::CAPABILITY {\n''',
    '''            .clone();\n        if let Some(pin) = &admission_pin {\n            if capability.contract_version != pin.capability_contract_version {\n                return Err(CoreError::Validation(format!(\n                    "capability {} contract version {} does not match required {}",\n                    capability.name, capability.contract_version, pin.capability_contract_version\n                )));\n            }\n        }\n\n        if spec.capability.as_str() == crate::scicapsule::CAPABILITY {\n''',
    "contract pin check",
)
replace_once(
    "crates/hub-core/src/orchestrator.rs",
    '''        record.reproduced_from = reproduced_from;\n        record.transition(RunState::Validated, now)?;\n''',
    '''        record.component_manifest_digest = Some(manifest_digest);\n        record.reproduced_from = reproduced_from;\n        record.transition(RunState::Validated, now)?;\n''',
    "persist manifest digest",
)
replace_once(
    "crates/hub-core/src/orchestrator.rs",
    '''        // Resolve the binding at execution time from the registered manifest.\n        let manifest = self\n            .components\n            .latest(&record.spec.component)?\n            .ok_or(CoreError::ComponentNotFound(record.spec.component))?;\n        if manifest.version != record.component_version {\n            return Err(CoreError::Validation(format!(\n                "component {} evolved to {} after run {} was queued (recorded {}); execution requires the recorded component version",\n                manifest.id, manifest.version, record.id, record.component_version\n            )));\n        }\n        let binding = manifest.execution.clone();\n''',
    '''        // Resolve the exact immutable manifest captured at submission, not\n        // whichever version is latest when execution eventually starts.\n        let manifest = self\n            .components\n            .get(&record.spec.component, &record.component_version)?\n            .ok_or(CoreError::ComponentNotFound(record.spec.component))?;\n        let manifest_digest = manifest.content_digest()?;\n        if let Some(expected_digest) = record.component_manifest_digest {\n            if manifest_digest != expected_digest {\n                return Err(CoreError::Validation(format!(\n                    "component {} version {} manifest digest changed from recorded {} to {}",\n                    manifest.id, manifest.version, expected_digest, manifest_digest\n                )));\n            }\n        } else {\n            // Backfill legacy records from the immutable exact `(id, version)`\n            // registry entry before starting execution.\n            record.component_manifest_digest = Some(manifest_digest);\n        }\n        let capability = manifest\n            .capability(&record.spec.capability)\n            .ok_or_else(|| CoreError::CapabilityNotDeclared {\n                component: record.spec.component,\n                capability: record.spec.capability.to_string(),\n            })?;\n        if capability.contract_version != record.contract_version {\n            return Err(CoreError::Validation(format!(\n                "capability {} contract version changed from recorded {} to {}",\n                capability.name, record.contract_version, capability.contract_version\n            )));\n        }\n        let binding = manifest.execution.clone();\n''',
    "exact execution manifest",
)
replace_once(
    "crates/hub-core/src/orchestrator.rs",
    '''        // The component must still exist at the same version so the spec's\n        // meaning cannot silently drift between the two executions.\n        let manifest = self\n            .components\n            .latest(&original.spec.component)?\n            .ok_or(CoreError::ComponentNotFound(original.spec.component))?;\n        if manifest.version != original.component_version {\n            return Err(CoreError::Validation(format!(\n                "component {} evolved to {} since the original run (recorded {}); \\\n                 reproduction requires the same version",\n                manifest.id, manifest.version, original.component_version\n            )));\n        }\n\n        // Input artifacts must still be resolvable.\n''',
    '''        // Reproduction resolves the exact immutable registry entry captured\n        // by the original run, even when a newer component version is now\n        // registered. Its canonical manifest digest and capability contract are\n        // rechecked before a new run is admitted.\n        let manifest = self\n            .components\n            .get(&original.spec.component, &original.component_version)?\n            .ok_or(CoreError::ComponentNotFound(original.spec.component))?;\n        let manifest_digest = manifest.content_digest()?;\n        if let Some(expected_digest) = original.component_manifest_digest {\n            if manifest_digest != expected_digest {\n                return Err(CoreError::Validation(format!(\n                    "component {} version {} manifest digest changed from recorded {} to {}",\n                    manifest.id, manifest.version, expected_digest, manifest_digest\n                )));\n            }\n        }\n        let capability = manifest\n            .capability(&original.spec.capability)\n            .ok_or_else(|| CoreError::CapabilityNotDeclared {\n                component: original.spec.component,\n                capability: original.spec.capability.to_string(),\n            })?;\n        if capability.contract_version != original.contract_version {\n            return Err(CoreError::Validation(format!(\n                "capability {} contract version changed from recorded {} to {}",\n                capability.name, original.contract_version, capability.contract_version\n            )));\n        }\n\n        // Input artifacts must still be resolvable.\n''',
    "reproduction exact manifest",
)
replace_once(
    "crates/hub-core/src/orchestrator.rs",
    '''        let reproduction = self.submit_run_internal(\n            original.spec.clone(),\n            Some(run_id),\n            Some(original.component_version.clone()),\n        )?;\n''',
    '''        let reproduction = self.submit_run_internal(\n            original.spec.clone(),\n            Some(run_id),\n            Some(ComponentAdmissionPin {\n                component_version: original.component_version.clone(),\n                manifest_digest,\n                capability_contract_version: original.contract_version.clone(),\n            }),\n        )?;\n''',
    "reproduction pinned admission",
)

# Integration regression covering latest-version drift and explicit pin failures.
Path("crates/hub-core/tests/exact_run_admission.rs").write_text(r'''use std::collections::BTreeMap;
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
    let root = std::env::temp_dir().join(format!("hub-exact-run-admission-{}", uuid::Uuid::new_v4()));
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
    Fixture { orch, components, root }
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

    let reproduced = f.orch.reproduce_run(finished.id).expect("reproduce exact v1");
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
    assert!(matches!(digest_error, Err(hub_core::CoreError::Validation(_))));

    let contract_error = f.orch.submit_run_pinned(
        spec(id),
        ComponentAdmissionPin {
            component_version: Version::parse("1.0.0").expect("version"),
            manifest_digest: v1_digest,
            capability_contract_version: Version::parse("9.9.9").expect("contract"),
        },
    );
    assert!(matches!(contract_error, Err(hub_core::CoreError::Validation(_))));
}
''')
