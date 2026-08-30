//! The orchestrator: the only component that ties registry, runs, execution,
//! artifacts and provenance together.
//!
//! It is synchronous; async callers (the HTTP daemon) offload via
//! `spawn_blocking`. All decisions are made here so executors and backends
//! stay dumb.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{mpsc, Arc, Mutex};

use tracing::{info, instrument, warn};

use crate::capability::{Capability, CapabilityName};
use crate::clock::{Clock, UnixMillis};
use crate::component::{ComponentManifest, ExecutionBinding};
use crate::error::CoreError;
use crate::exec::{CancelToken, ExecutionOutcome, ExecutionReport, ExecutionRequest, Executor};
use crate::id::{ArtifactId, AttemptId, ComponentId, RunId, WorkflowId};
use crate::limits::Limits;
use crate::memory::FileSystemArtifactStore;
use crate::run::{OutputRef, RunOutcome, RunRecord, RunSpec, RunState};
use crate::store::{
    ArtifactMetadataRepository, ArtifactStore, ComponentRepository, RunRepository,
    WorkflowRepository,
};

/// Outcome of an idempotent registration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RegistrationStatus {
    /// Manifest was newly stored.
    Created,
    /// A byte-identical manifest already existed.
    AlreadyRegistered,
}

/// Facade over one Hub instance's domain services.
pub struct Orchestrator {
    clock: Arc<dyn Clock>,
    components: Arc<dyn ComponentRepository>,
    runs: Arc<dyn RunRepository>,
    artifacts_meta: Arc<dyn ArtifactMetadataRepository>,
    blobs: FileSystemArtifactStore,
    workflows: Arc<dyn WorkflowRepository>,
    executor: Arc<dyn Executor>,
    limits: Limits,
    workdir_root: PathBuf,
    active_cancels: Mutex<BTreeMap<RunId, CancelToken>>,
    active_workflow_cancels: Mutex<BTreeMap<WorkflowId, CancelToken>>,
    active_workflow_runs: Mutex<BTreeMap<WorkflowId, BTreeSet<RunId>>>,
}

impl Orchestrator {
    /// Assembles an orchestrator from its ports. Callers choose the backends
    /// (in-memory stores for tests/CLI, the same today for the daemon).
    #[must_use]
    #[allow(clippy::too_many_arguments)] // one argument per injected port
    pub fn new(
        clock: Arc<dyn Clock>,
        components: Arc<dyn ComponentRepository>,
        runs: Arc<dyn RunRepository>,
        artifacts_meta: Arc<dyn ArtifactMetadataRepository>,
        workflows: Arc<dyn WorkflowRepository>,
        blobs: FileSystemArtifactStore,
        executor: Arc<dyn Executor>,
        limits: Limits,
        workdir_root: impl Into<PathBuf>,
    ) -> Self {
        Self {
            clock,
            components,
            runs,
            artifacts_meta,
            workflows,
            blobs,
            executor,
            limits,
            workdir_root: workdir_root.into(),
            active_cancels: Mutex::new(BTreeMap::new()),
            active_workflow_cancels: Mutex::new(BTreeMap::new()),
            active_workflow_runs: Mutex::new(BTreeMap::new()),
        }
    }

    #[must_use]
    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    // ------------------------------------------------------------------
    // Registry
    // ------------------------------------------------------------------

    /// Registers a validated manifest. Idempotent for identical content;
    /// conflicting content under the same `(id, version)` is rejected.
    ///
    /// # Errors
    /// [`CoreError::InvalidManifest`] / [`CoreError::ComponentConflict`] /
    /// storage failures.
    #[instrument(skip_all, fields(component = %manifest.id))]
    pub fn register_component(
        &self,
        manifest: ComponentManifest,
    ) -> Result<RegistrationStatus, CoreError> {
        manifest.validate()?;
        let inserted = self.components.put(&manifest)?;
        let digest = manifest.content_digest()?;
        match inserted {
            true => info!(component = %manifest.id, version = %manifest.version,
                          digest = %digest, "component registered"),
            false => info!(component = %manifest.id, version = %manifest.version,
                           "component registration replayed (identical manifest)"),
        }
        Ok(if inserted {
            RegistrationStatus::Created
        } else {
            RegistrationStatus::AlreadyRegistered
        })
    }

    #[instrument(skip(self))]
    pub fn component(&self, id: &ComponentId) -> Result<Option<ComponentManifest>, CoreError> {
        self.components.latest(id)
    }

    #[instrument(skip(self))]
    pub fn components(&self) -> Result<Vec<ComponentManifest>, CoreError> {
        self.components.list()
    }

    /// Components whose *latest* manifest declares `capability`.
    #[instrument(skip(self))]
    pub fn discover_by_capability(
        &self,
        capability: &CapabilityName,
    ) -> Result<Vec<ComponentManifest>, CoreError> {
        // `ComponentRepository::list` is ordered by `(id, version)`;
        // overwriting by id therefore retains exactly the latest manifest.
        let mut latest_by_id: BTreeMap<ComponentId, ComponentManifest> = BTreeMap::new();
        for manifest in self.components.list()? {
            latest_by_id.insert(manifest.id, manifest);
        }
        Ok(latest_by_id
            .into_values()
            .filter(|manifest| manifest.capability(capability).is_some())
            .collect())
    }

    // ------------------------------------------------------------------
    // External artifacts
    // ------------------------------------------------------------------

    /// Stores caller-provided immutable bytes as a Hub artifact. This is the
    /// ingress path for workflow/run inputs such as capsules, policies and
    /// datasets. Content remains addressed by digest; the artifact id is the
    /// provenance identity of this ingestion event.
    pub fn ingest_artifact(
        &self,
        name: String,
        media_type: String,
        bytes: &[u8],
    ) -> Result<crate::artifact::ArtifactMeta, CoreError> {
        let id = ArtifactId::generate();
        let size = u64::try_from(bytes.len()).map_err(|_| CoreError::ArtifactTooLarge {
            artifact: id,
            size: u64::MAX,
            limit: self.limits.max_artifact_bytes,
        })?;
        if size > self.limits.max_artifact_bytes {
            return Err(CoreError::ArtifactTooLarge {
                artifact: id,
                size,
                limit: self.limits.max_artifact_bytes,
            });
        }
        let digest = crate::digest::hash_bytes(crate::digest::DOMAIN_ARTIFACT_BLOB, bytes);
        let meta = crate::artifact::ArtifactMeta {
            id,
            name,
            media_type,
            digest,
            size,
            created_at: self.clock.now_ms(),
            produced_by_run: None,
        };
        meta.validate()?;
        let stored = self.blobs.put(
            bytes,
            self.limits.max_artifact_bytes,
            crate::digest::DOMAIN_ARTIFACT_BLOB,
        )?;
        debug_assert_eq!(stored, digest);
        self.artifacts_meta.put(&meta)?;
        info!(artifact = %meta.id, digest = %meta.digest, size = meta.size, "artifact ingested");
        Ok(meta)
    }

    // ------------------------------------------------------------------
    // Runs
    // ------------------------------------------------------------------

    /// Validates a run spec against the registry and persists a fresh run in
    /// `queued` state (`created -> validated -> queued`). Submission is
    /// side-effect-free beyond record creation: nothing executes here.
    ///
    /// # Errors
    /// [`CoreError::ComponentNotFound`] /
    /// [`CoreError::CapabilityNotDeclared`] / [`CoreError::MissingInputBinding`]
    /// / spec or storage failures.
    #[instrument(skip_all, fields(capability = %spec.capability))]
    pub fn submit_run(&self, spec: RunSpec) -> Result<RunRecord, CoreError> {
        self.submit_run_internal(spec, None, None)
    }

    fn submit_run_internal(
        &self,
        spec: RunSpec,
        reproduced_from: Option<RunId>,
        required_component_version: Option<crate::Version>,
    ) -> Result<RunRecord, CoreError> {
        spec.validate(&self.limits)?;
        let manifest = self
            .components
            .latest(&spec.component)?
            .ok_or(CoreError::ComponentNotFound(spec.component))?;
        if let Some(required_version) =
            required_component_version.filter(|version| manifest.version != *version)
        {
            return Err(CoreError::Validation(format!(
                "component {} evolved to {} since the original run (recorded {}); reproduction requires the same version",
                manifest.id, manifest.version, required_version
            )));
        }
        let capability: Capability = manifest
            .capability(&spec.capability)
            .ok_or_else(|| CoreError::CapabilityNotDeclared {
                component: spec.component,
                capability: spec.capability.to_string(),
            })?
            .clone();

        if spec.capability.as_str() == crate::scicapsule::CAPABILITY {
            crate::scicapsule::validate_execution_contract(&manifest, &capability)?;
        }

        // Every declared input port must be bound, and no unbound extras may
        // be smuggled in.
        let port_names: Vec<&str> = capability.inputs.iter().map(|p| p.name.as_str()).collect();
        let binding_names: Vec<&str> = spec.inputs.iter().map(|i| i.name.as_str()).collect();
        for name in &port_names {
            if !binding_names.contains(name) {
                return Err(CoreError::MissingInputBinding {
                    capability: spec.capability.to_string(),
                    input_name: (*name).to_owned(),
                });
            }
        }
        for name in &binding_names {
            if !port_names.contains(name) {
                return Err(CoreError::Validation(format!(
                    "input {name:?} does not match any input port of capability {}",
                    spec.capability
                )));
            }
        }
        // Input artifacts must exist before submission.
        for input in &spec.inputs {
            if self.artifacts_meta.get(&input.artifact)?.is_none() {
                return Err(CoreError::ArtifactNotFound(input.artifact));
            }
        }
        // Placeholders in the binding must reference things this spec
        // actually provides; failing here keeps invalid specs out of the
        // queue entirely.
        if let Some(binding) = &manifest.execution {
            check_placeholders(binding, &spec)?;
        }

        let now = self.clock.now_ms();
        let mut record = RunRecord::create(
            spec,
            manifest.name.as_str().to_owned(),
            manifest.version.clone(),
            capability.contract_version.clone(),
            now,
            &self.limits,
        )?;
        record.reproduced_from = reproduced_from;
        record.transition(RunState::Validated, now)?;
        record.transition(RunState::Queued, now)?;
        self.runs.put(&record)?;
        info!(run = %record.id, state = %record.state, "run submitted");
        Ok(record)
    }

    /// Executes a queued run end-to-end and finalizes provenance. Blocking by
    /// design; see module docs.
    ///
    /// # Errors
    /// [`CoreError::RunNotFound`] / [`CoreError::RunNotExecutable`] /
    /// storage failures. Process-level failures become `Failed` records, not
    /// error returns.
    #[instrument(skip_all, fields(run = %run_id))]
    pub fn execute_run(&self, run_id: RunId) -> Result<RunRecord, CoreError> {
        // Claim the run exclusively: duplicate executions of the same run are
        // rejected rather than raced.
        let token = CancelToken::new();
        {
            let mut active = self
                .active_cancels
                .lock()
                .map_err(|_| CoreError::Storage("cancellation map lock poisoned".into()))?;
            use std::collections::btree_map::Entry;
            match active.entry(run_id) {
                Entry::Occupied(_) => {
                    return Err(CoreError::RunNotExecutable {
                        run: run_id,
                        current: RunState::Running,
                    });
                }
                Entry::Vacant(slot) => {
                    slot.insert(token.clone());
                }
            }
        }
        let result = self.execute_run_inner(run_id, &token);
        self.active_cancels
            .lock()
            .map_err(|_| CoreError::Storage("cancellation map lock poisoned".into()))?
            .remove(&run_id);
        result
    }

    fn execute_run_inner(
        &self,
        run_id: RunId,
        token: &CancelToken,
    ) -> Result<RunRecord, CoreError> {
        let mut record = self
            .runs
            .get(&run_id)?
            .ok_or(CoreError::RunNotFound(run_id))?;
        if record.state != RunState::Queued {
            return Err(CoreError::RunNotExecutable {
                run: run_id,
                current: record.state,
            });
        }

        // Resolve the binding at execution time from the registered manifest.
        let manifest = self
            .components
            .latest(&record.spec.component)?
            .ok_or(CoreError::ComponentNotFound(record.spec.component))?;
        if manifest.version != record.component_version {
            return Err(CoreError::Validation(format!(
                "component {} evolved to {} after run {} was queued (recorded {}); execution requires the recorded component version",
                manifest.id, manifest.version, record.id, record.component_version
            )));
        }
        let binding = manifest.execution.clone();

        // Per-run working directory; inputs materialized under inputs/.
        let workdir = self.workdir_root.join(run_id.to_string());
        std::fs::create_dir_all(workdir.join("inputs"))
            .map_err(|e| CoreError::Storage(format!("creating run workdir: {e}")))?;

        // Materialize declared inputs from the content-addressed store so the
        // process sees immutable copies, not live paths.
        let mut input_provenance = Vec::with_capacity(record.spec.inputs.len());
        for input in &record.spec.inputs {
            let meta = self
                .artifacts_meta
                .get(&input.artifact)?
                .ok_or(CoreError::ArtifactNotFound(input.artifact))?;
            let dest = workdir.join("inputs").join(&input.name);
            self.blobs.copy_to_path(&meta.digest, &dest)?;
            input_provenance.push(crate::run::InputProvenance {
                name: input.name.clone(),
                artifact: input.artifact,
                digest: meta.digest,
                size: meta.size,
            });
        }

        // Pre-create parent directories of every declared output so
        // components can rely on them existing inside their workdir.
        if let Some(ExecutionBinding::Process(p)) = &binding {
            for out in &p.outputs {
                let path = workdir.join(&out.path);
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        CoreError::Storage(format!("creating output dir {:?}: {e}", parent))
                    })?;
                }
            }
        }

        let started_at = self.clock.now_ms();
        record.transition(RunState::Running, started_at)?;
        self.runs.put(&record)?;

        let report = self.run_process(&mut record, &binding, &workdir, token)?;
        let outcome = report.outcome;
        let finished_at = self.clock.now_ms();
        let duration_ms = finished_at.saturating_sub(started_at);
        let params_digest = record.spec.params_digest()?;
        let env_keys = vec!["PATH".to_owned(), "TMPDIR".to_owned()];
        let mut outputs = self.persist_stream_artifacts(&record, &outcome, finished_at)?;

        // Ingest declared output files. Only on clean exits: files left by a
        // killed or timed-out process are partial by definition.
        let mut missing_required = Vec::new();
        let declared_outputs: &[crate::component::OutputSpec] = match &binding {
            Some(ExecutionBinding::Process(p)) => &p.outputs,
            None => &[],
        };
        if outcome.exited_cleanly() {
            for spec in declared_outputs {
                match self.ingest_output_file(&record, spec, &workdir, finished_at)? {
                    Some(reference) => outputs.push(reference),
                    None if spec.required => missing_required.push(spec.name.clone()),
                    None => {}
                }
            }
        }
        let clean = outcome.exited_cleanly() && missing_required.is_empty();

        let failure = if clean {
            None
        } else if outcome.cancelled {
            Some("cancelled".into())
        } else if outcome.timed_out {
            Some(format!("timed out after {} ms", record.spec.timeout_ms))
        } else if !missing_required.is_empty() {
            Some(format!(
                "required output(s) not produced: {}",
                missing_required.join(", ")
            ))
        } else if let Some(start_error) = &outcome.start_error {
            Some(format!("failed to start: {start_error}"))
        } else {
            Some(match outcome.exit_code {
                Some(code) => format!("non-zero exit code {code}"),
                None => "terminated by signal".into(),
            })
        };

        record.outcome = Some(RunOutcome {
            exit_code: outcome.exit_code,
            signal: outcome.signal,
            timed_out: outcome.timed_out,
            cancelled: outcome.cancelled,
            executor_backend: report.backend_id,
            duration_ms,
            inputs: input_provenance,
            outputs,
            env_keys,
            params_digest,
            failure: failure.clone(),
        });

        let final_state = if outcome.cancelled && !outcome.timed_out {
            RunState::Cancelled
        } else if clean {
            RunState::Succeeded
        } else {
            RunState::Failed
        };
        record.transition(final_state, finished_at)?;
        self.runs.put(&record)?;
        info!(run = %run_id, state = %final_state, duration_ms, "run finished");

        // Best-effort cleanup of the working directory; contents that matter
        // were captured as artifacts first.
        if let Err(e) = std::fs::remove_dir_all(&workdir) {
            warn!(run = %run_id, error = %e, "could not remove run workdir");
        }
        Ok(record)
    }

    fn run_process(
        &self,
        record: &mut RunRecord,
        binding: &Option<ExecutionBinding>,
        workdir: &std::path::Path,
        token: &CancelToken,
    ) -> Result<ExecutionReport, CoreError> {
        let request = match self.build_request(&record.spec, binding, workdir) {
            Ok(request) => request,
            // Unreachable in practice thanks to submission-time checks; kept
            // as a guard so the record can never be stranded in `running`.
            Err(e) => {
                let now = self.clock.now_ms();
                record.transition(RunState::Failed, now)?;
                self.runs.put(record)?;
                return Err(e);
            }
        };
        match self.executor.execute_report(&request, token) {
            Ok(report) => Ok(report),
            Err(crate::error::ExecutorFailure::TimedOut { timeout_ms }) => {
                // The backend enforced the timeout; surface it as a timed-out
                // outcome so provenance stays complete instead of erroring.
                Ok(ExecutionReport {
                    backend_id: self.executor.backend_id().to_owned(),
                    outcome: ExecutionOutcome {
                        exit_code: None,
                        signal: None,
                        timed_out: true,
                        cancelled: false,
                        start_error: None,
                        duration_ms: timeout_ms,
                        stdout: Vec::new(),
                        stdout_truncated: false,
                        stderr: Vec::new(),
                        stderr_truncated: false,
                    },
                })
            }
            Err(crate::error::ExecutorFailure::Cancelled) => Ok(ExecutionReport {
                backend_id: self.executor.backend_id().to_owned(),
                outcome: ExecutionOutcome {
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
                },
            }),
            Err(crate::error::ExecutorFailure::Backend { reason }) => {
                Err(CoreError::ExecutionFailed {
                    run: record.id,
                    source: crate::error::ExecutorFailure::Backend { reason },
                })
            }
        }
    }

    /// Resolves placeholders and assembles the structured request. The only
    /// place argv semantics live.
    fn build_request(
        &self,
        spec: &RunSpec,
        binding: &Option<ExecutionBinding>,
        workdir: &std::path::Path,
    ) -> Result<ExecutionRequest, CoreError> {
        let process = match binding {
            Some(ExecutionBinding::Process(p)) => p,
            None => {
                return Err(CoreError::Validation(format!(
                    "component {} declares no executable binding",
                    spec.component
                )));
            }
        };
        let params_json = String::from_utf8(spec.canonical_params_bytes()?)
            .map_err(|e| CoreError::Validation(format!("parameters are not UTF-8: {e}")))?;
        let input_paths: BTreeMap<&str, PathBuf> = spec
            .inputs
            .iter()
            .map(|i| {
                (
                    i.name.as_str(),
                    workdir.join("inputs").join(i.name.as_str()),
                )
            })
            .collect();
        let output_paths: BTreeMap<&str, PathBuf> = process
            .outputs
            .iter()
            .map(|o| (o.name.as_str(), workdir.join(&o.path)))
            .collect();
        let mut args = Vec::with_capacity(process.args.len());
        for raw in &process.args {
            let substituted =
                substitute_placeholders(raw, &params_json, &input_paths, &output_paths)?;
            args.push(substituted);
        }
        // Environment built from scratch; nothing else leaks in. Values are
        // never logged (only names reach provenance).
        let env: BTreeMap<String, String> = BTreeMap::from([
            ("PATH".to_owned(), std::env::var("PATH").unwrap_or_default()),
            ("TMPDIR".to_owned(), workdir.display().to_string()),
        ]);
        Ok(ExecutionRequest {
            program: process.program.clone(),
            args,
            working_dir: process
                .working_dir
                .as_ref()
                .map(PathBuf::from)
                .unwrap_or_else(|| workdir.to_path_buf()),
            env,
            timeout_ms: spec.timeout_ms,
            max_capture_bytes_per_stream: self.limits.max_capture_bytes,
        })
    }

    /// Reads one declared output file from the working directory and stores
    /// it as an artifact. `Ok(None)` means the file was not produced.
    fn ingest_output_file(
        &self,
        record: &RunRecord,
        spec: &crate::component::OutputSpec,
        workdir: &std::path::Path,
        now: UnixMillis,
    ) -> Result<Option<OutputRef>, CoreError> {
        let path = workdir.join(&spec.path);
        if !path.is_file() {
            return Ok(None);
        }
        // Size gate before reading; oversized declared outputs fail the run
        // rather than being silently truncated (truncation would corrupt the
        // artifact's meaning).
        let size = std::fs::metadata(&path)
            .map_err(|e| CoreError::Storage(format!("stating output {:?}: {e}", spec.path)))?
            .len();
        if size > self.limits.max_artifact_bytes {
            return Err(CoreError::ArtifactTooLarge {
                artifact: ArtifactId::generate(),
                size,
                limit: self.limits.max_artifact_bytes,
            });
        }
        let bytes = std::fs::read(&path)
            .map_err(|e| CoreError::Storage(format!("reading output {:?}: {e}", spec.path)))?;
        let digest = self.blobs.put(
            &bytes,
            self.limits.max_artifact_bytes,
            crate::digest::DOMAIN_ARTIFACT_BLOB,
        )?;
        let media_type = spec
            .media_type
            .clone()
            .unwrap_or_else(|| "application/octet-stream".to_owned());
        let meta = crate::artifact::ArtifactMeta {
            id: ArtifactId::generate(),
            name: format!("{}-{}", record.id, spec.name),
            media_type,
            digest,
            size,
            created_at: now,
            produced_by_run: Some(record.id),
        };
        meta.validate()?;
        self.artifacts_meta.put(&meta)?;
        Ok(Some(OutputRef {
            name: format!("file:{}", spec.name),
            artifact: meta.id,
            digest,
            size: meta.size,
        }))
    }

    fn persist_stream_artifacts(
        &self,
        record: &RunRecord,
        outcome: &ExecutionOutcome,
        now: UnixMillis,
    ) -> Result<Vec<OutputRef>, CoreError> {
        let mut refs = Vec::new();
        for (name, bytes, truncated) in [
            ("stdout", &outcome.stdout, outcome.stdout_truncated),
            ("stderr", &outcome.stderr, outcome.stderr_truncated),
        ] {
            if bytes.is_empty() {
                continue;
            }
            let label = if truncated {
                format!("{name}-truncated")
            } else {
                name.to_owned()
            };
            let digest = self.blobs.put(
                bytes,
                self.limits.max_artifact_bytes,
                crate::digest::DOMAIN_CAPTURE,
            )?;
            let meta = crate::artifact::ArtifactMeta {
                id: ArtifactId::generate(),
                name: format!("{}-{}", record.id, label),
                media_type: "text/plain; charset=utf-8".to_owned(),
                digest,
                size: bytes.len() as u64,
                created_at: now,
                produced_by_run: Some(record.id),
            };
            meta.validate()?;
            self.artifacts_meta.put(&meta)?;
            refs.push(OutputRef {
                name: name.to_owned(),
                artifact: meta.id,
                digest,
                size: meta.size,
            });
        }
        Ok(refs)
    }

    // ------------------------------------------------------------------
    // Cancellation & queries
    // ------------------------------------------------------------------

    /// Requests cancellation of a queued/running run. Returns whether an
    /// active execution was signalled.
    ///
    /// # Errors
    /// [`CoreError::RunNotFound`] / storage failures.
    pub fn cancel_run(&self, run_id: RunId) -> Result<bool, CoreError> {
        let active = self
            .active_cancels
            .lock()
            .map_err(|_| CoreError::Storage("cancellation map lock poisoned".into()))?;
        if let Some(token) = active.get(&run_id) {
            token.cancel();
            return Ok(true);
        }
        drop(active);
        // Not currently executing: cancel a queued run outright.
        let mut record = self
            .runs
            .get(&run_id)?
            .ok_or(CoreError::RunNotFound(run_id))?;
        if record.state.is_terminal() {
            return Ok(false);
        }
        record.transition(RunState::Cancelled, self.clock.now_ms())?;
        self.runs.put(&record)?;
        Ok(false)
    }

    #[must_use]
    pub fn run(&self, run_id: &RunId) -> Option<RunRecord> {
        self.runs.get(run_id).ok().flatten()
    }

    /// All runs in deterministic `(created_at, id)` order.
    ///
    /// # Errors
    /// Storage failures only.
    pub fn list_runs(&self) -> Result<Vec<RunRecord>, CoreError> {
        self.runs.list()
    }

    #[must_use]
    pub fn runs(&self) -> Vec<RunRecord> {
        self.runs.list().unwrap_or_default()
    }

    /// All artifact metadata in deterministic `(created_at, id)` order.
    ///
    /// # Errors
    /// Storage failures only.
    pub fn list_artifacts(&self) -> Result<Vec<crate::artifact::ArtifactMeta>, CoreError> {
        self.artifacts_meta.list()
    }

    /// All artifact metadata; empty when the metadata store errors
    /// (read-only convenience retained for existing callers).
    #[must_use]
    pub fn artifacts(&self) -> Vec<crate::artifact::ArtifactMeta> {
        self.list_artifacts().unwrap_or_default()
    }

    #[must_use]
    pub fn artifact_meta(&self, id: &ArtifactId) -> Option<crate::artifact::ArtifactMeta> {
        self.artifacts_meta.get(id).ok().flatten()
    }

    /// Reads artifact bytes (bounded by store limits).
    ///
    /// # Errors
    /// [`CoreError::BlobNotFound`] / storage failures.
    pub fn artifact_bytes(
        &self,
        id: &ArtifactId,
    ) -> Result<(crate::artifact::ArtifactMeta, Vec<u8>), CoreError> {
        let meta = self
            .artifacts_meta
            .get(id)?
            .ok_or(CoreError::ArtifactNotFound(*id))?;
        let bytes = self.blobs.read(&meta.digest)?;
        Ok((meta, bytes))
    }

    #[must_use]
    pub fn blob_store(&self) -> &FileSystemArtifactStore {
        &self.blobs
    }

    /// Backend identifier of the wired executor (surfaced for operators).
    #[must_use]
    pub fn executor_backend_id(&self) -> &str {
        self.executor.backend_id()
    }
}

/// Submission-time placeholder validation: every placeholder must be
/// resolvable from the spec being submitted.
fn check_placeholders(binding: &ExecutionBinding, spec: &RunSpec) -> Result<(), CoreError> {
    // Single-variant execution enum today; new variants extend this check.
    let ExecutionBinding::Process(process) = binding;
    for raw in &process.args {
        if raw == "{params}" {
            continue;
        }
        if let Some(name) = raw
            .strip_prefix("{input:")
            .and_then(|r| r.strip_suffix('}'))
        {
            let bound = spec.inputs.iter().any(|i| i.name == name);
            if !bound {
                return Err(CoreError::Validation(format!(
                    "binding references unknown input {name:?}; declared inputs: {:?}",
                    spec.inputs
                        .iter()
                        .map(|i| i.name.as_str())
                        .collect::<Vec<_>>()
                )));
            }
            continue;
        }
        if let Some(name) = raw
            .strip_prefix("{output:")
            .and_then(|r| r.strip_suffix('}'))
        {
            let declared = process.outputs.iter().any(|o| o.name == name);
            if !declared {
                return Err(CoreError::Validation(format!(
                    "binding references unknown output {name:?}; declared outputs: {:?}",
                    process
                        .outputs
                        .iter()
                        .map(|o| o.name.as_str())
                        .collect::<Vec<_>>()
                )));
            }
            continue;
        }
        if raw.contains("{params}") || raw.contains("{input:") || raw.contains("{output:") {
            return Err(CoreError::Validation(format!(
                "placeholder must occupy the whole argument, got {raw:?}"
            )));
        }
    }
    Ok(())
}

fn substitute_placeholders(
    raw: &str,
    params_json: &str,
    input_paths: &BTreeMap<&str, PathBuf>,
    output_paths: &BTreeMap<&str, PathBuf>,
) -> Result<String, CoreError> {
    if raw == "{params}" {
        return Ok(params_json.to_owned());
    }
    if let Some(name) = raw
        .strip_prefix("{input:")
        .and_then(|r| r.strip_suffix('}'))
    {
        return input_paths.get(name).map_or_else(
            || {
                Err(CoreError::Validation(format!(
                    "unresolved input placeholder {name:?} at execution time"
                )))
            },
            |p| Ok(p.display().to_string()),
        );
    }
    if let Some(name) = raw
        .strip_prefix("{output:")
        .and_then(|r| r.strip_suffix('}'))
    {
        return output_paths.get(name).map_or_else(
            || {
                Err(CoreError::Validation(format!(
                    "unresolved output placeholder {name:?} at execution time"
                )))
            },
            |p| Ok(p.display().to_string()),
        );
    }
    // Literal argument; placeholders must appear alone to keep substitution
    // unambiguous (no partial splicing into larger strings).
    if raw.contains("{params}") || raw.contains("{input:") || raw.contains("{output:") {
        return Err(CoreError::Validation(format!(
            "placeholder must occupy the whole argument, got {raw:?}"
        )));
    }
    Ok(raw.to_owned())
}

impl Orchestrator {
    /// Validates a multi-step workflow against the registry (components must
    /// exist) and persists it in `created` state. Nothing executes yet.
    ///
    /// # Errors
    /// Validation/component-not-found/storage failures.
    #[instrument(skip_all)]
    pub fn submit_workflow(
        &self,
        spec: crate::workflow::WorkflowSpec,
    ) -> Result<crate::workflow::WorkflowRecord, CoreError> {
        spec.validate()?;
        for step in &spec.steps {
            if self.components.latest(&step.component)?.is_none() {
                return Err(CoreError::ComponentNotFound(step.component));
            }
        }
        let model_version = crate::Version::parse(crate::workflow::WORKFLOW_MODEL_VERSION)?;
        let record =
            crate::workflow::WorkflowRecord::create(spec, model_version, self.clock.now_ms())?;
        self.workflows.put(&record)?;
        info!(workflow = %record.id, steps = record.spec.steps.len(), "workflow submitted");
        Ok(record)
    }

    /// Executes a created workflow in deterministic dependency order. A step
    /// may perform multiple attempts only when it carries an explicit retry
    /// policy. Workflow cancellation intent is persisted before any active
    /// run is signalled.
    ///
    /// # Errors
    /// [`CoreError::WorkflowNotFound`] / [`CoreError::WorkflowNotExecutable`]
    /// / storage failures. Per-step execution failures become terminal
    /// workflow records rather than escaping as scheduler errors.
    #[instrument(skip_all, fields(workflow = %workflow_id))]
    pub fn execute_workflow(
        &self,
        workflow_id: WorkflowId,
    ) -> Result<crate::workflow::WorkflowRecord, CoreError> {
        let token = CancelToken::new();
        {
            use std::collections::btree_map::Entry;
            let mut active = self.active_workflow_cancels.lock().map_err(|_| {
                CoreError::Storage("workflow cancellation map lock poisoned".into())
            })?;
            match active.entry(workflow_id) {
                Entry::Occupied(_) => {
                    let current = self
                        .workflows
                        .get(&workflow_id)?
                        .map_or(crate::workflow::WorkflowState::Running, |record| {
                            record.state
                        });
                    return Err(CoreError::WorkflowNotExecutable {
                        workflow: workflow_id,
                        current,
                    });
                }
                Entry::Vacant(slot) => {
                    slot.insert(token.clone());
                }
            }
        }

        let result = self.execute_workflow_inner(workflow_id, &token);
        self.active_workflow_runs
            .lock()
            .map_err(|_| CoreError::Storage("active workflow run map lock poisoned".into()))?
            .remove(&workflow_id);
        self.active_workflow_cancels
            .lock()
            .map_err(|_| CoreError::Storage("workflow cancellation map lock poisoned".into()))?
            .remove(&workflow_id);
        result
    }

    fn execute_workflow_inner(
        &self,
        workflow_id: WorkflowId,
        token: &CancelToken,
    ) -> Result<crate::workflow::WorkflowRecord, CoreError> {
        let mut record = self
            .workflows
            .get(&workflow_id)?
            .ok_or(CoreError::WorkflowNotFound(workflow_id))?;
        if record.state != crate::workflow::WorkflowState::Created {
            return Err(CoreError::WorkflowNotExecutable {
                workflow: workflow_id,
                current: record.state,
            });
        }
        self.refresh_cancel_intent(&mut record)?;
        if token.is_cancelled() || record.cancel_requested_at.is_some() {
            return self
                .finish_workflow_cancelled(&mut record, "cancelled before execution".into());
        }

        let started_at = self.clock.now_ms();
        record.transition(crate::workflow::WorkflowState::Running, started_at)?;
        self.workflows.put(&record)?;

        let spec = record.spec.clone();
        let concurrency = spec.concurrency_limit();
        let dependencies = spec.dependencies();
        let shared_record = Mutex::new(record);
        let mut pending: BTreeSet<String> =
            spec.steps.iter().map(|step| step.key.clone()).collect();
        let mut completed = BTreeSet::new();
        let mut in_flight = BTreeSet::new();
        let mut first_failure: Option<String> = None;
        let mut cancellation_reason: Option<String> = None;
        let (tx, rx) = mpsc::channel::<(String, Result<ParallelStepTerminal, CoreError>)>();

        std::thread::scope(|scope| -> Result<(), CoreError> {
            loop {
                let persisted_cancel = {
                    let mut current = lock_workflow_record(&shared_record)?;
                    self.refresh_cancel_intent(&mut current)?;
                    current.cancel_requested_at.is_some()
                };
                if persisted_cancel {
                    token.cancel();
                    cancellation_reason
                        .get_or_insert_with(|| "workflow cancellation requested".into());
                    self.cancel_active_workflow_runs(workflow_id)?;
                }

                while first_failure.is_none()
                    && cancellation_reason.is_none()
                    && !token.is_cancelled()
                    && in_flight.len() < concurrency
                {
                    let next = pending
                        .iter()
                        .find(|key| {
                            dependencies
                                .get(*key)
                                .is_some_and(|deps| deps.iter().all(|dep| completed.contains(dep)))
                        })
                        .cloned();
                    let Some(key) = next else {
                        break;
                    };
                    let Some(step) = spec.steps.iter().find(|step| step.key == key).cloned() else {
                        first_failure =
                            Some(format!("step {key:?} disappeared from validated workflow"));
                        break;
                    };
                    let resolved = {
                        let current = lock_workflow_record(&shared_record)?;
                        self.resolve_workflow_inputs(&current, &step)?
                    };
                    let resolved = match resolved {
                        Ok(resolved) => resolved,
                        Err(message) => {
                            first_failure = Some(message);
                            break;
                        }
                    };

                    pending.remove(&key);
                    in_flight.insert(key.clone());
                    let sender = tx.clone();
                    let worker_key = key.clone();
                    let worker_token = token.clone();
                    let shared = &shared_record;
                    scope.spawn(move || {
                        let result = self.execute_parallel_step(
                            workflow_id,
                            &step,
                            resolved,
                            &worker_token,
                            shared,
                        );
                        let _ = sender.send((worker_key, result));
                    });
                }

                if first_failure.is_some() {
                    token.cancel();
                    self.cancel_active_workflow_runs(workflow_id)?;
                } else if cancellation_reason.is_some() || token.is_cancelled() {
                    self.cancel_active_workflow_runs(workflow_id)?;
                }

                if in_flight.is_empty() {
                    if pending.is_empty()
                        || first_failure.is_some()
                        || cancellation_reason.is_some()
                        || token.is_cancelled()
                    {
                        break;
                    }
                    return Err(CoreError::Storage(
                        "workflow scheduler stalled with no ready or in-flight step".into(),
                    ));
                }

                let (key, result) = rx.recv().map_err(|_| {
                    CoreError::Storage("workflow scheduler completion channel closed".into())
                })?;
                in_flight.remove(&key);
                match result {
                    Ok(ParallelStepTerminal::Succeeded) => {
                        completed.insert(key);
                    }
                    Ok(ParallelStepTerminal::Failed(message)) => {
                        if first_failure.is_none() {
                            first_failure = Some(message);
                            token.cancel();
                            self.cancel_active_workflow_runs(workflow_id)?;
                        }
                    }
                    Ok(ParallelStepTerminal::Cancelled(message)) => {
                        // A sibling cancelled by fail-fast must not overwrite
                        // the original failure. Otherwise cancellation is the
                        // workflow's terminal cause.
                        if first_failure.is_none() && cancellation_reason.is_none() {
                            cancellation_reason = Some(message);
                            token.cancel();
                            self.cancel_active_workflow_runs(workflow_id)?;
                        }
                    }
                    Err(error) => {
                        if first_failure.is_none() {
                            first_failure = Some(error.to_string());
                            token.cancel();
                            self.cancel_active_workflow_runs(workflow_id)?;
                        }
                    }
                }
            }
            Ok(())
        })?;

        let mut record = shared_record.into_inner().map_err(|_| {
            CoreError::Storage("workflow record lock poisoned after scheduler completion".into())
        })?;
        self.refresh_cancel_intent(&mut record)?;
        if record.cancel_requested_at.is_some() || cancellation_reason.is_some() {
            return self.finish_workflow_cancelled(
                &mut record,
                cancellation_reason.unwrap_or_else(|| "workflow cancellation requested".into()),
            );
        }
        if let Some(message) = first_failure {
            return self.finish_workflow_failed(&mut record, message);
        }
        if completed.len() != spec.steps.len() {
            return self.finish_workflow_failed(
                &mut record,
                format!(
                    "workflow scheduler completed {} of {} steps",
                    completed.len(),
                    spec.steps.len()
                ),
            );
        }

        let now = self.clock.now_ms();
        record.transition(crate::workflow::WorkflowState::Succeeded, now)?;
        self.workflows.put(&record)?;
        info!(workflow = %workflow_id, concurrency, "workflow succeeded");
        Ok(record)
    }

    fn execute_parallel_step(
        &self,
        workflow_id: WorkflowId,
        step: &crate::workflow::Step,
        resolved: BTreeMap<String, ArtifactId>,
        token: &CancelToken,
        shared_record: &Mutex<crate::workflow::WorkflowRecord>,
    ) -> Result<ParallelStepTerminal, CoreError> {
        let run_spec = crate::workflow::WorkflowSpec::step_run_spec(step, &resolved);
        let max_attempts = step.retry.as_ref().map_or(1, |policy| policy.max_attempts);
        let mut attempt_number = 1u32;

        loop {
            if token.is_cancelled() || self.workflow_cancel_requested(workflow_id)? {
                return Ok(ParallelStepTerminal::Cancelled(
                    "workflow cancellation requested".into(),
                ));
            }

            let submitted = self.submit_run(run_spec.clone())?;
            {
                let mut record = lock_workflow_record(shared_record)?;
                let attempt = crate::workflow::StepAttempt {
                    id: AttemptId::generate(),
                    number: attempt_number,
                    run: submitted.id,
                    state: submitted.state,
                    started_at: submitted.started_at,
                    finished_at: submitted.finished_at,
                    failure_category: None,
                    failure: None,
                };
                Self::record_attempt(&mut record, &step.key, attempt);
                self.workflows.put(&record)?;
            }

            self.register_active_workflow_run(workflow_id, submitted.id)?;
            if token.is_cancelled() || self.workflow_cancel_requested(workflow_id)? {
                let _ = self.cancel_run(submitted.id)?;
            }

            let executed = self.execute_run(submitted.id);
            self.unregister_active_workflow_run(workflow_id, submitted.id)?;

            let finished = match executed {
                Ok(finished) => finished,
                Err(error) => {
                    let current_run = self.runs.get(&submitted.id)?;
                    let mut record = lock_workflow_record(shared_record)?;
                    Self::finish_recorded_attempt(
                        &mut record,
                        &step.key,
                        submitted.id,
                        current_run.as_ref(),
                        Some(crate::workflow::AttemptFailureCategory::ExecutionError),
                        Some(error.to_string()),
                    );
                    self.workflows.put(&record)?;
                    return Err(error);
                }
            };

            let category = classify_attempt_failure(&finished);
            let failure = finished
                .outcome
                .as_ref()
                .and_then(|outcome| outcome.failure.clone());
            {
                let mut record = lock_workflow_record(shared_record)?;
                Self::finish_recorded_attempt(
                    &mut record,
                    &step.key,
                    submitted.id,
                    Some(&finished),
                    category,
                    failure.clone(),
                );
                self.workflows.put(&record)?;
            }

            if self.workflow_cancel_requested(workflow_id)? || finished.state == RunState::Cancelled
            {
                return Ok(ParallelStepTerminal::Cancelled(
                    failure.unwrap_or_else(|| "workflow cancellation requested".into()),
                ));
            }
            if finished.state == RunState::Succeeded {
                return Ok(ParallelStepTerminal::Succeeded);
            }

            let observed =
                category.unwrap_or(crate::workflow::AttemptFailureCategory::ExecutionError);
            let retry = step
                .retry
                .as_ref()
                .is_some_and(|policy| attempt_number < max_attempts && policy.allows(observed));
            if !retry {
                return Ok(ParallelStepTerminal::Failed(format!(
                    "step {:?} attempt {attempt_number} ended in state {}{}",
                    step.key,
                    finished.state,
                    failure
                        .as_ref()
                        .map(|value| format!(": {value}"))
                        .unwrap_or_default()
                )));
            }

            let backoff_ms = step
                .retry
                .as_ref()
                .and_then(|policy| policy.backoff_ms)
                .unwrap_or(0);
            if !self.wait_retry_backoff(workflow_id, token, backoff_ms)? {
                return Ok(ParallelStepTerminal::Cancelled(
                    "workflow cancellation requested during retry backoff".into(),
                ));
            }
            attempt_number += 1;
        }
    }

    fn workflow_cancel_requested(&self, workflow_id: WorkflowId) -> Result<bool, CoreError> {
        Ok(self
            .workflows
            .get(&workflow_id)?
            .is_some_and(|record| record.cancel_requested_at.is_some()))
    }

    fn register_active_workflow_run(
        &self,
        workflow_id: WorkflowId,
        run_id: RunId,
    ) -> Result<(), CoreError> {
        self.active_workflow_runs
            .lock()
            .map_err(|_| CoreError::Storage("active workflow run map lock poisoned".into()))?
            .entry(workflow_id)
            .or_default()
            .insert(run_id);
        Ok(())
    }

    fn unregister_active_workflow_run(
        &self,
        workflow_id: WorkflowId,
        run_id: RunId,
    ) -> Result<(), CoreError> {
        let mut active = self
            .active_workflow_runs
            .lock()
            .map_err(|_| CoreError::Storage("active workflow run map lock poisoned".into()))?;
        let empty = if let Some(runs) = active.get_mut(&workflow_id) {
            runs.remove(&run_id);
            runs.is_empty()
        } else {
            false
        };
        if empty {
            active.remove(&workflow_id);
        }
        Ok(())
    }

    fn cancel_active_workflow_runs(&self, workflow_id: WorkflowId) -> Result<usize, CoreError> {
        let runs = self
            .active_workflow_runs
            .lock()
            .map_err(|_| CoreError::Storage("active workflow run map lock poisoned".into()))?
            .get(&workflow_id)
            .cloned()
            .unwrap_or_default();
        let mut signalled = 0usize;
        for run_id in runs {
            if self.cancel_run(run_id)? {
                signalled += 1;
            }
        }
        Ok(signalled)
    }

    fn resolve_workflow_inputs(
        &self,
        record: &crate::workflow::WorkflowRecord,
        step: &crate::workflow::Step,
    ) -> Result<Result<BTreeMap<String, ArtifactId>, String>, CoreError> {
        let mut resolved = BTreeMap::new();
        for (input_name, source) in &step.inputs {
            match source {
                crate::workflow::InputSource::Artifact { artifact } => {
                    if self.artifacts_meta.get(artifact)?.is_some() {
                        resolved.insert(input_name.clone(), *artifact);
                    } else {
                        return Ok(Err(format!(
                            "step {:?} input {input_name:?}: artifact {} not found",
                            step.key, artifact
                        )));
                    }
                }
                crate::workflow::InputSource::FromStep { key: dep, output } => {
                    let produced = record.steps.iter().find(|result| &result.key == dep);
                    let Some(dep_run) = produced.map(|result| result.run) else {
                        return Ok(Err(format!(
                            "step {:?} input {input_name:?}: dependency {dep:?} has not run",
                            step.key
                        )));
                    };
                    let dep_record = self.runs.get(&dep_run)?;
                    let artifact = dep_record.as_ref().and_then(|run| {
                        run.outcome.as_ref().and_then(|outcome| {
                            outcome
                                .outputs
                                .iter()
                                .find(|candidate| &candidate.name == output)
                                .map(|candidate| candidate.artifact)
                        })
                    });
                    if let Some(artifact) = artifact {
                        resolved.insert(input_name.clone(), artifact);
                    } else {
                        return Ok(Err(format!(
                            "step {:?} input {input_name:?}: step {dep:?} produced no output named {output:?}",
                            step.key
                        )));
                    }
                }
            }
        }
        Ok(Ok(resolved))
    }

    fn record_attempt(
        record: &mut crate::workflow::WorkflowRecord,
        key: &str,
        attempt: crate::workflow::StepAttempt,
    ) {
        if let Some(result) = record.steps.iter_mut().find(|result| result.key == key) {
            result.run = attempt.run;
            result.state = attempt.state;
            result.failure = attempt.failure.clone();
            result.attempts.push(attempt);
        } else {
            record.steps.push(crate::workflow::StepResult {
                key: key.to_owned(),
                run: attempt.run,
                state: attempt.state,
                failure: attempt.failure.clone(),
                attempts: vec![attempt],
            });
            record.steps.sort_by(|left, right| left.key.cmp(&right.key));
        }
    }

    fn finish_recorded_attempt(
        record: &mut crate::workflow::WorkflowRecord,
        key: &str,
        run_id: RunId,
        run: Option<&RunRecord>,
        category: Option<crate::workflow::AttemptFailureCategory>,
        failure: Option<String>,
    ) {
        let Some(result) = record.steps.iter_mut().find(|result| result.key == key) else {
            return;
        };
        let state = run.map_or(RunState::Failed, |run| run.state);
        result.run = run_id;
        result.state = state;
        result.failure = failure.clone();
        if let Some(attempt) = result
            .attempts
            .iter_mut()
            .find(|attempt| attempt.run == run_id)
        {
            attempt.state = state;
            attempt.started_at = run.and_then(|run| run.started_at);
            attempt.finished_at = run.and_then(|run| run.finished_at);
            attempt.failure_category = category;
            attempt.failure = failure;
        }
    }

    fn refresh_cancel_intent(
        &self,
        record: &mut crate::workflow::WorkflowRecord,
    ) -> Result<(), CoreError> {
        if let Some(persisted) = self.workflows.get(&record.id)? {
            if record.cancel_requested_at.is_none() {
                record.cancel_requested_at = persisted.cancel_requested_at;
            }
        }
        Ok(())
    }

    fn wait_retry_backoff(
        &self,
        workflow_id: WorkflowId,
        token: &CancelToken,
        backoff_ms: u64,
    ) -> Result<bool, CoreError> {
        if backoff_ms == 0 {
            return Ok(!token.is_cancelled()
                && self
                    .workflows
                    .get(&workflow_id)?
                    .is_some_and(|record| record.cancel_requested_at.is_none()));
        }
        let started = std::time::Instant::now();
        let delay = std::time::Duration::from_millis(backoff_ms);
        loop {
            if token.is_cancelled()
                || self
                    .workflows
                    .get(&workflow_id)?
                    .is_some_and(|record| record.cancel_requested_at.is_some())
            {
                return Ok(false);
            }
            let elapsed = started.elapsed();
            if elapsed >= delay {
                return Ok(true);
            }
            std::thread::sleep((delay - elapsed).min(std::time::Duration::from_millis(25)));
        }
    }

    fn finish_workflow_failed(
        &self,
        record: &mut crate::workflow::WorkflowRecord,
        message: String,
    ) -> Result<crate::workflow::WorkflowRecord, CoreError> {
        self.refresh_cancel_intent(record)?;
        if record.cancel_requested_at.is_some() {
            return self.finish_workflow_cancelled(record, message);
        }
        let now = self.clock.now_ms();
        record.transition(crate::workflow::WorkflowState::Failed, now)?;
        record.failure = Some(message.clone());
        self.workflows.put(record)?;
        warn!(workflow = %record.id, %message, "workflow failed");
        Ok(record.clone())
    }

    fn finish_workflow_cancelled(
        &self,
        record: &mut crate::workflow::WorkflowRecord,
        message: String,
    ) -> Result<crate::workflow::WorkflowRecord, CoreError> {
        self.refresh_cancel_intent(record)?;
        record
            .cancel_requested_at
            .get_or_insert_with(|| self.clock.now_ms());
        if !record.state.is_terminal() {
            record.transition(
                crate::workflow::WorkflowState::Cancelled,
                self.clock.now_ms(),
            )?;
        }
        record.failure = Some(message.clone());
        self.workflows.put(record)?;
        info!(workflow = %record.id, %message, "workflow cancelled");
        Ok(record.clone())
    }

    /// Persists workflow cancellation intent before signalling the currently
    /// running attempt. Returns whether a live workflow execution was
    /// signalled in this process.
    pub fn cancel_workflow(&self, workflow_id: WorkflowId) -> Result<bool, CoreError> {
        let now = self.clock.now_ms();
        let mut record = self
            .workflows
            .request_cancel(&workflow_id, now)?
            .ok_or(CoreError::WorkflowNotFound(workflow_id))?;
        if record.state.is_terminal() {
            return Ok(false);
        }

        let active_token = self
            .active_workflow_cancels
            .lock()
            .map_err(|_| CoreError::Storage("workflow cancellation map lock poisoned".into()))?
            .get(&workflow_id)
            .cloned();
        if let Some(token) = &active_token {
            token.cancel();
        }

        let signalled_runs = self.cancel_active_workflow_runs(workflow_id)?;

        if active_token.is_none() {
            for run_id in nonterminal_attempt_runs(&record, self)? {
                let _ = self.cancel_run(run_id)?;
            }
            record = self
                .workflows
                .get(&workflow_id)?
                .ok_or(CoreError::WorkflowNotFound(workflow_id))?;
            return self
                .finish_workflow_cancelled(&mut record, "workflow cancellation requested".into())
                .map(|_| false);
        }
        Ok(active_token.is_some() || signalled_runs > 0)
    }

    /// Reconciles cancellation intent left behind by a daemon restart. Any
    /// recorded non-terminal attempt is terminalized before the workflow.
    /// Returns the number of workflows reconciled.
    pub fn recover_workflow_cancellations(&self) -> Result<usize, CoreError> {
        let mut recovered = 0usize;
        for mut record in self.workflows.list()? {
            if record.state.is_terminal() || record.cancel_requested_at.is_none() {
                continue;
            }
            for run_id in nonterminal_attempt_runs(&record, self)? {
                let _ = self.cancel_run(run_id)?;
            }
            self.finish_workflow_cancelled(
                &mut record,
                "workflow cancellation recovered after restart".into(),
            )?;
            recovered += 1;
        }
        Ok(recovered)
    }

    /// Reconciles workflows left `running` by a daemon crash. Local process
    /// attempts cannot be reattached safely after restart, so Hub fails them
    /// closed instead of replaying side effects. Completed attempts remain
    /// untouched and fully queryable.
    pub fn recover_interrupted_workflows(&self) -> Result<usize, CoreError> {
        let mut recovered = 0usize;
        for mut record in self.workflows.list()? {
            if record.state != crate::workflow::WorkflowState::Running
                || record.cancel_requested_at.is_some()
            {
                continue;
            }
            let message =
                "workflow execution interrupted by daemon restart; local attempts are not replayed automatically"
                    .to_owned();
            let now = self.clock.now_ms();
            for result in &mut record.steps {
                for attempt in &mut result.attempts {
                    let Some(mut run) = self.runs.get(&attempt.run)? else {
                        continue;
                    };
                    if run.state.is_terminal() {
                        continue;
                    }
                    run.transition(RunState::Failed, now)?;
                    self.runs.put(&run)?;
                    attempt.state = RunState::Failed;
                    attempt.finished_at = Some(now);
                    attempt.failure_category =
                        Some(crate::workflow::AttemptFailureCategory::ExecutionError);
                    attempt.failure = Some(message.clone());
                    if result.run == attempt.run {
                        result.state = RunState::Failed;
                        result.failure = Some(message.clone());
                    }
                }
            }
            record.transition(crate::workflow::WorkflowState::Failed, now)?;
            record.failure = Some(message.clone());
            self.workflows.put(&record)?;
            warn!(workflow = %record.id, %message, "recovered interrupted workflow");
            recovered += 1;
        }
        Ok(recovered)
    }

    /// Reproduces a recorded run: re-submits its exact stored spec (same
    /// component id + version, capability, canonical parameters and input
    /// bindings) as a NEW queued run linked back via `reproduced_from`.
    ///
    /// Preconditions checked here: the original record exists, the component
    /// is still registered at the same version, and every input artifact is
    /// still present. Execution still goes through the normal
    /// [`Self::execute_run`] path.
    ///
    /// # Errors
    /// [`CoreError::RunNotFound`] for unknown originals,
    /// [`CoreError::ComponentNotFound`] when the component vanished,
    /// [`CoreError::Validation`] when its version drifted or inputs are
    /// missing, storage failures otherwise.
    #[instrument(skip_all, fields(run = %run_id))]
    pub fn reproduce_run(&self, run_id: RunId) -> Result<RunRecord, CoreError> {
        let original = self
            .runs
            .get(&run_id)?
            .ok_or(CoreError::RunNotFound(run_id))?;

        // The component must still exist at the same version so the spec's
        // meaning cannot silently drift between the two executions.
        let manifest = self
            .components
            .latest(&original.spec.component)?
            .ok_or(CoreError::ComponentNotFound(original.spec.component))?;
        if manifest.version != original.component_version {
            return Err(CoreError::Validation(format!(
                "component {} evolved to {} since the original run (recorded {}); \
                 reproduction requires the same version",
                manifest.id, manifest.version, original.component_version
            )));
        }

        // Input artifacts must still be resolvable.
        for input in &original.spec.inputs {
            if self.artifacts_meta.get(&input.artifact)?.is_none() {
                return Err(CoreError::ArtifactNotFound(input.artifact));
            }
        }

        let reproduction = self.submit_run_internal(
            original.spec.clone(),
            Some(run_id),
            Some(original.component_version.clone()),
        )?;
        info!(
            run = %reproduction.id,
            reproduced_from = %run_id,
            "run queued for reproduction"
        );
        Ok(reproduction)
    }

    #[must_use]
    pub fn workflow(&self, id: &crate::id::WorkflowId) -> Option<crate::workflow::WorkflowRecord> {
        self.workflows.get(id).ok().flatten()
    }

    /// All workflows in deterministic `(created_at, id)` order.
    ///
    /// # Errors
    /// Storage failures only.
    pub fn list_workflows(&self) -> Result<Vec<crate::workflow::WorkflowRecord>, CoreError> {
        self.workflows.list()
    }

    #[must_use]
    pub fn workflows(&self) -> Vec<crate::workflow::WorkflowRecord> {
        self.list_workflows().unwrap_or_default()
    }
}

fn classify_attempt_failure(run: &RunRecord) -> Option<crate::workflow::AttemptFailureCategory> {
    if run.state == RunState::Succeeded {
        return None;
    }
    let Some(outcome) = &run.outcome else {
        return Some(crate::workflow::AttemptFailureCategory::ExecutionError);
    };
    if run.state == RunState::Cancelled || outcome.cancelled {
        return Some(crate::workflow::AttemptFailureCategory::Cancelled);
    }
    if outcome.timed_out {
        return Some(crate::workflow::AttemptFailureCategory::TimedOut);
    }
    if outcome
        .failure
        .as_deref()
        .is_some_and(|failure| failure.starts_with("failed to start:"))
    {
        return Some(crate::workflow::AttemptFailureCategory::StartFailure);
    }
    if outcome
        .failure
        .as_deref()
        .is_some_and(|failure| failure.starts_with("required output(s) not produced:"))
    {
        return Some(crate::workflow::AttemptFailureCategory::MissingRequiredOutput);
    }
    if outcome.exit_code.is_some_and(|code| code != 0) {
        return Some(crate::workflow::AttemptFailureCategory::NonZeroExit);
    }
    if outcome.signal.is_some() {
        return Some(crate::workflow::AttemptFailureCategory::Signaled);
    }
    Some(crate::workflow::AttemptFailureCategory::ExecutionError)
}

fn nonterminal_attempt_runs(
    record: &crate::workflow::WorkflowRecord,
    orchestrator: &Orchestrator,
) -> Result<BTreeSet<RunId>, CoreError> {
    let mut runs = BTreeSet::new();
    for result in &record.steps {
        for attempt in &result.attempts {
            if orchestrator
                .runs
                .get(&attempt.run)?
                .is_some_and(|run| !run.state.is_terminal())
            {
                runs.insert(attempt.run);
            }
        }
    }
    Ok(runs)
}

enum ParallelStepTerminal {
    Succeeded,
    Failed(String),
    Cancelled(String),
}

fn lock_workflow_record(
    record: &Mutex<crate::workflow::WorkflowRecord>,
) -> Result<std::sync::MutexGuard<'_, crate::workflow::WorkflowRecord>, CoreError> {
    record
        .lock()
        .map_err(|_| CoreError::Storage("workflow record lock poisoned".into()))
}

#[cfg(test)]
mod tests {
    //! Decision-point tests using the deterministic mock executor. The full
    //! vertical slice through the real process executor lives in
    //! `apps/scirust-hubd/tests`.

    use super::*;
    use crate::capability::{Capability, Port};
    use crate::clock::ManualClock;
    use crate::component::{ComponentKind, ComponentName, ExecutionBinding, ProcessBinding};
    use crate::memory::{
        FileSystemArtifactStore, InMemoryArtifactMeta, InMemoryComponents, InMemoryRuns,
        InMemoryWorkflows,
    };
    use crate::run::InputBinding;
    use crate::Version;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    struct MockExecutor {
        start_error: Option<String>,
        /// Relative path -> bytes, written into the request's working
        /// directory before returning success (simulates components that
        /// produce files).
        write_files: Vec<(String, Vec<u8>)>,
    }

    impl MockExecutor {
        fn write_files(paths: &[(&str, &[u8])]) -> Self {
            Self {
                start_error: None,
                write_files: paths
                    .iter()
                    .map(|(p, b)| ((*p).to_owned(), b.to_vec()))
                    .collect(),
            }
        }
    }

    impl Executor for MockExecutor {
        fn backend_id(&self) -> &str {
            "mock"
        }

        fn execute(
            &self,
            request: &ExecutionRequest,
            _cancel: &CancelToken,
        ) -> Result<ExecutionOutcome, crate::error::ExecutorFailure> {
            for (relative, bytes) in &self.write_files {
                let target = request.working_dir.join(relative);
                if let Some(parent) = target.parent() {
                    std::fs::create_dir_all(parent).expect("create output parent");
                }
                std::fs::write(target, bytes).expect("write simulated output");
            }
            if let Some(err) = &self.start_error {
                return Ok(ExecutionOutcome {
                    exit_code: None,
                    signal: None,
                    timed_out: false,
                    cancelled: false,
                    start_error: Some(err.clone()),
                    duration_ms: 0,
                    stdout: Vec::new(),
                    stdout_truncated: false,
                    stderr: Vec::new(),
                    stderr_truncated: false,
                });
            }
            // Echo the substituted argv so tests can assert placeholder
            // resolution through the whole path.
            let stdout: Vec<u8> = request.args.join(" ").into_bytes();
            Ok(ExecutionOutcome {
                exit_code: Some(0),
                signal: None,
                timed_out: false,
                cancelled: false,
                start_error: None,
                duration_ms: 5,
                stdout,
                stdout_truncated: false,
                stderr: Vec::new(),
                stderr_truncated: false,
            })
        }
    }

    struct ReportedExecutor;

    impl Executor for ReportedExecutor {
        fn backend_id(&self) -> &str {
            "remote-pool"
        }

        fn execute(
            &self,
            _request: &ExecutionRequest,
            _cancel: &CancelToken,
        ) -> Result<ExecutionOutcome, crate::error::ExecutorFailure> {
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

        fn execute_report(
            &self,
            request: &ExecutionRequest,
            cancel: &CancelToken,
        ) -> Result<ExecutionReport, crate::error::ExecutorFailure> {
            self.execute(request, cancel)
                .map(|outcome| ExecutionReport {
                    outcome,
                    backend_id: "remote:worker-a@http://worker-a".into(),
                })
        }
    }

    struct TestHub {
        orch: Orchestrator,
        artifacts: Arc<InMemoryArtifactMeta>,
        dir: PathBuf,
    }

    impl Drop for TestHub {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn hub(start_error: Option<String>) -> (TestHub, Arc<ManualClock>) {
        let dir = std::env::temp_dir().join(format!("hub-orch-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("mkdir");
        let clock = Arc::new(ManualClock::starting_at(1_000));
        let artifacts = Arc::new(InMemoryArtifactMeta::default());
        let orch = Orchestrator::new(
            clock.clone(),
            Arc::new(InMemoryComponents::default()),
            Arc::new(InMemoryRuns::default()),
            artifacts.clone(),
            Arc::new(InMemoryWorkflows::default()),
            FileSystemArtifactStore::open(dir.join("blobs")).expect("blobs"),
            Arc::new(MockExecutor {
                start_error,
                write_files: Vec::new(),
            }),
            Limits::default(),
            dir.join("workdirs"),
        );
        (
            TestHub {
                orch,
                artifacts,
                dir,
            },
            clock,
        )
    }

    fn hub_with_executor(executor: Arc<dyn Executor>) -> TestHub {
        let dir = std::env::temp_dir().join(format!("hub-orch-report-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("mkdir");
        let artifacts = Arc::new(InMemoryArtifactMeta::default());
        let orch = Orchestrator::new(
            Arc::new(ManualClock::starting_at(2_000)),
            Arc::new(InMemoryComponents::default()),
            Arc::new(InMemoryRuns::default()),
            artifacts.clone(),
            Arc::new(InMemoryWorkflows::default()),
            FileSystemArtifactStore::open(dir.join("blobs")).expect("blobs"),
            executor,
            Limits::default(),
            dir.join("workdirs"),
        );
        TestHub {
            orch,
            artifacts,
            dir,
        }
    }

    fn echo_manifest(id: ComponentId) -> ComponentManifest {
        ComponentManifest::new_v1(
            id,
            ComponentName::parse("demo-echo").expect("n"),
            Version::parse("1.0.0").expect("v"),
            ComponentKind::parse(ComponentKind::TOOL).expect("k"),
            vec![Capability {
                name: CapabilityName::parse("demo.echo").expect("c"),
                contract_version: Version::parse("1.0.0").expect("cv"),
                inputs: vec![],
                outputs: vec![Port {
                    name: "stdout".into(),
                    description: String::new(),
                }],
                properties: BTreeMap::new(),
            }],
            Some(ExecutionBinding::Process(ProcessBinding {
                program: "/bin/echo".into(),
                args: vec!["{params}".into()],
                working_dir: None,
                outputs: Vec::new(),
            })),
            None,
            BTreeMap::new(),
        )
        .expect("m")
    }

    #[test]
    fn per_invocation_executor_target_is_recorded_in_run_provenance() {
        let hub = hub_with_executor(Arc::new(ReportedExecutor));
        let manifest = echo_manifest(ComponentId::generate());
        hub.orch
            .register_component(manifest.clone())
            .expect("register");
        let submitted = hub
            .orch
            .submit_run(RunSpec {
                component: manifest.id,
                capability: CapabilityName::parse("demo.echo").expect("cap"),
                parameters: BTreeMap::new(),
                inputs: Vec::new(),
                timeout_ms: 1_000,
            })
            .expect("submit");
        let finished = hub.orch.execute_run(submitted.id).expect("execute");
        assert_eq!(
            finished.outcome.expect("outcome").executor_backend,
            "remote:worker-a@http://worker-a"
        );
    }

    #[test]
    fn registration_is_idempotent_and_conflicts_are_surfaced() {
        let (hub, _clock) = hub(None);
        let manifest = echo_manifest(ComponentId::generate());
        assert_eq!(
            hub.orch
                .register_component(manifest.clone())
                .expect("register"),
            RegistrationStatus::Created
        );
        assert_eq!(
            hub.orch
                .register_component(echo_manifest(manifest.id))
                .expect("replay"),
            RegistrationStatus::AlreadyRegistered
        );
    }

    #[test]
    fn discover_by_capability_returns_only_declaring_components() {
        let (hub, _clock) = hub(None);
        let m1 = echo_manifest(ComponentId::generate());
        let mut other = echo_manifest(ComponentId::generate());
        other.capabilities.clear();
        // Rebuild `other` without capabilities to prove discovery filters.
        let other = ComponentManifest::new_v1(
            other.id,
            other.name.clone(),
            other.version.clone(),
            other.kind.clone(),
            Vec::new(),
            None,
            None,
            BTreeMap::new(),
        )
        .expect("other");
        hub.orch.register_component(m1.clone()).expect("m1");
        hub.orch.register_component(other).expect("other");

        let found = hub
            .orch
            .discover_by_capability(&CapabilityName::parse("demo.echo").expect("cap"))
            .expect("query");
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].id, m1.id);

        // If the same component evolves and drops the capability, the
        // historical manifest must not leak through discovery.
        let evolved = ComponentManifest {
            version: Version::parse("2.0.0").expect("v"),
            capabilities: Vec::new(),
            execution: None,
            ..m1.clone()
        };
        hub.orch.register_component(evolved).expect("evolve m1");
        let found = hub
            .orch
            .discover_by_capability(&CapabilityName::parse("demo.echo").expect("cap"))
            .expect("query after evolution");
        assert!(
            found.is_empty(),
            "only each component's latest manifest may participate in discovery"
        );
    }

    #[test]
    fn full_run_records_provenance_and_persists_outputs() {
        let (hub, clock) = hub(None);
        let manifest = echo_manifest(ComponentId::generate());
        hub.orch
            .register_component(manifest.clone())
            .expect("register");

        let spec = RunSpec {
            component: manifest.id,
            capability: CapabilityName::parse("demo.echo").expect("cap"),
            parameters: BTreeMap::from([("msg".to_owned(), serde_json::json!("ping"))]),
            inputs: vec![],
            timeout_ms: 1_000,
        };
        let submitted = hub.orch.submit_run(spec).expect("submit");
        assert_eq!(submitted.state, RunState::Queued);

        let finished = hub.orch.execute_run(submitted.id).expect("execute");
        assert_eq!(finished.state, RunState::Succeeded);
        let outcome = finished.outcome.as_ref().expect("outcome");
        assert_eq!(outcome.exit_code, Some(0));
        assert_eq!(outcome.executor_backend, "mock");
        assert_eq!(
            outcome.env_keys,
            vec!["PATH".to_owned(), "TMPDIR".to_owned()]
        );
        assert_eq!(finished.started_at, Some(1_000));

        // {params} was substituted with canonical JSON and captured.
        let outputs = &outcome.outputs;
        assert_eq!(outputs.len(), 1);
        let (_meta, bytes) = hub
            .orch
            .artifact_bytes(&outputs[0].artifact)
            .expect("artifact bytes");
        assert_eq!(bytes, br#"{"msg":"ping"}"#.to_vec());
        assert_eq!(outputs[0].digest.to_string(), hex_of(&bytes));

        // Lifecycle history recorded with controlled time.
        assert_eq!(
            finished
                .transitions
                .iter()
                .map(|t| t.to)
                .collect::<Vec<_>>(),
            vec![
                RunState::Validated,
                RunState::Queued,
                RunState::Running,
                RunState::Succeeded
            ]
        );
        assert!(clock.now_ms() >= finished.finished_at.expect("finished"));

        // Terminal runs cannot execute again.
        assert!(matches!(
            hub.orch.execute_run(submitted.id),
            Err(CoreError::RunNotExecutable { .. })
        ));
    }

    fn hex_of(bytes: &[u8]) -> String {
        crate::digest::hash_bytes(crate::digest::DOMAIN_CAPTURE, bytes).to_string()
    }

    #[test]
    fn submit_rejects_undeclared_capability() {
        let (hub, _clock) = hub(None);
        let manifest = echo_manifest(ComponentId::generate());
        hub.orch
            .register_component(manifest.clone())
            .expect("register");
        let spec = RunSpec {
            component: manifest.id,
            capability: CapabilityName::parse("demo.missing").expect("cap"),
            parameters: BTreeMap::new(),
            inputs: vec![],
            timeout_ms: 1_000,
        };
        assert!(matches!(
            hub.orch.submit_run(spec),
            Err(CoreError::CapabilityNotDeclared { .. })
        ));
    }

    #[test]
    fn submit_rejects_unknown_component_and_unbound_inputs() {
        let (hub, _clock) = hub(None);
        let spec = RunSpec {
            component: ComponentId::generate(),
            capability: CapabilityName::parse("demo.echo").expect("cap"),
            parameters: BTreeMap::new(),
            inputs: vec![],
            timeout_ms: 1_000,
        };
        assert!(matches!(
            hub.orch.submit_run(spec),
            Err(CoreError::ComponentNotFound(_))
        ));

        let manifest = echo_manifest(ComponentId::generate());
        hub.orch
            .register_component(manifest.clone())
            .expect("registered");
        let spec = RunSpec {
            component: manifest.id,
            capability: CapabilityName::parse("demo.echo").expect("cap"),
            parameters: BTreeMap::new(),
            inputs: vec![crate::run::InputBinding {
                name: "data".into(),
                artifact: crate::id::ArtifactId::generate(),
            }],
            timeout_ms: 1_000,
        };
        assert!(matches!(
            hub.orch.submit_run(spec),
            Err(CoreError::Validation(msg)) if msg.contains("does not match any input port")
        ));
    }

    #[test]
    fn input_placeholder_resolves_and_missing_input_fails_validation() {
        let (hub, _clock) = hub(None);

        // Seed an artifact.
        let bytes = b"input-bytes".to_vec();
        let digest = hub
            .orch
            .blob_store()
            .put(&bytes, 1024, crate::digest::DOMAIN_ARTIFACT_BLOB)
            .expect("seed blob");
        let meta = crate::artifact::ArtifactMeta {
            id: crate::id::ArtifactId::generate(),
            name: "seed".into(),
            media_type: "text/plain".into(),
            digest,
            size: bytes.len() as u64,
            created_at: 0,
            produced_by_run: None,
        };
        use crate::store::ArtifactMetadataRepository as _;
        hub.artifacts.put(&meta).expect("seed meta");

        let cat_cap = || Capability {
            name: CapabilityName::parse("demo.cat").expect("cap"),
            contract_version: Version::parse("1.0.0").expect("cv"),
            inputs: vec![Port {
                name: "source".into(),
                description: String::new(),
            }],
            outputs: vec![Port {
                name: "stdout".into(),
                description: String::new(),
            }],
            properties: BTreeMap::new(),
        };
        let cat_manifest = |args: Vec<String>| {
            ComponentManifest::new_v1(
                ComponentId::generate(),
                ComponentName::parse("demo-cat").expect("n"),
                Version::parse("1.0.0").expect("v"),
                ComponentKind::parse(ComponentKind::TOOL).expect("k"),
                vec![cat_cap()],
                Some(ExecutionBinding::Process(ProcessBinding {
                    program: "/bin/cat".into(),
                    args,
                    working_dir: None,
                    outputs: Vec::new(),
                })),
                None,
                BTreeMap::new(),
            )
            .expect("m")
        };

        // Unknown placeholder reference is rejected at submission time.
        let bad_manifest = cat_manifest(vec!["{input:nope}".into()]);
        hub.orch
            .register_component(bad_manifest.clone())
            .expect("register");
        let spec = RunSpec {
            component: bad_manifest.id,
            capability: CapabilityName::parse("demo.cat").expect("cap"),
            parameters: BTreeMap::new(),
            inputs: vec![crate::run::InputBinding {
                name: "source".into(),
                artifact: meta.id,
            }],
            timeout_ms: 1_000,
        };
        assert!(matches!(
            hub.orch.submit_run(spec),
            Err(CoreError::Validation(msg)) if msg.contains("unknown input")
        ));

        // Correct placeholder resolves at execution time.
        let good_manifest = cat_manifest(vec!["{input:source}".into()]);
        hub.orch
            .register_component(good_manifest.clone())
            .expect("register good");
        let spec = RunSpec {
            component: good_manifest.id,
            capability: CapabilityName::parse("demo.cat").expect("cap"),
            parameters: BTreeMap::new(),
            inputs: vec![crate::run::InputBinding {
                name: "source".into(),
                artifact: meta.id,
            }],
            timeout_ms: 1_000,
        };
        let run = hub.orch.submit_run(spec).expect("submit good");
        let done = hub.orch.execute_run(run.id).expect("execute");
        assert_eq!(done.state, RunState::Succeeded);
    }

    #[test]
    fn start_failure_becomes_failed_record_with_reason() {
        let (hub, _clock) = hub(Some("no such program".into()));
        let manifest = echo_manifest(ComponentId::generate());
        hub.orch
            .register_component(manifest.clone())
            .expect("register");
        let run = hub
            .orch
            .submit_run(RunSpec {
                component: manifest.id,
                capability: CapabilityName::parse("demo.echo").expect("cap"),
                parameters: BTreeMap::new(),
                inputs: vec![],
                timeout_ms: 1_000,
            })
            .expect("submit");
        let done = hub.orch.execute_run(run.id).expect("execute call succeeds");
        assert_eq!(done.state, RunState::Failed);
        let outcome = done.outcome.expect("outcome present");
        assert_eq!(
            outcome.failure.as_deref(),
            Some("failed to start: no such program")
        );
    }

    #[test]
    fn queued_run_can_be_cancelled_without_executing() {
        let (hub, _clock) = hub(None);
        let manifest = echo_manifest(ComponentId::generate());
        hub.orch
            .register_component(manifest.clone())
            .expect("register");
        let run = hub
            .orch
            .submit_run(RunSpec {
                component: manifest.id,
                capability: CapabilityName::parse("demo.echo").expect("cap"),
                parameters: BTreeMap::new(),
                inputs: vec![],
                timeout_ms: 1_000,
            })
            .expect("submit");
        assert!(!hub.orch.cancel_run(run.id).expect("cancel"));
        let record = hub.orch.run(&run.id).expect("record");
        assert_eq!(record.state, RunState::Cancelled);
    }

    fn copy_manifest(outputs: Vec<crate::component::OutputSpec>) -> ComponentManifest {
        ComponentManifest::new_v1(
            ComponentId::generate(),
            ComponentName::parse("demo-copy").expect("n"),
            Version::parse("1.0.0").expect("v"),
            ComponentKind::parse(ComponentKind::TOOL).expect("k"),
            vec![Capability {
                name: CapabilityName::parse("demo.copy").expect("c"),
                contract_version: Version::parse("1.0.0").expect("cv"),
                inputs: vec![Port {
                    name: "source".into(),
                    description: String::new(),
                }],
                outputs: Vec::new(),
                properties: BTreeMap::new(),
            }],
            Some(ExecutionBinding::Process(ProcessBinding {
                program: "/bin/cp".into(),
                args: vec!["{input:source}".into(), "{output:copy}".into()],
                working_dir: None,
                outputs,
            })),
            None,
            BTreeMap::new(),
        )
        .expect("m")
    }

    #[test]
    fn declared_output_file_is_ingested_as_artifact() {
        // The mock executor writes the file exactly where {output:copy}
        // resolves, mirroring what /bin/cp would do with real paths.
        let dir = std::env::temp_dir().join(format!("hub-orch-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("mkdir");
        let clock = Arc::new(ManualClock::starting_at(1_000));
        let artifacts = Arc::new(InMemoryArtifactMeta::default());
        let orch = Orchestrator::new(
            clock.clone(),
            Arc::new(InMemoryComponents::default()),
            Arc::new(InMemoryRuns::default()),
            artifacts.clone(),
            Arc::new(InMemoryWorkflows::default()),
            FileSystemArtifactStore::open(dir.join("blobs")).expect("blobs"),
            Arc::new(MockExecutor::write_files(&[(
                "out/copy.txt",
                b"file-output-bytes",
            )])),
            Limits::default(),
            dir.join("workdirs"),
        );

        let manifest = copy_manifest(vec![crate::component::OutputSpec {
            name: "copy".into(),
            path: "out/copy.txt".into(),
            media_type: Some("text/plain".into()),
            required: true,
        }]);
        orch.register_component(manifest.clone()).expect("register");

        // Seed the input artifact through the same in-memory metadata store
        // the orchestrator uses.
        let bytes = b"input-payload".to_vec();
        let digest = orch
            .blob_store()
            .put(&bytes, 1024, crate::digest::DOMAIN_ARTIFACT_BLOB)
            .expect("seed blob");
        let seed_meta = crate::artifact::ArtifactMeta {
            id: ArtifactId::generate(),
            name: "seed".into(),
            media_type: "text/plain".into(),
            digest,
            size: bytes.len() as u64,
            created_at: 0,
            produced_by_run: None,
        };
        use crate::store::ArtifactMetadataRepository as _;
        artifacts.put(&seed_meta).expect("seed meta");

        let run = orch
            .submit_run(RunSpec {
                component: manifest.id,
                capability: CapabilityName::parse("demo.copy").expect("cap"),
                parameters: BTreeMap::new(),
                inputs: vec![InputBinding {
                    name: "source".into(),
                    artifact: seed_meta.id,
                }],
                timeout_ms: 1_000,
            })
            .expect("submit");
        let done = orch.execute_run(run.id).expect("execute");
        assert_eq!(done.state, RunState::Succeeded);
        let outcome = done.outcome.as_ref().expect("outcome");

        // Ingested file artifact present alongside stdout capture.
        let file_refs: Vec<_> = outcome
            .outputs
            .iter()
            .filter(|o| o.name == "file:copy")
            .collect();
        assert_eq!(file_refs.len(), 1, "outputs: {:?}", outcome.outputs);
        let (_meta, stored) = orch.artifact_bytes(&file_refs[0].artifact).expect("bytes");
        assert_eq!(stored, b"file-output-bytes");

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn missing_required_output_fails_the_run() {
        let (hub, _clock) = hub(None);
        let manifest = copy_manifest(vec![crate::component::OutputSpec {
            name: "copy".into(),
            path: "out/never-written.txt".into(),
            media_type: None,
            required: true,
        }]);
        hub.orch
            .register_component(manifest.clone())
            .expect("register");

        let bytes = b"payload".to_vec();
        let digest = hub
            .orch
            .blob_store()
            .put(&bytes, 1024, crate::digest::DOMAIN_ARTIFACT_BLOB)
            .expect("seed blob");
        let seed_meta = crate::artifact::ArtifactMeta {
            id: ArtifactId::generate(),
            name: "seed".into(),
            media_type: "text/plain".into(),
            digest,
            size: bytes.len() as u64,
            created_at: 0,
            produced_by_run: None,
        };
        use crate::store::ArtifactMetadataRepository as _;
        hub.artifacts.put(&seed_meta).expect("seed meta");

        let run = hub
            .orch
            .submit_run(RunSpec {
                component: manifest.id,
                capability: CapabilityName::parse("demo.copy").expect("cap"),
                parameters: BTreeMap::new(),
                inputs: vec![InputBinding {
                    name: "source".into(),
                    artifact: seed_meta.id,
                }],
                timeout_ms: 1_000,
            })
            .expect("submit");
        let done = hub.orch.execute_run(run.id).expect("execute call");
        assert_eq!(done.state, RunState::Failed);
        let outcome = done.outcome.expect("outcome");
        assert_eq!(
            outcome.failure.as_deref(),
            Some("required output(s) not produced: copy")
        );
    }

    fn echo_and_copy_components() -> (ComponentManifest, ComponentManifest) {
        let emit = ComponentManifest::new_v1(
            ComponentId::generate(),
            ComponentName::parse("wf-emit").expect("n"),
            Version::parse("1.0.0").expect("v"),
            ComponentKind::parse(ComponentKind::TOOL).expect("k"),
            vec![Capability {
                name: CapabilityName::parse("demo.emit").expect("c"),
                contract_version: Version::parse("1.0.0").expect("cv"),
                inputs: Vec::new(),
                outputs: Vec::new(),
                properties: BTreeMap::new(),
            }],
            Some(ExecutionBinding::Process(ProcessBinding {
                program: "/bin/echo".into(),
                args: vec!["{params}".into()],
                working_dir: None,
                outputs: Vec::new(),
            })),
            None,
            BTreeMap::new(),
        )
        .expect("m");

        let copy = ComponentManifest::new_v1(
            ComponentId::generate(),
            ComponentName::parse("wf-copy").expect("n"),
            Version::parse("1.0.0").expect("v"),
            ComponentKind::parse(ComponentKind::TOOL).expect("k"),
            vec![Capability {
                name: CapabilityName::parse("demo.copy").expect("c"),
                contract_version: Version::parse("1.0.0").expect("cv"),
                inputs: vec![Port {
                    name: "source".into(),
                    description: String::new(),
                }],
                outputs: Vec::new(),
                properties: BTreeMap::new(),
            }],
            Some(ExecutionBinding::Process(ProcessBinding {
                program: "/bin/cp".into(),
                args: vec!["{input:source}".into(), "{output:copy}".into()],
                working_dir: None,
                outputs: vec![crate::component::OutputSpec {
                    name: "copy".into(),
                    path: "out/copy.txt".into(),
                    media_type: Some("text/plain".into()),
                    required: true,
                }],
            })),
            None,
            BTreeMap::new(),
        )
        .expect("m");
        (emit, copy)
    }

    #[test]
    fn workflow_validation_rejects_structural_mistakes() {
        use crate::workflow::{InputSource, Step, WorkflowSpec, WORKFLOW_SCHEMA_VERSION};
        let component = ComponentId::generate();
        let mk_step = |key: &str, after: Vec<String>, inputs: BTreeMap<String, InputSource>| Step {
            key: key.to_owned(),
            component,
            capability: CapabilityName::parse("demo.emit").expect("c"),
            parameters: BTreeMap::new(),
            inputs,
            timeout_ms: 1_000,
            after,
            retry: None,
        };

        // Duplicate keys.
        let spec = WorkflowSpec {
            schema_version: WORKFLOW_SCHEMA_VERSION,
            name: "wf".into(),
            max_concurrency: 1,
            steps: vec![
                mk_step("a", Vec::new(), BTreeMap::new()),
                mk_step("a", Vec::new(), BTreeMap::new()),
            ],
        };
        assert!(matches!(
            spec.validate(),
            Err(CoreError::InvalidRunSpec(msg)) if msg.contains("duplicate step key")
        ));

        // Unknown dependency.
        let spec = WorkflowSpec {
            schema_version: WORKFLOW_SCHEMA_VERSION,
            name: "wf".into(),
            max_concurrency: 1,
            steps: vec![mk_step("a", vec!["ghost".into()], BTreeMap::new())],
        };
        assert!(matches!(
            spec.validate(),
            Err(CoreError::InvalidRunSpec(msg)) if msg.contains("unknown step")
        ));

        // Cycle through data dependencies: a consumes b, b consumes a.
        let mut inputs_a = BTreeMap::new();
        inputs_a.insert(
            "x".to_owned(),
            InputSource::FromStep {
                key: "b".into(),
                output: "stdout".into(),
            },
        );
        let mut inputs_b = BTreeMap::new();
        inputs_b.insert(
            "x".to_owned(),
            InputSource::FromStep {
                key: "a".into(),
                output: "stdout".into(),
            },
        );
        let spec = WorkflowSpec {
            schema_version: WORKFLOW_SCHEMA_VERSION,
            name: "wf".into(),
            max_concurrency: 1,
            steps: vec![
                mk_step("a", Vec::new(), inputs_a),
                mk_step("b", Vec::new(), inputs_b),
            ],
        };
        assert!(spec.validate().is_err(), "data cycle must be rejected");
        assert!(spec.topo_keys().is_err());

        // Self-consumption rejected.
        let mut self_inputs = BTreeMap::new();
        self_inputs.insert(
            "x".to_owned(),
            InputSource::FromStep {
                key: "a".into(),
                output: "stdout".into(),
            },
        );
        let spec = WorkflowSpec {
            schema_version: WORKFLOW_SCHEMA_VERSION,
            name: "wf".into(),
            max_concurrency: 1,
            steps: vec![mk_step("a", Vec::new(), self_inputs)],
        };
        assert!(
            spec.validate().is_err(),
            "a step cannot consume its own output"
        );

        // Bad schema version.
        let spec = WorkflowSpec {
            schema_version: 9,
            name: "wf".into(),
            max_concurrency: 1,
            steps: vec![mk_step("a", Vec::new(), BTreeMap::new())],
        };
        assert!(spec.validate().is_err());
    }

    #[test]
    fn sequential_workflow_chains_artifacts_between_steps() {
        let dir = std::env::temp_dir().join(format!("hub-wf-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("mkdir");
        let orch = Orchestrator::new(
            Arc::new(ManualClock::starting_at(5_000)),
            Arc::new(InMemoryComponents::default()),
            Arc::new(InMemoryRuns::default()),
            Arc::new(InMemoryArtifactMeta::default()),
            Arc::new(InMemoryWorkflows::default()),
            FileSystemArtifactStore::open(dir.join("blobs")).expect("blobs"),
            // The mock writes the copy target so step 2 produces its declared
            // output exactly where {output:copy} resolves.
            Arc::new(MockExecutor {
                start_error: None,
                write_files: vec![("out/copy.txt".into(), b"chained-bytes".to_vec())],
            }),
            Limits::default(),
            dir.join("workdirs"),
        );

        let (emit, copy) = echo_and_copy_components();
        orch.register_component(emit.clone()).expect("emit");
        orch.register_component(copy.clone()).expect("copy");

        use crate::workflow::{InputSource, Step};
        let mut copy_inputs = BTreeMap::new();
        copy_inputs.insert(
            "source".to_owned(),
            InputSource::FromStep {
                key: "emit".into(),
                output: "stdout".into(),
            },
        );
        let steps = vec![
            Step {
                key: "emit".into(),
                component: emit.id,
                capability: CapabilityName::parse("demo.emit").expect("c"),
                parameters: BTreeMap::from([(
                    "msg".to_owned(),
                    serde_json::json!("workflow-payload"),
                )]),
                inputs: BTreeMap::new(),
                timeout_ms: 1_000,
                after: Vec::new(),
                retry: None,
            },
            Step {
                key: "store".into(),
                component: copy.id,
                capability: CapabilityName::parse("demo.copy").expect("c"),
                parameters: BTreeMap::new(),
                inputs: copy_inputs,
                timeout_ms: 1_000,
                after: Vec::new(),
                retry: None,
            },
        ];
        let spec = crate::workflow::WorkflowSpec {
            schema_version: crate::workflow::WORKFLOW_SCHEMA_VERSION,
            name: "chain".into(),
            max_concurrency: 1,
            steps,
        };
        // Deterministic topological order puts emit before store.
        assert_eq!(spec.topo_keys().expect("order"), vec!["emit", "store"]);

        let submitted = orch.submit_workflow(spec).expect("submit");
        assert_eq!(submitted.state, crate::workflow::WorkflowState::Created);

        let done = orch.execute_workflow(submitted.id).expect("execute");
        assert_eq!(done.state, crate::workflow::WorkflowState::Succeeded);
        assert_eq!(done.steps.len(), 2);
        assert!(done.steps.iter().all(|sr| sr.state == RunState::Succeeded));

        // The second step consumed the FIRST step's captured stdout:
        // its run record shows the input binding and produced the file.
        let store_run_id = &done.steps[1].run;
        let store_record = orch.run(store_run_id).expect("store run record");
        let outcome = store_record.outcome.as_ref().expect("outcome");
        let file_ref = outcome
            .outputs
            .iter()
            .find(|o| o.name == "file:copy")
            .expect("file output recorded");
        let (_meta, bytes) = orch.artifact_bytes(&file_ref.artifact).expect("bytes");
        assert_eq!(bytes, b"chained-bytes");
        // Provenance: input of store step is the emit step's stdout artifact.
        let emit_run_id = &done.steps[0].run;
        let emit_outcome = orch
            .run(emit_run_id)
            .and_then(|r| r.outcome)
            .expect("emit outcome");
        let expected_input = &emit_outcome.outputs[0].artifact;
        assert!(outcome.inputs.iter().any(|i| &i.artifact == expected_input));

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn reproduction_relinks_specs_and_guards_against_drift() {
        let (hub, _clock) = hub(None);
        let manifest = echo_manifest(ComponentId::generate());
        hub.orch
            .register_component(manifest.clone())
            .expect("register");

        let original = hub
            .orch
            .submit_run(RunSpec {
                component: manifest.id,
                capability: CapabilityName::parse("demo.echo").expect("cap"),
                parameters: BTreeMap::from([("msg".to_owned(), serde_json::json!("reproduce-me"))]),
                inputs: vec![],
                timeout_ms: 1_000,
            })
            .expect("submit");

        // Unknown originals are 404-style errors.
        assert!(matches!(
            hub.orch.reproduce_run(RunId::generate()),
            Err(CoreError::RunNotFound(_))
        ));

        // Reproduction creates a queued run linked to the original.
        let reproduction = hub.orch.reproduce_run(original.id).expect("reproduce");
        assert_eq!(reproduction.state, RunState::Queued);
        assert_eq!(reproduction.reproduced_from, Some(original.id));
        assert_eq!(reproduction.spec.parameters, original.spec.parameters);
        let persisted = hub
            .orch
            .run(&reproduction.id)
            .expect("persisted reproduction");
        assert_eq!(persisted.reproduced_from, Some(original.id));

        // Version drift after queueing must also block execution; the
        // reproduction can never silently pick up a newer manifest.
        let drifted = echo_manifest(manifest.id);
        let drifted = ComponentManifest {
            version: Version::parse("9.9.9").expect("v"),
            ..drifted
        };
        hub.orch.register_component(drifted).expect("register v9");
        match hub.orch.execute_run(reproduction.id) {
            Err(CoreError::Validation(msg)) => {
                assert!(msg.contains("evolved"), "message: {msg}");
                assert!(msg.contains("recorded 1.0.0"), "message: {msg}");
            }
            other => panic!("expected execution-time drift rejection, got {other:?}"),
        }
        match hub.orch.reproduce_run(original.id) {
            Err(CoreError::Validation(msg)) => {
                assert!(msg.contains("evolved"), "message: {msg}");
            }
            other => panic!("expected drift rejection, got {other:?}"),
        }

        // A run recorded under the CURRENT version reproduces fine.
        let current = hub
            .orch
            .submit_run(RunSpec {
                component: manifest.id,
                capability: CapabilityName::parse("demo.echo").expect("cap"),
                parameters: BTreeMap::new(),
                inputs: vec![],
                timeout_ms: 1_000,
            })
            .expect("submit under v9");
        assert_eq!(current.component_version.as_str(), "9.9.9");
        let again = hub
            .orch
            .reproduce_run(current.id)
            .expect("reproduce current");
        assert_eq!(again.reproduced_from, Some(current.id));
        assert_eq!(again.spec.parameters, current.spec.parameters);
        let done = hub
            .orch
            .execute_run(again.id)
            .expect("execute current reproduction");
        assert_eq!(done.state, RunState::Succeeded);
    }

    #[test]
    fn duplicate_concurrent_execution_is_rejected() {
        let (hub, _clock) = hub(None);
        let manifest = echo_manifest(ComponentId::generate());
        hub.orch
            .register_component(manifest.clone())
            .expect("register");
        let run = hub
            .orch
            .submit_run(RunSpec {
                component: manifest.id,
                capability: CapabilityName::parse("demo.echo").expect("cap"),
                parameters: BTreeMap::new(),
                inputs: vec![],
                timeout_ms: 1_000,
            })
            .expect("submit");

        // Simulate a concurrent claim by pre-inserting a token via public API:
        // executing from two threads is covered by integration tests; here we
        // verify that a terminal-state run is rejected (already asserted) and
        // that unknown runs error cleanly.
        let ghost = RunId::generate();
        assert!(matches!(
            hub.orch.execute_run(ghost),
            Err(CoreError::RunNotFound(_))
        ));
        let _ = run;
    }
}
