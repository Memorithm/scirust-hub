from pathlib import Path


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected exactly one match, found {count}")
    return text.replace(old, new, 1)


p = Path("crates/hub-core/src/orchestrator.rs")
s = p.read_text()
s = replace_once(
    s,
    "use crate::memory::FileSystemArtifactStore;\nuse crate::run::{OutputRef, RunOutcome, RunRecord, RunSpec, RunState};",
    "use crate::memory::FileSystemArtifactStore;\nuse crate::publication::{\n    AuthoritativeStepPublication, InMemoryPublicationFences, PublicationFenceRepository,\n};\nuse crate::run::{OutputRef, RunOutcome, RunRecord, RunSpec, RunState};",
    "orchestrator publication imports",
)
s = replace_once(
    s,
    "    workflows: Arc<dyn WorkflowRepository>,\n    executor: Arc<dyn Executor>,",
    "    workflows: Arc<dyn WorkflowRepository>,\n    publication_fences: Arc<dyn PublicationFenceRepository>,\n    executor: Arc<dyn Executor>,",
    "orchestrator publication field",
)
s = replace_once(
    s,
    "            workflows,\n            blobs,\n            executor,",
    "            workflows,\n            publication_fences: Arc::new(InMemoryPublicationFences::default()),\n            blobs,\n            executor,",
    "orchestrator default publication repository",
)
s = replace_once(
    s,
    "    #[must_use]\n    pub fn limits(&self) -> &Limits {\n        &self.limits\n    }",
    "    /// Replaces the ephemeral publication authority used by [`Self::new`].\n    ///\n    /// Durable deployments must inject a restart-safe implementation such as\n    /// the SQLite store. Tests and throwaway callers may keep the in-memory\n    /// reference repository.\n    #[must_use]\n    pub fn with_publication_fences(\n        mut self,\n        publication_fences: Arc<dyn PublicationFenceRepository>,\n    ) -> Self {\n        self.publication_fences = publication_fences;\n        self\n    }\n\n    #[must_use]\n    pub fn limits(&self) -> &Limits {\n        &self.limits\n    }",
    "publication repository builder",
)
old_attempt = '''            let submitted = self.submit_run(run_spec.clone())?;
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

            self.register_active_workflow_run(workflow_id, submitted.id)?;'''
new_attempt = '''            let submitted = self.submit_run(run_spec.clone())?;
            let attempt_id = AttemptId::generate();
            {
                let mut record = lock_workflow_record(shared_record)?;
                let attempt = crate::workflow::StepAttempt {
                    id: attempt_id,
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
            // The durable repository validates that this exact attempt is the
            // persisted current attempt before issuing a fresh generation.
            // A retry therefore supersedes every older attempt before it can
            // publish authoritative outputs.
            let publication_fence = self.publication_fences.advance_publication_fence(
                workflow_id,
                &step.key,
                attempt_id,
            )?;

            self.register_active_workflow_run(workflow_id, submitted.id)?;'''
s = replace_once(s, old_attempt, new_attempt, "attempt fence issuance")
old_success = '''            if finished.state == RunState::Succeeded {
                return Ok(ParallelStepTerminal::Succeeded);
            }
'''
new_success = '''            if finished.state == RunState::Succeeded {
                let outcome = finished.outcome.as_ref().ok_or_else(|| {
                    CoreError::Storage(format!(
                        "succeeded run {} has no recorded outcome for authoritative publication",
                        finished.id
                    ))
                })?;
                let mut outputs = BTreeMap::new();
                for output in &outcome.outputs {
                    if outputs
                        .insert(output.name.clone(), output.artifact)
                        .is_some()
                    {
                        return Err(CoreError::Validation(format!(
                            "step {:?} produced duplicate output label {:?}; authoritative publication is ambiguous",
                            step.key, output.name
                        )));
                    }
                }
                self.publication_fences.publish_authoritative_outputs(
                    &AuthoritativeStepPublication {
                        fence: publication_fence,
                        outputs,
                    },
                )?;
                return Ok(ParallelStepTerminal::Succeeded);
            }
'''
s = replace_once(s, old_success, new_success, "authoritative success publication")
old_from_step = '''                crate::workflow::InputSource::FromStep { key: dep, output } => {
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
                }'''
new_from_step = '''                crate::workflow::InputSource::FromStep { key: dep, output } => {
                    let publication = self
                        .publication_fences
                        .authoritative_publication(record.id, dep)?;
                    let Some(publication) = publication else {
                        return Ok(Err(format!(
                            "step {:?} input {input_name:?}: dependency {dep:?} has no authoritative publication",
                            step.key
                        )));
                    };
                    if let Some(artifact) = publication.outputs.get(output).copied() {
                        resolved.insert(input_name.clone(), artifact);
                    } else {
                        return Ok(Err(format!(
                            "step {:?} input {input_name:?}: authoritative publication for step {dep:?} has no output named {output:?}",
                            step.key
                        )));
                    }
                }'''
s = replace_once(s, old_from_step, new_from_step, "authoritative FromStep resolution")
p.write_text(s)

p = Path("apps/scirust-hubd/src/main.rs")
s = p.read_text()
s = replace_once(
    s,
    "use hub_core::memory::{FileSystemArtifactStore, InMemoryHubStore};\nuse hub_core::store::{",
    "use hub_core::memory::{FileSystemArtifactStore, InMemoryHubStore};\nuse hub_core::publication::{InMemoryPublicationFences, PublicationFenceRepository};\nuse hub_core::store::{",
    "daemon publication imports",
)
s = replace_once(
    s,
    "    workflows: Arc<dyn WorkflowRepository>,\n    blob_store: FileSystemArtifactStore,",
    "    workflows: Arc<dyn WorkflowRepository>,\n    publication_fences: Arc<dyn PublicationFenceRepository>,\n    blob_store: FileSystemArtifactStore,",
    "daemon builder publication argument",
)
old_builder = '''    Arc::new(Orchestrator::new(
        Arc::new(SystemClock),
        components,
        runs,
        artifacts_meta,
        workflows,
        blob_store,
        executor,
        Limits::default(),
        workdir_root,
    ))'''
new_builder = '''    Arc::new(
        Orchestrator::new(
            Arc::new(SystemClock),
            components,
            runs,
            artifacts_meta,
            workflows,
            blob_store,
            executor,
            Limits::default(),
            workdir_root,
        )
        .with_publication_fences(publication_fences),
    )'''
s = replace_once(s, old_builder, new_builder, "daemon builder wiring")
sqlite_call = '''                let orchestrator = build_orchestrator(
                    store.clone(),
                    store.clone(),
                    store.clone(),
                    store.clone(),
                    blob_store,
                    executor.clone(),
                    workdir_root,
                );'''
sqlite_new = '''                let orchestrator = build_orchestrator(
                    store.clone(),
                    store.clone(),
                    store.clone(),
                    store.clone(),
                    store.clone(),
                    blob_store,
                    executor.clone(),
                    workdir_root,
                );'''
s = replace_once(s, sqlite_call, sqlite_new, "sqlite durable publication injection")
memory_call = '''                let orchestrator = build_orchestrator(
                    store.clone(),
                    store.clone(),
                    store.clone(),
                    store.clone(),
                    blob_store,
                    executor.clone(),
                    workdir_root,
                );'''
memory_new = '''                let orchestrator = build_orchestrator(
                    store.clone(),
                    store.clone(),
                    store.clone(),
                    store.clone(),
                    Arc::new(InMemoryPublicationFences::default()),
                    blob_store,
                    executor.clone(),
                    workdir_root,
                );'''
s = replace_once(s, memory_call, memory_new, "memory publication injection")
p.write_text(s)
