from pathlib import Path


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected exactly one match, found {count}")
    return text.replace(old, new, 1)

# hub-core: expose a read-only authoritative publication query on Orchestrator.
p = Path("crates/hub-core/src/orchestrator.rs")
s = p.read_text()
s = replace_once(
    s,
    '''    #[must_use]\n    pub fn workflow(&self, id: &crate::id::WorkflowId) -> Option<crate::workflow::WorkflowRecord> {\n        self.workflows.get(id).ok().flatten()\n    }\n\n    /// All workflows in deterministic `(created_at, id)` order.''',
    '''    #[must_use]\n    pub fn workflow(&self, id: &crate::id::WorkflowId) -> Option<crate::workflow::WorkflowRecord> {\n        self.workflows.get(id).ok().flatten()\n    }\n\n    /// Returns the authoritative output publication for one workflow step.\n    ///\n    /// This is a read-only view over Hub-owned publication authority. Callers\n    /// cannot mint or advance fences through this surface.\n    ///\n    /// # Errors\n    /// Storage failures only.\n    pub fn authoritative_step_publication(\n        &self,\n        workflow: WorkflowId,\n        step_key: &str,\n    ) -> Result<Option<AuthoritativeStepPublication>, CoreError> {\n        self.publication_fences\n            .authoritative_publication(workflow, step_key)\n    }\n\n    /// All workflows in deterministic `(created_at, id)` order.''',
    "orchestrator authoritative query",
)
p.write_text(s)

# protocol: stable DTO for the qualified authoritative publication contract.
p = Path("crates/hub-protocol/src/lib.rs")
s = p.read_text()
s = replace_once(
    s,
    '''#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]\npub struct CancelWorkflowResponse {\n    pub workflow_id: hub_core::WorkflowId,\n    pub signalled_active_execution: bool,\n}\n\n#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]\npub struct WorkflowListResponse {''',
    '''#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]\npub struct CancelWorkflowResponse {\n    pub workflow_id: hub_core::WorkflowId,\n    pub signalled_active_execution: bool,\n}\n\n/// Read-only wire form of the Hub-owned authoritative step publication.\n///\n/// The schema version is the publication-fence contract version, not the HTTP\n/// protocol version. The output map contains immutable Hub artifact ids.\n#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]\npub struct AuthoritativeStepPublicationDto {\n    pub schema_version: u16,\n    pub workflow: hub_core::WorkflowId,\n    pub step_key: String,\n    pub attempt: hub_core::AttemptId,\n    pub generation: u64,\n    pub outputs: BTreeMap<String, ArtifactId>,\n}\n\nimpl From<&hub_core::AuthoritativeStepPublication> for AuthoritativeStepPublicationDto {\n    fn from(publication: &hub_core::AuthoritativeStepPublication) -> Self {\n        Self {\n            schema_version: publication.fence.schema_version,\n            workflow: publication.fence.workflow,\n            step_key: publication.fence.step_key.clone(),\n            attempt: publication.fence.attempt,\n            generation: publication.fence.generation,\n            outputs: publication.outputs.clone(),\n        }\n    }\n}\n\n#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]\npub struct WorkflowListResponse {''',
    "protocol authoritative DTO",
)
p.write_text(s)

# API: authenticated inspect-only route. GET is already classified as Inspect.
p = Path("crates/hub-api/src/lib.rs")
s = p.read_text()
s = replace_once(
    s,
    '''        .route("/api/v1/workflows/{id}", get(get_workflow))\n        .route("/api/v1/workflows/{id}/cancel", post(cancel_workflow))''',
    '''        .route("/api/v1/workflows/{id}", get(get_workflow))\n        .route(\n            "/api/v1/workflows/{id}/steps/{step_key}/publication",\n            get(get_authoritative_step_publication),\n        )\n        .route("/api/v1/workflows/{id}/cancel", post(cancel_workflow))''',
    "publication route",
)
s = replace_once(
    s,
    '''async fn cancel_workflow(State(state): State<HubState>, Path(id): Path<String>) -> Response {''',
    '''async fn get_authoritative_step_publication(\n    State(state): State<HubState>,\n    Path((id, step_key)): Path<(String, String)>,\n) -> Response {\n    let Some(parsed) = typed_id::<hub_core::WorkflowId>(&id) else {\n        return not_found("workflow", &id);\n    };\n    let orch = state.orchestrator.clone();\n    match joined(\n        tokio::task::spawn_blocking(move || {\n            orch.authoritative_step_publication(parsed, &step_key)\n        })\n        .await,\n    ) {\n        Ok(Some(publication)) => {\n            Json(proto::AuthoritativeStepPublicationDto::from(&publication)).into_response()\n        }\n        Ok(None) => not_found(\n            "authoritative workflow-step publication",\n            &format!("{id}/{step_key}"),\n        ),\n        Err(response) => response,\n    }\n}\n\nasync fn cancel_workflow(State(state): State<HubState>, Path(id): Path<String>) -> Response {''',
    "publication handler",
)
p.write_text(s)

# Real-daemon e2e: absent before execution, authoritative and queryable after.
p = Path("apps/scirust-hubd/tests/http_e2e.rs")
s = p.read_text()
s = replace_once(
    s,
    '''    let workflow_id: String = json_field(&body, "\\\"id\\\":\\\"").expect("workflow id");\n\n    // Execute and verify success with both steps recorded.''',
    '''    let workflow_id: String = json_field(&body, "\\\"id\\\":\\\"").expect("workflow id");\n\n    // Publication authority does not exist before a concrete successful attempt.\n    let (status, body) = http(\n        port,\n        "GET",\n        &format!("/api/v1/workflows/{workflow_id}/steps/emit/publication"),\n        None,\n    )\n    .expect("pre-execution publication query");\n    assert_eq!(status, 404, "body: {body}");\n\n    // Execute and verify success with both steps recorded.''',
    "pre-execution publication e2e",
)
s = replace_once(
    s,
    '''    assert!(executed.contains("\\\"store\\\""), "body: {executed}");\n\n    // The copied file artifact must exist alongside the emit stdout capture:''',
    '''    assert!(executed.contains("\\\"store\\\""), "body: {executed}");\n\n    // The Hub-owned authoritative publication is now inspectable through the\n    // authenticated GET surface. This is the publication consumed by downstream\n    // FromStep resolution; raw RunRecord outputs are not authoritative.\n    let (status, publication) = http(\n        port,\n        "GET",\n        &format!("/api/v1/workflows/{workflow_id}/steps/emit/publication"),\n        None,\n    )\n    .expect("authoritative publication query");\n    assert_eq!(status, 200, "body: {publication}");\n    let publication_json: serde_json::Value =\n        serde_json::from_str(&publication).expect("publication json");\n    assert_eq!(publication_json["schema_version"], 1);\n    assert_eq!(publication_json["workflow"], workflow_id);\n    assert_eq!(publication_json["step_key"], "emit");\n    assert_eq!(publication_json["generation"], 1);\n    assert!(\n        publication_json["outputs"].get("stdout").is_some(),\n        "emit stdout missing from authoritative publication: {publication}"\n    );\n\n    // The copied file artifact must exist alongside the emit stdout capture:''',
    "post-execution publication e2e",
)
p.write_text(s)
