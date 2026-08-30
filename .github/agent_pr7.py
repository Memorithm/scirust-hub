from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one replacement, found {count}: {old[:160]!r}")
    p.write_text(text.replace(old, new, 1))


# ---------------------------------------------------------------------------
# Lifecycle event read port: O(1)/O(log n) high-water query for metrics.
# ---------------------------------------------------------------------------
replace_once(
    "crates/hub-core/src/event.rs",
    '''    fn list_after(&self, after_sequence: u64, limit: u32)
        -> Result<Vec<LifecycleEvent>, CoreError>;
}
''',
    '''    fn list_after(&self, after_sequence: u64, limit: u32)
        -> Result<Vec<LifecycleEvent>, CoreError>;

    /// Highest committed event sequence, or zero when the log is empty.
    ///
    /// # Errors
    /// Storage failures only.
    fn high_water_sequence(&self) -> Result<u64, CoreError>;
}
''',
)
replace_once(
    "crates/hub-core/src/event.rs",
    '''        Ok(inner
            .events
            .iter()
            .filter(|event| event.sequence > after_sequence)
            .take(limit as usize)
            .cloned()
            .collect())
    }
}
''',
    '''        Ok(inner
            .events
            .iter()
            .filter(|event| event.sequence > after_sequence)
            .take(limit as usize)
            .cloned()
            .collect())
    }

    fn high_water_sequence(&self) -> Result<u64, CoreError> {
        let inner = self
            .0
            .lock()
            .map_err(|_| CoreError::Storage("lifecycle event lock poisoned".into()))?;
        Ok(inner.next_sequence)
    }
}
''',
)
replace_once(
    "crates/hub-core/src/event.rs",
    '''        let next = store.list_after(2, 2).unwrap();
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].sequence, 3);
        assert!(store.list_after(0, 0).is_err());
''',
    '''        let next = store.list_after(2, 2).unwrap();
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].sequence, 3);
        assert_eq!(store.high_water_sequence().unwrap(), 3);
        assert!(store.list_after(0, 0).is_err());
''',
)

# Composite memory store delegates high-water query.
replace_once(
    "crates/hub-core/src/memory.rs",
    '''        self.events.list_after(after_sequence, limit)
    }
}
''',
    '''        self.events.list_after(after_sequence, limit)
    }

    fn high_water_sequence(&self) -> Result<u64, CoreError> {
        self.events.high_water_sequence()
    }
}
''',
)

# SQLite high-water query.
replace_once(
    "crates/hub-store-sqlite/src/lib.rs",
    '''        Ok(events)
    }
}

#[cfg(test)]
''',
    '''        Ok(events)
    }

    fn high_water_sequence(&self) -> Result<u64, CoreError> {
        let conn = self.lock()?;
        let sequence: i64 = conn
            .query_row(
                "SELECT COALESCE(MAX(sequence), 0) FROM lifecycle_events",
                [],
                |row| row.get(0),
            )
            .map_err(storage("reading lifecycle event high-water mark"))?;
        u64::try_from(sequence)
            .map_err(|_| CoreError::Storage("negative lifecycle event high-water mark".into()))
    }
}

#[cfg(test)]
''',
)
replace_once(
    "crates/hub-store-sqlite/src/lib.rs",
    '''        assert_eq!(events.last().unwrap().attributes["to"], "queued");
        assert!(events
''',
    '''        assert_eq!(events.last().unwrap().attributes["to"], "queued");
        assert_eq!(LifecycleEventRepository::high_water_sequence(&store).unwrap(), 4);
        assert!(events
''',
)

# ---------------------------------------------------------------------------
# Orchestrator: truthful fallible read methods for metrics collection.
# Existing convenience methods remain for compatibility.
# ---------------------------------------------------------------------------
replace_once(
    "crates/hub-core/src/orchestrator.rs",
    '''    /// All artifact metadata in deterministic `(created_at, id)` order;
    /// empty when the metadata store errors (read-only convenience).
    #[must_use]
    pub fn artifacts(&self) -> Vec<crate::artifact::ArtifactMeta> {
        self.artifacts_meta.list().unwrap_or_default()
    }
''',
    '''    /// All artifact metadata in deterministic `(created_at, id)` order.
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
''',
)
replace_once(
    "crates/hub-core/src/orchestrator.rs",
    '''    #[must_use]
    pub fn workflows(&self) -> Vec<crate::workflow::WorkflowRecord> {
        self.workflows.list().unwrap_or_default()
    }
}
''',
    '''    /// All workflows in deterministic `(created_at, id)` order.
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
''',
)

# ---------------------------------------------------------------------------
# Pure metrics collector/renderer in HTTP adapter. No mutable counters.
# ---------------------------------------------------------------------------
Path("crates/hub-api/src/metrics.rs").write_text(r'''//! Low-cardinality operational metrics derived from authoritative Hub state.
//!
//! No counters are incremented on the write path. Every exposition is a
//! deterministic snapshot of persisted manifests/runs/workflows/artifacts plus
//! the lifecycle-event high-water sequence.

use hub_core::error::CoreError;
use hub_core::{LifecycleEventRepository, Orchestrator, RunState};

#[derive(Clone, Debug, PartialEq, Eq)]
struct MetricsSnapshot {
    component_manifests: u64,
    runs: [u64; 7],
    workflows: [u64; 5],
    artifacts: u64,
    artifact_bytes: u128,
    workflow_attempts: u128,
    lifecycle_event_high_water: u64,
    executor_backend: String,
}

pub(crate) fn collect_and_render(
    orchestrator: &Orchestrator,
    events: &dyn LifecycleEventRepository,
) -> Result<String, CoreError> {
    let components = orchestrator.components()?;
    let runs = orchestrator.list_runs()?;
    let workflows = orchestrator.list_workflows()?;
    let artifacts = orchestrator.list_artifacts()?;
    let lifecycle_event_high_water = events.high_water_sequence()?;

    let mut run_counts = [0u64; 7];
    for run in &runs {
        let index = match run.state {
            RunState::Created => 0,
            RunState::Validated => 1,
            RunState::Queued => 2,
            RunState::Running => 3,
            RunState::Succeeded => 4,
            RunState::Failed => 5,
            RunState::Cancelled => 6,
        };
        run_counts[index] = run_counts[index].saturating_add(1);
    }

    let mut workflow_counts = [0u64; 5];
    let mut workflow_attempts = 0u128;
    for workflow in &workflows {
        let index = match workflow.state {
            hub_core::workflow::WorkflowState::Created => 0,
            hub_core::workflow::WorkflowState::Running => 1,
            hub_core::workflow::WorkflowState::Succeeded => 2,
            hub_core::workflow::WorkflowState::Failed => 3,
            hub_core::workflow::WorkflowState::Cancelled => 4,
        };
        workflow_counts[index] = workflow_counts[index].saturating_add(1);
        for step in &workflow.steps {
            workflow_attempts = workflow_attempts.saturating_add(step.attempts.len() as u128);
        }
    }

    let artifact_bytes = artifacts
        .iter()
        .fold(0u128, |total, artifact| total.saturating_add(u128::from(artifact.size)));

    let snapshot = MetricsSnapshot {
        component_manifests: u64::try_from(components.len()).unwrap_or(u64::MAX),
        runs: run_counts,
        workflows: workflow_counts,
        artifacts: u64::try_from(artifacts.len()).unwrap_or(u64::MAX),
        artifact_bytes,
        workflow_attempts,
        lifecycle_event_high_water,
        executor_backend: orchestrator.executor_backend_id().to_owned(),
    };
    Ok(render(&snapshot))
}

fn render(snapshot: &MetricsSnapshot) -> String {
    let mut out = String::new();
    metric_header(
        &mut out,
        "scirust_hub_build_info",
        "Build information for the running SciRust Hub.",
    );
    out.push_str(&format!(
        "scirust_hub_build_info{{version=\"{}\"}} 1\n",
        escape_label(env!("CARGO_PKG_VERSION"))
    ));

    metric_header(
        &mut out,
        "scirust_hub_executor_info",
        "Configured execution backend for this Hub daemon.",
    );
    out.push_str(&format!(
        "scirust_hub_executor_info{{backend=\"{}\"}} 1\n",
        escape_label(&snapshot.executor_backend)
    ));

    metric_header(
        &mut out,
        "scirust_hub_component_manifests",
        "Number of registered component manifest versions.",
    );
    sample(
        &mut out,
        "scirust_hub_component_manifests",
        snapshot.component_manifests,
    );

    metric_header(
        &mut out,
        "scirust_hub_run_records",
        "Number of persisted run records by current state.",
    );
    for (state, value) in [
        ("created", snapshot.runs[0]),
        ("validated", snapshot.runs[1]),
        ("queued", snapshot.runs[2]),
        ("running", snapshot.runs[3]),
        ("succeeded", snapshot.runs[4]),
        ("failed", snapshot.runs[5]),
        ("cancelled", snapshot.runs[6]),
    ] {
        out.push_str(&format!(
            "scirust_hub_run_records{{state=\"{state}\"}} {value}\n"
        ));
    }

    metric_header(
        &mut out,
        "scirust_hub_workflow_records",
        "Number of persisted workflow records by current state.",
    );
    for (state, value) in [
        ("created", snapshot.workflows[0]),
        ("running", snapshot.workflows[1]),
        ("succeeded", snapshot.workflows[2]),
        ("failed", snapshot.workflows[3]),
        ("cancelled", snapshot.workflows[4]),
    ] {
        out.push_str(&format!(
            "scirust_hub_workflow_records{{state=\"{state}\"}} {value}\n"
        ));
    }

    metric_header(
        &mut out,
        "scirust_hub_workflow_attempts",
        "Number of workflow step attempts currently retained in provenance.",
    );
    sample(
        &mut out,
        "scirust_hub_workflow_attempts",
        snapshot.workflow_attempts,
    );

    metric_header(
        &mut out,
        "scirust_hub_artifacts",
        "Number of persisted artifact metadata records.",
    );
    sample(&mut out, "scirust_hub_artifacts", snapshot.artifacts);

    metric_header(
        &mut out,
        "scirust_hub_artifact_bytes",
        "Sum of logical artifact sizes referenced by persisted artifact metadata.",
    );
    sample(
        &mut out,
        "scirust_hub_artifact_bytes",
        snapshot.artifact_bytes,
    );

    metric_header(
        &mut out,
        "scirust_hub_lifecycle_event_high_water_sequence",
        "Highest committed store-local lifecycle event sequence, or zero when empty.",
    );
    sample(
        &mut out,
        "scirust_hub_lifecycle_event_high_water_sequence",
        snapshot.lifecycle_event_high_water,
    );
    out
}

fn metric_header(out: &mut String, name: &str, help: &str) {
    out.push_str("# HELP ");
    out.push_str(name);
    out.push(' ');
    out.push_str(help);
    out.push('\n');
    out.push_str("# TYPE ");
    out.push_str(name);
    out.push_str(" gauge\n");
}

fn sample(out: &mut String, name: &str, value: impl std::fmt::Display) {
    out.push_str(name);
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renderer_is_stable_low_cardinality_and_line_terminated() {
        let snapshot = MetricsSnapshot {
            component_manifests: 2,
            runs: [1, 2, 3, 4, 5, 6, 7],
            workflows: [8, 9, 10, 11, 12],
            artifacts: 13,
            artifact_bytes: 14,
            workflow_attempts: 15,
            lifecycle_event_high_water: 16,
            executor_backend: "remote\nworker\"x".into(),
        };
        let text = render(&snapshot);
        assert!(text.ends_with('\n'));
        assert!(text.contains("scirust_hub_run_records{state=\"failed\"} 6\n"));
        assert!(text.contains("scirust_hub_artifact_bytes 14\n"));
        assert!(text.contains("backend=\"remote\\nworker\\\"x\""));
        assert!(!text.contains("run_id="));
        assert!(!text.contains("workflow_id="));
        assert!(!text.contains("artifact_id="));
    }
}
''')

# ---------------------------------------------------------------------------
# HTTP /metrics, protected by exactly the same bearer middleware as /api/v1.
# ---------------------------------------------------------------------------
replace_once(
    "crates/hub-api/src/lib.rs",
    "//! `spawn_blocking`.\n\n",
    "//! `spawn_blocking`.\n\nmod metrics;\n\n",
)
replace_once(
    "crates/hub-api/src/lib.rs",
    '''    /// Enables bearer authentication for all `/api/v1/*` routes. Only a
    /// SHA-256 verifier is retained in shared state; the plaintext token is
''',
    '''    /// Enables bearer authentication for protected control-plane routes
    /// (`/api/v1/*` and `/metrics`). Only a SHA-256 verifier is retained in
    /// shared state; the plaintext token is
''',
)
replace_once(
    "crates/hub-api/src/lib.rs",
    '''        .route("/api/v1/events", get(list_lifecycle_events))
        .route_layer(middleware::from_fn_with_state(
''',
    '''        .route("/api/v1/events", get(list_lifecycle_events))
        .route("/metrics", get(prometheus_metrics))
        .route_layer(middleware::from_fn_with_state(
''',
)
replace_once(
    "crates/hub-api/src/lib.rs",
    "// ----------------------------------------------------------------------\n// Handlers\n// ----------------------------------------------------------------------\n\n",
    r'''// ----------------------------------------------------------------------
// Handlers
// ----------------------------------------------------------------------

async fn prometheus_metrics(State(state): State<HubState>) -> Response {
    let orchestrator = state.orchestrator.clone();
    let events = state.events.clone();
    let rendered = tokio::task::spawn_blocking(move || {
        metrics::collect_and_render(orchestrator.as_ref(), events.as_ref())
    })
    .await;
    match joined(rendered) {
        Ok(text) => (
            [(
                header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8",
            )],
            text,
        )
            .into_response(),
        Err(response) => response,
    }
}

''',
)

# Extend auth regression to ensure /metrics is protected too.
replace_once(
    "crates/hub-api/src/lib.rs",
    '''        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body["error"]["code"], "unauthorized");

        let (status, body) = send(
            app.clone(),
            Request::builder()
                .uri("/api/v1/components")
                .header("authorization", "Bearer control-secret")
''',
    r'''        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body["error"]["code"], "unauthorized");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .expect("req"),
            )
            .await
            .expect("metrics response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let (status, body) = send(
            app.clone(),
            Request::builder()
                .uri("/api/v1/components")
                .header("authorization", "Bearer control-secret")
''',
)

# Add metrics integration test immediately before lifecycle endpoint test.
replace_once(
    "crates/hub-api/src/lib.rs",
    '''    #[tokio::test]
    async fn lifecycle_events_endpoint_is_cursor_paginated() {
''',
    r'''    #[tokio::test]
    async fn metrics_are_prometheus_text_and_derived_from_authoritative_state() {
        let (state, _clock, _dir) = test_state();
        let events = Arc::new(InMemoryLifecycleEvents::default());
        events
            .record(hub_core::NewLifecycleEvent::new(
                10,
                hub_core::LifecycleEventKind::RunCreated,
                hub_core::LifecycleEntityType::Run,
                "example-run",
                BTreeMap::new(),
            ))
            .unwrap();
        let request: proto::RegisterComponentRequest =
            serde_json::from_str(&sample_manifest_json()).unwrap();
        let manifest: ComponentManifest = request.manifest.into();
        state.orchestrator.register_component(manifest).unwrap();
        state
            .orchestrator
            .ingest_artifact(
                "metrics-input".into(),
                "application/octet-stream".into(),
                b"abc",
            )
            .unwrap();

        let app = router(state.with_event_repository(events));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("content-type").and_then(|v| v.to_str().ok()),
            Some("text/plain; version=0.0.4; charset=utf-8")
        );
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(text.ends_with('\n'));
        assert!(text.contains("scirust_hub_component_manifests 1\n"));
        assert!(text.contains("scirust_hub_artifacts 1\n"));
        assert!(text.contains("scirust_hub_artifact_bytes 3\n"));
        assert!(text.contains("scirust_hub_lifecycle_event_high_water_sequence 1\n"));
        assert!(text.contains("scirust_hub_executor_info{backend=\"process\"} 1\n"));
        assert!(text.contains("scirust_hub_run_records{state=\"queued\"} 0\n"));
    }

    #[tokio::test]
    async fn lifecycle_events_endpoint_is_cursor_paginated() {
''',
)

# ---------------------------------------------------------------------------
# ADR + README + CHANGELOG + report follow-up.
# ---------------------------------------------------------------------------
Path("docs/adr/0013-operational-metrics.md").write_text(r'''# ADR 0013 — Derived low-cardinality operational metrics

Status: accepted

## Decision

SciRust Hub exposes Prometheus text-format metrics at `GET /metrics`. The
endpoint uses the Prometheus text exposition format `0.0.4` with content type
`text/plain; version=0.0.4; charset=utf-8`, deterministic ordering and a final
newline.

Metrics are **derived on every scrape** from authoritative persisted Hub state:
registered component manifests, run records, workflow records, artifact
metadata and the lifecycle-event high-water sequence. No mutable metric counter
is updated on Hub write paths, so metrics cannot diverge from the control-plane
records after a restart or partial failure.

The initial series are intentionally low-cardinality:

- `scirust_hub_build_info{version=...}`;
- `scirust_hub_executor_info{backend=...}`;
- `scirust_hub_component_manifests`;
- `scirust_hub_run_records{state=...}`;
- `scirust_hub_workflow_records{state=...}`;
- `scirust_hub_workflow_attempts`;
- `scirust_hub_artifacts`;
- `scirust_hub_artifact_bytes`;
- `scirust_hub_lifecycle_event_high_water_sequence`.

No component, run, workflow, artifact or attempt UUID is exported as a metric
label. This avoids unbounded time-series cardinality and unnecessary exposure
of per-execution identifiers.

All snapshot series are gauges. Even quantities that currently tend to grow are
named as current persisted-record counts rather than counters, because future
retention/compaction could legitimately decrease them.

## Security boundary

`/metrics` is placed behind the same optional bearer middleware as `/api/v1`.
When `SCIRUST_HUB_TOKEN` is configured, scraping requires that bearer token.
Loopback deployments without a token retain the existing local-development
behavior. `/health` and `/ready` remain the only intentionally unauthenticated
operational probes.

Metrics contain aggregate counts, artifact byte totals, build version and the
configured executor backend. They do not contain bearer credentials,
environment values, process output, artifact contents or entity UUID labels.

## Cost model

A scrape lists current component/run/workflow/artifact metadata and therefore
costs O(number of retained records). The lifecycle high-water mark has a direct
repository operation and does not scan the event stream. This favors a small,
truthful implementation over a second mutable aggregation database. If future
retention volume makes scrape-time aggregation expensive, a separately proven
snapshot/cache design can be introduced without changing metric semantics.
''')

# README: add metrics section and remove missing-metrics limitation.
p = Path("README.md")
text = p.read_text()
marker = "## MCP introspection\n"
metrics_section = r'''## Metrics

Hub exposes low-cardinality Prometheus text metrics at `GET /metrics`. The
snapshot is recomputed from authoritative persisted records on every scrape;
there is no independent mutable counter store to drift after restart.

```bash
curl -H "Authorization: Bearer $SCIRUST_HUB_TOKEN" \
  http://127.0.0.1:8477/metrics
```

The initial surface reports build/executor info, component-manifest count,
run/workflow counts by state, retained workflow-attempt count, artifact count
and logical bytes, plus the lifecycle-event high-water sequence. Entity UUIDs
are deliberately not used as labels. `/metrics` follows the same bearer policy
as `/api/v1`.

'''
if text.count(marker) != 1:
    raise SystemExit("README MCP marker missing")
text = text.replace(marker, metrics_section + marker, 1)
text = text.replace(
    "- The lifecycle stream exists, but an exported metrics surface has not yet\n  landed.\n",
    "",
)
p.write_text(text)

# CHANGELOG add metrics bullet after lifecycle log.
p = Path("CHANGELOG.md")
text = p.read_text()
needle = '''- Durable append-only lifecycle event log (ADR-0012): SQLite migration v3
  records component, artifact, run, workflow and workflow-attempt lifecycle
  changes in the same transaction as authoritative metadata writes. Cursor
  reads are exposed as `GET /api/v1/events?after=&limit=`, `scirust-hub event
  list`, and read-only MCP tool `hub.list_events`; ephemeral memory mode uses
  an equivalent composite store.
'''
addition = needle + '''- Derived low-cardinality operational metrics (ADR-0013): authenticated
  `GET /metrics` emits Prometheus text format 0.0.4 from authoritative
  component/run/workflow/artifact state plus the lifecycle event high-water
  mark. No mutable metrics store or entity-UUID labels are introduced.
'''
if text.count(needle) != 1:
    raise SystemExit("CHANGELOG lifecycle bullet missing")
p.write_text(text.replace(needle, addition, 1))

# Report: mark metrics landed and choose the next explicit remaining systems gap.
p = Path("docs/AUTONOMOUS_IMPLEMENTATION_REPORT.md")
text = p.read_text()
text = text.replace(
    "12. a durable append-only lifecycle chronology, committed transactionally with\n    authoritative SQLite metadata mutations and exposed through HTTP, CLI and\n    MCP cursor reads.\n",
    "12. a durable append-only lifecycle chronology, committed transactionally with\n    authoritative SQLite metadata mutations and exposed through HTTP, CLI and\n    MCP cursor reads;\n13. a low-cardinality Prometheus metrics surface derived on scrape from\n    authoritative persisted records and the event high-water mark.\n",
)
text = text.replace(
    "- An exported metrics surface is still missing even though the durable event\n  chronology now provides a strong source for operational counters.\n",
    "",
)
old_recommendation = '''## Recommended next implementation

Build a small operational metrics surface from authoritative Hub state and/or
the durable lifecycle chronology without introducing a second mutable counter
store. Prefer low-cardinality metrics (run/workflow terminal states, artifact
counts/bytes, event high-water mark, executor/backend identity where safe) and
avoid component/run/workflow UUID labels. The endpoint's authentication and
information-disclosure boundary must be explicit, and metric derivation must
remain reproducible from persisted Hub data.
'''
new_recommendation = '''## Recommended next implementation

The next largest execution-scale gap is multi-worker discovery/placement. The
current remote backend deliberately targets one configured worker endpoint.
Any expansion should preserve the existing `Executor` authority, lease/result
idempotency and fail-closed evidence model rather than hiding distributed state
behind a best-effort load balancer. Capability/resource matching, worker
registration expiry and deterministic placement evidence should be designed as
an explicit scheduler contract before implementation.
'''
if old_recommendation not in text:
    raise SystemExit("report recommendation block missing")
p.write_text(text.replace(old_recommendation, new_recommendation, 1))

print("metrics surface transformations complete")
