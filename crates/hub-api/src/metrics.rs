//! Low-cardinality operational metrics derived from authoritative Hub state.
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

    let artifact_bytes = artifacts.iter().fold(0u128, |total, artifact| {
        total.saturating_add(u128::from(artifact.size))
    });

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
