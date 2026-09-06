use hub_core::{InputSource, WorkflowSpec};

const WORKFLOW: &str = include_str!("../../../examples/soup-nnis-preflight-workflow.json");

#[test]
fn soup_nnis_preflight_workflow_preserves_typed_artifact_chain() {
    let envelope: serde_json::Value = serde_json::from_str(WORKFLOW).expect("parse workflow envelope");
    assert_eq!(envelope["schema_version"], 1);

    let workflow: WorkflowSpec =
        serde_json::from_value(envelope["workflow"].clone()).expect("parse workflow spec");
    workflow.validate().expect("validate workflow spec");

    assert_eq!(workflow.name, "soup-train-merge-nnis-preflight");
    assert_eq!(workflow.max_concurrency, 1);
    assert_eq!(workflow.steps.len(), 3);
    assert_eq!(
        workflow.topo_keys().expect("topological order"),
        vec!["train", "merge", "preflight"]
    );

    let train = &workflow.steps[0];
    assert_eq!(train.key, "train");
    assert_eq!(train.component.to_string(), "00000000-0000-0000-0000-000000000003");
    assert_eq!(train.capability.as_str(), "llm.train");
    assert!(matches!(train.inputs.get("config"), Some(InputSource::Artifact { .. })));
    assert!(matches!(train.inputs.get("dataset"), Some(InputSource::Artifact { .. })));

    let merge = &workflow.steps[1];
    assert_eq!(merge.key, "merge");
    assert_eq!(merge.component.to_string(), "6df1e39d-b861-4eb9-a2a6-4d696c74bc75");
    assert_eq!(merge.capability.as_str(), "llm.merge.nnis");
    match merge.inputs.get("adapter_bundle") {
        Some(InputSource::FromStep { key, output }) => {
            assert_eq!(key, "train");
            assert_eq!(output, "file:model_bundle");
        }
        other => panic!("unexpected merge input source: {other:?}"),
    }

    let preflight = &workflow.steps[2];
    assert_eq!(preflight.key, "preflight");
    assert_eq!(
        preflight.component.to_string(),
        "9f13791c-0a54-4c2c-8abf-0d0644d33437"
    );
    assert_eq!(preflight.capability.as_str(), "inference.nnis.hf_preflight");
    match preflight.inputs.get("model_bundle") {
        Some(InputSource::FromStep { key, output }) => {
            assert_eq!(key, "merge");
            assert_eq!(output, "file:merged_model_bundle");
        }
        other => panic!("unexpected preflight input source: {other:?}"),
    }

    let dependencies = workflow.dependencies();
    assert!(dependencies["train"].is_empty());
    assert_eq!(dependencies["merge"].iter().map(String::as_str).collect::<Vec<_>>(), vec!["train"]);
    assert_eq!(
        dependencies["preflight"]
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        vec!["merge"]
    );
}
