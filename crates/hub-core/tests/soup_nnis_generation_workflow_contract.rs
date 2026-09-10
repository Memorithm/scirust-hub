use hub_core::{InputSource, WorkflowSpec};

const WORKFLOW: &str = include_str!("../../../examples/soup-nnis-generation-workflow.json");

#[test]
fn soup_nnis_generation_workflow_orders_preflight_before_cuda_generation() {
    let envelope: serde_json::Value =
        serde_json::from_str(WORKFLOW).expect("parse workflow envelope");
    assert_eq!(envelope["schema_version"], 1);

    let workflow: WorkflowSpec =
        serde_json::from_value(envelope["workflow"].clone()).expect("parse workflow spec");
    workflow.validate().expect("validate workflow spec");

    assert_eq!(workflow.name, "soup-train-merge-nnis-preflight-generate");
    assert_eq!(workflow.max_concurrency, 1);
    assert_eq!(workflow.steps.len(), 4);
    assert_eq!(
        workflow.topo_keys().expect("topological order"),
        vec!["train", "merge", "preflight", "generate"]
    );

    let train = &workflow.steps[0];
    assert_eq!(train.capability.as_str(), "llm.train");
    assert!(matches!(
        train.inputs.get("config"),
        Some(InputSource::Artifact { .. })
    ));
    assert!(matches!(
        train.inputs.get("dataset"),
        Some(InputSource::Artifact { .. })
    ));

    let merge = &workflow.steps[1];
    assert_eq!(merge.capability.as_str(), "llm.merge.nnis");
    match merge.inputs.get("adapter_bundle") {
        Some(InputSource::FromStep { key, output }) => {
            assert_eq!(key, "train");
            assert_eq!(output, "file:model_bundle");
        }
        other => panic!("unexpected merge input source: {other:?}"),
    }

    let preflight = &workflow.steps[2];
    assert_eq!(preflight.capability.as_str(), "inference.nnis.hf_preflight");
    match preflight.inputs.get("model_bundle") {
        Some(InputSource::FromStep { key, output }) => {
            assert_eq!(key, "merge");
            assert_eq!(output, "file:merged_model_bundle");
        }
        other => panic!("unexpected preflight input source: {other:?}"),
    }

    let generate = &workflow.steps[3];
    assert_eq!(
        generate.component.to_string(),
        "0e52d02f-f74f-40cf-a74e-37edc0427f41"
    );
    assert_eq!(generate.capability.as_str(), "inference.nnis.hf_generate");
    assert_eq!(generate.after, vec!["preflight"]);
    assert_eq!(generate.parameters["prompt"], "Hello from SOUP to NNIS");
    assert_eq!(generate.parameters["device"], 0);
    assert_eq!(generate.parameters["max_new_tokens"], 16);
    match generate.inputs.get("model_bundle") {
        Some(InputSource::FromStep { key, output }) => {
            assert_eq!(key, "merge");
            assert_eq!(output, "file:merged_model_bundle");
        }
        other => panic!("unexpected generation input source: {other:?}"),
    }

    let dependencies = workflow.dependencies();
    assert_eq!(
        dependencies["generate"]
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        vec!["merge", "preflight"]
    );
}
