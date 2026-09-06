use hub_core::{CapabilityName, ComponentManifest, ExecutionBinding};

const SOUP_NNIS_MERGE_MANIFEST: &str =
    include_str!("../../../examples/soup-nnis-merge-component.json");

#[test]
fn soup_nnis_merge_component_keeps_merge_in_soup_and_admission_in_nnis() {
    let manifest: ComponentManifest =
        serde_json::from_str(SOUP_NNIS_MERGE_MANIFEST).expect("parse SOUP NNIS merge manifest");
    manifest.validate().expect("validate manifest");

    let name = CapabilityName::parse("llm.merge.nnis").expect("capability name");
    let capability = manifest
        .capability(&name)
        .expect("SOUP NNIS merge capability");
    assert_eq!(capability.contract_version.as_str(), "1.0.0");

    assert_eq!(capability.inputs.len(), 1);
    assert_eq!(capability.inputs[0].name, "adapter_bundle");
    assert!(capability.inputs[0]
        .description
        .starts_with("application/vnd.scirust-hub.soup-bundle.v1+tar"));
    assert!(capability.inputs[0]
        .description
        .contains("adapter_config.json"));

    assert_eq!(capability.outputs.len(), 2);
    assert_eq!(capability.outputs[0].name, "merged_model_bundle");
    assert_eq!(capability.outputs[1].name, "report");

    for (key, expected) in [
        ("soup.operation", "merge"),
        ("soup.semantics_owner", "SOUP"),
        (
            "soup.source_head",
            "9f1e48b33a5fcda7e621e612ec9305cb52a38e07",
        ),
        ("soup.merge_dtype", "float32"),
        ("soup.merge_save_format", "fp16"),
        ("soup.merge_hub", "hf"),
        ("soup.base_resolution", "adapter_config_auto_detect"),
        ("soup.trust_remote_code", "false"),
        (
            "soup.network_access",
            "environment_dependent_base_resolution",
        ),
        ("nnis.consumer_contract", "nnis.hf-preflight@1"),
        (
            "nnis.consumer_capability",
            "inference.nnis.hf_preflight@1.0.0",
        ),
        ("nnis.admission_owner", "NNIS"),
        ("hub.merge_implementation", "forbidden"),
        (
            "hub.output_validation",
            "directory_and_deterministic_bundle_only",
        ),
        ("ml.resource_contract", "hub.ml.resource-requirements@1.0.0"),
        ("ml.backend", "soup"),
        ("ml.device", "cpu"),
        ("ml.dtype", "float32"),
        ("ml.accelerator", "none"),
        ("ml.memory", "operation_defined"),
        ("ml.placement_enforcement", "component_preflight"),
        ("sandbox", "none"),
    ] {
        assert_eq!(
            capability.properties.get(key).map(String::as_str),
            Some(expected),
            "property {key}"
        );
    }

    let execution = manifest.execution.as_ref().expect("execution binding");
    let ExecutionBinding::Process(process) = execution;
    assert_eq!(process.program, "python3");
    assert_eq!(
        process.args,
        vec![
            "/opt/scirust-hub/libexec/soup_nnis_merge_hub_adapter.py",
            "--adapter-bundle",
            "{input:adapter_bundle}",
            "--merged-bundle",
            "{output:merged_model_bundle}",
            "--report",
            "{output:report}",
            "--params",
            "{params}",
        ]
    );
    assert_eq!(process.outputs.len(), 2);
    assert_eq!(process.outputs[0].name, "merged_model_bundle");
    assert_eq!(
        process.outputs[0].media_type.as_deref(),
        Some("application/vnd.scirust-hub.soup-bundle.v1+tar")
    );
    assert!(process.outputs[0].required);
    assert_eq!(process.outputs[1].name, "report");
    assert_eq!(
        process.outputs[1].media_type.as_deref(),
        Some("application/vnd.scirust-hub.soup-nnis-merge-report.v1+json")
    );
    assert!(process.outputs[1].required);
}
