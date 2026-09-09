use hub_core::{CapabilityName, ComponentManifest, ExecutionBinding};

const NNIS_HF_GENERATION_MANIFEST: &str =
    include_str!("../../../examples/nnis-hf-generation-component.json");

#[test]
fn nnis_hf_generation_component_preserves_nnis_semantics_and_artifact_boundary() {
    let manifest: ComponentManifest = serde_json::from_str(NNIS_HF_GENERATION_MANIFEST)
        .expect("parse NNIS HF generation manifest");
    manifest.validate().expect("validate manifest");

    let name = CapabilityName::parse("inference.nnis.hf_generate").expect("capability name");
    let capability = manifest
        .capability(&name)
        .expect("NNIS HF generation capability");
    assert_eq!(capability.contract_version.as_str(), "1.0.0");

    assert_eq!(capability.inputs.len(), 1);
    assert_eq!(capability.inputs[0].name, "model_bundle");
    assert!(capability.inputs[0]
        .description
        .starts_with("application/vnd.scirust-hub.soup-bundle.v1+tar"));

    assert_eq!(capability.outputs.len(), 1);
    assert_eq!(capability.outputs[0].name, "generation");
    assert!(capability.outputs[0]
        .description
        .starts_with("application/vnd.nnis.hf-generation.v1+json"));

    for (key, expected) in [
        (
            "bundle_schema",
            "application/vnd.scirust-hub.soup-bundle.v1+tar",
        ),
        ("unknown_parameters", "rejected"),
        ("nnis.contract", "nnis.hf-generation@1.0.0"),
        (
            "nnis.result_media_type",
            "application/vnd.nnis.hf-generation.v1+json",
        ),
        ("nnis.result_schema_version", "1"),
        (
            "nnis.source_head",
            "e5bf07b932c3d734bd28d449ddfb6cf48cd2fc61",
        ),
        (
            "nnis.source_merge",
            "21370baee6f77f2d8538660092cd0ed5630c2180",
        ),
        ("nnis.semantics_owner", "NNIS"),
        ("nnis.preflight_contract", "nnis.hf-preflight@1"),
        ("nnis.preflight_artifact_consumed", "false"),
        ("nnis.cuda_used_by_generation", "true"),
        ("nnis.network_used_by_generation", "false"),
        ("nnis.promotion_authorized", "false"),
        ("nnis.serving_performance_verified", "false"),
        ("nnis.numerical_equivalence_verified", "false"),
        ("nnis.general_model_family_support_verified", "false"),
        ("hub.policy_interpretation", "forbidden"),
        ("hub.result_contract_validation", "envelope_only"),
        ("hub.preflight_ordering", "workflow_dependency"),
        ("ml.resource_contract", "hub.ml.resource-requirements@1.0.0"),
        ("ml.backend", "nnis"),
        ("ml.device", "operation_defined"),
        ("ml.dtype", "operation_defined"),
        ("ml.accelerator", "required"),
        ("ml.memory", "runtime_preflight"),
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
            "/opt/scirust-hub/libexec/nnis_hf_generate_hub_adapter.py",
            "--bundle",
            "{input:model_bundle}",
            "--generation",
            "{output:generation}",
            "--params",
            "{params}",
        ]
    );
    assert_eq!(process.outputs.len(), 1);
    assert_eq!(process.outputs[0].name, "generation");
    assert_eq!(process.outputs[0].path, "outputs/nnis-hf-generation.json");
    assert_eq!(
        process.outputs[0].media_type.as_deref(),
        Some("application/vnd.nnis.hf-generation.v1+json")
    );
    assert!(process.outputs[0].required);
}
