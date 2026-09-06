use hub_core::{CapabilityName, ComponentManifest, ExecutionBinding};

const NNIS_HF_PREFLIGHT_MANIFEST: &str =
    include_str!("../../../examples/nnis-hf-preflight-component.json");

#[test]
fn nnis_hf_preflight_component_preserves_nnis_semantics_and_artifact_boundary() {
    let manifest: ComponentManifest = serde_json::from_str(NNIS_HF_PREFLIGHT_MANIFEST)
        .expect("parse NNIS HF preflight manifest");
    manifest.validate().expect("validate manifest");

    let name = CapabilityName::parse("inference.nnis.hf_preflight").expect("capability name");
    let capability = manifest.capability(&name).expect("NNIS HF preflight capability");
    assert_eq!(capability.contract_version.as_str(), "1.0.0");

    assert_eq!(capability.inputs.len(), 1);
    assert_eq!(capability.inputs[0].name, "model_bundle");
    assert!(capability.inputs[0]
        .description
        .starts_with("application/vnd.scirust-hub.soup-bundle.v1+tar"));

    assert_eq!(capability.outputs.len(), 1);
    assert_eq!(capability.outputs[0].name, "preflight");
    assert!(capability.outputs[0].description.starts_with("application/json"));
    assert!(capability.outputs[0]
        .description
        .contains("schema=nnis.hf-preflight@1"));

    for (key, expected) in [
        ("bundle_schema", "application/vnd.scirust-hub.soup-bundle.v1+tar"),
        ("unknown_parameters", "rejected"),
        ("nnis.contract", "nnis.hf-preflight@1"),
        ("nnis.report_schema", "nnis.hf-preflight@1"),
        (
            "nnis.source_head",
            "5975b4a01c7c0825ad8ca97ce4c9e384d0cc4b67",
        ),
        (
            "nnis.source_merge",
            "b1e2766ace9a2076d4a5721f6dca2d2c7d48d04d",
        ),
        ("nnis.semantics_owner", "NNIS"),
        ("nnis.direct_execution_readiness_owner", "NNIS"),
        ("nnis.cuda_used_by_preflight", "false"),
        ("nnis.network_used_by_preflight", "false"),
        ("nnis.promotion_authorized", "false"),
        ("nnis.serving_performance_verified", "false"),
        ("nnis.general_model_family_support_verified", "false"),
        ("nnis.new_physical_evidence_created", "false"),
        ("hub.policy_interpretation", "forbidden"),
        ("hub.report_contract_validation", "schema_only"),
        ("ml.resource_contract", "hub.ml.resource-requirements@1.0.0"),
        ("ml.backend", "nnis"),
        ("ml.device", "operation_defined"),
        ("ml.dtype", "model_defined"),
        ("ml.accelerator", "none"),
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
            "/opt/scirust-hub/libexec/nnis_hf_preflight_hub_adapter.py",
            "--bundle",
            "{input:model_bundle}",
            "--report",
            "{output:preflight}",
            "--params",
            "{params}",
        ]
    );
    assert_eq!(process.outputs.len(), 1);
    assert_eq!(process.outputs[0].name, "preflight");
    assert_eq!(process.outputs[0].path, "outputs/nnis-hf-preflight.json");
    assert_eq!(process.outputs[0].media_type.as_deref(), Some("application/json"));
    assert!(process.outputs[0].required);
}
