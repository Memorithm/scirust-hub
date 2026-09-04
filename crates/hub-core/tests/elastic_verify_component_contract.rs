use hub_core::capability::CapabilityName;
use hub_core::component::{ComponentManifest, ExecutionBinding};

const VERIFY_PROCESS_HEAD: &str = "c685e3d596ab7f5e77106e001792a0c3b5d59837";
const VERIFY_PROCESS_MERGE: &str = "d65c924699d1b1061d0359669884ca9742a3baa6";
const ELASTIC_PROCESS_HEAD: &str = "571d0deb8921df54502fbb35909dd8830cbf4fb4";
const ELASTIC_PROCESS_MERGE: &str = "9e51879b96e54c812b6a265fe5901e960bbe6250";
const HUB_ELASTIC_RUNTIME_MERGE: &str = "c946ca413c145128dacb22c964fb1f87b39bfc61";

#[test]
fn elastic_verify_contract_is_explicit_versioned_and_source_pinned() {
    let manifest: ComponentManifest = serde_json::from_str(include_str!(
        "../../../examples/elastic-verify-component.json"
    ))
    .expect("Elastic Verify component manifest must deserialize");
    manifest
        .validate()
        .expect("Elastic Verify component manifest must satisfy Hub v1 validation");

    let name = CapabilityName::parse("resource.elastic.verify").expect("capability name");
    let capability = manifest
        .capability(&name)
        .expect("manifest must publish resource.elastic.verify");
    assert_eq!(capability.contract_version.to_string(), "1.0.0");
    assert_eq!(
        capability
            .inputs
            .iter()
            .map(|port| port.name.as_str())
            .collect::<Vec<_>>(),
        ["evidence"]
    );
    assert_eq!(
        capability
            .outputs
            .iter()
            .map(|port| port.name.as_str())
            .collect::<Vec<_>>(),
        ["dossier"]
    );

    for (property, expected) in [
        ("verify.process_merge", VERIFY_PROCESS_MERGE),
        ("verify.process_final_exact_head", VERIFY_PROCESS_HEAD),
        ("elastic.source_merge", ELASTIC_PROCESS_MERGE),
        ("elastic.source_final_exact_head", ELASTIC_PROCESS_HEAD),
        ("hub.elastic_runtime_merge", HUB_ELASTIC_RUNTIME_MERGE),
    ] {
        assert_eq!(
            capability.properties.get(property).map(String::as_str),
            Some(expected),
            "property {property} drifted"
        );
    }
    assert_eq!(
        capability
            .properties
            .get("elastic.commit_rollback_owner")
            .map(String::as_str),
        Some("ElasticXxx")
    );
    assert_eq!(
        capability
            .properties
            .get("hub.policy_interpretation")
            .map(String::as_str),
        Some("forbidden")
    );
    assert_eq!(
        capability
            .properties
            .get("verify.contract_verdict_scope")
            .map(String::as_str),
        Some("structural_evidence_conformance_only")
    );

    let ExecutionBinding::Process(binding) =
        manifest.execution.as_ref().expect("execution binding");
    assert_eq!(
        binding.program,
        "/opt/scirust-hub/libexec/scirust-verify-elastic"
    );
    assert_eq!(
        binding.args,
        [
            "--evidence",
            "{input:evidence}",
            "--output",
            "{output:dossier}",
        ]
    );
    assert_eq!(binding.outputs.len(), 1);
    assert_eq!(binding.outputs[0].name, "dossier");
    assert_eq!(
        binding.outputs[0].media_type.as_deref(),
        Some("application/vnd.scirust-verify.dossier.v1+tar")
    );
    assert!(binding.outputs[0].required);
}

#[test]
fn elastic_verify_edge_remains_separate_from_runtime_actuation() {
    let verify_manifest: ComponentManifest = serde_json::from_str(include_str!(
        "../../../examples/elastic-verify-component.json"
    ))
    .expect("Verify manifest");
    let runtime_manifest: ComponentManifest = serde_json::from_str(include_str!(
        "../../../examples/elastic-runtime-component.json"
    ))
    .expect("Elastic runtime manifest");

    verify_manifest.validate().expect("Verify manifest valid");
    runtime_manifest
        .validate()
        .expect("Elastic runtime manifest valid");

    let verify_name = CapabilityName::parse("resource.elastic.verify").expect("Verify capability");
    let run_name = CapabilityName::parse("resource.elastic.run").expect("runtime capability");

    assert!(verify_manifest.capability(&verify_name).is_some());
    assert!(verify_manifest.capability(&run_name).is_none());
    assert!(runtime_manifest.capability(&run_name).is_some());
    assert!(runtime_manifest.capability(&verify_name).is_none());
}
