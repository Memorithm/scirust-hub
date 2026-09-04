use hub_core::capability::CapabilityName;
use hub_core::component::{ComponentManifest, ExecutionBinding};

const VERIFY_ADAPTER_MERGE: &str = "89f485633e842017781778eb1568b5306e0a5570";
const VERIFY_PROCESS_HEAD: &str = "71062d4b28ff1ed8bd26bbb76a643d15e07354cb";
const VERIFY_PROCESS_MERGE: &str = "4c2db0832d8148f62261e67254f8bf00f80e808c";
const HUB_FORGE_SOUP_MERGE: &str = "074cf2c6e00a0b142fe46d1558c8b32df9228859";
const FORGE_DOMAIN_MERGE: &str = "1385c71a541419f15a558a5e94bc8a4a60567a4a";
const FORGE_RUNNER_MERGE: &str = "9e1f3fc568c176f401735c121780d9fbe6834f5d";
const SOUP_QUALIFIED_COMMIT: &str = "05b646523727925990530667e7012ede50bd30b2";

#[test]
fn forge_soup_verify_contract_is_explicit_versioned_and_source_pinned() {
    let manifest: ComponentManifest = serde_json::from_str(include_str!(
        "../../../examples/forge-soup-verify-component.json"
    ))
    .expect("Forge SOUP Verify component manifest must deserialize");
    manifest
        .validate()
        .expect("Forge SOUP Verify component manifest must satisfy Hub v1 validation");

    let name = CapabilityName::parse("llm.verify.forge_soup").expect("capability name");
    let capability = manifest
        .capability(&name)
        .expect("manifest must publish llm.verify.forge_soup");
    assert_eq!(capability.contract_version.to_string(), "1.0.0");
    assert_eq!(
        capability
            .inputs
            .iter()
            .map(|port| port.name.as_str())
            .collect::<Vec<_>>(),
        ["report", "evidence_bundle"]
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
        ("verify.adapter_merge", VERIFY_ADAPTER_MERGE),
        ("verify.process_merge", VERIFY_PROCESS_MERGE),
        ("verify.process_final_exact_head", VERIFY_PROCESS_HEAD),
        ("hub.forge_soup_merge", HUB_FORGE_SOUP_MERGE),
        ("forge.domain_merge", FORGE_DOMAIN_MERGE),
        ("forge.runner_merge", FORGE_RUNNER_MERGE),
        ("soup.qualified_commit", SOUP_QUALIFIED_COMMIT),
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
            .get("verify.model_quality_claim")
            .map(String::as_str),
        Some("not_established")
    );
    assert_eq!(
        capability
            .properties
            .get("verify.performance_claim")
            .map(String::as_str),
        Some("not_established")
    );

    let ExecutionBinding::Process(binding) =
        manifest.execution.as_ref().expect("execution binding");
    assert_eq!(
        binding.program,
        "/opt/scirust-hub/libexec/scirust-verify-forge-soup"
    );
    assert_eq!(
        binding.args,
        [
            "--report",
            "{input:report}",
            "--evidence-bundle",
            "{input:evidence_bundle}",
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
fn forge_soup_verify_edge_remains_separate_from_search_and_training() {
    let verify_manifest: ComponentManifest = serde_json::from_str(include_str!(
        "../../../examples/forge-soup-verify-component.json"
    ))
    .expect("Verify manifest");
    let forge_manifest: ComponentManifest = serde_json::from_str(include_str!(
        "../../../examples/forge-soup-posttrain-component.json"
    ))
    .expect("Forge manifest");
    let train_manifest: ComponentManifest =
        serde_json::from_str(include_str!("../../../examples/soup-train-component.json"))
            .expect("SOUP train manifest");

    verify_manifest.validate().expect("Verify manifest valid");
    forge_manifest.validate().expect("Forge manifest valid");
    train_manifest.validate().expect("SOUP train manifest valid");

    let verify_name = CapabilityName::parse("llm.verify.forge_soup").expect("Verify capability");
    let forge_name = CapabilityName::parse("llm.optimize.forge_soup").expect("Forge capability");
    let train_name = CapabilityName::parse("llm.train").expect("train capability");

    assert!(verify_manifest.capability(&verify_name).is_some());
    assert!(verify_manifest.capability(&forge_name).is_none());
    assert!(verify_manifest.capability(&train_name).is_none());
    assert!(forge_manifest.capability(&verify_name).is_none());
    assert!(train_manifest.capability(&verify_name).is_none());
}
