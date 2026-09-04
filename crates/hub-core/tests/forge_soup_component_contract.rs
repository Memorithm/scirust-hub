use hub_core::capability::CapabilityName;
use hub_core::component::{ComponentManifest, ExecutionBinding};

const FORGE_RUNNER_HEAD: &str = "dc3189591436da6e27734b2953e94eaad7057f1e";
const FORGE_RUNNER_MERGE: &str = "9e1f3fc568c176f401735c121780d9fbe6834f5d";
const FORGE_DOMAIN_MERGE: &str = "1385c71a541419f15a558a5e94bc8a4a60567a4a";
const SOUP_QUALIFIED_COMMIT: &str = "05b646523727925990530667e7012ede50bd30b2";

#[test]
fn forge_soup_posttrain_contract_is_explicit_and_versioned() {
    let manifest: ComponentManifest = serde_json::from_str(include_str!(
        "../../../examples/forge-soup-posttrain-component.json"
    ))
    .expect("Forge SOUP component manifest must deserialize");
    manifest
        .validate()
        .expect("Forge SOUP component manifest must satisfy Hub v1 validation");

    let name = CapabilityName::parse("llm.optimize.forge_soup").expect("capability name");
    let capability = manifest
        .capability(&name)
        .expect("manifest must publish llm.optimize.forge_soup");
    assert_eq!(capability.contract_version.to_string(), "1.0.0");
    assert_eq!(
        capability
            .inputs
            .iter()
            .map(|port| port.name.as_str())
            .collect::<Vec<_>>(),
        ["campaign", "config", "dataset"]
    );
    assert_eq!(
        capability
            .outputs
            .iter()
            .map(|port| port.name.as_str())
            .collect::<Vec<_>>(),
        ["report", "evidence_bundle"]
    );
    assert_eq!(
        capability
            .properties
            .get("forge.runner_merge")
            .map(String::as_str),
        Some(FORGE_RUNNER_MERGE)
    );
    assert_eq!(
        capability
            .properties
            .get("forge.domain_merge")
            .map(String::as_str),
        Some(FORGE_DOMAIN_MERGE)
    );
    assert_eq!(
        capability
            .properties
            .get("forge.distributed_workers")
            .map(String::as_str),
        Some("disabled_v1")
    );
    assert_eq!(
        capability
            .properties
            .get("forge.external_isolation")
            .map(String::as_str),
        Some("not_provided_fail_closed")
    );
    assert_eq!(
        capability
            .properties
            .get("soup.qualified_commit")
            .map(String::as_str),
        Some(SOUP_QUALIFIED_COMMIT)
    );

    assert_eq!(
        manifest
            .metadata
            .get("forge_runner_final_exact_head")
            .map(String::as_str),
        Some(FORGE_RUNNER_HEAD)
    );
    assert_eq!(
        manifest
            .metadata
            .get("forge_runner_merge")
            .map(String::as_str),
        Some(FORGE_RUNNER_MERGE)
    );

    let ExecutionBinding::Process(binding) =
        manifest.execution.as_ref().expect("execution binding");
    assert_eq!(binding.program, "python3");
    assert!(binding
        .args
        .iter()
        .any(|arg| arg == "/opt/scirust-hub/libexec/forge_soup_hub_adapter.py"));
    for expected in [
        "{input:campaign}",
        "{input:config}",
        "{input:dataset}",
        "{output:report}",
        "{output:evidence_bundle}",
    ] {
        assert!(
            binding.args.iter().any(|arg| arg == expected),
            "missing execution placeholder {expected}"
        );
    }
}

#[test]
fn forge_soup_edge_remains_separate_from_direct_soup_training() {
    let forge_manifest: ComponentManifest = serde_json::from_str(include_str!(
        "../../../examples/forge-soup-posttrain-component.json"
    ))
    .expect("Forge SOUP manifest");
    let soup_manifest: ComponentManifest =
        serde_json::from_str(include_str!("../../../examples/soup-train-component.json"))
            .expect("SOUP train manifest");

    forge_manifest
        .validate()
        .expect("Forge SOUP manifest valid");
    soup_manifest.validate().expect("SOUP train manifest valid");

    let forge_name = CapabilityName::parse("llm.optimize.forge_soup").expect("Forge capability");
    let soup_name = CapabilityName::parse("llm.train").expect("SOUP capability");
    assert!(forge_manifest.capability(&forge_name).is_some());
    assert!(soup_manifest.capability(&soup_name).is_some());
    assert!(forge_manifest.capability(&soup_name).is_none());
    assert!(soup_manifest.capability(&forge_name).is_none());
}
