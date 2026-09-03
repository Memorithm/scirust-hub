use hub_core::capability::CapabilityName;
use hub_core::component::{ComponentManifest, ExecutionBinding};

const ELASTIC_PLAN_CONTRACT: &str = "elastic.soup.run-resource-plan@1.0.0";
const ELASTIC_PLAN_MEDIA_TYPE: &str = "application/vnd.elastic.soup.run-resource-plan.v1+json";
const ELASTIC_SOURCE_MERGE: &str = "6e0952e59842eb9c14f808266d6f3eb0b1f33014";
const SOUP_QUALIFIED_COMMIT: &str = "05b646523727925990530667e7012ede50bd30b2";

#[test]
fn soup_elastic_training_contract_is_explicit_and_versioned() {
    let manifest: ComponentManifest = serde_json::from_str(include_str!(
        "../../../examples/soup-train-elastic-component.json"
    ))
    .expect("SOUP Elastic manifest must deserialize");
    manifest
        .validate()
        .expect("SOUP Elastic manifest must satisfy Hub v1 validation");

    let capability_name = CapabilityName::parse("llm.train.elastic").expect("capability name");
    let capability = manifest
        .capability(&capability_name)
        .expect("manifest must publish llm.train.elastic");
    assert_eq!(capability.contract_version.to_string(), "1.0.0");
    assert_eq!(
        capability
            .inputs
            .iter()
            .map(|port| port.name.as_str())
            .collect::<Vec<_>>(),
        ["config", "dataset", "resource_plan"]
    );
    assert_eq!(
        capability
            .outputs
            .iter()
            .map(|port| port.name.as_str())
            .collect::<Vec<_>>(),
        ["model_bundle", "report"]
    );
    assert_eq!(
        capability
            .properties
            .get("elastic.resource_plan_contract")
            .map(String::as_str),
        Some(ELASTIC_PLAN_CONTRACT)
    );
    assert_eq!(
        capability
            .properties
            .get("elastic.resource_plan_media_type")
            .map(String::as_str),
        Some(ELASTIC_PLAN_MEDIA_TYPE)
    );
    assert_eq!(
        capability
            .properties
            .get("elastic.source_merge")
            .map(String::as_str),
        Some(ELASTIC_SOURCE_MERGE)
    );
    assert_eq!(
        capability
            .properties
            .get("ml.placement_enforcement")
            .map(String::as_str),
        Some("component_preflight")
    );
    assert_eq!(
        capability.properties.get("ml.memory").map(String::as_str),
        Some("elastic_plan_plus_soup_runtime_preflight")
    );

    assert_eq!(
        manifest
            .metadata
            .get("upstream_qualified_commit")
            .map(String::as_str),
        Some(SOUP_QUALIFIED_COMMIT)
    );
    assert_eq!(
        manifest
            .metadata
            .get("elastic_source_merge")
            .map(String::as_str),
        Some(ELASTIC_SOURCE_MERGE)
    );

    let ExecutionBinding::Process(binding) =
        manifest.execution.as_ref().expect("execution binding");
    assert_eq!(binding.program, "python3");
    assert!(binding
        .args
        .iter()
        .any(|arg| arg == "/opt/scirust-hub/libexec/soup_elastic_hub_adapter.py"));
    for expected in [
        "{input:config}",
        "{input:dataset}",
        "{input:resource_plan}",
        "{output:model_bundle}",
        "{output:report}",
    ] {
        assert!(
            binding.args.iter().any(|arg| arg == expected),
            "missing execution placeholder {expected}"
        );
    }
}

#[test]
fn legacy_soup_training_contract_does_not_silently_gain_an_elastic_input() {
    let manifest: ComponentManifest =
        serde_json::from_str(include_str!("../../../examples/soup-train-component.json"))
            .expect("legacy SOUP training manifest must deserialize");
    manifest.validate().expect("legacy manifest remains valid");

    let capability_name = CapabilityName::parse("llm.train").expect("capability name");
    let capability = manifest
        .capability(&capability_name)
        .expect("legacy manifest must still publish llm.train");
    assert_eq!(capability.contract_version.to_string(), "1.0.0");
    assert_eq!(
        capability
            .inputs
            .iter()
            .map(|port| port.name.as_str())
            .collect::<Vec<_>>(),
        ["config", "dataset"]
    );
    assert!(!capability
        .properties
        .contains_key("elastic.resource_plan_contract"));
}
