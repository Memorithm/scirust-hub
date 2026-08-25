//! Capability declarations: what a component can do, independent of how.
//!
//! Names are validated open strings (`namespace.action`) because the
//! ecosystem nomenclature is still stabilizing; the Hub indexes whatever is
//! truthfully declared instead of imposing a closed vocabulary.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::CoreError;
use crate::version::Version;

const MAX_NAME_LEN: usize = 128;

/// A capability name such as `tensor.execute` or `demo.echo`.
///
/// Grammar: one or more dot-separated segments; each segment starts with a
/// lowercase ASCII letter followed by lowercase letters, digits or `_`,
/// 1..=64 characters each. Validation happens on every construction path,
/// including deserialization.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CapabilityName(String);

impl CapabilityName {
    /// # Errors
    /// [`CoreError::Validation`] when the name violates the grammar above.
    pub fn parse(raw: &str) -> Result<Self, CoreError> {
        if raw.is_empty() || raw.len() > MAX_NAME_LEN {
            return Err(CoreError::Validation(format!(
                "capability name must be 1..={MAX_NAME_LEN} characters"
            )));
        }
        for segment in raw.split('.') {
            validate_segment(segment)?;
        }
        Ok(Self(raw.to_owned()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn validate_segment(segment: &str) -> Result<(), CoreError> {
    let valid = !segment.is_empty()
        && segment.len() <= 64
        && segment.starts_with(|c: char| c.is_ascii_lowercase())
        && segment
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if valid {
        Ok(())
    } else {
        Err(CoreError::Validation(format!(
            "capability segment {segment:?} must match [a-z][a-z0-9_]{{0,63}}"
        )))
    }
}

impl fmt::Display for CapabilityName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for CapabilityName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CapabilityName({})", self.0)
    }
}

impl Serialize for CapabilityName {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for CapabilityName {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Self::parse(&raw).map_err(serde::de::Error::custom)
    }
}

/// One named data slot of a capability (input or output).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Port {
    pub name: String,
    /// Free-form media/schema label; contracts will tighten as ecosystem
    /// interfaces stabilize. Kept descriptive on purpose.
    #[serde(default)]
    pub description: String,
}

/// What a component declares it can do. Purely declarative: registration of a
/// capability never runs anything.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capability {
    pub name: CapabilityName,
    /// Version of this capability's contract, not of the component.
    pub contract_version: Version,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inputs: Vec<Port>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outputs: Vec<Port>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub properties: BTreeMap<String, String>,
}

impl Capability {
    /// Validates structural invariants (unique port names per side).
    ///
    /// # Errors
    /// [`CoreError::InvalidManifest`] when port names repeat within one side.
    pub fn validate(&self) -> Result<(), CoreError> {
        for side in [&self.inputs, &self.outputs] {
            let mut seen = std::collections::BTreeSet::new();
            for port in side {
                if port.name.is_empty() || port.name.len() > 64 {
                    return Err(CoreError::InvalidManifest(format!(
                        "port name {:?} must be 1..=64 characters",
                        port.name
                    )));
                }
                if !seen.insert(port.name.as_str()) {
                    return Err(CoreError::InvalidManifest(format!(
                        "duplicate port name {:?} in capability {}",
                        port.name, self.name
                    )));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_valid_names() {
        for ok in ["a.b", "tensor.execute", "x", "demo.echo_v2", "a.b.c.d"] {
            assert!(CapabilityName::parse(ok).is_ok(), "{ok} should be valid");
        }
    }

    #[test]
    fn rejects_invalid_names() {
        for bad in [
            "",
            "Tensor.execute",
            "1abc.x",
            "a..b",
            ".a",
            "a.",
            "a b.c",
            &"a".repeat(129),
        ] {
            assert!(
                CapabilityName::parse(bad).is_err(),
                "{bad:?} should be invalid"
            );
        }
    }

    #[test]
    fn deserialization_validates_names() {
        assert!(serde_json::from_str::<CapabilityName>("\"good.name\"").is_ok());
        assert!(serde_json::from_str::<CapabilityName>("\"Bad.Name\"").is_err());
    }

    #[test]
    fn capability_rejects_duplicate_ports() {
        let cap = Capability {
            name: CapabilityName::parse("t.a").expect("valid"),
            contract_version: Version::parse("1.0.0").expect("valid"),
            inputs: vec![
                Port {
                    name: "in".into(),
                    description: String::new(),
                },
                Port {
                    name: "in".into(),
                    description: String::new(),
                },
            ],
            outputs: vec![],
            properties: BTreeMap::new(),
        };
        assert!(matches!(
            cap.validate(),
            Err(CoreError::InvalidManifest(msg)) if msg.contains("duplicate port")
        ));
    }

    #[test]
    fn capability_round_trips() {
        let cap = Capability {
            name: CapabilityName::parse("artifact.produce").expect("valid"),
            contract_version: Version::parse("0.1.0").expect("valid"),
            inputs: vec![Port {
                name: "source".into(),
                description: "bytes".into(),
            }],
            outputs: vec![Port {
                name: "blob".into(),
                description: String::new(),
            }],
            properties: BTreeMap::from([("deterministic".to_owned(), "true".to_owned())]),
        };
        let encoded = serde_json::to_string(&cap).expect("serialize");
        let decoded: Capability = serde_json::from_str(&encoded).expect("deserialize");
        assert_eq!(decoded, cap);
    }
}
