//! Validated semver-shaped version strings.
//!
//! A full semver crate is not justified yet; the Hub only needs a validated,
//! orderable-by-string version interchange format. Comparison is plain
//! lexicographic on the string, which is documented behavior — versions are
//! labels here, not ordering semantics.

use std::cmp::Ordering;
use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// `MAJOR.MINOR.PATCH` with an optional `-prerelease` of ASCII identifier
/// characters. Length-capped to keep storage and logs sane.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Version(String);

impl Version {
    pub const MAX_LEN: usize = 64;

    /// Validates and wraps a version string.
    ///
    /// # Errors
    /// Returns [`crate::error::CoreError::Validation`] when the string does
    /// not match the accepted shape.
    pub fn parse(raw: &str) -> Result<Self, crate::error::CoreError> {
        if raw.is_empty() || raw.len() > Self::MAX_LEN {
            return Err(crate::error::CoreError::Validation(format!(
                "version must be 1..={} characters, got {}",
                Self::MAX_LEN,
                raw.len()
            )));
        }
        let (core, prerelease) = match raw.split_once('-') {
            Some((c, p)) => (c, Some(p)),
            None => (raw, None),
        };
        let numbers: Vec<&str> = core.split('.').collect();
        if numbers.len() != 3 {
            return Err(crate::error::CoreError::Validation(format!(
                "version {raw:?} must have exactly three numeric components"
            )));
        }
        for n in &numbers {
            if n.is_empty() || !n.bytes().all(|b| b.is_ascii_digit()) {
                return Err(crate::error::CoreError::Validation(format!(
                    "version component {n:?} in {raw:?} must be numeric"
                )));
            }
            // No leading zeros except the literal zero itself.
            if n.len() > 1 && n.starts_with('0') {
                return Err(crate::error::CoreError::Validation(format!(
                    "version component {n:?} in {raw:?} must not have leading zeros"
                )));
            }
        }
        if let Some(pre) = prerelease {
            if pre.is_empty()
                || !pre
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-')
            {
                return Err(crate::error::CoreError::Validation(format!(
                    "prerelease {pre:?} in {raw:?} contains invalid characters"
                )));
            }
        }
        Ok(Self(raw.to_owned()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Version({})", self.0)
    }
}

impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Serialize for Version {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Version {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        // Wire input is untrusted: route through the same validation as
        // explicit parsing so no invalid version can enter the domain.
        Self::parse(&raw).map_err(serde::de::Error::custom)
    }
}

impl Ord for Version {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CoreError;

    fn v(s: &str) -> Result<Version, CoreError> {
        Version::parse(s)
    }

    #[test]
    fn accepts_plain_versions() {
        assert!(v("1.2.3").is_ok());
        assert!(v("0.1.0").is_ok());
        assert!(v("10.20.30-rc.1").is_ok());
        assert!(v("1.0.0-alpha-Beta.2").is_ok());
    }

    #[test]
    fn rejects_malformed_versions() {
        assert!(v("").is_err());
        assert!(v("1.2").is_err());
        assert!(v("1.2.3.4").is_err());
        assert!(v("01.2.3").is_err());
        assert!(v("1.x.3").is_err());
        assert!(v("1.2.3-").is_err());
        assert!(v("1.2.3 bad").is_err());
        let long = format!("{}.2.3", "9".repeat(65));
        assert!(v(&long).is_err());
    }

    #[test]
    fn serializes_transparently() {
        let ver = v("1.2.3").expect("valid");
        let json = serde_json::to_string(&ver).expect("serialize");
        assert_eq!(json, "\"1.2.3\"");
        let back: Version = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, ver);
    }

    #[test]
    fn deserialization_validates() {
        assert!(serde_json::from_str::<Version>("\"garbage\"").is_err());
        assert!(serde_json::from_str::<Version>("\"1.2\"").is_err());
        assert!(serde_json::from_str::<Version>("\"1.2.3-\"").is_err());
    }

    #[test]
    fn ordering_is_lexicographic_documented_behavior() {
        let a = v("1.10.0").expect("valid");
        let b = v("1.2.0").expect("valid");
        assert!(a < b); // "10" < "2" lexicographically; documented, not semver ordering
    }
}
