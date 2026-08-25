//! Strongly typed identifiers.
//!
//! Identifiers are logical handles. They are deliberately distinct from
//! content digests (immutability), versions (evolution) and locations
//! (storage); see `docs/adr/0003-component-and-capability-model.md`.

use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

macro_rules! uuid_id {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            /// Generates a fresh random identifier.
            #[must_use]
            pub fn generate() -> Self {
                Self(Uuid::new_v4())
            }

            /// Wraps an existing UUID (tests and ingestion of externally
            /// minted identifiers).
            #[must_use]
            pub const fn from_uuid(uuid: Uuid) -> Self {
                Self(uuid)
            }

            #[must_use]
            pub const fn as_uuid(&self) -> Uuid {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                // Hyphenated lowercase is the canonical interchange form.
                write!(f, "{}", self.0.hyphenated())
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), self.0.hyphenated())
            }
        }

        impl std::str::FromStr for $name {
            type Err = uuid::Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ok(Self(Uuid::parse_str(s)?))
            }
        }
    };
}

uuid_id! {
    /// Logical identity of a registered component, stable across versions.
    ComponentId
}
uuid_id! {
    /// Identity of one submitted run.
    RunId
}
uuid_id! {
    /// Handle to artifact metadata; contents live in the artifact store.
    ArtifactId
}
uuid_id! {
    /// Identity of one submitted workflow (multi-step orchestration).
    WorkflowId
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_is_hyphenated_lowercase() {
        let id = ComponentId::from_uuid(Uuid::nil());
        assert_eq!(id.to_string(), "00000000-0000-0000-0000-000000000000");
    }

    #[test]
    fn round_trips_through_string() {
        let id = RunId::generate();
        let parsed: RunId = id.to_string().parse().expect("valid run id");
        assert_eq!(parsed, id);
    }

    #[test]
    fn rejects_garbage_parse() {
        assert!("not-a-uuid".parse::<ArtifactId>().is_err());
    }

    #[test]
    fn types_do_not_compare_across_kinds() {
        // Compile-level guarantee via distinct types; runtime sanity check:
        let c = ComponentId::generate();
        let r = RunId::from_uuid(c.as_uuid());
        // Same inner UUID but different types means no accidental equality.
        assert_ne!(
            std::any::TypeId::of::<ComponentId>(),
            std::any::TypeId::of::<RunId>()
        );
        let _ = r;
    }
}
