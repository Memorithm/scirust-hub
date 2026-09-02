use std::collections::BTreeSet;
use std::fmt;

use axum::http::Method;
use sha2::{Digest, Sha256};

/// Versioned control-plane authorization capabilities.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum AuthPermission {
    Inspect,
    Control,
    Metrics,
}

impl AuthPermission {
    pub const ALL: [Self; 3] = [Self::Inspect, Self::Control, Self::Metrics];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Inspect => "inspect",
            Self::Control => "control",
            Self::Metrics => "metrics",
        }
    }

    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "inspect" => Some(Self::Inspect),
            "control" => Some(Self::Control),
            "metrics" => Some(Self::Metrics),
            _ => None,
        }
    }
}

/// Non-secret authenticated identity inserted into request extensions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthenticatedPrincipal {
    id: String,
}

impl AuthenticatedPrincipal {
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }
}

/// Static principal credential. The plaintext bearer is never retained.
#[derive(Clone)]
pub struct StaticPrincipal {
    id: String,
    token_sha256: [u8; 32],
    permissions: BTreeSet<AuthPermission>,
}

impl StaticPrincipal {
    /// Builds one static principal and immediately reduces the token to SHA-256.
    ///
    /// # Errors
    /// Returns a fail-closed configuration error for invalid identity, empty
    /// credentials/permissions, or duplicate permissions.
    pub fn new(
        id: impl Into<String>,
        token: impl AsRef<str>,
        permissions: impl IntoIterator<Item = AuthPermission>,
    ) -> Result<Self, AuthConfigError> {
        let id = id.into();
        validate_principal_id(&id)?;
        let token = token.as_ref();
        if token.is_empty() {
            return Err(AuthConfigError::EmptyCredential { principal_id: id });
        }
        let mut permission_set = BTreeSet::new();
        for permission in permissions {
            if !permission_set.insert(permission) {
                return Err(AuthConfigError::DuplicatePermission {
                    principal_id: id,
                    permission,
                });
            }
        }
        if permission_set.is_empty() {
            return Err(AuthConfigError::EmptyPermissions { principal_id: id });
        }
        Ok(Self {
            id,
            token_sha256: Sha256::digest(token.as_bytes()).into(),
            permissions: permission_set,
        })
    }

    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    #[must_use]
    pub fn permits(&self, permission: AuthPermission) -> bool {
        self.permissions.contains(&permission)
    }

    pub(crate) fn token_digest(&self) -> &[u8; 32] {
        &self.token_sha256
    }

    pub(crate) fn authenticated_identity(&self) -> AuthenticatedPrincipal {
        AuthenticatedPrincipal {
            id: self.id.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthConfigError {
    EmptyPrincipalSet,
    InvalidPrincipalId {
        principal_id: String,
    },
    EmptyCredential {
        principal_id: String,
    },
    EmptyPermissions {
        principal_id: String,
    },
    DuplicatePermission {
        principal_id: String,
        permission: AuthPermission,
    },
    DuplicatePrincipalId {
        principal_id: String,
    },
    DuplicateCredential,
}

impl fmt::Display for AuthConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyPrincipalSet => f.write_str("static principal set must not be empty"),
            Self::InvalidPrincipalId { principal_id } => {
                write!(f, "invalid static principal id {principal_id:?}")
            }
            Self::EmptyCredential { principal_id } => {
                write!(f, "empty bearer credential for principal {principal_id:?}")
            }
            Self::EmptyPermissions { principal_id } => {
                write!(
                    f,
                    "principal {principal_id:?} must have at least one permission"
                )
            }
            Self::DuplicatePermission {
                principal_id,
                permission,
            } => write!(
                f,
                "principal {principal_id:?} contains duplicate permission {:?}",
                permission.as_str()
            ),
            Self::DuplicatePrincipalId { principal_id } => {
                write!(f, "duplicate static principal id {principal_id:?}")
            }
            Self::DuplicateCredential => {
                f.write_str("two static principals must not share one bearer credential")
            }
        }
    }
}

impl std::error::Error for AuthConfigError {}

pub fn validate_static_principals(principals: &[StaticPrincipal]) -> Result<(), AuthConfigError> {
    if principals.is_empty() {
        return Err(AuthConfigError::EmptyPrincipalSet);
    }
    let mut ids = BTreeSet::new();
    let mut digests = BTreeSet::new();
    for principal in principals {
        if !ids.insert(principal.id.clone()) {
            return Err(AuthConfigError::DuplicatePrincipalId {
                principal_id: principal.id.clone(),
            });
        }
        if !digests.insert(principal.token_sha256) {
            return Err(AuthConfigError::DuplicateCredential);
        }
    }
    Ok(())
}

pub(crate) fn authenticate<'a>(
    principals: &'a [StaticPrincipal],
    token: &str,
) -> Option<&'a StaticPrincipal> {
    let supplied: [u8; 32] = Sha256::digest(token.as_bytes()).into();
    let mut matched = None;
    for principal in principals {
        if digest_eq(principal.token_digest(), &supplied) {
            matched = Some(principal);
        }
    }
    matched
}

#[must_use]
pub(crate) fn required_permission(method: &Method, path: &str) -> AuthPermission {
    if path == "/metrics" {
        return AuthPermission::Metrics;
    }
    if method == Method::GET || method == Method::HEAD {
        AuthPermission::Inspect
    } else {
        AuthPermission::Control
    }
}

fn validate_principal_id(id: &str) -> Result<(), AuthConfigError> {
    let valid = (1..=64).contains(&id.len())
        && id.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_' | b'.')
        });
    if valid {
        Ok(())
    } else {
        Err(AuthConfigError::InvalidPrincipalId {
            principal_id: id.to_owned(),
        })
    }
}

fn digest_eq(left: &[u8; 32], right: &[u8; 32]) -> bool {
    let mut difference = 0u8;
    for (&a, &b) in left.iter().zip(right.iter()) {
        difference |= a ^ b;
    }
    difference == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn principal_validation_is_fail_closed_without_secret_debugging() {
        assert!(StaticPrincipal::new("observer", "secret", [AuthPermission::Inspect]).is_ok());
        assert!(
            StaticPrincipal::new("Bad Principal", "secret", [AuthPermission::Inspect]).is_err()
        );
        assert!(StaticPrincipal::new("observer", "", [AuthPermission::Inspect]).is_err());
        assert!(StaticPrincipal::new("observer", "secret", []).is_err());
        assert!(StaticPrincipal::new(
            "observer",
            "secret",
            [AuthPermission::Inspect, AuthPermission::Inspect]
        )
        .is_err());
    }

    #[test]
    fn duplicate_principal_ids_and_credentials_are_rejected() {
        let a = StaticPrincipal::new("a", "secret-a", [AuthPermission::Inspect]).unwrap();
        let duplicate_id =
            StaticPrincipal::new("a", "secret-b", [AuthPermission::Control]).unwrap();
        assert!(matches!(
            validate_static_principals(&[a.clone(), duplicate_id]),
            Err(AuthConfigError::DuplicatePrincipalId { .. })
        ));
        let duplicate_token =
            StaticPrincipal::new("b", "secret-a", [AuthPermission::Metrics]).unwrap();
        assert!(matches!(
            validate_static_principals(&[a, duplicate_token]),
            Err(AuthConfigError::DuplicateCredential)
        ));
    }

    #[test]
    fn route_classes_are_explicit() {
        for path in [
            "/api/v1/components",
            "/api/v1/capabilities",
            "/api/v1/runs/abc",
            "/api/v1/workflows",
            "/api/v1/artifacts/abc",
            "/api/v1/events",
        ] {
            assert_eq!(
                required_permission(&Method::GET, path),
                AuthPermission::Inspect
            );
        }
        assert_eq!(
            required_permission(&Method::GET, "/metrics"),
            AuthPermission::Metrics
        );
        for path in [
            "/api/v1/components",
            "/api/v1/runs",
            "/api/v1/runs/abc/cancel",
            "/api/v1/runs/abc/reproduce",
            "/api/v1/executions",
            "/api/v1/workflows",
            "/api/v1/workflows/abc/cancel",
            "/api/v1/workflows/abc/executions",
            "/api/v1/artifacts",
        ] {
            assert_eq!(
                required_permission(&Method::POST, path),
                AuthPermission::Control
            );
        }
    }

    #[test]
    fn authentication_uses_digest_and_returns_non_secret_identity() {
        let principal = StaticPrincipal::new(
            "observer",
            "very-secret",
            [AuthPermission::Inspect, AuthPermission::Metrics],
        )
        .unwrap();
        let principals = [principal];
        let matched = authenticate(&principals, "very-secret").expect("match");
        assert_eq!(matched.authenticated_identity().id(), "observer");
        assert!(authenticate(&principals, "wrong").is_none());
    }
}
