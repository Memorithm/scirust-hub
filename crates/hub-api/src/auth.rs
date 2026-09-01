use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

/// Authorization contract version for the static control-plane principal model.
pub const AUTHORIZATION_VERSION: u16 = 1;
const MAX_PRINCIPAL_ID_BYTES: usize = 64;

/// Permissions understood by the HTTP control plane.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ControlPlanePermission {
    /// Read-only inspection, including `/metrics`.
    Read,
    /// State-changing control-plane operations.
    Control,
}

/// Built-in static roles. Roles are deliberately small in v1; callers may not
/// self-assert them in request bodies or headers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrincipalRole {
    ReadOnly,
    Control,
}

impl PrincipalRole {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "read_only",
            Self::Control => "control",
        }
    }

    #[must_use]
    pub const fn allows(self, permission: ControlPlanePermission) -> bool {
        match (self, permission) {
            (Self::Control, _) | (Self::ReadOnly, ControlPlanePermission::Read) => true,
            (Self::ReadOnly, ControlPlanePermission::Control) => false,
        }
    }
}

/// Non-secret authenticated request identity inserted into request extensions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthenticatedPrincipal {
    id: String,
    role: PrincipalRole,
}

impl AuthenticatedPrincipal {
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    #[must_use]
    pub const fn role(&self) -> PrincipalRole {
        self.role
    }
}

/// A static principal verifier. The plaintext bearer token is hashed during
/// construction and is never retained or exposed through `Debug`.
#[derive(Clone, PartialEq, Eq)]
pub struct StaticPrincipal {
    id: String,
    role: PrincipalRole,
    bearer_sha256: [u8; 32],
}

impl std::fmt::Debug for StaticPrincipal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticPrincipal")
            .field("id", &self.id)
            .field("role", &self.role)
            .finish_non_exhaustive()
    }
}

impl StaticPrincipal {
    /// Builds a verifier and immediately discards the plaintext token.
    ///
    /// # Errors
    /// Returns [`AuthorizationConfigError`] for an invalid principal id or an
    /// empty bearer token.
    pub fn new(
        id: impl Into<String>,
        role: PrincipalRole,
        bearer_token: impl AsRef<str>,
    ) -> Result<Self, AuthorizationConfigError> {
        let id = id.into();
        validate_principal_id(&id)?;
        let token = bearer_token.as_ref();
        if token.is_empty() {
            return Err(AuthorizationConfigError::EmptyBearerToken { principal: id });
        }
        Ok(Self {
            id,
            role,
            bearer_sha256: Sha256::digest(token.as_bytes()).into(),
        })
    }

    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    #[must_use]
    pub const fn role(&self) -> PrincipalRole {
        self.role
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AuthorizationConfigError {
    #[error("at least one static principal is required")]
    EmptyPrincipalSet,
    #[error("invalid principal id {0:?}: use 1..={MAX_PRINCIPAL_ID_BYTES} ASCII letters, digits, '.', '_' or '-'")]
    InvalidPrincipalId(String),
    #[error("bearer token must not be empty for principal {principal:?}")]
    EmptyBearerToken { principal: String },
    #[error("duplicate principal id {0:?}")]
    DuplicatePrincipalId(String),
    #[error("duplicate bearer credential across static principals")]
    DuplicateBearerCredential,
}

#[derive(Clone)]
pub(crate) struct AuthorizationState {
    principals: Vec<StaticPrincipal>,
}

impl AuthorizationState {
    pub(crate) fn legacy(token: &str) -> Self {
        debug_assert!(!token.is_empty());
        Self {
            principals: vec![StaticPrincipal {
                id: "legacy-control".to_owned(),
                role: PrincipalRole::Control,
                bearer_sha256: Sha256::digest(token.as_bytes()).into(),
            }],
        }
    }

    pub(crate) fn new(
        principals: Vec<StaticPrincipal>,
    ) -> Result<Self, AuthorizationConfigError> {
        if principals.is_empty() {
            return Err(AuthorizationConfigError::EmptyPrincipalSet);
        }
        let mut ids = BTreeSet::new();
        let mut credentials = BTreeSet::new();
        for principal in &principals {
            if !ids.insert(principal.id.clone()) {
                return Err(AuthorizationConfigError::DuplicatePrincipalId(
                    principal.id.clone(),
                ));
            }
            if !credentials.insert(principal.bearer_sha256) {
                return Err(AuthorizationConfigError::DuplicateBearerCredential);
            }
        }
        Ok(Self { principals })
    }

    pub(crate) fn authenticate(&self, token: &str) -> Option<AuthenticatedPrincipal> {
        let actual: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        let mut matched = None;
        for principal in &self.principals {
            if digest_eq(&principal.bearer_sha256, &actual) {
                matched = Some(AuthenticatedPrincipal {
                    id: principal.id.clone(),
                    role: principal.role,
                });
            }
        }
        matched
    }
}

fn validate_principal_id(id: &str) -> Result<(), AuthorizationConfigError> {
    if id.is_empty()
        || id.len() > MAX_PRINCIPAL_ID_BYTES
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(AuthorizationConfigError::InvalidPrincipalId(id.to_owned()));
    }
    Ok(())
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
    fn verifier_debug_never_contains_token_or_digest() {
        let principal = StaticPrincipal::new("auditor", PrincipalRole::ReadOnly, "top-secret")
            .expect("principal");
        let rendered = format!("{principal:?}");
        assert!(rendered.contains("auditor"));
        assert!(rendered.contains("ReadOnly"));
        assert!(!rendered.contains("top-secret"));
        assert!(!rendered.contains("bearer_sha256"));
    }

    #[test]
    fn static_configuration_rejects_ambiguous_credentials_and_ids() {
        let first = StaticPrincipal::new("one", PrincipalRole::ReadOnly, "same").unwrap();
        let duplicate_token = StaticPrincipal::new("two", PrincipalRole::Control, "same").unwrap();
        assert_eq!(
            AuthorizationState::new(vec![first.clone(), duplicate_token]).unwrap_err(),
            AuthorizationConfigError::DuplicateBearerCredential
        );

        let duplicate_id = StaticPrincipal::new("one", PrincipalRole::Control, "other").unwrap();
        assert_eq!(
            AuthorizationState::new(vec![first, duplicate_id]).unwrap_err(),
            AuthorizationConfigError::DuplicatePrincipalId("one".to_owned())
        );
    }

    #[test]
    fn principal_ids_and_empty_tokens_fail_closed() {
        assert!(StaticPrincipal::new("", PrincipalRole::ReadOnly, "token").is_err());
        assert!(StaticPrincipal::new("bad id", PrincipalRole::ReadOnly, "token").is_err());
        assert!(StaticPrincipal::new("auditor", PrincipalRole::ReadOnly, "").is_err());
        assert_eq!(
            AuthorizationState::new(Vec::new()).unwrap_err(),
            AuthorizationConfigError::EmptyPrincipalSet
        );
    }

    #[test]
    fn authentication_checks_roles_without_retaining_plaintext() {
        let state = AuthorizationState::new(vec![
            StaticPrincipal::new("auditor", PrincipalRole::ReadOnly, "read-secret").unwrap(),
            StaticPrincipal::new("operator", PrincipalRole::Control, "control-secret").unwrap(),
        ])
        .unwrap();
        let reader = state.authenticate("read-secret").expect("reader");
        assert_eq!(reader.id(), "auditor");
        assert!(reader.role().allows(ControlPlanePermission::Read));
        assert!(!reader.role().allows(ControlPlanePermission::Control));
        let controller = state.authenticate("control-secret").expect("controller");
        assert!(controller.role().allows(ControlPlanePermission::Read));
        assert!(controller.role().allows(ControlPlanePermission::Control));
        assert!(state.authenticate("wrong").is_none());
    }
}
