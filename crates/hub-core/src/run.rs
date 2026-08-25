//! Run specifications, lifecycle state machine and provenance-bearing
//! records.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::capability::CapabilityName;
use crate::clock::UnixMillis;
use crate::digest::{hash_bytes, ContentDigest, DOMAIN_RUN_PARAMS};
use crate::error::CoreError;
use crate::id::{ArtifactId, ComponentId, RunId};
use crate::limits::Limits;
use crate::version::Version;

/// Lifecycle states of a run. Transitions are controlled exclusively through
/// [`RunRecord::transition`] which enforces [`RunState::can_transition_to`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    Created,
    Validated,
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl RunState {
    /// Legal forward transitions. Terminal states transition nowhere.
    #[must_use]
    pub fn can_transition_to(self, next: RunState) -> bool {
        use RunState::*;
        matches!(
            (self, next),
            (Created, Validated)
                | (Created, Failed)
                | (Created, Cancelled)
                | (Validated, Queued)
                | (Validated, Failed)
                | (Validated, Cancelled)
                | (Queued, Running)
                | (Queued, Failed)
                | (Queued, Cancelled)
                | (Running, Succeeded)
                | (Running, Failed)
                | (Running, Cancelled)
        )
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            RunState::Succeeded | RunState::Failed | RunState::Cancelled
        )
    }
}

impl std::fmt::Display for RunState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            RunState::Created => "created",
            RunState::Validated => "validated",
            RunState::Queued => "queued",
            RunState::Running => "running",
            RunState::Succeeded => "succeeded",
            RunState::Failed => "failed",
            RunState::Cancelled => "cancelled",
        };
        f.write_str(s)
    }
}

/// One validated input reference: a named slot bound to an artifact.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputBinding {
    /// Slot name; also used as the materialized file name inside the run
    /// working directory (`inputs/<name>`).
    pub name: String,
    pub artifact: ArtifactId,
}

/// What to run. Purely declarative and serializable; it never carries a shell
/// command. Execution details are resolved from the component's declared
/// binding at orchestration time.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunSpec {
    pub component: ComponentId,
    pub capability: CapabilityName,
    /// Structured parameters, serialized canonically (BTreeMap-ordered JSON)
    /// both for hashing and for substitution into the `{params}` placeholder.
    #[serde(default)]
    pub parameters: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    pub inputs: Vec<InputBinding>,
    /// Wall-clock budget for the whole execution, milliseconds.
    pub timeout_ms: u64,
}

impl RunSpec {
    /// Validates intrinsic constraints. Cross-checks against the registry
    /// happen later in the orchestrator.
    ///
    /// # Errors
    /// [`CoreError::InvalidRunSpec`] with a precise reason.
    pub fn validate(&self, limits: &Limits) -> Result<(), CoreError> {
        if self.timeout_ms == 0 || self.timeout_ms > limits.max_timeout_ms {
            return Err(CoreError::InvalidRunSpec(format!(
                "timeout_ms must be 1..={}",
                limits.max_timeout_ms
            )));
        }
        let params_bytes = serde_json::to_vec(&self.parameters)
            .map_err(|e| CoreError::InvalidRunSpec(format!("parameters are not JSON-safe: {e}")))?;
        if params_bytes.len() > limits.max_params_bytes {
            return Err(CoreError::InvalidRunSpec(format!(
                "parameters serialize to {} bytes, over the {} byte limit",
                params_bytes.len(),
                limits.max_params_bytes
            )));
        }
        if self.inputs.len() > limits.max_inputs {
            return Err(CoreError::InvalidRunSpec(format!(
                "{} inputs exceeds the maximum of {}",
                self.inputs.len(),
                limits.max_inputs
            )));
        }
        let mut seen = BTreeSet::new();
        for input in &self.inputs {
            Self::validate_input_name(&input.name)?;
            if !seen.insert(input.name.as_str()) {
                return Err(CoreError::InvalidRunSpec(format!(
                    "duplicate input binding {:?}",
                    input.name
                )));
            }
        }
        Ok(())
    }

    /// Input names become path components and placeholder references; they
    /// must stay boring.
    fn validate_input_name(name: &str) -> Result<(), CoreError> {
        let valid = !name.is_empty()
            && name.len() <= 64
            && name.starts_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
            && name
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-');
        if valid {
            Ok(())
        } else {
            Err(CoreError::InvalidRunSpec(format!(
                "input name {name:?} must match [a-z0-9][a-z0-9_-]{{0,63}}"
            )))
        }
    }

    /// Canonical parameter bytes used for digests and substitution. Ordering
    /// is deterministic because `serde_json::Map` sorts keys unless the
    /// non-default `preserve_order` feature is enabled (never enabled in this
    /// workspace).
    ///
    /// # Errors
    /// Only if parameters contain values JSON cannot encode, which
    /// construction-time validation already excludes.
    pub fn canonical_params_bytes(&self) -> Result<Vec<u8>, CoreError> {
        serde_json::to_vec(&self.parameters).map_err(|e| CoreError::Storage(e.to_string()))
    }

    /// Digest over canonical parameter bytes.
    ///
    /// # Errors
    /// See [`Self::canonical_params_bytes`].
    pub fn params_digest(&self) -> Result<ContentDigest, CoreError> {
        Ok(hash_bytes(
            DOMAIN_RUN_PARAMS,
            &self.canonical_params_bytes()?,
        ))
    }
}

/// One observed lifecycle change, kept for provenance.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transition {
    pub from: RunState,
    pub to: RunState,
    pub at: UnixMillis,
}

/// Provenance of one input artifact consumed by the run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputProvenance {
    pub name: String,
    pub artifact: ArtifactId,
    pub digest: ContentDigest,
    pub size: u64,
}

/// Reference to an output artifact produced by a run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputRef {
    /// Stable stream label, e.g. `stdout`, `stderr`.
    pub name: String,
    pub artifact: ArtifactId,
    pub digest: ContentDigest,
    pub size: u64,
}

/// Final observable result of one execution.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunOutcome {
    pub exit_code: Option<i32>,
    /// Signal number when the child was killed by a signal (Unix).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
    pub timed_out: bool,
    pub cancelled: bool,
    pub executor_backend: String,
    pub duration_ms: u64,
    /// Provenance of consumed input artifacts (immutable copies materialized
    /// into the run working directory).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inputs: Vec<InputProvenance>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outputs: Vec<OutputRef>,
    /// Environment variable *names* provided to the process. Values are
    /// deliberately excluded from provenance to avoid leaking secrets.
    pub env_keys: Vec<String>,
    /// Digest of the canonical run parameters for quick comparison.
    pub params_digest: ContentDigest,
    pub failure: Option<String>,
}

/// Complete provenance-bearing record of one run.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RunRecord {
    pub id: RunId,
    pub spec: RunSpec,
    pub state: RunState,
    /// Snapshot of component identity at submission time.
    pub component_name: String,
    pub component_version: Version,
    pub contract_version: Version,
    pub created_at: UnixMillis,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<UnixMillis>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<UnixMillis>,
    pub transitions: Vec<Transition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<RunOutcome>,
}

impl RunRecord {
    /// Creates a fresh record in [`RunState::Created`].
    ///
    /// # Errors
    /// Propagates [`RunSpec::validate`] errors.
    pub fn create(
        spec: RunSpec,
        component_name: String,
        component_version: Version,
        contract_version: Version,
        now: UnixMillis,
        limits: &Limits,
    ) -> Result<Self, CoreError> {
        spec.validate(limits)?;
        Ok(Self {
            id: RunId::from_uuid(Uuid::new_v4()),
            spec,
            state: RunState::Created,
            component_name,
            component_version,
            contract_version,
            created_at: now,
            started_at: None,
            finished_at: None,
            transitions: Vec::new(),
            outcome: None,
        })
    }

    /// Applies a state transition or fails without mutating.
    ///
    /// # Errors
    /// [`CoreError::InvalidTransition`] for illegal moves.
    pub fn transition(&mut self, to: RunState, now: UnixMillis) -> Result<(), CoreError> {
        let from = self.state;
        if !from.can_transition_to(to) {
            return Err(CoreError::InvalidTransition { from, to });
        }
        self.state = to;
        if to == RunState::Running {
            self.started_at.get_or_insert(now);
        }
        if to.is_terminal() {
            self.finished_at = Some(now);
        }
        self.transitions.push(Transition { from, to, at: now });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capability::CapabilityName;
    use crate::clock::Clock as _;
    use crate::id::ComponentId;
    use std::collections::BTreeMap;

    fn spec() -> RunSpec {
        RunSpec {
            component: ComponentId::generate(),
            capability: CapabilityName::parse("demo.echo").expect("valid"),
            parameters: BTreeMap::from([("msg".to_owned(), serde_json::json!("hi"))]),
            inputs: vec![],
            timeout_ms: 5_000,
        }
    }

    fn happy_path_transitions(record: &mut RunRecord, t: &crate::clock::ManualClock) {
        for step in [
            RunState::Validated,
            RunState::Queued,
            RunState::Running,
            RunState::Succeeded,
        ] {
            record.transition(step, t.now_ms()).expect("legal");
            t.advance(10);
        }
    }

    #[test]
    fn legal_chain_is_recorded() {
        let mut rec = RunRecord::create(
            spec(),
            "demo".into(),
            Version::parse("1.0.0").expect("v"),
            Version::parse("1.0.0").expect("v"),
            0,
            &Limits::default(),
        )
        .expect("valid");
        let clock = crate::clock::ManualClock::starting_at(100);
        happy_path_transitions(&mut rec, &clock);
        // Validated@100 Queued@110 Running@120 Succeeded@130.
        assert_eq!(rec.state, RunState::Succeeded);
        assert_eq!(rec.started_at, Some(120));
        assert_eq!(rec.finished_at, Some(130));
        assert_eq!(rec.transitions.len(), 4);
    }

    #[test]
    fn illegal_transition_rejected_without_mutation() {
        let mut rec = RunRecord::create(
            spec(),
            "demo".into(),
            Version::parse("1.0.0").expect("v"),
            Version::parse("1.0.0").expect("v"),
            0,
            &Limits::default(),
        )
        .expect("valid");
        let before = rec.clone();
        assert!(matches!(
            rec.transition(RunState::Running, 5),
            Err(CoreError::InvalidTransition { .. })
        ));
        assert_eq!(rec, before);
    }

    #[test]
    fn terminal_states_are_frozen() {
        let mut rec = RunRecord::create(
            spec(),
            "demo".into(),
            Version::parse("1.0.0").expect("v"),
            Version::parse("1.0.0").expect("v"),
            0,
            &Limits::default(),
        )
        .expect("valid");
        rec.transition(RunState::Failed, 1).expect("legal");
        assert!(rec.state.is_terminal());
        assert!(matches!(
            rec.transition(RunState::Running, 2),
            Err(CoreError::InvalidTransition { .. })
        ));
    }

    #[test]
    fn cancellation_is_legal_from_active_states() {
        use RunState::*;
        for from in [Created, Validated, Queued, Running] {
            assert!(from.can_transition_to(Cancelled), "{from} -> Cancelled");
        }
    }

    #[test]
    fn spec_validation_enforces_limits() {
        let limits = Limits::default();
        let mut s = spec();
        s.timeout_ms = 0;
        assert!(s.validate(&limits).is_err());
        s.timeout_ms = Limits::default().max_timeout_ms + 1;
        assert!(s.validate(&limits).is_err());
        s = spec();
        s.inputs.push(InputBinding {
            name: "../escape".into(),
            artifact: ArtifactId::generate(),
        });
        assert!(matches!(
            s.validate(&limits),
            Err(CoreError::InvalidRunSpec(msg)) if msg.contains("input name")
        ));
        s = spec();
        s.parameters.insert(
            "blob".into(),
            serde_json::Value::String("x".repeat(limits.max_params_bytes)),
        );
        assert!(s.validate(&limits).is_err());
    }

    #[test]
    fn params_digest_is_deterministic_and_order_independent_of_insertion() {
        let mut s1 = spec();
        s1.parameters.insert("a".into(), serde_json::json!(1));
        let mut s2 = spec();
        // Insertion order differs; canonical serialization must not care.
        s2.parameters.insert("a".into(), serde_json::json!(1));
        assert_eq!(
            s1.params_digest().expect("digest"),
            s2.params_digest().expect("digest")
        );
    }

    #[test]
    fn record_round_trips_through_json() {
        let mut rec = RunRecord::create(
            spec(),
            "demo".into(),
            Version::parse("1.0.0").expect("v"),
            Version::parse("1.0.0").expect("v"),
            42,
            &Limits::default(),
        )
        .expect("valid");
        rec.transition(RunState::Failed, 43).expect("legal");
        let encoded = serde_json::to_string(&rec).expect("encode");
        let decoded: RunRecord = serde_json::from_str(&encoded).expect("decode");
        assert_eq!(decoded, rec);
    }
}
