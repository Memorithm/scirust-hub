//! Hard input limits shared by validation layers.
//!
//! Centralized so the API layer, manifest validation and executor agree on
//! one set of numbers. These are resource-exhaustion guards, not security
//! boundaries.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Maximum serialized component manifest size accepted at any boundary.
    pub max_manifest_bytes: usize,
    /// Maximum number of argv entries in a process binding.
    pub max_args: usize,
    /// Maximum length of a single argv entry.
    pub max_arg_bytes: usize,
    /// Maximum serialized run parameter JSON.
    pub max_params_bytes: usize,
    /// Maximum number of input artifact references per run.
    pub max_inputs: usize,
    /// Maximum number of declared output files per component binding.
    pub max_outputs: usize,
    /// Maximum bytes captured from one output stream (stdout or stderr).
    /// Excess is truncated and flagged, not dropped silently.
    pub max_capture_bytes: usize,
    /// Maximum size of an artifact stored through the Hub.
    pub max_artifact_bytes: u64,
    /// Maximum wall-clock timeout for one execution.
    pub max_timeout_ms: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_manifest_bytes: 256 * 1024,
            max_args: 64,
            max_arg_bytes: 4096,
            max_params_bytes: 16 * 1024,
            max_inputs: 32,
            max_outputs: 16,
            max_capture_bytes: 1024 * 1024,
            max_artifact_bytes: 16 * 1024 * 1024,
            max_timeout_ms: 60 * 60 * 1000,
        }
    }
}
