//! Deterministic configured-worker placement over authenticated remote executors.
//!
//! Discovery happens before dispatch. Unavailable/incompatible workers are
//! skipped while no lease exists. After a target is selected, execution stays
//! pinned to it: an ambiguous lease-create/transport failure is never retried
//! on another worker because the first worker may already be executing.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use hub_core::error::ExecutorFailure;
use hub_core::exec::{CancelToken, ExecutionOutcome, ExecutionReport, ExecutionRequest, Executor};

use crate::RemoteExecutor;

#[derive(Debug)]
pub struct RemotePoolExecutor {
    workers: Vec<RemoteExecutor>,
    in_flight: Arc<Mutex<BTreeMap<String, u64>>>,
}

impl RemotePoolExecutor {
    /// Builds a pool from two or more configured endpoints sharing one worker
    /// bearer credential. Endpoint strings must be unique.
    pub fn new(endpoints: Vec<String>, token: impl Into<String>) -> Result<Self, String> {
        let token = token.into();
        if token.is_empty() {
            return Err("remote worker bearer token must not be empty".into());
        }
        Self::from_credentials(
            endpoints
                .into_iter()
                .map(|endpoint| (endpoint, token.clone()))
                .collect(),
        )
    }

    /// Builds a pool from two or more endpoint-specific bearer credentials.
    /// Tokens are never included in validation errors or backend identities.
    pub fn from_credentials(credentials: Vec<(String, String)>) -> Result<Self, String> {
        if credentials.len() < 2 {
            return Err("remote worker pool requires at least two endpoints".into());
        }
        let mut seen = BTreeMap::<String, ()>::new();
        let mut workers = Vec::with_capacity(credentials.len());
        for (endpoint, token) in credentials {
            let normalized = endpoint.trim_end_matches('/').to_owned();
            if seen.insert(normalized.clone(), ()).is_some() {
                return Err(format!("duplicate remote worker endpoint {normalized:?}"));
            }
            if token.is_empty() {
                return Err(format!(
                    "remote worker bearer token must not be empty for endpoint {normalized:?}"
                ));
            }
            workers.push(RemoteExecutor::new(normalized, token)?);
        }
        Ok(Self {
            workers,
            in_flight: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    /// Applies the same transport-size policy to every configured worker.
    #[must_use]
    pub fn with_max_payload_bytes(mut self, value: usize) -> Self {
        for worker in &mut self.workers {
            *worker = worker.clone().with_max_payload_bytes(value);
        }
        self
    }

    fn select(&self) -> Result<(RemoteExecutor, String, InFlightGuard), String> {
        let mut eligible = Vec::new();
        let mut diagnostics = Vec::new();
        let mut identities = BTreeMap::<String, String>::new();
        for worker in &self.workers {
            match worker.discover_eligible() {
                Ok(descriptor) => {
                    if let Some(previous_endpoint) = identities
                        .insert(descriptor.worker_id.clone(), worker.endpoint().to_owned())
                    {
                        return Err(format!(
                            "duplicate worker identity {:?} advertised by {:?} and {:?}",
                            descriptor.worker_id,
                            previous_endpoint,
                            worker.endpoint()
                        ));
                    }
                    eligible.push((worker.clone(), descriptor.worker_id));
                }
                Err(error) => diagnostics.push(format!("{}: {error}", worker.endpoint())),
            }
        }
        if eligible.is_empty() {
            return Err(if diagnostics.is_empty() {
                "no eligible remote workers".into()
            } else {
                format!("no eligible remote workers: {}", diagnostics.join("; "))
            });
        }

        let mut loads = self
            .in_flight
            .lock()
            .map_err(|_| "remote worker pool load map lock poisoned".to_owned())?;
        eligible.sort_by(|(left_worker, left_id), (right_worker, right_id)| {
            let left_load = loads.get(left_worker.endpoint()).copied().unwrap_or(0);
            let right_load = loads.get(right_worker.endpoint()).copied().unwrap_or(0);
            (left_load, left_id, left_worker.endpoint()).cmp(&(
                right_load,
                right_id,
                right_worker.endpoint(),
            ))
        });
        let (worker, worker_id) = eligible.remove(0);
        let key = worker.endpoint().to_owned();
        let count = loads.entry(key.clone()).or_insert(0);
        *count = count.saturating_add(1);
        drop(loads);
        let guard = InFlightGuard {
            key,
            loads: self.in_flight.clone(),
        };
        Ok((worker, worker_id, guard))
    }

    fn failure_report(&self, started: Instant, reason: String) -> ExecutionReport {
        ExecutionReport {
            backend_id: self.backend_id().to_owned(),
            outcome: ExecutionOutcome {
                exit_code: None,
                signal: None,
                timed_out: false,
                cancelled: false,
                start_error: Some(reason),
                duration_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                stdout: Vec::new(),
                stdout_truncated: false,
                stderr: Vec::new(),
                stderr_truncated: false,
            },
        }
    }
}

struct InFlightGuard {
    key: String,
    loads: Arc<Mutex<BTreeMap<String, u64>>>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        if let Ok(mut loads) = self.loads.lock() {
            if let Some(count) = loads.get_mut(&self.key) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    loads.remove(&self.key);
                }
            }
        }
    }
}

impl Executor for RemotePoolExecutor {
    fn backend_id(&self) -> &str {
        "remote-pool"
    }

    fn execute(
        &self,
        request: &ExecutionRequest,
        cancel: &CancelToken,
    ) -> Result<ExecutionOutcome, ExecutorFailure> {
        self.execute_report(request, cancel)
            .map(|report| report.outcome)
    }

    fn execute_report(
        &self,
        request: &ExecutionRequest,
        cancel: &CancelToken,
    ) -> Result<ExecutionReport, ExecutorFailure> {
        let started = Instant::now();
        if cancel.is_cancelled() {
            return Ok(ExecutionReport {
                backend_id: self.backend_id().to_owned(),
                outcome: ExecutionOutcome {
                    exit_code: None,
                    signal: None,
                    timed_out: false,
                    cancelled: true,
                    start_error: None,
                    duration_ms: 0,
                    stdout: Vec::new(),
                    stdout_truncated: false,
                    stderr: Vec::new(),
                    stderr_truncated: false,
                },
            });
        }
        let (worker, worker_id, _guard) = match self.select() {
            Ok(selection) => selection,
            Err(error) => return Ok(self.failure_report(started, error)),
        };
        let backend_id = format!("remote:{worker_id}@{}", worker.endpoint());
        worker
            .with_expected_worker_id(worker_id)
            .execute(request, cancel)
            .map(|outcome| ExecutionReport {
                outcome,
                backend_id,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_specific_credentials_validate_without_exposing_tokens() {
        let pool = RemotePoolExecutor::from_credentials(vec![
            ("http://worker-a:8488/".into(), "secret-a".into()),
            ("http://worker-b:8488".into(), "secret-b".into()),
        ])
        .expect("pool");
        let debug = format!("{pool:?}");
        assert!(!debug.contains("secret-a"));
        assert!(!debug.contains("secret-b"));
    }

    #[test]
    fn endpoint_specific_credentials_reject_duplicate_normalized_endpoint() {
        let error = RemotePoolExecutor::from_credentials(vec![
            ("http://worker-a:8488".into(), "secret-a".into()),
            ("http://worker-a:8488/".into(), "secret-b".into()),
        ])
        .expect_err("duplicate endpoint");
        assert!(error.contains("duplicate remote worker endpoint"));
        assert!(!error.contains("secret-a"));
        assert!(!error.contains("secret-b"));
    }

    #[test]
    fn endpoint_specific_credentials_reject_empty_token_without_other_secret_values() {
        let error = RemotePoolExecutor::from_credentials(vec![
            ("http://worker-a:8488".into(), "secret-a".into()),
            ("http://worker-b:8488".into(), String::new()),
        ])
        .expect_err("empty token");
        assert!(error.contains("worker-b"));
        assert!(!error.contains("secret-a"));
    }
}
