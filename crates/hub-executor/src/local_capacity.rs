//! Shared local execution slots, not an adaptive resource policy or RAM quota.

use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use hub_core::exec::{CancelToken, ExecutionOutcome, ExecutionRequest, Executor};
use hub_core::ExecutorFailure;

use crate::ProcessExecutor;

#[derive(Default)]
struct State {
    active: usize,
    quarantined: bool,
}

/// One process-local ceiling shared by every caller of this executor instance.
///
/// Queue wait consumes the request timeout; waiting cancellation never spawns.
/// A backend error/panic quarantines the instance because child termination is
/// then not proven. There is deliberately no unsafe reset/expiry operation.
/// This is not a persistent lease, OS quota, cross-daemon pool or fairness promise.
pub struct LocalCapacityExecutor {
    inner: Arc<dyn Executor>,
    limit: usize,
    backend: String,
    state: Mutex<State>,
    changed: Condvar,
}

impl LocalCapacityExecutor {
    /// Build a bounded local process executor. No work is started here.
    ///
    /// # Errors
    /// Rejects limits outside 1..=256.
    pub fn new(limit: usize) -> Result<Self, String> {
        if !(1..=256).contains(&limit) {
            return Err("local max inflight must be in 1..=256".into());
        }
        Ok(Self {
            inner: Arc::new(ProcessExecutor::new()),
            limit,
            backend: format!("process-capacity/v1/slots/{limit}"),
            state: Mutex::new(State::default()),
            changed: Condvar::new(),
        })
    }

    fn acquire(
        &self,
        cancel: &CancelToken,
        started: Instant,
        timeout_ms: u64,
    ) -> Result<Slot<'_>, ExecutorFailure> {
        let budget = Duration::from_millis(timeout_ms);
        let mut state = self.state.lock().map_err(|_| unavailable())?;
        loop {
            if state.quarantined {
                return Err(unavailable());
            }
            if cancel.is_cancelled() {
                return Err(ExecutorFailure::Cancelled);
            }
            let remaining = budget.saturating_sub(started.elapsed());
            if remaining < Duration::from_millis(1) {
                return Err(ExecutorFailure::TimedOut { timeout_ms });
            }
            if state.active < self.limit {
                state.active += 1;
                return Ok(Slot {
                    executor: self,
                    completed: false,
                });
            }
            // CancelToken is an atomic flag, not a notification source.
            state = self
                .changed
                .wait_timeout(state, remaining.min(Duration::from_millis(10)))
                .map_err(|_| unavailable())?
                .0;
        }
    }
}

fn unavailable() -> ExecutorFailure {
    ExecutorFailure::Backend {
        reason: "local capacity quarantined or poisoned; execution termination is unproven".into(),
    }
}

struct Slot<'a> {
    executor: &'a LocalCapacityExecutor,
    completed: bool,
}

impl Drop for Slot<'_> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.executor.state.lock() {
            if self.completed {
                state.active -= 1;
            } else {
                // No optimistic release after an ambiguous error or unwinding.
                state.quarantined = true;
            }
        }
        self.executor.changed.notify_all();
    }
}

impl Executor for LocalCapacityExecutor {
    fn backend_id(&self) -> &str {
        &self.backend
    }

    fn execute(
        &self,
        request: &ExecutionRequest,
        cancel: &CancelToken,
    ) -> Result<ExecutionOutcome, ExecutorFailure> {
        let started = Instant::now();
        let mut slot = self.acquire(cancel, started, request.timeout_ms)?;
        // Recheck after admission: cancellation/deadline may race the wake-up.
        if cancel.is_cancelled() {
            slot.completed = true;
            return Err(ExecutorFailure::Cancelled);
        }
        let mut bounded = request.clone();
        bounded.timeout_ms = u64::try_from(
            Duration::from_millis(request.timeout_ms)
                .saturating_sub(started.elapsed())
                .as_millis(),
        )
        .unwrap_or(0);
        if bounded.timeout_ms == 0 {
            slot.completed = true;
            return Err(ExecutorFailure::TimedOut {
                timeout_ms: request.timeout_ms,
            });
        }
        let result = self.inner.execute(&bounded, cancel);
        slot.completed = result.is_ok();
        result.map(|mut outcome| {
            outcome.duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
            outcome
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::thread;

    fn request(timeout_ms: u64) -> ExecutionRequest {
        ExecutionRequest {
            program: "unused".into(),
            args: vec![],
            working_dir: std::env::temp_dir(),
            env: Default::default(),
            timeout_ms,
            max_capture_bytes_per_stream: 1024,
        }
    }

    #[test]
    fn bounds_cancel_and_timeout_do_not_consume_slots() {
        assert!(LocalCapacityExecutor::new(0).is_err());
        assert!(LocalCapacityExecutor::new(257).is_err());
        let executor = LocalCapacityExecutor::new(1).unwrap();
        let token = CancelToken::new();
        token.cancel();
        assert_eq!(
            executor.execute(&request(100), &token),
            Err(ExecutorFailure::Cancelled)
        );
        assert_eq!(
            executor.execute(&request(0), &CancelToken::new()),
            Err(ExecutorFailure::TimedOut { timeout_ms: 0 })
        );
        assert_eq!(executor.state.lock().unwrap().active, 0);
    }

    #[test]
    fn waiters_cancel_or_timeout_without_dispatch() {
        let executor = Arc::new(LocalCapacityExecutor::new(1).unwrap());
        let mut held = executor
            .acquire(&CancelToken::new(), Instant::now(), 1000)
            .unwrap();
        let cancel = CancelToken::new();
        let (sent, received) = mpsc::channel();
        let worker = Arc::clone(&executor);
        let worker_cancel = cancel.clone();
        let join = thread::spawn(move || {
            sent.send(worker.execute(&request(1000), &worker_cancel))
                .unwrap()
        });
        cancel.cancel();
        assert_eq!(
            received.recv_timeout(Duration::from_secs(2)).unwrap(),
            Err(ExecutorFailure::Cancelled)
        );
        join.join().unwrap();
        assert_eq!(
            executor.execute(&request(5), &CancelToken::new()),
            Err(ExecutorFailure::TimedOut { timeout_ms: 5 })
        );
        assert_eq!(executor.state.lock().unwrap().active, 1);
        held.completed = true;
        drop(held);
        assert_eq!(executor.state.lock().unwrap().active, 0);
    }

    #[test]
    fn unwind_quarantines_instead_of_releasing_ambiguous_capacity() {
        let executor = LocalCapacityExecutor::new(1).unwrap();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _held = executor
                .acquire(&CancelToken::new(), Instant::now(), 1000)
                .unwrap();
            panic!("synthetic backend panic");
        }));
        assert!(matches!(
            executor.execute(&request(100), &CancelToken::new()),
            Err(ExecutorFailure::Backend { .. })
        ));
    }

    struct Blocking {
        entered: mpsc::Sender<u64>,
        release: Mutex<mpsc::Receiver<()>>,
    }

    struct Ambiguous(std::sync::atomic::AtomicUsize);
    impl Executor for Ambiguous {
        fn backend_id(&self) -> &str {
            "test-error"
        }
        fn execute(
            &self,
            _: &ExecutionRequest,
            _: &CancelToken,
        ) -> Result<ExecutionOutcome, ExecutorFailure> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(ExecutorFailure::Backend {
                reason: "synthetic unknown termination".into(),
            })
        }
    }

    #[test]
    fn ambiguous_error_never_dispatches_another_call() {
        let inner = Arc::new(Ambiguous(std::sync::atomic::AtomicUsize::new(0)));
        let mut executor = LocalCapacityExecutor::new(2).unwrap();
        executor.inner = inner.clone();
        for _ in 0..2 {
            assert!(matches!(
                executor.execute(&request(1000), &CancelToken::new()),
                Err(ExecutorFailure::Backend { .. })
            ));
        }
        assert_eq!(inner.0.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(executor.state.lock().unwrap().active, 1);
        assert!(executor.state.lock().unwrap().quarantined);
    }
    impl Executor for Blocking {
        fn backend_id(&self) -> &str {
            "test-only"
        }
        fn execute(
            &self,
            request: &ExecutionRequest,
            _: &CancelToken,
        ) -> Result<ExecutionOutcome, ExecutorFailure> {
            self.entered.send(request.timeout_ms).unwrap();
            self.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
            Ok(ExecutionOutcome {
                exit_code: Some(0),
                signal: None,
                timed_out: false,
                cancelled: false,
                start_error: None,
                duration_ms: 0,
                stdout: vec![],
                stdout_truncated: false,
                stderr: vec![],
                stderr_truncated: false,
            })
        }
    }

    #[test]
    fn shared_callers_never_enter_backend_above_limit_and_slots_are_reused() {
        let (entered, observed) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let mut executor = LocalCapacityExecutor::new(2).unwrap();
        executor.inner = Arc::new(Blocking {
            entered,
            release: Mutex::new(released),
        });
        let executor = Arc::new(executor);
        let joins: Vec<_> = (0..6)
            .map(|_| {
                let worker = Arc::clone(&executor);
                thread::spawn(move || worker.execute(&request(5000), &CancelToken::new()).unwrap())
            })
            .collect();
        for _ in 0..2 {
            assert!(observed.recv_timeout(Duration::from_secs(2)).unwrap() <= 5000);
        }
        assert!(observed.recv_timeout(Duration::from_millis(30)).is_err());
        for _ in 0..4 {
            release.send(()).unwrap();
            assert!(observed.recv_timeout(Duration::from_secs(2)).unwrap() < 5000);
        }
        release.send(()).unwrap();
        release.send(()).unwrap();
        for join in joins {
            assert!(join.join().unwrap().exited_cleanly());
        }
        assert_eq!(executor.state.lock().unwrap().active, 0);
    }

    #[cfg(unix)]
    #[test]
    fn actual_process_success_failure_timeout_and_capacity_reuse() {
        let executor = LocalCapacityExecutor::new(1).unwrap();
        let mut req = request(1000);
        req.program = "/bin/sh".into();
        req.args = vec!["-c".into(), "printf actual".into()];
        assert_eq!(
            executor.execute(&req, &CancelToken::new()).unwrap().stdout,
            b"actual"
        );
        req.program = "/nonexistent/hub-test-command".into();
        assert!(executor
            .execute(&req, &CancelToken::new())
            .unwrap()
            .start_error
            .is_some());
        req.program = "/bin/sh".into();
        req.args = vec!["-c".into(), "sleep 5".into()];
        req.timeout_ms = 30;
        assert!(
            executor
                .execute(&req, &CancelToken::new())
                .unwrap()
                .timed_out
        );
        assert_eq!(executor.state.lock().unwrap().active, 0);
    }
}
