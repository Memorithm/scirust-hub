from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one replacement, found {count}: {old[:180]!r}")
    p.write_text(text.replace(old, new, 1))

# Pin a pool-selected worker identity through RemoteExecutor's second descriptor
# read immediately before lease creation. If the endpoint now advertises a
# different identity, fail before any lease is submitted.
replace_once(
    "crates/hub-executor/src/remote.rs",
    '''    lost_after: Duration,
    max_payload_bytes: usize,
}''',
    '''    lost_after: Duration,
    max_payload_bytes: usize,
    expected_worker_id: Option<String>,
}''',
)
replace_once(
    "crates/hub-executor/src/remote.rs",
    '''            lost_after: Duration::from_millis(DEFAULT_LOST_AFTER_MS),
            max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES,
        })''',
    '''            lost_after: Duration::from_millis(DEFAULT_LOST_AFTER_MS),
            max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES,
            expected_worker_id: None,
        })''',
)
replace_once(
    "crates/hub-executor/src/remote.rs",
    '''    pub fn with_max_payload_bytes(mut self, value: usize) -> Self {
        self.max_payload_bytes = value.max(1);
        self
    }

    pub(crate) fn endpoint(&self) -> &str {''',
    '''    pub fn with_max_payload_bytes(mut self, value: usize) -> Self {
        self.max_payload_bytes = value.max(1);
        self
    }

    pub(crate) fn with_expected_worker_id(mut self, worker_id: impl Into<String>) -> Self {
        self.expected_worker_id = Some(worker_id.into());
        self
    }

    pub(crate) fn endpoint(&self) -> &str {''',
)
replace_once(
    "crates/hub-executor/src/remote.rs",
    '''        let descriptor = self.describe().map_err(|error| match error {
            RemoteCallError::Authorization => "authorization refused".to_owned(),
            other => format!("unavailable: {other}"),
        })?;
        if descriptor.protocol_version != WORKER_PROTOCOL_VERSION {''',
    '''        let descriptor = self.describe().map_err(|error| match error {
            RemoteCallError::Authorization => "authorization refused".to_owned(),
            other => format!("unavailable: {other}"),
        })?;
        if descriptor.worker_id.trim().is_empty() {
            return Err("worker identity is empty".into());
        }
        if descriptor.protocol_version != WORKER_PROTOCOL_VERSION {''',
)
replace_once(
    "crates/hub-executor/src/remote.rs",
    '''        let descriptor = match self.describe() {
            Ok(descriptor) => descriptor,
            Err(RemoteCallError::Authorization) => {
                return Ok(self.remote_failure(started, "remote worker authorization refused"));
            }
            Err(error) => {
                return Ok(
                    self.remote_failure(started, format!("remote worker unavailable: {error}"))
                );
            }
        };
        if descriptor.protocol_version != WORKER_PROTOCOL_VERSION {''',
    '''        let descriptor = match self.describe() {
            Ok(descriptor) => descriptor,
            Err(RemoteCallError::Authorization) => {
                return Ok(self.remote_failure(started, "remote worker authorization refused"));
            }
            Err(error) => {
                return Ok(
                    self.remote_failure(started, format!("remote worker unavailable: {error}"))
                );
            }
        };
        if descriptor.worker_id.trim().is_empty() {
            return Ok(self.remote_failure(started, "remote worker identity is empty"));
        }
        if let Some(expected) = &self.expected_worker_id {
            if descriptor.worker_id != *expected {
                return Ok(self.remote_failure(
                    started,
                    format!(
                        "remote worker identity changed before lease dispatch: expected {expected:?}, found {:?}",
                        descriptor.worker_id
                    ),
                ));
            }
        }
        if descriptor.protocol_version != WORKER_PROTOCOL_VERSION {''',
)

replace_once(
    "crates/hub-executor/src/pool.rs",
    '''        let backend_id = format!("remote:{worker_id}@{}", worker.endpoint());
        worker
            .execute(request, cancel)''',
    '''        let backend_id = format!("remote:{worker_id}@{}", worker.endpoint());
        worker
            .with_expected_worker_id(worker_id)
            .execute(request, cancel)''',
)

replace_once(
    "docs/adr/0014-configured-multi-worker-placement.md",
    '''After selection, the invocation is pinned to that endpoint and the existing
lease protocol remains authoritative. The pool does **not** fail over an
ambiguous lease creation or active lease to a second worker.
''',
    '''After selection, the invocation is pinned to that endpoint and worker identity.
`RemoteExecutor` re-reads the descriptor immediately before lease creation and
requires the selected `worker_id` to remain unchanged; identity drift fails
closed before a lease is submitted. The existing lease/status identity checks
remain authoritative afterwards. The pool does **not** fail over an ambiguous
lease creation or active lease to a second worker.
''',
)

print("selected worker identity pinning applied")
