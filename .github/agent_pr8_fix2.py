from pathlib import Path

path = Path(".github/agent_pr8.py")
text = path.read_text()

old_pool = '''        Ok(Self {
            workers,
            in_flight: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    fn select(&self) -> Result<(RemoteExecutor, String, InFlightGuard), String> {'''
new_pool = '''        Ok(Self {
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

    fn select(&self) -> Result<(RemoteExecutor, String, InFlightGuard), String> {'''
if text.count(old_pool) != 1:
    raise SystemExit(f"expected one pool insertion point, found {text.count(old_pool)}")
text = text.replace(old_pool, new_pool, 1)

old_parallel = '''    let pool = std::sync::Arc::new(
        RemotePoolExecutor::new(vec![endpoint_b, endpoint_a], "secret").expect("pool"),
    );'''
new_parallel = '''    let pool = std::sync::Arc::new(
        RemotePoolExecutor::new(vec![endpoint_b, endpoint_a], "secret")
            .expect("pool")
            .with_max_payload_bytes(8 * 1024 * 1024),
    );'''
if text.count(old_parallel) != 1:
    raise SystemExit(f"expected one parallel pool construction, found {text.count(old_parallel)}")
text = text.replace(old_parallel, new_parallel, 1)

old_duplicate = '''    let pool = RemotePoolExecutor::new(vec![endpoint_a, endpoint_b], "secret").expect("pool");'''
new_duplicate = '''    let pool = RemotePoolExecutor::new(vec![endpoint_a, endpoint_b], "secret")
        .expect("pool")
        .with_max_payload_bytes(8 * 1024 * 1024);'''
if text.count(old_duplicate) != 1:
    raise SystemExit(f"expected one duplicate-id pool construction, found {text.count(old_duplicate)}")
text = text.replace(old_duplicate, new_duplicate, 1)

path.write_text(text)
print("generator pool payload configuration fixed")
