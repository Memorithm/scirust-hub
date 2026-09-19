# Shared local execution ceiling v1

`scirust-hubd --local-max-inflight 2` (or
`SCIRUST_HUB_LOCAL_MAX_INFLIGHT=2`) enables a **single process-local ceiling**
across all standalone runs and all workflows served by that daemon. The value
must be 1..256 and is rejected with a remote execution backend. Omission keeps
the existing behavior. Per-workflow `max_concurrency` remains independently
enforced: it cannot override the shared ceiling.

The daemon creates one `LocalCapacityExecutor` and passes that same instance to
its orchestrator. Each call acquires one slot immediately before invoking the
local process executor. Waiting consumes the request timeout. The remaining
budget is passed to the process executor; queue time does not grant additional
execution time. Cancellation is observed while waiting (10 ms polling cadence)
and checked again before dispatch. No fairness or real-time latency is promised.
Successful outcomes, including ordinary nonzero exits, spawn failures and
confirmed timeout/cancellation outcomes, release their slot. A backend error,
poisoned lock or panic fails closed: uncertain execution does not free capacity,
and new dispatch is quarantined. There is no blind reset or expiry operation.

Provenance identifies this opt-in path as `process-capacity/v1/slots/N`.
Successful executor observations include queue wait in `duration_ms`; queue
rejection uses existing typed cancellation/timeout errors. This is not a new
durable lifecycle state or a new retry authorization.

## Ownership and limits

Hub owns enforcement across campaigns; ElasticXxx continues to own measurement,
adaptive policy and validated resource transitions. No Elastic planning logic
is duplicated. An operator may configure a qualified capacity ceiling, but this
slice does **not** dynamically wire an Elastic observation into Hub, authenticate
such evidence, or resize the ceiling at runtime. TDI's existing per-graph
Elastic admission can coexist with this additional deployment ceiling.

One slot means one synchronous supervised invocation, not one CPU core or one
OS process: the child may create threads/descendants. This does not reserve RAM,
isolate CPUs, bound all pending request threads, or protect other Hub daemons.
Slots are not a durable lease ledger. A daemon crash can leave orphan processes;
before restarting, the operator must independently ensure previous execution
has stopped (e.g. through deployment-owned process/cgroup supervision). Do not
infer crash-safe global resource accounting from clean process-local tests.
Ambiguous backend errors likewise require independent termination verification,
not simply a restart to clear quarantine.

The HTTP qualification runs two simultaneous workflows, each asking for two
parallel steps, through the real daemon and real shell subprocesses. With one
shared slot, a filesystem exclusion oracle detects any overlapping worker
execution. Unit tests cover sharing/reuse, cancellation, deadline consumption,
invalid configuration and fail-closed unwinding. No throughput or ML maturity
claim is made by this correctness feature.

```sh
cargo test --locked -p hub-executor local_capacity
cargo test --locked -p scirust-hubd local_capacity
cargo test --locked -p scirust-hubd --test http_e2e shared_local_capacity
```
