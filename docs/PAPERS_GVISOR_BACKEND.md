# PAPERS gVisor execution boundary

SciRust Hub owns the typed execution request and receipt used by PAPERS for
hostile Rust compilation. The concrete adapter invokes Docker with the runsc
userspace kernel, a digest-pinned image, default-deny networking, a read-only
root filesystem, an explicitly confined writable workspace, dropped capabilities,
no-new-privileges, and explicit CPU, RAM, PID, wall-clock and output limits.

The operator supplies an executor-owned workspace root. Each request must name
an existing task directory strictly below that canonical root, so hostile code
cannot turn an arbitrary host directory into a writable bind mount. The
deny-all network backend does not advertise endpoint-allowlist support.

The adapter rejects mutable images, relaxed network/root policies, missing
bounds and oversized inputs before spawning Docker; one task phase is capped at
one hour, and its deadline is checked before creation. A single phase budget
includes container creation and attached execution. Docker creates an immutable
container ID before start; cleanup never signals a matching name without that
ownership token. Existing `execution_guard` handles fresh client process groups,
bounded nonblocking reap confirmation and concurrent capped capture. After parent
exit, group cleanup precedes the shared one-second pipe drain deadline. Detached
pipe holders or uncertain termination produce backend errors, not successful
receipts.

Exact-ID removal and a successful empty daemon query share a three-second control
deadline. A client kill alone is not proof of daemon/container termination. Failed
queries, truncated controls, cleanup timeout, or ambiguous creation fail closed.
Ambiguous creation has no safe owned ID: do not blindly remove a colliding named
container or replay that attempt. Such backend errors require operator quarantine
and reconciliation before workspace reclamation. Memory and swap limits are both
set to the requested memory budget. A confined bind mount is **not an aggregate
workspace disk quota**, and PID limits remain unqualified against adversarial
guest forks in this slice.

The dedicated gVisor qualification workflow installs the fixed official gVisor
20261005.0 archive after SHA-256 verification, registers runsc with Docker, and
executes a real PAPERS-shaped Rust compile/run probe through the public typed
CLI. It retains both the receipt and a versioned manifest binding the exact
source commit, workflow run and receipt digest.

A plain executor remains explicitly unqualified. A production caller may enable
the qualification claim only by loading both the immutable manifest and the
retained receipt through `from_qualification_artifacts`. The loader requires
the manifest source commit to match the commit embedded at compile time, hashes
the actual receipt bytes, and validates the successful real-probe receipt before
constructing an evidence identifier with the commit, run and receipt SHA-256.
Unit tests validate stale-build and altered-receipt rejection without treating a
mock as backend evidence.

`hub_executor::execution_guard::execute_guarded_command` is the reusable
Rust guard for outer consumers. It accepts an explicitly constructed `Command`,
a checked absolute deadline and a one-byte-to-one-MiB per-stream capture budget.
It preserves ordinary process-group cleanup, bounded reap and drain semantics;
the caller still owns environment/argv, backend admission and domain authority.
Consumers must pin the qualified owner revision and may not reinterpret this
process guard as their OS isolation boundary.

This boundary covers execution isolation and resource enforcement. It does not
qualify the candidate's scientific result, authorize publication, or give the
container access to Hub credentials.
