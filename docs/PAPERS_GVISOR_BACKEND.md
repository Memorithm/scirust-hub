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
workspace disk quota**.

The guest additionally receives soft/hard `RLIMIT_NPROC` limits; host
`--pids-limit` alone is not treated as evidence of a guest process/thread count.
The optional `guest_pids_limit` must be1..`pids_limit`; omitting it preserves
the existing request wire and uses the host budget as the guest limit. The
host ceiling is never widened implicitly. PID budgets are restricted to1..4096 and UID0 workspace owners are refused before
container creation because root must not bypass the guest limit. The real gate
uses a finite mixed probe of eight sleeping processes and at most96 threads,
checks nonroot identity and effective soft/hard limits, requires EAGAIN below
guest budget64, and requires all104 child tasks to succeed under a128 positive
control. Both cases keep the separate host ceiling fixed at512, above the entire
finite probe range: changing only the guest limit disambiguates its enforcement.
This is bounded isolated qualification, not an exponential fork workload or a
scientific/hardware-performance result. Aggregate disk quotas remain incomplete.

## Durable attempts and quarantine

The controller reuses Hub's SQLite store (WAL, FULL synchronous commits), an
additive forward migration and the existing lifecycle event log. Before Docker
creation it reserves a unique attempt nonce and request digest. Before start it
commits the confirmed immutable container ID. The private 0700 controller state
directory is a sibling of task workspaces and may never be mounted as a guest
workspace; its files are owned, regular, no-follow, single-link and 0600. The
controller root must not be group/world writable. Safe nonblocking File locks
(Rust MSRV1.89) fence a live task and workspace and release on controller exit;
the durable journal refuses task-ID reuse, even after cleanup, and quarantines
overlapping ancestor/descendant workspaces. It caps uncleaned attempts at256.
Locks and journal are controller metadata, not another scheduler or proof schema.

`scirust-hub-gvisor --inspect TASK_ID WORKSPACE_ROOT` returns metadata, never a
success receipt. `--reconcile TASK_ID WORKSPACE_ROOT` performs only bounded
read-only daemon queries. An ambiguous creation can acquire an immutable handle
only from one full-ID result matching both its random nonce and request digest.
Zero/multiple/truncated/failed responses keep the reservation quarantined; zero
is not proof that an asynchronous create cannot appear later. A live owned
container stays quarantined for explicit operator cleanup. Only a successful
exact-ID absence observation moves owned state to cleaned. Reconciliation never
starts, replays or deletes a container and never grants scientific authority.
Cleaned metadata permits a new task ID on the reclaimed workspace, not replay
of the original task. Unconfirmed creation without a recoverable resource keeps
its workspace quarantined; no unsafe automated override is provided.

The real qualification gate kills its own controller after positive guest entry,
verifies the container remains running and refuses replay/reconciliation until
explicit exact-ID cleanup. A separate fault transport loses the response of a
real stopped container creation; restart recovers ownership via both labels but
never starts it. Retained records/observations cover this recovery contract, not
production adoption, quota enforcement or hardware/scientific qualification.
The consumer's locked source graph must include hub-store-sqlite in addition to
hub-executor, hub-core and hub-protocol when adopting this revision.

Every regular file write is limited by the guest OS `RLIMIT_FSIZE` soft/hard
ceiling of 16 MiB (matching PAPERS' native artifact admission). The real gate
attempts one bounded 17 MiB write and requires an unsuccessful non-timeout exit
with exactly 16 MiB produced. This per-file limit is not an aggregate quota for
many files. Runtime read-only-root qualification also requires a positive `ro`
root mount observation; nonroot permission denial alone is insufficient.

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
