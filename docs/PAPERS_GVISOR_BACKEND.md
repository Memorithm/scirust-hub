# PAPERS gVisor execution boundary

SciRust Hub owns the typed execution request and receipt used by PAPERS for
hostile Rust compilation. The concrete adapter invokes Docker with the runsc
userspace kernel, a digest-pinned image, default-deny networking, a read-only
root filesystem, a bounded writable workspace, dropped capabilities,
no-new-privileges, and explicit CPU, RAM, PID, wall-clock and output limits.

The operator supplies an executor-owned workspace root. Each request must name
an existing task directory strictly below that canonical root, so hostile code
cannot turn an arbitrary host directory into a writable bind mount. The
deny-all network backend does not advertise endpoint-allowlist support.

The adapter rejects mutable images, relaxed network/root policies, missing
bounds and oversized inputs before spawning Docker. Timeout handling kills the
named container and reaps the Docker client. Output drains concurrently into
bounded buffers.

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

This boundary covers execution isolation and resource enforcement. It does not
qualify the candidate's scientific result, authorize publication, or give the
container access to Hub credentials.
