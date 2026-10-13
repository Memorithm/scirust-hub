# PAPERS gVisor execution boundary

SciRust Hub owns the typed execution request and receipt used by PAPERS for
hostile Rust compilation. The concrete adapter invokes Docker with the runsc
userspace kernel, a digest-pinned image, default-deny networking, a read-only
root filesystem, a bounded writable workspace, dropped capabilities,
no-new-privileges, and explicit CPU, RAM, PID, wall-clock and output limits.

The adapter rejects mutable images, relaxed network/root policies, missing
bounds and oversized inputs before spawning Docker. Timeout handling kills the
named container and reaps the Docker client. Output drains concurrently into
bounded buffers.

The dedicated gVisor qualification workflow installs the fixed official gVisor
20260921 release after SHA-512 verification, registers runsc with Docker, and
executes a real PAPERS-shaped Rust compile/run probe through the public typed
CLI. Its receipt is retained as a workflow artifact. Unit tests validate the
exact Docker controls and negative admission paths without claiming that a mock
is backend evidence.

This boundary covers execution isolation and resource enforcement. It does not
qualify the candidate's scientific result, authorize publication, or give the
container access to Hub credentials.
