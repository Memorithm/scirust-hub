# ADR 0017 — Unix process-group supervision

Status: accepted

## Context

`ProcessExecutor` enforces wall-clock timeout and cooperative cancellation, but
its termination path historically called `Child::kill()` on only the direct
child. A process may create ordinary descendants that inherit its stdout/stderr
and continue running after the direct child is killed. That can retain pipes,
delay executor completion and consume resources beyond the cancelled/expired
run.

The execution model is explicitly not a security sandbox. The goal here is
therefore narrower: make the existing timeout/cancellation resource boundary
apply to ordinary Unix descendants without claiming containment against a
process that deliberately escapes supervision.

## Decision

On Unix, `ProcessExecutor` configures every spawned command with
`CommandExt::process_group(0)`. The child becomes leader of a fresh process
group whose PGID equals its PID. Ordinary descendants inherit that group.

On timeout or cancellation, the executor sends `SIGKILL` to the child's process
group using the safe `nix::sys::signal::killpg` wrapper, then still calls
`Child::kill()` as a best-effort fallback before waiting/reaping the direct
child. No `unsafe` block is introduced into the workspace.

On non-Unix targets process-group APIs are not available through this design,
so the previous direct-child kill behavior is retained.

## Invariants

- Structured argv/environment semantics are unchanged; no implicit shell is
  introduced.
- Timeout and cancellation remain observable through the existing
  `ExecutionOutcome` fields.
- The Hub process is not placed in the execution group and must never be
  signalled by execution cancellation.
- The remote worker inherits the same behavior because it delegates to
  `ProcessExecutor`.
- Spawn failures remain observed outcomes rather than backend panics.

## Limits / non-goals

This remains **resource supervision, not a sandbox**. A descendant can call
`setsid`/`setpgid` or otherwise deliberately leave the inherited process group.
The change does not add namespaces, cgroups, seccomp, containers, Windows Job
Objects, privilege dropping, syscall filtering, or durable process ownership.
Those require separate platform/security designs.
