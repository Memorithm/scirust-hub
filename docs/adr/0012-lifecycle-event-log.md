# ADR 0012 — Durable lifecycle event log

Status: accepted

## Decision

SciRust Hub records an append-only, monotonically sequenced lifecycle event
stream for successful authoritative metadata mutations. Events cover component
registration, artifact recording, run creation/state transitions, workflow
creation/state transitions/cancellation, and workflow attempt creation/state
changes.

The event stream is **not** a second state machine. Component manifests, run
records, workflow records and artifact metadata remain authoritative. Events
are derived from those successful writes and exist for operations,
observability, incremental consumers and later metrics.

For SQLite, event rows are inserted in the same transaction as the metadata
mutation that caused them. A committed mutation therefore cannot be separated
from its corresponding lifecycle append by a process crash. Identical component
registration replays and repeated unchanged snapshots produce no duplicate
lifecycle entries.

The in-memory daemon mode uses a composite store exposing the same repository
ports and event chronology; it remains intentionally ephemeral.

## Read contract

`GET /api/v1/events?after=<sequence>&limit=<n>` returns events whose sequence is
strictly greater than the supplied cursor, ordered oldest first. Page size is
bounded to 1..=1000 and the response returns `next_after`. The CLI exposes
`scirust-hub event list`, and the read-only MCP adapter exposes
`hub.list_events`.

Sequences are local to one Hub database. They establish append order, not a
global distributed clock. `recorded_at` is the best domain timestamp available
for the mutation and must not be used to reorder equal/concurrent events.

## Security and privacy

Lifecycle attributes are deliberately small and structured. They contain
identifiers, states, names, versions, digests and sizes; they do not record
environment values, bearer tokens or subprocess stdout/stderr payloads. Access
to `/api/v1/events` follows the same control-plane authentication policy as the
rest of `/api/v1`.
