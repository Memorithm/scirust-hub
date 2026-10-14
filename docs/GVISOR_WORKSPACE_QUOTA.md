# PAPERS-01: persistent aggregate task workspace admission

The optional `workspace_quota` request object contains `max_bytes` and
`max_inodes`. Absence preserves legacy wire behavior and makes **no aggregate
quota claim**. Bounds are positive, at most 1 GiB / 65,536 inodes.

For this profile the operator must provision a dedicated persistent ext2/3/4
filesystem mounted at an immediate child of the private executor root. Its
total data-block capacity and total inode count must fit both ceilings. Hub
checks the opened directory's filesystem geometry; free space, per-file
RLIMIT_FSIZE and a manifest declaration are not accepted as aggregate bounds.
An ordinary directory on the controller filesystem is refused before Docker
or a durable reservation. The private journal stays on the other filesystem.
The guest has no capabilities, cannot resize/remount the provisioned volume,
and receives a nonrecursive private bind mount: nested host mounts are excluded.

Provisioning, retention and eventual disposal remain the owning runtime's
operator/provider responsibility. Hub does not mount, format, resize or delete
operator volumes. The trusted local Docker daemon and filesystem administrator
must share the controller's filesystem view and must not replace/resize mounts
concurrently. Remote Docker/Docker Desktop and untrusted host administrators are
not qualified by this profile. It does not promise a free-space reservation or
protect a task from an administrator filling/resizing its dedicated filesystem.

The hosted Linux runsc gate uses a new 64 MiB loop-backed ext4 image with 256
inodes. It compiles inside runsc, unmounts/remounts the host fixture, executes the
persisted binary and marker in a new phase, then proves actual ENOSPC from a
finite multi-file write (each file 4 MiB, below the independent 16 MiB per-file
limit) and a finite empty-file inode probe. A larger nested host tmpfs is not
imported. Each phase must reach `cleaned` in the journal outside the full volume.
The fixture is not production adoption, consumer qualification, a scientific
promotion or hardware evidence. PAPERS-01 stays open until its consumer adopts
and qualifies this profile with preserved admission/quarantine semantics.
