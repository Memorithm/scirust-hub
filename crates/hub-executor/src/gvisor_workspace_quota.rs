//! Admission of a bounded, persistent host-provisioned task filesystem.
//! No privileged provisioning, resizing or quota claims from free space.

use std::path::Path;

use crate::gvisor::GvisorWorkspaceQuotaV1;

fn capacity(
    quota: &GvisorWorkspaceQuotaV1,
    block_size: u64,
    blocks: u64,
    inodes: u64,
) -> Result<(), String> {
    if quota.max_bytes == 0
        || quota.max_bytes > 1024 * 1024 * 1024
        || quota.max_inodes == 0
        || quota.max_inodes > 65536
    {
        return Err("invalid persistent workspace quota bounds".into());
    }
    let bytes = block_size
        .checked_mul(blocks)
        .filter(|bytes| *bytes > 0)
        .ok_or("persistent filesystem capacity is zero or overflowing")?;
    if bytes > quota.max_bytes || inodes == 0 || inodes > quota.max_inodes {
        return Err("persistent filesystem total capacity/inodes exceed workspace quota".into());
    }
    Ok(())
}

#[cfg(target_os = "linux")]
pub(crate) fn admit(
    root: &Path,
    workspace: &Path,
    quota: &GvisorWorkspaceQuotaV1,
) -> Result<(), String> {
    use nix::sys::statfs::{fstatfs, FsType};
    use std::fs::{File, OpenOptions};
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

    // A bounded persistent volume is an immediate child mount. Task directories
    // may live inside it; the ceiling bounds the WHOLE volume, not free-space
    // reservations for concurrent tasks. Only the task directory is exposed.
    let child = workspace
        .strip_prefix(root)
        .map_err(|e| e.to_string())?
        .components()
        .next()
        .ok_or("missing persistent volume child")?;
    let volume = root.join(child.as_os_str());
    let parent = File::open(root).map_err(|e| e.to_string())?;
    let mounted = OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_DIRECTORY)
        .open(workspace)
        .map_err(|e| e.to_string())?;
    let device = mounted.metadata().map_err(|e| e.to_string())?.dev();
    let volume_device = std::fs::metadata(volume).map_err(|e| e.to_string())?.dev();
    if parent.metadata().map_err(|e| e.to_string())?.dev() == device || volume_device != device {
        return Err("quota workspace must not share the controller filesystem".into());
    }
    let geometry =
        fstatfs(&mounted).map_err(|e| format!("filesystem quota observation failed: {e}"))?;
    // ext2/3/4 persistent fixed block/inode geometry. Reject tmpfs, overlay,
    // network and user-space filesystems rather than infer persistence/limits.
    if geometry.filesystem_type() != FsType(0xef53) {
        return Err("quota workspace requires a persistent ext-family filesystem".into());
    }
    let size = u64::try_from(geometry.block_size()).map_err(|e| e.to_string())?;
    capacity(quota, size, geometry.blocks(), geometry.files())?;
    // runsc/gofer behavior with a nested source mount is not assumed from
    // Docker's nonrecursive option. Refuse it before container creation.
    use std::io::Read;
    let mut mounts = Vec::new();
    File::open("/proc/self/mountinfo")
        .map_err(|e| e.to_string())?
        .take(1024 * 1024 + 1)
        .read_to_end(&mut mounts)
        .map_err(|e| e.to_string())?;
    if mounts.len() > 1024 * 1024 {
        return Err("mount inventory exceeds quota admission bound".into());
    }
    no_submounts(
        workspace,
        std::str::from_utf8(&mounts).map_err(|e| e.to_string())?,
    )
}

fn no_submounts(workspace: &Path, inventory: &str) -> Result<(), String> {
    let mut count = 0;
    for line in inventory.lines() {
        count += 1;
        if count > 8192 {
            return Err("mount inventory exceeds entry bound".into());
        }
        let encoded = line
            .split_whitespace()
            .nth(4)
            .ok_or("invalid mount inventory")?;
        let decoded = encoded
            .replace("\\040", " ")
            .replace("\\011", "\t")
            .replace("\\012", "\n")
            .replace("\\134", "\\");
        let mount = Path::new(&decoded);
        if mount != workspace && mount.starts_with(workspace) {
            return Err("nested host mounts are not admissible in a quota workspace".into());
        }
    }
    if count == 0 {
        return Err("empty mount inventory".into());
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn admit(
    _root: &Path,
    _workspace: &Path,
    _quota: &GvisorWorkspaceQuotaV1,
) -> Result<(), String> {
    Err("persistent workspace quota admission requires qualified Linux support".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mount_inventory_refuses_nested_and_preserves_path_boundaries() {
        let root = Path::new("/owned/task");
        assert!(no_submounts(root, "1 0 1:1 / /owned/task rw - ext4 /dev/loop0 rw").is_ok());
        assert!(no_submounts(root, "1 0 1:1 / /owned/task-other rw - ext4 /dev/loop0 rw").is_ok());
        assert!(no_submounts(root, "1 0 1:1 / /owned/task/nested rw - tmpfs tmpfs rw").is_err());
        assert!(no_submounts(
            Path::new("/owned/my task"),
            "1 0 1:1 / /owned/my\\040task/nested rw - tmpfs tmpfs rw"
        )
        .is_err());
        assert!(no_submounts(root, "").is_err());
        assert!(no_submounts(root, "bad").is_err());
    }

    #[test]
    fn capacity_uses_total_geometry_not_available_space() {
        let quota = GvisorWorkspaceQuotaV1 {
            max_bytes: 65536,
            max_inodes: 64,
        };
        assert!(capacity(&quota, 4096, 16, 64).is_ok());
        assert!(capacity(&quota, 4096, 17, 64).is_err());
        assert!(capacity(&quota, 4096, 16, 65).is_err());
        assert!(capacity(&quota, 0, 16, 64).is_err());
        assert!(capacity(&quota, u64::MAX, 16, 64).is_err());
        assert!(capacity(&quota, 4096, 16, 0).is_err());
    }

    #[test]
    fn zero_or_unbounded_request_is_not_admissible() {
        for quota in [
            GvisorWorkspaceQuotaV1 {
                max_bytes: 0,
                max_inodes: 64,
            },
            GvisorWorkspaceQuotaV1 {
                max_bytes: 65536,
                max_inodes: 0,
            },
            GvisorWorkspaceQuotaV1 {
                max_bytes: u64::MAX,
                max_inodes: 64,
            },
            GvisorWorkspaceQuotaV1 {
                max_bytes: 65536,
                max_inodes: u64::MAX,
            },
        ] {
            assert!(capacity(&quota, 4096, 16, 64).is_err());
        }
    }
}
