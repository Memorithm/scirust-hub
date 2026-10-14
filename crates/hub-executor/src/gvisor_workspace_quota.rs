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

    // Require an immediate child mount, not an arbitrary subdirectory on a
    // shared host disk. The private journal remains on root's OTHER device.
    if workspace.parent() != Some(root) {
        return Err("quota workspace must be a dedicated immediate child mount".into());
    }
    let parent = File::open(root).map_err(|e| e.to_string())?;
    let mounted = OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_DIRECTORY)
        .open(workspace)
        .map_err(|e| e.to_string())?;
    if parent.metadata().map_err(|e| e.to_string())?.dev()
        == mounted.metadata().map_err(|e| e.to_string())?.dev()
    {
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
    capacity(quota, size, geometry.blocks(), geometry.files())
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
