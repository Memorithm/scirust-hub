use hub_core::digest::DOMAIN_ARTIFACT_BLOB;
use hub_core::store::ArtifactStore;
use hub_core::{CoreError, FileSystemArtifactStore};

fn temp_root(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "hub-artifact-file-{label}-{}",
        uuid::Uuid::new_v4()
    ))
}

#[test]
fn regular_file_ingest_matches_byte_ingest_identity() {
    let root = temp_root("regular");
    std::fs::create_dir_all(&root).expect("temp root");
    let input = root.join("adapter.safetensors");
    let payload = b"deterministic model artifact";
    std::fs::write(&input, payload).expect("write input");

    let store = FileSystemArtifactStore::open(root.join("store")).expect("store");
    let expected = store
        .put(payload, 1024, DOMAIN_ARTIFACT_BLOB)
        .expect("byte ingest");
    let (actual, size) = store
        .put_file(&input, 1024, DOMAIN_ARTIFACT_BLOB)
        .expect("file ingest");

    assert_eq!(actual, expected);
    assert_eq!(size, payload.len() as u64);
    assert_eq!(store.read(&actual).expect("read"), payload);

    std::fs::remove_dir_all(&root).expect("cleanup");
}

#[test]
fn oversized_file_is_rejected_before_publication() {
    let root = temp_root("oversize");
    std::fs::create_dir_all(&root).expect("temp root");
    let input = root.join("checkpoint.bin");
    std::fs::write(&input, vec![0xA5; 65]).expect("write input");

    let store = FileSystemArtifactStore::open(root.join("store")).expect("store");
    assert!(matches!(
        store.put_file(&input, 64, DOMAIN_ARTIFACT_BLOB),
        Err(CoreError::ArtifactTooLarge {
            size: 65,
            limit: 64,
            ..
        })
    ));

    std::fs::remove_dir_all(&root).expect("cleanup");
}

#[test]
fn directory_is_rejected_as_non_regular_artifact() {
    let root = temp_root("directory");
    let input = root.join("model-dir");
    std::fs::create_dir_all(&input).expect("input dir");

    let store = FileSystemArtifactStore::open(root.join("store")).expect("store");
    let err = store
        .put_file(&input, 1024, DOMAIN_ARTIFACT_BLOB)
        .expect_err("directory must fail");
    assert!(matches!(err, CoreError::Storage(message) if message.contains("non-regular")));

    std::fs::remove_dir_all(&root).expect("cleanup");
}

#[cfg(unix)]
#[test]
fn symbolic_link_is_rejected_even_when_target_is_regular() {
    use std::os::unix::fs::symlink;

    let root = temp_root("symlink");
    std::fs::create_dir_all(&root).expect("temp root");
    let target = root.join("real.bin");
    let link = root.join("declared-output.bin");
    std::fs::write(&target, b"outside declared identity").expect("write target");
    symlink(&target, &link).expect("create symlink");

    let store = FileSystemArtifactStore::open(root.join("store")).expect("store");
    let err = store
        .put_file(&link, 1024, DOMAIN_ARTIFACT_BLOB)
        .expect_err("symlink must fail");
    assert!(matches!(err, CoreError::Storage(message) if message.contains("symbolic-link")));

    std::fs::remove_dir_all(&root).expect("cleanup");
}
