//! Exact-revision Git workspace materialization for task execution.
//!
//! This module is deliberately executor-side: it performs filesystem and Git
//! I/O but does not own task lifecycle semantics. The resulting
//! [`WorkspaceMaterializationEvidence`] is persisted by the Hub before a task
//! may enter the `Admitted` state.
//!
//! v1 intentionally does not initialize Git submodules, run LFS smudges, or
//! claim read-only filesystem enforcement. Those are separate capabilities.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use hub_core::digest::{DigestState, DOMAIN_TASK_WORKSPACE_REPOSITORY};
use hub_core::{
    ContentDigest, CoreError, MaterializedRepositoryEvidence, WorkspaceMaterializationEvidence,
    WorkspaceSpec,
};

/// Trusted resolution from a workspace repository identity to a Git clone
/// source. Values may be local paths or Git URLs. Sources are control-plane
/// configuration, not task-provided shell fragments.
pub type WorkspaceSourceMap = BTreeMap<String, String>;

/// Runtime result of one fully materialized workspace.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterializedWorkspace {
    pub root: PathBuf,
    pub repository_paths: BTreeMap<String, PathBuf>,
    pub evidence: WorkspaceMaterializationEvidence,
}

/// Git-backed exact-revision workspace materializer.
///
/// `git_program` is configurable for hermetic tests and deployments but is
/// invoked directly through argv; no shell is involved.
#[derive(Clone, Debug)]
pub struct GitWorkspaceMaterializer {
    git_program: PathBuf,
}

impl Default for GitWorkspaceMaterializer {
    fn default() -> Self {
        Self::new("git")
    }
}

impl GitWorkspaceMaterializer {
    #[must_use]
    pub fn new(git_program: impl Into<PathBuf>) -> Self {
        Self {
            git_program: git_program.into(),
        }
    }

    /// Materializes every repository into a fresh deterministic subdirectory,
    /// verifies the detached HEAD equals the requested commit, rejects
    /// submodules in v1, verifies the worktree is clean before and after
    /// content hashing, and returns canonical evidence.
    ///
    /// The caller supplies trusted source resolution separately from
    /// [`WorkspaceSpec`] so task manifests cannot silently redirect a logical
    /// repository identity to an arbitrary credential-bearing URL.
    ///
    /// # Errors
    /// Fails closed when the target already exists, a source is missing, Git
    /// cannot resolve/checkout the exact commit, a submodule is present, the
    /// worktree is dirty, a special/non-UTF8 path is encountered, or evidence
    /// validation fails.
    pub fn materialize(
        &self,
        spec: &WorkspaceSpec,
        root: &Path,
        sources: &WorkspaceSourceMap,
    ) -> Result<MaterializedWorkspace, CoreError> {
        spec.validate()?;
        if root.exists() {
            return Err(CoreError::Validation(format!(
                "workspace root {:?} already exists; refusing to reuse mutable state",
                root
            )));
        }

        std::fs::create_dir_all(root)
            .map_err(|error| CoreError::Storage(format!("creating workspace root: {error}")))?;

        let result = self.materialize_created_root(spec, root, sources);
        if result.is_err() {
            let _ = std::fs::remove_dir_all(root);
        }
        result
    }

    fn materialize_created_root(
        &self,
        spec: &WorkspaceSpec,
        root: &Path,
        sources: &WorkspaceSourceMap,
    ) -> Result<MaterializedWorkspace, CoreError> {
        let mut evidence = Vec::with_capacity(spec.repositories.len());
        let mut repository_paths = BTreeMap::new();

        for (index, repository) in spec.repositories.iter().enumerate() {
            let source = sources.get(&repository.repository).ok_or_else(|| {
                CoreError::Validation(format!(
                    "no trusted workspace source configured for repository {:?}",
                    repository.repository
                ))
            })?;
            if source.is_empty() || source.trim() != source || source.chars().any(char::is_control)
            {
                return Err(CoreError::Validation(format!(
                    "workspace source for repository {:?} is malformed",
                    repository.repository
                )));
            }

            let relative = format!("repos/{index:04}");
            let destination = root.join(&relative);
            if let Some(parent) = destination.parent() {
                std::fs::create_dir_all(parent).map_err(|error| {
                    CoreError::Storage(format!("creating repository parent directory: {error}"))
                })?;
            }

            self.git_clone_without_checkout(source, &destination)?;
            self.verify_commit_exists(&destination, &repository.revision)?;
            self.reject_submodules(&destination, &repository.revision)?;
            self.checkout_detached(&destination, &repository.revision)?;

            let observed_revision =
                self.git_stdout(&destination, &["rev-parse", "--verify", "HEAD"])?;
            if observed_revision != repository.revision {
                return Err(CoreError::Validation(format!(
                    "repository {} materialized HEAD {} instead of requested {}",
                    repository.repository, observed_revision, repository.revision
                )));
            }
            let tree_id =
                self.git_stdout(&destination, &["rev-parse", "--verify", "HEAD^{tree}"])?;
            self.require_clean_worktree(&destination)?;
            let content_digest = digest_materialized_checkout(&destination)?;
            self.require_clean_worktree(&destination)?;

            repository_paths.insert(repository.repository.clone(), PathBuf::from(relative));
            evidence.push(MaterializedRepositoryEvidence {
                repository: repository.repository.clone(),
                requested_revision: repository.revision.clone(),
                observed_revision,
                tree_id,
                content_digest,
                read_only_requested: repository.read_only,
            });
        }

        let evidence = WorkspaceMaterializationEvidence::new(spec, evidence)?;
        evidence.validate_against(spec)?;
        Ok(MaterializedWorkspace {
            root: root.to_path_buf(),
            repository_paths,
            evidence,
        })
    }

    fn git_clone_without_checkout(
        &self,
        source: &str,
        destination: &Path,
    ) -> Result<(), CoreError> {
        let output = Command::new(&self.git_program)
            .arg("clone")
            .arg("--quiet")
            .arg("--no-checkout")
            .arg("--no-hardlinks")
            .arg("--")
            .arg(source)
            .arg(destination)
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_LFS_SKIP_SMUDGE", "1")
            .output()
            .map_err(|error| CoreError::Storage(format!("starting git clone: {error}")))?;
        require_git_success("git clone", output)
    }

    fn verify_commit_exists(&self, repository: &Path, revision: &str) -> Result<(), CoreError> {
        let commit = format!("{revision}^{{commit}}");
        let observed = self.git_stdout(repository, &["rev-parse", "--verify", &commit])?;
        if observed != revision {
            return Err(CoreError::Validation(format!(
                "requested Git object {revision} is not the exact commit selected by rev-parse ({observed})"
            )));
        }
        Ok(())
    }

    fn reject_submodules(&self, repository: &Path, revision: &str) -> Result<(), CoreError> {
        let output = self.git_stdout(repository, &["ls-tree", "-r", revision])?;
        if output.lines().any(|line| line.starts_with("160000 ")) {
            return Err(CoreError::Validation(
                "workspace materializer v1 rejects Git submodules; declare them as explicit workspace repositories"
                    .to_owned(),
            ));
        }
        Ok(())
    }

    fn checkout_detached(&self, repository: &Path, revision: &str) -> Result<(), CoreError> {
        let output = self.git_output(repository, &["checkout", "--detach", "--quiet", revision])?;
        require_git_success("git checkout --detach", output)
    }

    fn require_clean_worktree(&self, repository: &Path) -> Result<(), CoreError> {
        let status = self.git_stdout(
            repository,
            &["status", "--porcelain=v1", "--untracked-files=all"],
        )?;
        if status.is_empty() {
            Ok(())
        } else {
            Err(CoreError::Validation(format!(
                "materialized workspace is not clean: {status:?}"
            )))
        }
    }

    fn git_stdout(&self, repository: &Path, args: &[&str]) -> Result<String, CoreError> {
        let output = self.git_output(repository, args)?;
        if !output.status.success() {
            return Err(git_failure(args.join(" "), &output));
        }
        let stdout = String::from_utf8(output.stdout)
            .map_err(|_| CoreError::Storage("git output was not valid UTF-8".to_owned()))?;
        Ok(stdout.trim().to_owned())
    }

    fn git_output(&self, repository: &Path, args: &[&str]) -> Result<Output, CoreError> {
        Command::new(&self.git_program)
            .arg("-C")
            .arg(repository)
            .args(args)
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_LFS_SKIP_SMUDGE", "1")
            .output()
            .map_err(|error| CoreError::Storage(format!("starting git command: {error}")))
    }
}

fn require_git_success(operation: &str, output: Output) -> Result<(), CoreError> {
    if output.status.success() {
        Ok(())
    } else {
        Err(git_failure(operation.to_owned(), &output))
    }
}

fn git_failure(operation: String, output: &Output) -> CoreError {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let bounded: String = stderr.chars().take(4096).collect();
    CoreError::Storage(format!(
        "{operation} failed with status {}: {bounded}",
        output.status
    ))
}

/// Computes a digest over the actual checked-out bytes while excluding Git's
/// private metadata directory. Symlinks are hashed as link targets and are
/// never followed. Special files and non-UTF8 paths fail closed.
///
/// # Errors
/// Filesystem errors, special files, non-UTF8 names, or a file changing size
/// while it is being hashed.
pub fn digest_materialized_checkout(root: &Path) -> Result<ContentDigest, CoreError> {
    let mut state = DigestState::new(DOMAIN_TASK_WORKSPACE_REPOSITORY);
    digest_directory(root, root, &mut state)?;
    Ok(state.finalize())
}

fn digest_directory(
    root: &Path,
    directory: &Path,
    state: &mut DigestState,
) -> Result<(), CoreError> {
    let mut entries = std::fs::read_dir(directory)
        .map_err(|error| CoreError::Storage(format!("reading workspace directory: {error}")))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            CoreError::Storage(format!("reading workspace directory entry: {error}"))
        })?;

    entries.sort_by(|left, right| {
        left.file_name()
            .to_string_lossy()
            .cmp(&right.file_name().to_string_lossy())
    });

    for entry in entries {
        if directory == root && entry.file_name() == ".git" {
            continue;
        }
        let path = entry.path();
        let metadata = std::fs::symlink_metadata(&path)
            .map_err(|error| CoreError::Storage(format!("stating workspace entry: {error}")))?;
        let relative = path
            .strip_prefix(root)
            .map_err(|_| CoreError::Storage("workspace path escaped digest root".to_owned()))?;
        let relative = portable_relative_path(relative)?;

        if metadata.file_type().is_symlink() {
            state.update(b"L");
            update_frame(state, relative.as_bytes());
            let target = std::fs::read_link(&path).map_err(|error| {
                CoreError::Storage(format!("reading workspace symlink: {error}"))
            })?;
            let target = target.to_str().ok_or_else(|| {
                CoreError::Validation("workspace symlink target is not valid UTF-8".to_owned())
            })?;
            update_frame(state, target.as_bytes());
        } else if metadata.is_dir() {
            digest_directory(root, &path, state)?;
        } else if metadata.is_file() {
            state.update(b"F");
            update_frame(state, relative.as_bytes());
            state.update(&metadata.len().to_le_bytes());

            let mut file = File::open(&path)
                .map_err(|error| CoreError::Storage(format!("opening workspace file: {error}")))?;
            let mut observed_len = 0u64;
            let mut buffer = [0u8; 64 * 1024];
            loop {
                match file.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(read) => {
                        observed_len = observed_len.checked_add(read as u64).ok_or_else(|| {
                            CoreError::Storage("workspace file byte count overflow".to_owned())
                        })?;
                        state.update(&buffer[..read]);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(error) => {
                        return Err(CoreError::Storage(format!(
                            "reading workspace file: {error}"
                        )));
                    }
                }
            }
            if observed_len != metadata.len() {
                return Err(CoreError::Validation(format!(
                    "workspace file {relative:?} changed size while hashing"
                )));
            }
        } else {
            return Err(CoreError::Validation(format!(
                "workspace entry {relative:?} is not a regular file, directory, or symlink"
            )));
        }
    }
    Ok(())
}

fn portable_relative_path(path: &Path) -> Result<String, CoreError> {
    let mut parts = Vec::new();
    for component in path.components() {
        let value = component
            .as_os_str()
            .to_str()
            .ok_or_else(|| CoreError::Validation("workspace path is not valid UTF-8".to_owned()))?;
        parts.push(value);
    }
    Ok(parts.join("/"))
}

fn update_frame(state: &mut DigestState, bytes: &[u8]) {
    state.update(&(bytes.len() as u64).to_le_bytes());
    state.update(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_git(repository: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .arg("-C")
            .arg(repository)
            .args(args)
            .output()
            .expect("git");
        assert!(
            output.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout)
            .expect("utf8")
            .trim()
            .to_owned()
    }

    fn source_repository() -> (PathBuf, String) {
        let root = std::env::temp_dir().join(format!("hub-workspace-src-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).expect("mkdir");
        let status = Command::new("git")
            .arg("init")
            .arg("--quiet")
            .arg(&root)
            .status()
            .expect("git init");
        assert!(status.success());
        run_git(
            &root,
            &["config", "user.email", "workspace@example.invalid"],
        );
        run_git(&root, &["config", "user.name", "Workspace Test"]);
        run_git(&root, &["config", "commit.gpgsign", "false"]);
        std::fs::write(root.join("alpha.txt"), b"alpha\n").expect("write");
        run_git(&root, &["add", "--", "alpha.txt"]);
        run_git(&root, &["commit", "--quiet", "-m", "alpha"]);
        let revision = run_git(&root, &["rev-parse", "HEAD"]);

        std::fs::write(root.join("alpha.txt"), b"later\n").expect("write later");
        run_git(&root, &["add", "--", "alpha.txt"]);
        run_git(&root, &["commit", "--quiet", "-m", "later"]);
        (root, revision)
    }

    fn spec(revision: String) -> WorkspaceSpec {
        WorkspaceSpec {
            repositories: vec![hub_core::WorkspaceRepository {
                repository: "Memorithm/example".to_owned(),
                revision,
                read_only: true,
            }],
            mcp_servers: vec!["github.read".to_owned()],
            skills: vec!["rust.ci".to_owned()],
        }
    }

    #[test]
    fn exact_commit_materializes_with_stable_evidence() {
        let (source, revision) = source_repository();
        let spec = spec(revision.clone());
        let sources =
            BTreeMap::from([("Memorithm/example".to_owned(), source.display().to_string())]);
        let first_root =
            std::env::temp_dir().join(format!("hub-workspace-out-{}", uuid::Uuid::new_v4()));
        let second_root =
            std::env::temp_dir().join(format!("hub-workspace-out-{}", uuid::Uuid::new_v4()));
        let materializer = GitWorkspaceMaterializer::default();

        let first = materializer
            .materialize(&spec, &first_root, &sources)
            .expect("first materialization");
        let second = materializer
            .materialize(&spec, &second_root, &sources)
            .expect("second materialization");

        assert_eq!(first.evidence, second.evidence);
        assert_eq!(first.evidence.repositories[0].observed_revision, revision);
        assert_eq!(
            std::fs::read_to_string(first_root.join("repos/0000/alpha.txt")).expect("read"),
            "alpha\n"
        );
        first
            .evidence
            .validate_against(&spec)
            .expect("validated evidence");

        let _ = std::fs::remove_dir_all(source);
        let _ = std::fs::remove_dir_all(first_root);
        let _ = std::fs::remove_dir_all(second_root);
    }

    #[test]
    fn unknown_exact_commit_fails_and_cleans_target() {
        let (source, _) = source_repository();
        let spec = spec("0000000000000000000000000000000000000000".to_owned());
        let sources = BTreeMap::from([(
            "Memorithm/example".to_owned(),
            source.display().to_string(),
        )]);
        let root = std::env::temp_dir().join(format!("hub-workspace-out-{}", uuid::Uuid::new_v4()));
        let error = GitWorkspaceMaterializer::default()
            .materialize(&spec, &root, &sources)
            .expect_err("unknown revision must fail");
        assert!(matches!(
            error,
            CoreError::Storage(_) | CoreError::Validation(_)
        ));
        assert!(!root.exists(), "failed materialization must clean target");
        let _ = std::fs::remove_dir_all(source);
    }

    #[test]
    fn target_reuse_is_rejected() {
        let root =
            std::env::temp_dir().join(format!("hub-workspace-existing-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).expect("mkdir");
        let error = GitWorkspaceMaterializer::default()
            .materialize(&WorkspaceSpec::default(), &root, &BTreeMap::new())
            .expect_err("reuse must fail");
        assert!(
            matches!(error, CoreError::Validation(message) if message.contains("already exists"))
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn content_digest_changes_with_materialized_bytes() {
        let root =
            std::env::temp_dir().join(format!("hub-workspace-digest-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).expect("mkdir");
        std::fs::write(root.join("value.txt"), b"one").expect("write");
        let one = digest_materialized_checkout(&root).expect("digest one");
        std::fs::write(root.join("value.txt"), b"two").expect("write");
        let two = digest_materialized_checkout(&root).expect("digest two");
        assert_ne!(one, two);
        let _ = std::fs::remove_dir_all(root);
    }
}
