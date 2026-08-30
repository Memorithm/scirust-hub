from pathlib import Path
import runpy


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one replacement, found {count}: {old[:100]!r}")
    p.write_text(text.replace(old, new, 1))


Path("crates/hub-executor/tests").mkdir(parents=True, exist_ok=True)
runpy.run_path(".github/agent_pr4.py", run_name="__main__")

# Empty directories are part of the execution workdir contract: Hub creates
# declared output parents before invoking an executor. A remote backend must
# preserve that layout rather than relying on a shared filesystem.
replace_once(
    "crates/hub-protocol/src/distributed.rs",
    "    pub max_capture_bytes_per_stream: usize,\n    pub files: Vec<RemoteFile>,\n",
    "    pub max_capture_bytes_per_stream: usize,\n    pub directories: Vec<String>,\n    pub files: Vec<RemoteFile>,\n",
)

replace_once(
    "crates/hub-executor/src/remote.rs",
    "        let files = collect_files(&request.working_dir, self.max_payload_bytes)?;\n",
    "        let directories = collect_directories(&request.working_dir)?;\n        let files = collect_files(&request.working_dir, self.max_payload_bytes)?;\n",
)
replace_once(
    "crates/hub-executor/src/remote.rs",
    "            max_capture_bytes_per_stream: request.max_capture_bytes_per_stream,\n            files,\n",
    "            max_capture_bytes_per_stream: request.max_capture_bytes_per_stream,\n            directories,\n            files,\n",
)
replace_once(
    "crates/hub-executor/src/remote.rs",
    "fn collect_files(root: &Path, limit: usize) -> Result<Vec<RemoteFile>, String> {\n",
    r'''fn collect_directories(root: &Path) -> Result<Vec<String>, String> {
    let mut directories = Vec::new();
    collect_directory_paths(root, root, &mut directories)?;
    directories.sort();
    Ok(directories)
}

fn collect_directory_paths(
    root: &Path,
    current: &Path,
    directories: &mut Vec<String>,
) -> Result<(), String> {
    for entry in fs::read_dir(current).map_err(|e| format!("reading workdir {current:?}: {e}"))? {
        let entry = entry.map_err(|e| format!("reading workdir entry: {e}"))?;
        let file_type = entry
            .file_type()
            .map_err(|e| format!("reading workdir file type: {e}"))?;
        if file_type.is_symlink() {
            return Err(format!(
                "remote transport refuses symlink {:?}",
                entry.path().strip_prefix(root).unwrap_or(entry.path().as_path())
            ));
        }
        if file_type.is_dir() {
            let path = entry.path();
            let relative = path
                .strip_prefix(root)
                .map_err(|_| "transport directory escaped workdir".to_owned())?;
            directories.push(relative.to_string_lossy().into_owned());
            collect_directory_paths(root, &path, directories)?;
        }
    }
    Ok(())
}

fn collect_files(root: &Path, limit: usize) -> Result<Vec<RemoteFile>, String> {
''',
)

replace_once(
    "crates/hub-executor/src/worker.rs",
    "        if let Err(error) = prepare_workdir(&root, &execution.files, self.inner.max_payload_bytes) {\n",
    "        if let Err(error) = prepare_workdir(\n            &root,\n            &execution.directories,\n            &execution.files,\n            self.inner.max_payload_bytes,\n        ) {\n",
)
replace_once(
    "crates/hub-executor/src/worker.rs",
    "fn prepare_workdir(root: &Path, files: &[RemoteFile], limit: usize) -> Result<(), String> {\n",
    "fn prepare_workdir(\n    root: &Path,\n    directories: &[String],\n    files: &[RemoteFile],\n    limit: usize,\n) -> Result<(), String> {\n",
)
replace_once(
    "crates/hub-executor/src/worker.rs",
    "    fs::create_dir_all(root).map_err(|e| format!(\"creating worker workdir: {e}\"))?;\n    let mut total = 0usize;\n",
    r'''    fs::create_dir_all(root).map_err(|e| format!("creating worker workdir: {e}"))?;
    for directory in directories {
        let relative = checked_relative_path(directory)?;
        fs::create_dir_all(root.join(relative))
            .map_err(|e| format!("creating worker workdir directory {directory:?}: {e}"))?;
    }
    let mut total = 0usize;
''',
)
replace_once(
    "crates/hub-executor/src/worker.rs",
    "                max_capture_bytes_per_stream: 1024,\n                files: Vec::new(),\n",
    "                max_capture_bytes_per_stream: 1024,\n                directories: Vec::new(),\n                files: Vec::new(),\n",
)

print("remote workdir directory transport patch applied")
