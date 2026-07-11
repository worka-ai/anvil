use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .to_path_buf()
}

fn workspace_relative_path(path: &Path) -> String {
    path.strip_prefix(workspace_root())
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn collect_workspace_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let ignored_dirs = BTreeSet::from([".git", "target", "anvil-data"]);
    for entry in
        fs::read_dir(dir).unwrap_or_else(|error| panic!("read dir {}: {error}", dir.display()))
    {
        let entry = entry.expect("read directory entry");
        let path = entry.path();
        if path.is_dir() {
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if !ignored_dirs.contains(name) {
                collect_workspace_rs_files(&path, out);
            }
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn rfc_0007_no_rust_source_file_exceeds_2000_lines() {
    let mut files = Vec::new();
    collect_workspace_rs_files(&workspace_root(), &mut files);

    let violations = files
        .into_iter()
        .filter_map(|path| {
            let source = fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
            let line_count = source.lines().count();
            (line_count > 2_000).then(|| (workspace_relative_path(&path), line_count))
        })
        .collect::<Vec<_>>();

    assert!(
        violations.is_empty(),
        "Rust source files must stay at or below 2000 lines:\n{}",
        violations
            .iter()
            .map(|(path, lines)| format!("{path}: {lines} lines"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}
