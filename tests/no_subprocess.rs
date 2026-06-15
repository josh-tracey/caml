use std::fs;
use std::path::Path;

fn check_dir(dir: &Path) {
    if !dir.is_dir() {
        return;
    }

    for entry in fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();

        if path.is_dir() {
            check_dir(&path);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            let content = fs::read_to_string(&path).unwrap_or_default();

            for (line_num, line) in content.lines().enumerate() {
                if line.contains("std::process::Command")
                    || line.contains("Command::new")
                    || line.contains("kill -9")
                {
                    if line.contains("allow-subprocess") {
                        continue;
                    }
                    panic!(
                        "Found prohibited subprocess call in {}:{}: '{}'. \
                        caml is a pure-native runtime and MUST NOT shell out. \
                        If this is a test utility, append '// allow-subprocess' to the line.",
                        path.display(),
                        line_num + 1,
                        line.trim()
                    );
                }
            }
        }
    }
}

#[test]
fn test_no_subprocesses_in_crates() {
    // Tests are run from the workspace root (where the top-level Cargo.toml is)
    // when running `cargo test` on the top-level package or `--workspace`.
    // CARGO_MANIFEST_DIR points to the directory of the crate being tested (caml).
    let crates_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("crates");

    if crates_dir.exists() {
        check_dir(&crates_dir);
    } else {
        // Fallback for cases where tests might be run from another working directory
        let fallback = Path::new("crates");
        if fallback.exists() {
            check_dir(fallback);
        } else {
            panic!("Could not find crates/ directory to run static analysis");
        }
    }
}
