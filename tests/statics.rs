//! A library keeps no process-wide state: no `static`, `OnceLock`, `LazyLock` or `thread_local!` in
//! `src/`.

use std::fs;
use std::path::Path;

const MARKERS: &[&str] = &["static ", "OnceLock", "LazyLock", "thread_local!"];

fn scan(directory: &Path, found: &mut Vec<String>) {
    for entry in fs::read_dir(directory).expect("src/ is readable") {
        let path = entry.expect("a directory entry").path();
        if path.is_dir() {
            scan(&path, found);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            let text = fs::read_to_string(&path).expect("a source file is UTF-8");
            for line in text.lines() {
                let code = line.split("//").next().unwrap_or_default();
                if MARKERS.iter().any(|marker| code.contains(marker)) && !code.contains("'static") {
                    found.push(format!("{}: {}", path.display(), line.trim()));
                }
            }
        }
    }
}

#[test]
fn the_source_holds_no_process_wide_state() {
    let mut found = Vec::new();
    scan(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut found,
    );
    assert!(found.is_empty(), "process-wide state: {found:?}");
}
