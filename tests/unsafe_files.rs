//! `unsafe` stays in the files that meet the operating system, and every block says why it holds.

use std::fs;
use std::path::Path;

const ALLOWED: &[&str] = &["sys/unix_os.rs", "sys/windows.rs"];

fn files(directory: &Path, found: &mut Vec<std::path::PathBuf>) {
    for entry in fs::read_dir(directory).expect("src/ is readable") {
        let path = entry.expect("a directory entry").path();
        if path.is_dir() {
            files(&path, found);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            found.push(path);
        }
    }
}

fn source() -> Vec<(String, String)> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut paths = Vec::new();
    files(&root, &mut paths);
    paths
        .into_iter()
        .map(|path| {
            let name = path
                .strip_prefix(&root)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            (name, fs::read_to_string(&path).unwrap())
        })
        .collect()
}

/// Lines of code holding the keyword, comments aside.
fn unsafe_lines(text: &str) -> Vec<(usize, &str)> {
    text.lines()
        .enumerate()
        .filter(|(_, line)| {
            let code = line.split("//").next().unwrap_or_default();
            code.split(|c: char| !c.is_alphanumeric() && c != '_')
                .any(|word| word == "unsafe")
        })
        .collect()
}

#[test]
fn only_the_listed_files_hold_unsafe_code_and_each_of_them_needs_it() {
    let mut holding = Vec::new();
    for (name, text) in source() {
        if !unsafe_lines(&text).is_empty() {
            holding.push(name);
        }
    }
    holding.sort();
    assert_eq!(holding, ALLOWED);
}

#[test]
fn every_module_without_unsafe_code_forbids_it() {
    for (name, text) in source() {
        let allowed = ALLOWED.contains(&name.as_str()) || name == "lib.rs" || name == "sys/mod.rs";
        assert!(
            allowed || text.contains("#![forbid(unsafe_code)]"),
            "{name} does not forbid unsafe code"
        );
    }
}

#[test]
fn every_unsafe_block_says_why_it_holds() {
    for (name, text) in source() {
        let lines: Vec<&str> = text.lines().collect();
        for (index, line) in unsafe_lines(&text) {
            let is_declaration = line.contains("unsafe fn") || line.contains("forbid");
            if is_declaration {
                continue;
            }
            // The comment is above the statement holding the block; a statement may begin above it.
            let context = &lines[index.saturating_sub(6)..index];
            assert!(
                context.iter().any(|above| above.contains("// SAFETY:")),
                "{name}:{}: an unsafe block with no SAFETY comment",
                index + 1
            );
        }
    }
}
