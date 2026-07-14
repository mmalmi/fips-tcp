use std::fs;
use std::path::Path;

#[test]
fn rust_source_files_stay_below_one_thousand_lines() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    check_directory(workspace);
}

fn check_directory(directory: &Path) {
    for entry in fs::read_dir(directory).expect("read Rust source directory") {
        let path = entry.expect("read source entry").path();
        if path.is_dir() {
            if path.file_name().and_then(|value| value.to_str()) != Some("target") {
                check_directory(&path);
            }
            continue;
        }
        if path.extension().and_then(|value| value.to_str()) != Some("rs") {
            continue;
        }
        let lines = fs::read_to_string(&path)
            .expect("read Rust source")
            .lines()
            .count();
        assert!(
            lines <= 1_000,
            "{} has {lines} lines; Rust source files are limited to 1000",
            path.display()
        );
    }
}
